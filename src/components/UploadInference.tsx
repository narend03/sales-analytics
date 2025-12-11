"use client";

import { useMemo, useState } from "react";
import { AsyncDuckDB, AsyncDuckDBConnection, ConsoleLogger } from "@duckdb/duckdb-wasm";
import { CanonicalField, SchemaInferenceResult } from "@/lib/contracts";
import {
  buildColumnMap,
  getAnomalies,
  getChannels,
  getGeo,
  getProducts,
  getSummary,
  getTimeseries,
  materializeCleanedView,
} from "@/lib/analytics";

const bundleOverride = {
  mainModule: "/duckdb/duckdb-eh.wasm",
  mainWorker: "/duckdb/duckdb-browser-eh.worker.js",
};

const MAX_COLUMNS = 200;
const MAX_STRING_LEN = 120;

type Mapping = {
  originalName: string;
  canonicalName?: CanonicalField;
  confidence: number;
  reason?: string;
};

type InferenceResult = {
  schema: SchemaInferenceResult;
  previewRows: unknown[];
};

type AggregateResult = {
  summary: Awaited<ReturnType<typeof getSummary>> | null;
  timeseries: Awaited<ReturnType<typeof getTimeseries>> | null;
  products: Awaited<ReturnType<typeof getProducts>> | null;
  geo: Awaited<ReturnType<typeof getGeo>> | null;
  channels: Awaited<ReturnType<typeof getChannels>> | null;
  anomalies: Awaited<ReturnType<typeof getAnomalies>> | null;
};

type ChatMessage = {
  role: "user" | "assistant";
  content: string;
  table?: any;
};

export function UploadInference() {
  const [db, setDb] = useState<AsyncDuckDB | null>(null);
  const [conn, setConn] = useState<AsyncDuckDBConnection | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<InferenceResult | null>(null);
  const [agg, setAgg] = useState<AggregateResult | null>(null);
  const [insights, setInsights] = useState<string[] | null>(null);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insightsError, setInsightsError] = useState<string | null>(null);
  const [chatInput, setChatInput] = useState("");
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatLoading, setChatLoading] = useState(false);
  const [chatError, setChatError] = useState<string | null>(null);

  const ensureDb = useMemo(
    () => async () => {
      if (db && conn) return { db, conn };
      if (!bundleOverride.mainWorker || !bundleOverride.mainModule) {
        throw new Error("DuckDB WASM bundle not configured");
      }
      const worker = new Worker(bundleOverride.mainWorker, { type: "module" });
      const logger = new ConsoleLogger();
      const nextDb = new AsyncDuckDB(logger, worker);
      await nextDb.instantiate(bundleOverride.mainModule, null);
      const nextConn = await nextDb.connect();
      setDb(nextDb);
      setConn(nextConn);
      return { db: nextDb, conn: nextConn };
    },
    [db, conn],
  );

  const handleFile = async (file: File | null) => {
    if (!file) return;
    setError(null);
    setResult(null);
    setAgg(null);
    setInsights(null);
    setInsightsError(null);
    setInsightsLoading(false);
    setChatMessages([]);
    setChatError(null);
    setChatInput("");
    setLoading(true);
    try {
      const { db: duck, conn: connection } = await ensureDb();
      const buf = new Uint8Array(await file.arrayBuffer());
      const filename = `upload_${Date.now()}.csv`;
      await duck.registerFileBuffer(filename, buf);

      const SAMPLE_SIZE = 20000;
      const createSql = `
        CREATE OR REPLACE TABLE data AS
        SELECT * FROM read_csv_auto('${filename}', sample_size=${SAMPLE_SIZE}, ignore_errors=true);
      `;
      await connection.query(createSql);

      const schemaRes = await connection.query("PRAGMA table_info('data');");
      const columns = schemaRes.toArray().map((row) => (row as { name: string }).name);
      if (columns.length > MAX_COLUMNS) {
        setError(`Too many columns: ${columns.length} (max ${MAX_COLUMNS})`);
        return;
      }

      const mappings = columns.map(inferCanonical);
      const tsMapping = mappings.find((m) => m.canonicalName === "timestamp");
      const tsColForPreview = tsMapping?.originalName;

      const countRes = await connection.query("SELECT COUNT(*) as count FROM data;");
      const rowCount = (countRes.toArray()[0] as { count: number }).count;

      const previewRes = await connection.query("SELECT * FROM data LIMIT 20;");
      const rawPreviewRows = previewRes.toArray();
      const bigintReplacer = (_key: string, value: unknown) =>
        typeof value === "bigint" ? value.toString() : value;
      const previewRows = JSON.parse(JSON.stringify(rawPreviewRows, bigintReplacer)).map((row: Record<string, unknown>) => {
        const next: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(row)) {
          if (tsColForPreview && k === tsColForPreview && typeof v === "number") {
            next[k] = new Date(v as number).toISOString();
          } else if (typeof v === "string" && v.length > MAX_STRING_LEN) {
            next[k] = v.slice(0, MAX_STRING_LEN) + "…";
          } else {
            next[k] = v;
          }
        }
        return next;
      });

      let minDate: string | undefined;
      let maxDate: string | undefined;
      if (tsMapping) {
        const tsCol = tsMapping.originalName;
        const dateRes = await connection.query(
          `SELECT MIN(${tsCol}) as min_date, MAX(${tsCol}) as max_date FROM data;`,
        );
        const [row] = dateRes.toArray() as { min_date?: string; max_date?: string }[];
        minDate = row?.min_date ? new Date(row.min_date).toISOString() : undefined;
        maxDate = row?.max_date ? new Date(row.max_date).toISOString() : undefined;
      }

      const distinctCounts: Record<string, number> = {};
      for (const m of mappings.filter((m) => m.canonicalName)) {
        const col = m.originalName;
        const res = await connection.query(`SELECT COUNT(DISTINCT ${col}) as c FROM data;`);
        distinctCounts[col] = (res.toArray()[0] as { c: number }).c;
      }

      const response: SchemaInferenceResult = {
        columns: mappings.map((m) => ({
          originalName: m.originalName,
          canonicalName: m.canonicalName,
          confidence: m.confidence,
          reason: m.reason,
        })),
        rowCount,
        minDate,
        maxDate,
        distinctCounts,
      };

      console.info("Ingestion summary", { rows: rowCount, columns: columns.length, mappings });
      setResult({ schema: response, previewRows });
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Upload failed";
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  const runAggregates = async () => {
    if (!conn || !result) return;
    try {
      setError(null);
      const map = buildColumnMap(result.schema);
      await materializeCleanedView(conn, map);
      const summary = await getSummary(conn);
      const timeseries = await getTimeseries(conn);
      const products = await getProducts(conn, 10, 0);
      const geo = await getGeo(conn);
      const channels = await getChannels(conn);
      const anomalies = await getAnomalies(conn);

      const toIso = (v: any) => (typeof v === "number" ? new Date(v).toISOString() : v);
      const display: AggregateResult = {
        summary: summary
          ? {
              ...summary,
              minDate: summary.minDate ? toIso(summary.minDate as any) : summary.minDate,
              maxDate: summary.maxDate ? toIso(summary.maxDate as any) : summary.maxDate,
            }
          : summary,
        timeseries: timeseries
          ? {
              daily: timeseries.daily.map((d: any) => ({ ...d, date: toIso(d.date) })),
              monthly: timeseries.monthly.map((m: any) => ({ ...m, month: toIso(m.month) })),
            }
          : timeseries,
        products,
        geo,
        channels,
        anomalies: anomalies?.map((a: any) => ({ ...a, date: toIso(a.date) })) ?? anomalies,
      };

      setAgg(display);
      await fetchInsights(display);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Aggregations failed";
      setError(msg);
    }
  };

  const fetchInsights = async (context: AggregateResult) => {
    try {
      setInsightsLoading(true);
      setInsightsError(null);
      setInsights(null);
      const payload = buildInsightPayload(context);
      const res = await fetch("/api/insights", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.error || "insights error");
      }
      setInsights(cleanInsights(data.insights));
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "insights failed";
      setInsightsError(msg);
    } finally {
      setInsightsLoading(false);
    }
  };

  const runChat = async () => {
    if (!conn || !agg || !agg.summary || !chatInput.trim()) return;
    setChatError(null);
    setChatLoading(true);
    try {
      await materializeCleanedView(conn, buildColumnMap(result!.schema));
      const templateResult = await routeChatQuestion(conn, agg, chatInput.trim());
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question: chatInput.trim(),
          template: templateResult.template,
          summary: agg.summary,
          table: templateResult.table,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "chat error");
      setChatMessages((prev) => [
        ...prev,
        { role: "user", content: chatInput.trim() },
        { role: "assistant", content: data.answer, table: templateResult.table },
      ]);
      setChatInput("");
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "chat failed";
      setChatError(msg);
    } finally {
      setChatLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      <div className="border-2 border-dashed border-gray-400 rounded p-6 text-center">
        <input
          type="file"
          accept=".csv,text/csv"
          onChange={(e) => handleFile(e.target.files?.[0] ?? null)}
          className="mx-auto text-sm"
        />
        <p className="text-sm text-gray-500 mt-2">Drop a CSV to infer schema locally (WASM).</p>
      </div>

      {loading && <p className="text-sm text-blue-600">Processing CSV...</p>}
      {error && <p className="text-sm text-red-600">Error: {error}</p>}

      {result && (
        <div className="space-y-3">
          <div className="border rounded p-3 space-y-1">
            <h3 className="font-semibold">Summary</h3>
            <p className="text-sm">Rows: {result.schema.rowCount}</p>
            <p className="text-sm">
              Date range: {result.schema.minDate ?? "n/a"} → {result.schema.maxDate ?? "n/a"}
            </p>
            {Object.keys(result.schema.distinctCounts ?? {}).length > 0 && (
              <div className="text-sm text-gray-700">
                Distinct counts: {" "}
                {Object.entries(result.schema.distinctCounts ?? {})
                  .map(([col, cnt]) => `${col}: ${cnt}`)
                  .join(", " )}
              </div>
            )}
          </div>

          <div className="border rounded p-3">
            <h3 className="font-semibold mb-2">Column Mapping</h3>
            <div className="space-y-1 text-sm">
              {result.schema.columns.map((col) => (
                <div key={col.originalName} className="flex justify-between">
                  <span>{col.originalName}</span>
                  <span className="text-gray-600">
                    {col.canonicalName ?? "unmapped"} ({Math.round(col.confidence * 100)}%)
                  </span>
                </div>
              ))}
            </div>
          </div>

          <div className="border rounded p-3">
            <h3 className="font-semibold mb-2">Preview (first 20 rows)</h3>
            <pre className="overflow-auto text-xs bg-gray-50 p-2 rounded">
              {JSON.stringify(result.previewRows, null, 2)}
            </pre>
          </div>

          <div className="border rounded p-3 space-y-3">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <h3 className="font-semibold">Dashboard</h3>
                <p className="text-xs text-gray-500">Click Run to compute aggregates</p>
              </div>
              <button
                onClick={runAggregates}
                className="rounded bg-indigo-600 px-3 py-1 text-sm font-medium text-white hover:bg-indigo-700 disabled:bg-gray-400"
                disabled={loading}
              >
                Run
              </button>
            </div>

            {!agg && <p className="text-sm text-gray-600">No aggregates yet.</p>}

            {agg && (
              <div className="grid gap-3 md:grid-cols-2">
                <div className="rounded border p-3 space-y-1">
                  <h4 className="text-sm font-semibold">Summary</h4>
                  <p className="text-sm text-gray-800">Revenue: {formatCurrency(agg.summary?.totalRevenue)}</p>
                  <p className="text-sm text-gray-800">Units: {agg.summary?.totalQuantity ?? "n/a"}</p>
                  <p className="text-xs text-gray-600">
                    Date range: {agg.summary?.minDate ?? "n/a"} → {agg.summary?.maxDate ?? "n/a"}
                  </p>
                  <p className="text-xs text-gray-600">
                    MoM: {formatPct(agg.summary?.momGrowthPct)} • YoY: {formatPct(agg.summary?.yoyGrowthPct)}
                  </p>
                </div>

                <div className="rounded border p-3 space-y-2">
                  <h4 className="text-sm font-semibold">Timeseries (daily)</h4>
                  <div className="space-y-1 text-xs text-gray-800 max-h-32 overflow-auto">
                    {(agg.timeseries?.daily ?? []).slice(0, 7).map((d, i) => (
                      <div key={i} className="flex justify-between">
                        <span>{d.date as string}</span>
                        <span>{formatCurrency(d.revenue)}</span>
                      </div>
                    ))}
                    {(agg.timeseries?.daily?.length ?? 0) === 0 && <p>No dates.</p>}
                  </div>
                </div>

                <div className="rounded border p-3 space-y-2">
                  <h4 className="text-sm font-semibold">Top Products</h4>
                  <div className="space-y-1 text-xs text-gray-800">
                    {agg.products?.map((p, i) => (
                      <div key={i}>
                        <div className="flex justify-between">
                          <span>{p.product}</span>
                          <span>{formatCurrency(p.revenue)}</span>
                        </div>
                        <div className="h-2 rounded bg-gray-100">
                          <div
                            className="h-2 rounded bg-indigo-500"
                            style={{ width: barWidth(p.revenue, agg.products) }}
                          />
                        </div>
                      </div>
                    ))}
                    {(!agg.products || agg.products.length === 0) && <p>No products.</p>}
                  </div>
                </div>

                <div className="rounded border p-3 space-y-2">
                  <h4 className="text-sm font-semibold">Channels</h4>
                  <div className="space-y-1 text-xs text-gray-800">
                    {agg.channels?.map((c, i) => (
                      <div key={i} className="flex justify-between">
                        <span>{c.channel ?? "(unknown)"}</span>
                        <span>{formatCurrency(c.revenue)}</span>
                      </div>
                    ))}
                    {(!agg.channels || agg.channels.length === 0) && <p>No channels.</p>}
                  </div>
                </div>

                <div className="rounded border p-3 space-y-2">
                  <h4 className="text-sm font-semibold">Geo</h4>
                  <div className="space-y-1 text-xs text-gray-800 max-h-28 overflow-auto">
                    {agg.geo?.map((g, i) => (
                      <div key={i} className="flex justify-between">
                        <span>{[g.city, g.state].filter(Boolean).join(', ') || '(unknown)'}</span>
                        <span>{formatCurrency(g.revenue)}</span>
                      </div>
                    ))}
                    {(!agg.geo || agg.geo.length === 0) && <p>No geo.</p>}
                  </div>
                </div>

                <div className="rounded border p-3 space-y-1">
                  <h4 className="text-sm font-semibold">Anomalies</h4>
                  <div className="space-y-1 text-xs text-gray-800">
                    {agg.anomalies?.map((a, i) => (
                      <div key={i} className="flex justify-between">
                        <span>{a.date as string}</span>
                        <span>{formatCurrency(a.revenue)} (z={a.zscore?.toFixed?.(2) ?? 'n/a'})</span>
                      </div>
                    ))}
                    {(!agg.anomalies || agg.anomalies.length === 0) && <p>No anomalies.</p>}
                  </div>
                </div>
              </div>
            )}
          </div>

          <div className="border rounded p-3 space-y-2">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold">Insights</h3>
              {insightsLoading && <span className="text-xs text-gray-500">Generating…</span>}
            </div>
            {insightsError && <p className="text-sm text-red-600">{insightsError}</p>}
            {insights && insights.length > 0 && (
              <ul className="list-disc list-inside text-sm space-y-1">
                {insights.map((item, idx) => (
                  <li key={idx}>{item}</li>
                ))}
              </ul>
            )}
            {insights && insights.length === 0 && !insightsLoading && (
              <p className="text-sm text-gray-600">No insights returned.</p>
            )}
          </div>

          <div className="border rounded p-3 space-y-2">
            <h3 className="font-semibold">Chat</h3>
            <div className="space-y-2">
              <input
                value={chatInput}
                onChange={(e) => setChatInput(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); runChat(); } }}
                placeholder="Ask a question (e.g., top products, channels, growth, anomalies)"
                className="w-full rounded border px-3 py-2 text-sm"
              />
              <button
                onClick={runChat}
                className="rounded bg-indigo-600 px-3 py-1 text-sm font-medium text-white hover:bg-indigo-700 disabled:bg-gray-400"
                disabled={chatLoading || !chatInput.trim()}
              >
                Ask
              </button>
              {chatError && <p className="text-sm text-red-600">{chatError}</p>}
            </div>
            <div className="space-y-2">
              {chatMessages.map((m, idx) => (
                <div key={idx} className="rounded border border-gray-200 bg-gray-50 p-2 text-sm">
                  <div className="font-semibold text-gray-700">{m.role === "user" ? "You" : "AI"}</div>
                  <div className="whitespace-pre-wrap text-gray-800">{m.content}</div>
                </div>
              ))}
            </div>
          </div>


        </div>
      )}
    </div>
  );
}


function formatCurrency(v: number | null | undefined) {
  if (v === null || v === undefined) return "n/a";
  return `$${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

function formatPct(v: number | null | undefined) {
  if (v === null || v === undefined) return "n/a";
  return `${(v * 100).toFixed(1)}%`;
}

function barWidth(value: number | null | undefined, rows?: { revenue: number }[]) {
  if (!rows || !rows.length || value === null || value === undefined) return "0%";
  const max = Math.max(...rows.map((r) => r.revenue || 0));
  if (max <= 0) return "0%";
  const pct = Math.round((value / max) * 100);
  return `${Math.min(100, pct)}%`;
}

function buildInsightPayload(agg: AggregateResult) {
  if (!agg.summary) throw new Error("No summary for insights");
  return {
    summary: agg.summary,
    topProducts: (agg.products ?? []).slice(0,5).map((p) => ({ product: (p as any).product, revenue: p.revenue, quantity: p.quantity })),
    topChannels: (agg.channels ?? []).slice(0,5).map((c) => ({ channel: (c as any).channel, revenue: c.revenue, quantity: c.quantity })),
    topGeo: (agg.geo ?? []).slice(0,5).map((g) => ({ city: (g as any).city, state: (g as any).state, revenue: g.revenue, quantity: g.quantity })),
    timeseries: (agg.timeseries?.daily ?? []).slice(0,7).map((d) => ({ date: (d as any).date, revenue: d.revenue, quantity: d.quantity })),
    anomalies: agg.anomalies ?? [],
  };
}

function cleanInsights(raw: any): string[] {
  if (!raw) return [];
  const arr = Array.isArray(raw) ? raw : [raw];
  const stripFence = (s: string) => {
    if (!s) return s;
    let out = s.trim();
    if (out.startsWith("```")) {
      out = out.replace(/^```[a-zA-Z]*\s*/, "").replace(/```$/, "").trim();
    }
    return out;
  };
  const flat: string[] = [];
  for (const item of arr) {
    if (typeof item === "string") {
      const stripped = stripFence(item);
      if (stripped.startsWith("[") || stripped.startsWith("{")) {
        try {
          const parsed = JSON.parse(stripped);
          if (Array.isArray(parsed)) {
            flat.push(...parsed.map((v) => (typeof v === "string" ? v : JSON.stringify(v))));
            continue;
          }
        } catch (_) {}
      }
      flat.push(stripped);
    }
  }
  return flat.filter(Boolean);
}

async function routeChatQuestion(conn: AsyncDuckDBConnection, agg: AggregateResult, q: string) {
  const text = q.toLowerCase();
  const run = async (sql: string) => (await conn.query(sql)).toArray();

  if (text.includes("top") && text.includes("product")) {
    return {
      template: "top_products",
      table: await run("SELECT product, SUM(revenue) AS revenue, SUM(quantity) AS quantity FROM cleaned GROUP BY 1 ORDER BY revenue DESC LIMIT 5;"),
    };
  }
  if (text.includes("channel")) {
    return {
      template: "channels",
      table: await run("SELECT channel, SUM(revenue) AS revenue, SUM(quantity) AS quantity FROM cleaned GROUP BY 1 ORDER BY revenue DESC LIMIT 5;"),
    };
  }
  if (text.includes("state") || text.includes("city") || text.includes("geo")) {
    return {
      template: "geo",
      table: await run("SELECT state, city, SUM(revenue) AS revenue, SUM(quantity) AS quantity FROM cleaned GROUP BY 1,2 ORDER BY revenue DESC LIMIT 5;"),
    };
  }
  if (text.includes("anomaly") || text.includes("outlier")) {
    return {
      template: "anomalies",
      table: await run(`WITH daily AS (SELECT DATE_TRUNC('day', ts) AS d, SUM(revenue) AS revenue FROM cleaned WHERE ts IS NOT NULL GROUP BY 1), stats AS (SELECT AVG(revenue) AS avg_rev, STDDEV_SAMP(revenue) AS sd_rev FROM daily) SELECT d AS date, revenue, (revenue - stats.avg_rev) / NULLIF(stats.sd_rev, 0) AS zscore FROM daily, stats WHERE stats.sd_rev IS NOT NULL ORDER BY ABS((revenue - stats.avg_rev) / NULLIF(stats.sd_rev, 0)) DESC LIMIT 5;`),
    };
  }
  if (text.includes("mom") || text.includes("month") || text.includes("growth")) {
    return {
      template: "mom_growth",
      table: await run(`WITH m AS (SELECT DATE_TRUNC('month', ts) AS m, SUM(revenue) AS rev FROM cleaned WHERE ts IS NOT NULL GROUP BY 1) SELECT m AS month, rev FROM m ORDER BY m DESC LIMIT 2;`),
    };
  }
  if (text.includes("yoy") || text.includes("year")) {
    return {
      template: "yoy_growth",
      table: await run(`WITH y AS (SELECT DATE_TRUNC('year', ts) AS y, SUM(revenue) AS rev FROM cleaned WHERE ts IS NOT NULL GROUP BY 1) SELECT y AS year, rev FROM y ORDER BY y DESC LIMIT 2;`),
    };
  }
  // default: summary
  return {
    template: "summary",
    table: agg.summary ? [agg.summary] : [],
  };
}

function inferCanonical(name: string): Mapping {
  const lower = name.toLowerCase().trim();
  const reason: string[] = [];
  const score = (cond: boolean, bump = 0.25) => (cond ? bump : 0);
  let confidence = 0;
  let canonical: CanonicalField | undefined;

  if (!canonical && (/order|id/.test(lower) || /^id$/.test(lower))) {
    canonical = "order_id";
    confidence += score(true, 0.4);
    reason.push("matches order/id");
  }
  if (!canonical && /product|item|sku/.test(lower)) {
    canonical = "product";
    confidence += score(true, 0.4);
    reason.push("matches product/item/sku");
  }
  if (!canonical && /date|time/.test(lower)) {
    canonical = "timestamp";
    confidence += score(true, 0.4);
    reason.push("matches date/time");
  }
  if (!canonical && /qty|quantity|units?/.test(lower)) {
    canonical = "quantity";
    confidence += score(true, 0.4);
    reason.push("matches quantity");
  }
  if (!canonical && /price|amount|cost/.test(lower)) {
    canonical = "price";
    confidence += score(true, 0.4);
    reason.push("matches price/amount");
  }
  if (!canonical && /revenue|sales|total/.test(lower)) {
    canonical = "revenue";
    confidence += score(true, 0.4);
    reason.push("matches revenue/total");
  }
  if (!canonical && /channel|market|source/.test(lower)) {
    canonical = "channel";
    confidence += score(true, 0.4);
    reason.push("matches channel/market/source");
  }
  if (!canonical && /city/.test(lower)) {
    canonical = "city";
    confidence += score(true, 0.4);
    reason.push("matches city");
  }
  if (!canonical && /\bstate\b|province/.test(lower)) {
    canonical = "state";
    confidence += score(true, 0.4);
    reason.push("matches state/province");
  }
  if (!canonical && /\bzip\b|postal/.test(lower)) {
    canonical = "zip";
    confidence += score(true, 0.4);
    reason.push("matches zip/postal");
  }

  return {
    originalName: name,
    canonicalName: canonical,
    confidence: canonical ? Math.min(1, 0.5 + confidence) : 0,
    reason: reason.join("; "),
  };
}
