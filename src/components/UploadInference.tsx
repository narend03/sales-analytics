"use client";

import { memo, useMemo, useState, useDeferredValue, useCallback, useRef, useEffect } from "react";
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
const CACHE_PREFIX = "upload-cache-v1";
const HISTORY_KEY = "upload-history-v1";
const LAST_SESSION_KEY = "upload-last-session";
const LLM_ENABLED =
  typeof process !== "undefined"
    ? process.env.NEXT_PUBLIC_ENABLE_LLM !== "false"
    : true;

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

type CachePayload = {
  result: InferenceResult;
  agg?: AggregateResult | null;
  insights?: string[] | null;
  lastRefreshed?: string | null;
  chatMessages?: ChatMessage[];
};

type HistoryEntry = {
  hash: string;
  title: string;
  rowCount: number;
  lastRefreshed?: string | null;
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
  const [aggLoading, setAggLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<InferenceResult | null>(null);
  const [agg, setAgg] = useState<AggregateResult | null>(null);
  const [aggView, setAggView] = useState<AggregateResult | null>(null);
  const [insights, setInsights] = useState<string[] | null>(null);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insightsError, setInsightsError] = useState<string | null>(null);
  const [chatInput, setChatInput] = useState("");
  const deferredChatInput = useDeferredValue(chatInput);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatLoading, setChatLoading] = useState(false);
  const [chatError, setChatError] = useState<string | null>(null);
  const [lastRefreshed, setLastRefreshed] = useState<string | null>(null);
  const [fileHash, setFileHash] = useState<string | null>(null);
  const [cacheHit, setCacheHit] = useState(false);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [selectedHash, setSelectedHash] = useState<string | null>(null);
  const [titleInput, setTitleInput] = useState<string>("");
  const sortedHistory = useMemo(() => {
    return [...history].sort((a, b) => {
      const aTime = a.lastRefreshed ? new Date(a.lastRefreshed).getTime() : 0;
      const bTime = b.lastRefreshed ? new Date(b.lastRefreshed).getTime() : 0;
      if (bTime !== aTime) return bTime - aTime;
      return (b.rowCount ?? 0) - (a.rowCount ?? 0);
    });
  }, [history]);

  const persistSession = useCallback(
    (payload: Partial<CachePayload>) => {
      if (!fileHash) return;
      console.info("[persistSession]", fileHash, payload);
      saveCache(fileHash, payload);
      saveLastSession(fileHash);
    },
    [fileHash],
  );

  const updateHistoryEntry = useCallback(
    (hash: string, overrides: Partial<HistoryEntry>) => {
      setHistory((current) => {
        const existing = current.find((entry) => entry.hash === hash);
        const rowCount = Number(overrides.rowCount ?? existing?.rowCount ?? 0);
        const title = overrides.title ?? existing?.title ?? hash;
        const lastRefreshed = overrides.lastRefreshed ?? existing?.lastRefreshed ?? null;
        const nextEntry: HistoryEntry = { hash, title, rowCount, lastRefreshed };
        console.info("[history entry]", nextEntry);
        const next = upsertHistoryEntry(current, nextEntry);
        saveHistoryEntries(next);
        return next;
      });
    },
    [],
  );

  const handleTitleChange = useCallback(
    (value: string) => {
      setTitleInput(value);
      if (fileHash) {
        updateHistoryEntry(fileHash, { title: value });
      }
    },
    [fileHash, updateHistoryEntry],
  );

  const restoreFromHistory = useCallback(
    (entry: HistoryEntry) => {
      const cached = loadCache(entry.hash);
      console.info("[restoreFromHistory]", entry.hash, !!cached);
      if (!cached) return;
      setCacheHit(true);
      setFileHash(entry.hash);
      setResult(cached.result);
      setAgg(cached.agg ?? null);
      setAggView(cached.agg ?? null);
      setInsights(cached.insights ?? null);
      setChatMessages(cached.chatMessages ?? []);
      setLastRefreshed(entry.lastRefreshed ?? cached.lastRefreshed ?? null);
      setSelectedHash(entry.hash);
      setTitleInput(entry.title);
      updateHistoryEntry(entry.hash, {
        title: entry.title,
        lastRefreshed: entry.lastRefreshed ?? cached.lastRefreshed ?? null,
      });
      saveLastSession(entry.hash);
    },
    [updateHistoryEntry],
  );

  const clearHistory = useCallback(() => {
    if (typeof window === "undefined") return;
    localStorage.removeItem(HISTORY_KEY);
    localStorage.removeItem(LAST_SESSION_KEY);
    setHistory([]);
    setSelectedHash(null);
  }, []);
  const deleteHistoryEntry = useCallback((hash: string) => {
    if (typeof window === "undefined") return;
    localStorage.removeItem(`${CACHE_PREFIX}:${hash}`);
    setHistory((current) => {
      const next = current.filter((entry) => entry.hash !== hash);
      saveHistoryEntries(next);
      return next;
    });
    setSelectedHash((prev) => (prev === hash ? null : prev));
    const last = loadLastSession();
    if (last?.hash === hash) {
      localStorage.removeItem(LAST_SESSION_KEY);
    }
  }, []);
  const mappedColumns = useMemo(() => result?.schema.columns.filter((c) => c.canonicalName) ?? [], [result]);
  const mappedDistinctEntries = useMemo(() => {
    if (!result?.schema.distinctCounts) return [];
    const mappedNames = new Set(mappedColumns.map((c) => c.originalName));
    return Object.entries(result.schema.distinctCounts)
      .filter(([name]) => mappedNames.has(name))
      .sort((a, b) => Number(b[1] ?? 0) - Number(a[1] ?? 0))
      .slice(0, 8);
  }, [result, mappedColumns]);
  const productsFullRef = useRef<any[] | null>(null);
  const channelsFullRef = useRef<any[] | null>(null);
  const geoFullRef = useRef<any[] | null>(null);
  const anomaliesFullRef = useRef<any[] | null>(null);
  const previewJson = useMemo(
    () => (result ? JSON.stringify(result.previewRows, null, 2) : ""),
    [result],
  );

  useEffect(() => {
    const entries = loadHistoryEntries();
    setHistory(entries);
    const last = loadLastSession();
    if (last?.hash) {
      const cached = loadCache(last.hash);
      if (cached) {
        setCacheHit(true);
        setFileHash(last.hash);
        setResult(cached.result);
        setAgg(cached.agg ?? null);
        setAggView(cached.agg ?? null);
        setInsights(cached.insights ?? null);
        setChatMessages(cached.chatMessages ?? []);
        setLastRefreshed(cached.lastRefreshed ?? null);
        setSelectedHash(last.hash);
        const matched = entries.find((entry) => entry.hash === last.hash);
        if (matched) {
          setTitleInput(matched.title);
        }
      }
    }
  }, []);

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
      const bufArray = await file.arrayBuffer();
      const buf = new Uint8Array(bufArray);
      const hash = await sha256Hex(bufArray);
      setFileHash(hash);
      setSelectedHash(hash);
      const cached = loadCache(hash);
      if (cached) {
        setCacheHit(true);
        setResult(cached.result);
        setAgg(cached.agg ?? null);
        setInsights(cached.insights ?? null);
        setChatMessages(cached.chatMessages ?? []);
        setLastRefreshed(cached.lastRefreshed ?? null);
        const existingHistory = history.find((entry) => entry.hash === hash);
        const computedTitle = existingHistory?.title ?? file.name ?? hash;
        setTitleInput(computedTitle);
        updateHistoryEntry(hash, {
          title: computedTitle,
          lastRefreshed: cached.lastRefreshed ?? null,
        });
        saveLastSession(hash);
        setLoading(false);
        return;
      }
      setCacheHit(false);
      setTitleInput(file.name ?? hash);

      const { db: duck, conn: connection } = await ensureDb();
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
          if (tsColForPreview && k === tsColForPreview && (typeof v === "number" || typeof v === "string")) {
            const iso = safeToIso(v);
            next[k] = iso ?? v;
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
          `SELECT MIN(${quoteIdent(tsCol)}) as min_date, MAX(${quoteIdent(tsCol)}) as max_date FROM data;`,
        );
        const [row] = dateRes.toArray() as { min_date?: string; max_date?: string }[];
        minDate = row?.min_date ? safeToIso(row.min_date) ?? undefined : undefined;
        maxDate = row?.max_date ? safeToIso(row.max_date) ?? undefined : undefined;
      }

      const distinctCounts: Record<string, number> = {};
      for (const m of mappings.filter((m) => m.canonicalName)) {
        const col = m.originalName;
        const res = await connection.query(`SELECT COUNT(DISTINCT ${quoteIdent(col)}) as c FROM data;`);
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
      saveCache(hash, { result: { schema: response, previewRows } });
      saveLastSession(hash);
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
      setAggLoading(true);
      setError(null);
      const map = buildColumnMap(result.schema);
      await materializeCleanedView(conn, map);
      const summary = await getSummary(conn);
      const timeseries = await getTimeseries(conn);
      const products = await getProducts(conn, 10, 0);
      const geo = await getGeo(conn);
      const channels = await getChannels(conn);
      const anomalies = await getAnomalies(conn);

      const toIso = (v: any) => safeToIso(v) ?? v;
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

      productsFullRef.current = products as any;
      channelsFullRef.current = channels as any;
      geoFullRef.current = geo as any;
      anomaliesFullRef.current = anomalies as any;
      const view: AggregateResult = {
        summary: display.summary,
        timeseries: display.timeseries,
        products: products?.slice(0, 50) as any,
        geo: geo?.slice(0, 50) as any,
        channels: channels?.slice(0, 50) as any,
        anomalies: anomalies?.slice(0, 50) as any,
      };
      setAgg(view);
      setAggView(view);
      const refreshed = new Date().toISOString();
      setLastRefreshed(refreshed);
      const insightsResult = await fetchInsights(view);
      if (fileHash) {
        persistSession({
          result,
          agg: view,
          insights: insightsResult,
          chatMessages,
          lastRefreshed: refreshed,
        });
        const entry: HistoryEntry = {
          hash: fileHash,
          title: titleInput || history.find((h) => h.hash === fileHash)?.title || fileHash,
          rowCount: Number(result.schema.rowCount ?? 0),
          lastRefreshed: refreshed,
        };
        updateHistoryEntry(fileHash, entry);
        setSelectedHash(fileHash);
      }
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Aggregations failed";
      setError(msg);
    } finally {
      setAggLoading(false);
    }
  };
const fetchInsights = async (context: AggregateResult): Promise<string[] | null> => {
    if (!LLM_ENABLED) {
      setInsightsError("LLM disabled (set NEXT_PUBLIC_ENABLE_LLM=true to enable).");
      setInsights([]);
      return [];
    }
    setInsightsLoading(true);
    setInsightsError(null);
    setInsights(null);
    const payload = buildInsightPayload(context);
    try {
      const payloadStr = JSON.stringify(payload);
      if (payloadStr.length > 4000) {
        setInsightsError("Context too large, skipped insights to save tokens.");
        setInsights([]);
        return [];
      }
      const res = await fetch("/api/insights", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: payloadStr,
      });
      const data = await res.json();
      if (!res.ok) {
        const errMsg = data?.error || "insights error";
        setInsightsError(errMsg);
        setInsights([]);
        return [];
      }
      const cleaned = cleanInsights(data.insights);
      setInsights(cleaned);
      return cleaned;
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "insights failed";
      setInsightsError(msg);
      setInsights([]);
      return [];
    } finally {
      setInsightsLoading(false);
    }
  };
const runChat = async () => {
    if (!conn || !aggView || !aggView.summary || !chatInput.trim()) return;
    setChatError(null);
    setChatLoading(true);
    try {
      if (!LLM_ENABLED) {
        throw new Error("LLM disabled (set NEXT_PUBLIC_ENABLE_LLM=true to enable).");
      }
      await materializeCleanedView(conn, buildColumnMap(result!.schema));
      const templateResult = await routeChatQuestion(conn, aggView, chatInput.trim());
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question: chatInput.trim(),
          template: templateResult.template,
          summary: aggView.summary,
          table: templateResult.table,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "chat error");
      const userMessage: ChatMessage = { role: "user", content: chatInput.trim() };
      const assistantMessage: ChatMessage = {
        role: "assistant",
        content: data.answer,
        table: templateResult.table,
      };
      setChatMessages((prev) => {
        const next = [...prev, userMessage, assistantMessage];
        persistSession({ chatMessages: next });
        return next;
      });
      setChatInput("");
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "chat failed";
      setChatError(msg);
    } finally {
      setChatLoading(false);
    }
  };
  return (
    <div className="grid gap-6 lg:grid-cols-[280px,1fr]">
      <aside className="rounded-3xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-semibold text-slate-900">History</h3>
          <button
            type="button"
            onClick={clearHistory}
            disabled={sortedHistory.length === 0}
            className="text-[11px] font-semibold text-indigo-600 disabled:text-slate-400"
          >
            Clear
          </button>
        </div>
        <div className="mt-3 space-y-2 max-h-[60vh] overflow-auto pr-1">
          {sortedHistory.length === 0 ? (
            <p className="text-xs text-gray-500">Upload a CSV to start saving snapshots.</p>
          ) : (
            sortedHistory.map((entry) => (
              <div
                key={entry.hash}
                role="button"
                tabIndex={0}
                onClick={() => restoreFromHistory(entry)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    restoreFromHistory(entry);
                  }
                }}
                className={`flex w-full cursor-pointer flex-col rounded-xl border px-3 py-2 shadow-sm transition ${
                  selectedHash === entry.hash
                    ? "border-indigo-400 bg-indigo-50"
                    : "border-slate-200 bg-white hover:border-slate-400"
                }`}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="text-sm font-semibold text-slate-900 truncate">{entry.title}</span>
                  <div className="flex items-center gap-2">
                    <span className="text-[11px] text-slate-500">{entry.rowCount} rows</span>
                    <button
                      type="button"
                      onClick={(event) => {
                        event.stopPropagation();
                        deleteHistoryEntry(entry.hash);
                      }}
                      className="text-[11px] font-semibold text-rose-600 hover:text-rose-800"
                    >
                      Delete
                    </button>
                  </div>
                </div>
                <p className="text-[10px] text-slate-500">
                  {entry.lastRefreshed ? `Refreshed ${formatDateDisplay(entry.lastRefreshed)}` : "Run aggregates to save insights"}
                </p>
              </div>
            ))
          )}
        </div>
        <div className="mt-4 space-y-1">
          <label className="text-[11px] font-semibold uppercase tracking-wide text-slate-400">Current title</label>
          <input
            value={titleInput}
            onChange={(e) => handleTitleChange(e.target.value)}
            placeholder="Describe this upload"
            className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm focus:border-indigo-500 focus:ring focus:ring-indigo-200"
          />
        </div>
      </aside>
      <div className="space-y-5">
      <div className="border-2 border-dashed border-slate-200 rounded-xl p-6 text-center shadow-sm bg-white">
        <div className="flex flex-col items-center gap-3">
          <input
            id="file-input"
            type="file"
            accept=".csv,text/csv"
            onChange={(e) => handleFile(e.target.files?.[0] ?? null)}
            className="hidden"
          />
          <label
            htmlFor="file-input"
            className="inline-flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow hover:bg-indigo-700 cursor-pointer"
          >
            Choose CSV
          </label>
          <p className="text-sm text-gray-500">Drop a CSV or click the button above to load locally (WASM).</p>
          {cacheHit && (
            <p className="text-xs text-green-600">Loaded from cache. Click Run to refresh aggregates.</p>
          )}
        </div>
      </div>

      {loading && <p className="text-sm text-emerald-600">Processing CSV...</p>}
      {error && <p className="text-sm text-rose-600">Error: {error}</p>}

      {result && (
        <div className="space-y-3">
          <div className="rounded-xl border border-slate-200 bg-white p-4 space-y-1 shadow-sm">
            <h3 className="font-semibold text-slate-900">Summary</h3>
            <p className="text-sm">Rows: {result.schema.rowCount}</p>
            <p className="text-sm">
              Date range: {formatDateDisplay(result.schema.minDate)} → {formatDateDisplay(result.schema.maxDate)}
            </p>
            {mappedDistinctEntries.length > 0 && (
              <div className="text-sm text-gray-700 space-y-0.5">
                <div className="font-semibold text-xs text-gray-600">Distinct (mapped fields)</div>
                <div className="max-h-24 overflow-auto pr-1">
                  {mappedDistinctEntries.map(([col, cnt]) => (
                    <div key={col} className="flex justify-between text-xs text-gray-700">
                      <span>{col}</span>
                      <span>{cnt as number}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
            {lastRefreshed && <p className="text-xs text-gray-500">Last refreshed: {lastRefreshed}</p>}
          </div>

          <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="flex items-center justify-between mb-2">
              <h3 className="font-semibold text-slate-900">Column Mapping</h3>
              <span className="text-xs text-gray-500">
                Mapped {mappedColumns.length} of {result.schema.columns.length}
              </span>
            </div>
            <div className="space-y-1 text-sm max-h-56 overflow-auto pr-1">
              {mappedColumns.map((col) => (
                <div key={col.originalName} className="flex justify-between">
                  <span>{col.originalName}</span>
                  <span className="text-gray-600">
                    {col.canonicalName} ({Math.round(col.confidence * 100)}%)
                  </span>
                </div>
              ))}
              {mappedColumns.length === 0 && (
                <p className="text-sm text-gray-600">No columns mapped.</p>
              )}
            </div>
          </div>

          <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <h3 className="font-semibold text-slate-900 mb-2">Preview (first 20 rows)</h3>
            <pre className="overflow-auto max-h-72 text-xs bg-slate-50 border border-slate-200 p-3 rounded-lg text-slate-900">
              {previewJson}
            </pre>
          </div>

          <div className="rounded-xl border border-slate-200 bg-white p-4 space-y-3 shadow-sm">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <h3 className="font-semibold text-slate-900">Dashboard</h3>
                <p className="text-xs text-gray-500">Click Run to compute aggregates</p>
              </div>
              <button
                onClick={runAggregates}
                className="rounded-lg bg-gradient-to-r from-indigo-500 to-cyan-500 px-4 py-2 text-sm font-medium text-white shadow-lg hover:brightness-110 disabled:from-gray-400 disabled:to-gray-400"
                disabled={loading || aggLoading}
              >
                {aggLoading ? "Running..." : "Run"}
              </button>
            </div>

            {!aggView && !aggLoading && <p className="text-sm text-gray-600">No aggregates yet.</p>}
            {aggLoading && (
              <div className="grid gap-3 md:grid-cols-2 animate-pulse">
                {[...Array(4)].map((_, i) => (
                  <div key={i} className="rounded border p-3 space-y-2">
                    <div className="h-4 bg-gray-200 rounded w-1/2" />
                    <div className="h-3 bg-gray-200 rounded w-3/4" />
                    <div className="h-3 bg-gray-200 rounded w-2/3" />
                  </div>
                ))}
              </div>
            )}

            {aggView && (
              <div className="grid gap-3 md:grid-cols-2">
                <div className="rounded border p-3 space-y-1">
                  <h4 className="text-sm font-semibold">Summary</h4>
                  <p className="text-sm text-gray-800">Revenue: {formatCurrency(aggView.summary?.totalRevenue)}</p>
                  <p className="text-sm text-gray-800">Units: {aggView.summary?.totalQuantity ?? "n/a"}</p>
                  <p className="text-xs text-gray-600">
                    Date range: {aggView.summary?.minDate ?? "n/a"} → {aggView.summary?.maxDate ?? "n/a"}
                  </p>
                  <p className="text-xs text-gray-600">
                    MoM: {formatPct(aggView.summary?.momGrowthPct)} • YoY: {formatPct(aggView.summary?.yoyGrowthPct)}
                  </p>
                </div>

                <div className="rounded border p-3 space-y-2">
                  <h4 className="text-sm font-semibold">Timeseries (daily)</h4>
                  <div className="space-y-1 text-xs text-gray-800 max-h-40 overflow-auto">
                    {(aggView.timeseries?.daily ?? []).slice(0, 7).map((d, i) => (
                      <div key={i} className="flex justify-between">
                        <span>{d.date as string}</span>
                        <span>{formatCurrency(d.revenue)}</span>
                      </div>
                    ))}
                    {(aggView.timeseries?.daily?.length ?? 0) === 0 && <p>No dates.</p>}
                  </div>
                </div>

                <div className="rounded border p-3 space-y-2">
                  <h4 className="text-sm font-semibold">Top Products</h4>
                  <div className="space-y-1 text-xs text-gray-800 max-h-64 overflow-auto pr-1">
                    {aggView.products?.map((p, i) => (
                      <div key={i}>
                        <div className="flex justify-between">
                          <span>{p.product}</span>
                          <span>{formatCurrency(p.revenue)}</span>
                        </div>
                        <div className="h-2 rounded bg-gray-100">
                          <div
                            className="h-2 rounded bg-indigo-500"
                            style={{ width: barWidth(p.revenue, aggView?.products ?? []) }}
                          />
                        </div>
                      </div>
                    ))}
                    {(!aggView.products || aggView.products.length === 0) && <p>No products.</p>}
                  </div>
                </div>

                <div className="rounded border p-3 space-y-2">
                  <h4 className="text-sm font-semibold">Channels</h4>
                  <div className="space-y-1 text-xs text-gray-800 max-h-56 overflow-auto pr-1">
                    {aggView.channels?.map((c, i) => (
                      <div key={i} className="flex justify-between">
                        <span>{c.channel ?? "(unknown)"}</span>
                        <span>{formatCurrency(c.revenue)}</span>
                      </div>
                    ))}
                    {(!aggView.channels || aggView.channels.length === 0) && <p>No channels.</p>}
                  </div>
                </div>

                <div className="rounded border p-3 space-y-2">
                  <h4 className="text-sm font-semibold">Geo</h4>
                  <div className="space-y-1 text-xs text-gray-800 max-h-28 overflow-auto">
                    {aggView.geo?.map((g, i) => (
                      <div key={i} className="flex justify-between">
                        <span>{[g.city, g.state].filter(Boolean).join(', ') || '(unknown)'}</span>
                        <span>{formatCurrency(g.revenue)}</span>
                      </div>
                    ))}
                    {(!aggView.geo || aggView.geo.length === 0) && <p>No geo.</p>}
                  </div>
                </div>

                <div className="rounded border p-3 space-y-2">
                  <div className="flex items-start justify-between">
                    <h4 className="text-sm font-semibold">Anomalies</h4>
                    <span className="rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-semibold text-amber-700">
                      Spikes & dips
                    </span>
                  </div>
                  <p className="text-xs text-gray-600">
                    Days where revenue was far from the usual daily average. Positive z = spike; negative z = dip.
                  </p>
                  <div className="space-y-2 text-xs text-gray-800">
                    {aggView.anomalies?.map((a, i) => {
                      const z = a.zscore ?? 0;
                      const isHigh = z >= 0;
                      return (
                        <div
                          key={i}
                          className="flex items-center justify-between rounded-lg border border-slate-200 px-2 py-2"
                        >
                          <div className="space-y-0.5">
                            <div className="text-sm text-gray-900">{a.date as string}</div>
                            <div className="text-gray-700">
                              {formatCurrency(a.revenue)} • z={z?.toFixed?.(2) ?? "n/a"}
                            </div>
                          </div>
                          <span
                            className={
                              "rounded-full px-2 py-0.5 text-[10px] font-semibold " +
                              (isHigh ? "bg-emerald-100 text-emerald-700" : "bg-rose-100 text-rose-700")
                            }
                          >
                            {isHigh ? "High spike" : "Drop"}
                          </span>
                        </div>
                      );
                    })}
                    {(!aggView.anomalies || aggView.anomalies.length === 0) && <p>No anomalies.</p>}
                  </div>
                </div>
              </div>
            )}
          </div>
          <div className="rounded-xl border border-slate-200 bg-white p-4 space-y-2 shadow-sm">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-slate-900">Insights</h3>
              {insightsLoading && <span className="text-xs text-gray-500">Generating…</span>}
            </div>
            {insightsError && <p className="text-sm text-rose-600">{insightsError}</p>}
            {insights && insights.length > 0 && (
              <div className="max-h-64 overflow-auto pr-1">
                <ul className="list-disc list-inside text-sm space-y-1 text-slate-800">
                  {insights.map((item, idx) => (
                    <li key={idx}>{item}</li>
                  ))}
                </ul>
              </div>
            )}
            {insights && insights.length === 0 && !insightsLoading && (
              <p className="text-sm text-gray-600">No insights returned.</p>
            )}
          </div>

          <ChatPanel
            input={chatInput}
            deferredInput={deferredChatInput}
            onInputChange={setChatInput}
            onSend={runChat}
            loading={chatLoading}
            error={chatError}
            messages={chatMessages}
          />


        </div>
      )}
    </div>
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

function saveCache(hash: string, data: Partial<CachePayload>) {
  if (typeof window === "undefined") return;
  try {
    const existing = loadCache(hash);
    const merged = { ...(existing ?? {}), ...data };
    const serialized = JSON.stringify(merged, (_key, value) =>
      typeof value === "bigint" ? Number(value) : value,
    );
    localStorage.setItem(`${CACHE_PREFIX}:${hash}`, serialized);
  } catch (_) {
    // ignore cache failures
  }
}

function loadCache(hash: string): CachePayload | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = localStorage.getItem(`${CACHE_PREFIX}:${hash}`);
    if (!raw) return null;
    return JSON.parse(raw) as CachePayload;
  } catch (_) {
    return null;
  }
}

function loadHistoryEntries(): HistoryEntry[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    if (!raw) return [];
    return JSON.parse(raw) as HistoryEntry[];
  } catch (_) {
    return [];
  }
}

function saveHistoryEntries(entries: HistoryEntry[]) {
  if (typeof window === "undefined") return;
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(entries));
  } catch (_) {}
}

function upsertHistoryEntry(entries: HistoryEntry[], entry: HistoryEntry) {
  const idx = entries.findIndex((e) => e.hash === entry.hash);
  if (idx === -1) return [...entries, entry];
  const copy = [...entries];
  copy[idx] = { ...copy[idx], ...entry };
  return copy;
}

function loadLastSession() {
  if (typeof window === "undefined") return null;
  try {
    const raw = localStorage.getItem(LAST_SESSION_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch (_) {
    return null;
  }
}

function saveLastSession(hash: string) {
  if (typeof window === "undefined") return;
  try {
    localStorage.setItem(LAST_SESSION_KEY, JSON.stringify({ hash }));
  } catch (_) {}
}

async function sha256Hex(buf: ArrayBuffer) {
  const hashBuf = await crypto.subtle.digest("SHA-256", buf);
  const bytes = new Uint8Array(hashBuf);
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
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


function safeToIso(v: any) {
  if (v === null || v === undefined) return null;
  const d = new Date(v as any);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}


function quoteIdent(name: string) {
  return `"${name.replace(/"/g, '')}"`;
}


function formatDateDisplay(v: any) {
  if (!v) return "n/a";
  const d = new Date(v as any);
  if (Number.isNaN(d.getTime())) return "n/a";
  const year = d.getFullYear();
  if (year < 1900 || year > 2100) return "n/a";
  return d.toISOString().split("T")[0];
}

const ChatPanel = memo(function ChatPanel({ input, deferredInput, onInputChange, onSend, loading, error, messages }: { input: string; deferredInput: string; onInputChange: (v: string) => void; onSend: () => void; loading: boolean; error: string | null; messages: { role: "user" | "assistant"; content: string }[]; }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4 space-y-2 shadow-sm">
      <h3 className="font-semibold text-slate-900">Chat</h3>
      <div className="space-y-2">
        <input
          value={input}
          onChange={(e) => onInputChange(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); onSend(); } }}
          placeholder="Ask a question (e.g., top products, channels, growth, anomalies)"
          className="w-full rounded border px-3 py-2 text-sm"
        />
        <button
          onClick={onSend}
          className="rounded-lg bg-gradient-to-r from-indigo-500 to-cyan-500 px-4 py-2 text-sm font-medium text-white shadow-lg hover:brightness-110 disabled:from-gray-400 disabled:to-gray-400"
          disabled={loading || !deferredInput.trim()}
        >
          {loading ? "Sending..." : "Ask"}
        </button>
        {error && <p className="text-sm text-rose-600">{error}</p>}
      </div>
      <div className="space-y-2 max-h-64 overflow-auto pr-1">
        {messages.map((m, idx) => (
          <div key={idx} className="rounded-lg border border-slate-200 bg-white p-3 text-sm text-slate-900">
            <div className="font-semibold text-gray-700">{m.role === "user" ? "You" : "AI"}</div>
            <div className="whitespace-pre-wrap text-gray-800">{m.content}</div>
          </div>
        ))}
      </div>
    </div>
  );
});
