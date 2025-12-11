import { NextResponse } from "next/server";
import { getDuckDB } from "@/lib/duckdb";
import { CanonicalField, SchemaInferenceResult } from "@/lib/contracts";
import { INGEST_TIMEOUT_MS, MAX_COLUMNS, MAX_UPLOAD_BYTES, SUPPORTED_MIME_TYPES } from "@/lib/limits";

const PREVIEW_LIMIT = 20;
const SAMPLE_SIZE = 20000;

export const runtime = "nodejs";

function inferCanonical(name: string) {
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

async function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout;
  return Promise.race([
    promise.finally(() => clearTimeout(timer)),
    new Promise<T>((_, reject) => {
      timer = setTimeout(() => reject(new Error("Ingestion timed out")), ms);
    }),
  ]);
}

export async function POST(request: Request) {
  try {
    const formData = await request.formData();
    const file = formData.get("file");
    if (!(file instanceof File)) {
      return NextResponse.json({ error: "file is required" }, { status: 400 });
    }

    if (file.size > MAX_UPLOAD_BYTES) {
      return NextResponse.json({ error: "file too large" }, { status: 413 });
    }
    if (SUPPORTED_MIME_TYPES.length && !SUPPORTED_MIME_TYPES.includes(file.type)) {
      console.warn("Unrecognized MIME type:", file.type);
    }

    const arrayBuffer = await file.arrayBuffer();
    const db = await getDuckDB();
    const conn = await db.connect();

    const filename = `upload_${Date.now()}.csv`;
    await db.registerFileBuffer(filename, new Uint8Array(arrayBuffer));

    const createSql = `
      CREATE OR REPLACE TABLE data AS
      SELECT * FROM read_csv_auto('${filename}', SAMPLE_SIZE ${SAMPLE_SIZE}, IGNORE_ERRORS true);
    `;

    await withTimeout(conn.query(createSql), INGEST_TIMEOUT_MS);

    const schemaRes = await conn.query("PRAGMA table_info('data');");
    const columns = schemaRes.toArray().map((row) => (row as { name: string }).name);
    if (columns.length > MAX_COLUMNS) {
      await conn.close();
      return NextResponse.json({ error: "too many columns" }, { status: 400 });
    }

    const countRes = await conn.query("SELECT COUNT(*) as count FROM data;");
    const rowCount = (countRes.toArray()[0] as { count: number }).count;

    const previewRes = await conn.query(`SELECT * FROM data LIMIT ${PREVIEW_LIMIT};`);
    const previewRows = previewRes.toArray();

    const mappings = columns.map(inferCanonical);
    const tsMapping = mappings.find((m) => m.canonicalName === "timestamp");
    let minDate: string | undefined;
    let maxDate: string | undefined;
    if (tsMapping) {
      const tsCol = tsMapping.originalName;
      const dateRes = await conn.query(
        `SELECT MIN(${tsCol}) as min_date, MAX(${tsCol}) as max_date FROM data;`,
      );
      const [row] = dateRes.toArray() as { min_date?: string; max_date?: string }[];
      minDate = row?.min_date ? new Date(row.min_date).toISOString() : undefined;
      maxDate = row?.max_date ? new Date(row.max_date).toISOString() : undefined;
    }

    const distinctCounts: Record<string, number> = {};
    for (const m of mappings.filter((m) => m.canonicalName)) {
      const col = m.originalName;
      const res = await conn.query(`SELECT COUNT(DISTINCT ${col}) as c FROM data;`);
      distinctCounts[col] = (res.toArray()[0] as { c: number }).c;
    }

    await conn.close();

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

    return NextResponse.json({
      schema: response,
      previewRows,
    });
  } catch (err: unknown) {
    console.error("Upload inference error", err);
    return NextResponse.json({ error: err?.message ?? "server error" }, { status: 500 });
  }
}
