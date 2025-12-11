import { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { CanonicalField, SchemaInferenceResult } from "./contracts";

export type ColumnMap = Record<CanonicalField, string | undefined>;

export type SummaryResult = {
  totalRevenue?: number;
  totalQuantity?: number;
  minDate?: string;
  maxDate?: string;
  momGrowthPct?: number | null;
  yoyGrowthPct?: number | null;
  hasDate: boolean;
};

export type TimeseriesPoint = { date: string; revenue: number; quantity: number };
export type ProductRow = { product: string; revenue: number; quantity: number };
export type GeoRow = { state?: string; city?: string; revenue: number; quantity: number };
export type ChannelRow = { channel: string; revenue: number; quantity: number };
export type AnomalyRow = { date: string; revenue: number; zscore: number };

export async function materializeCleanedView(conn: AsyncDuckDBConnection, mapping: ColumnMap) {
  const qi = (v?: string) => (v ? `"${v.replace(/"/g, '""')}"` : "NULL");
  const dateCol = mapping.timestamp ?? mapping.date ?? mapping["original_column"];
  const priceCol = mapping.price;
  const qtyCol = mapping.quantity;
  const revenueExpr =
    mapping.revenue
      ? `TRY_CAST(${qi(mapping.revenue)} AS DOUBLE)`
      : priceCol && qtyCol
        ? `TRY_CAST(${qi(priceCol)} AS DOUBLE) * TRY_CAST(${qi(qtyCol)} AS DOUBLE)`
        : "NULL::DOUBLE";

  const sql = `
    CREATE OR REPLACE VIEW cleaned AS
    SELECT
      ${mapping.order_id ? qi(mapping.order_id) : "NULL"} AS order_id,
      ${mapping.product ? qi(mapping.product) : "NULL"} AS product,
      ${dateCol ? `TRY_CAST(${qi(dateCol)} AS TIMESTAMP)` : "NULL"} AS ts,
      ${qtyCol ? `TRY_CAST(${qi(qtyCol)} AS DOUBLE)` : "NULL"} AS quantity,
      ${priceCol ? `TRY_CAST(${qi(priceCol)} AS DOUBLE)` : "NULL"} AS price,
      ${mapping.channel ? qi(mapping.channel) : "NULL"} AS channel,
      ${mapping.city ? qi(mapping.city) : "NULL"} AS city,
      ${mapping.state ? qi(mapping.state) : "NULL"} AS state,
      ${mapping.zip ? qi(mapping.zip) : "NULL"} AS zip,
      ${revenueExpr} AS revenue
    FROM data;
  `;
  await conn.query(sql);
}

export async function getSummary(conn: AsyncDuckDBConnection): Promise<SummaryResult> {
  const res = await conn.query(`
    SELECT
      SUM(revenue) AS total_revenue,
      SUM(quantity) AS total_qty,
      MIN(ts) AS min_ts,
      MAX(ts) AS max_ts
    FROM cleaned;
  `);
  const row = res.toArray()[0] as {
    total_revenue: number | null;
    total_qty: number | null;
    min_ts: string | null;
    max_ts: string | null;
  };

  const hasDate = !!row?.min_ts;
  let momGrowthPct: number | null = null;
  let yoyGrowthPct: number | null = null;

  if (hasDate) {
    const growth = await conn.query(`
      WITH daily AS (
        SELECT DATE_TRUNC('day', ts) AS d, SUM(revenue) AS rev
        FROM cleaned
        WHERE ts IS NOT NULL
        GROUP BY 1
      ),
      agg AS (
        SELECT
          SUM(CASE WHEN d >= DATE_TRUNC('month', CURRENT_DATE) THEN rev ELSE 0 END) AS this_month,
          SUM(CASE WHEN d >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 1 MONTH
                   AND d < DATE_TRUNC('month', CURRENT_DATE) THEN rev ELSE 0 END) AS prev_month,
          SUM(CASE WHEN d >= DATE_TRUNC('year', CURRENT_DATE) THEN rev ELSE 0 END) AS this_year,
          SUM(CASE WHEN d >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL 1 YEAR
                   AND d < DATE_TRUNC('year', CURRENT_DATE) THEN rev ELSE 0 END) AS prev_year
        FROM daily
      )
      SELECT * FROM agg;
    `);
    const g = growth.toArray()[0] as {
      this_month: number | null;
      prev_month: number | null;
      this_year: number | null;
      prev_year: number | null;
    };
    momGrowthPct =
      g.prev_month && g.prev_month !== 0 ? ((g.this_month ?? 0) - g.prev_month) / g.prev_month : null;
    yoyGrowthPct =
      g.prev_year && g.prev_year !== 0 ? ((g.this_year ?? 0) - g.prev_year) / g.prev_year : null;
  }

  return {
    totalRevenue: row?.total_revenue ?? undefined,
    totalQuantity: row?.total_qty ?? undefined,
    minDate: row?.min_ts ?? undefined,
    maxDate: row?.max_ts ?? undefined,
    momGrowthPct,
    yoyGrowthPct,
    hasDate,
  };
}

export async function getTimeseries(conn: AsyncDuckDBConnection) {
  const dailyRes = await conn.query(`
    SELECT DATE_TRUNC('day', ts) AS date, SUM(revenue) AS revenue, SUM(quantity) AS quantity
    FROM cleaned
    WHERE ts IS NOT NULL
    GROUP BY 1
    ORDER BY 1;
  `);
  const monthlyRes = await conn.query(`
    SELECT DATE_TRUNC('month', ts) AS month, SUM(revenue) AS revenue, SUM(quantity) AS quantity
    FROM cleaned
    WHERE ts IS NOT NULL
    GROUP BY 1
    ORDER BY 1;
  `);
  return {
    daily: dailyRes.toArray() as TimeseriesPoint[],
    monthly: monthlyRes.toArray() as { month: string; revenue: number; quantity: number }[],
  };
}

export async function getProducts(conn: AsyncDuckDBConnection, limit = 10, offset = 0, order: "desc" | "asc" = "desc") {
  const res = await conn.query(`
    SELECT product, SUM(revenue) AS revenue, SUM(quantity) AS quantity
    FROM cleaned
    WHERE product IS NOT NULL
    GROUP BY 1
    ORDER BY revenue ${order === "asc" ? "ASC" : "DESC"}
    LIMIT ${limit} OFFSET ${offset};
  `);
  return res.toArray() as ProductRow[];
}

export async function getGeo(conn: AsyncDuckDBConnection) {
  const res = await conn.query(`
    SELECT state, city, SUM(revenue) AS revenue, SUM(quantity) AS quantity
    FROM cleaned
    WHERE state IS NOT NULL OR city IS NOT NULL
    GROUP BY 1,2
    ORDER BY revenue DESC;
  `);
  return res.toArray() as GeoRow[];
}

export async function getChannels(conn: AsyncDuckDBConnection) {
  const res = await conn.query(`
    SELECT channel, SUM(revenue) AS revenue, SUM(quantity) AS quantity
    FROM cleaned
    WHERE channel IS NOT NULL
    GROUP BY 1
    ORDER BY revenue DESC;
  `);
  return res.toArray() as ChannelRow[];
}

export async function getAnomalies(conn: AsyncDuckDBConnection) {
  const res = await conn.query(`
    WITH daily AS (
      SELECT DATE_TRUNC('day', ts) AS d, SUM(revenue) AS revenue
      FROM cleaned
      WHERE ts IS NOT NULL
      GROUP BY 1
    ),
    stats AS (
      SELECT AVG(revenue) AS avg_rev, STDDEV_SAMP(revenue) AS sd_rev FROM daily
    )
    SELECT d AS date, revenue, (revenue - stats.avg_rev) / NULLIF(stats.sd_rev, 0) AS zscore
    FROM daily, stats
    WHERE stats.sd_rev IS NOT NULL AND ABS((revenue - stats.avg_rev) / NULLIF(stats.sd_rev, 0)) > 2
    ORDER BY ABS((revenue - stats.avg_rev) / NULLIF(stats.sd_rev, 0)) DESC;
  `);
  return res.toArray() as AnomalyRow[];
}

export function buildColumnMap(schema: SchemaInferenceResult): ColumnMap {
  const map: ColumnMap = {} as ColumnMap;
  for (const col of schema.columns) {
    if (col.canonicalName) {
      map[col.canonicalName] = col.originalName;
    }
  }
  return map;
}
