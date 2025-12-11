// Canonical schema definitions for uploaded sales data.
export type CanonicalField =
  | "order_id"
  | "product"
  | "timestamp"
  | "quantity"
  | "price"
  | "revenue"
  | "channel"
  | "city"
  | "state"
  | "zip"
  | "source_file"
  | "original_column";

export const CANONICAL_FIELDS: CanonicalField[] = [
  "order_id",
  "product",
  "timestamp",
  "quantity",
  "price",
  "revenue",
  "channel",
  "city",
  "state",
  "zip",
  "source_file",
  "original_column",
];

export type CanonicalRow = {
  order_id?: string;
  product?: string;
  timestamp?: string; // ISO datetime
  quantity?: number;
  price?: number;
  revenue?: number;
  channel?: string;
  city?: string;
  state?: string;
  zip?: string;
  source_file?: string;
  // Map of canonical field to original column name for traceability.
  original_column?: Record<string, string>;
};

export type ColumnInference = {
  originalName: string;
  canonicalName?: CanonicalField;
  confidence: number; // 0–1
  reason?: string;
};

export type SchemaInferenceResult = {
  columns: ColumnInference[];
  rowCount: number;
  nullCounts?: Record<string, number>;
  distinctCounts?: Record<string, number>;
  minDate?: string;
  maxDate?: string;
};
