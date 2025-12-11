// Centralized limits and guardrails for ingestion and runtime behavior.
export const MAX_UPLOAD_BYTES = 50 * 1024 * 1024; // 50 MB hard cap
export const MAX_PREVIEW_ROWS = 5_000; // cap preview/sample rows for UI
export const MAX_COLUMNS = 200; // guard against extremely wide CSVs
export const INGEST_TIMEOUT_MS = 30_000; // 30s upload/ingest timeout
export const MAX_CHAT_QUERY_ROWS = 10_000; // limit chat query result sizes

export const SUPPORTED_MIME_TYPES = [
  "text/csv",
  "application/vnd.ms-excel",
  "application/csv",
];
