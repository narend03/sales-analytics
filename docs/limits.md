# Limits and Guardrails

- Max upload size: 50 MB (`MAX_UPLOAD_BYTES`)
- Max preview/sample rows: 5,000 (`MAX_PREVIEW_ROWS`)
- Max columns: 200 (`MAX_COLUMNS`)
- Ingest timeout: 30s (`INGEST_TIMEOUT_MS`)
- Chat query max rows: 10,000 (`MAX_CHAT_QUERY_ROWS`)
- Allowed MIME types: `text/csv`, `application/vnd.ms-excel`, `application/csv`
- CSV sanitization: reject files exceeding caps; normalize line endings; avoid loading full file into memory—stream to disk/worker.
