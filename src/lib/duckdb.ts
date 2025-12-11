import { AsyncDuckDB, ConsoleLogger, DuckDBBundles, selectBundle } from "@duckdb/duckdb-wasm";

let dbSingleton: AsyncDuckDB | null = null;

/**
 * Lazily instantiate a DuckDB WASM instance using the provided bundles.
 */
export async function getDuckDB() {
  if (dbSingleton) return dbSingleton;

  const bundle = await selectBundle(DuckDBBundles);
  if (!bundle.mainWorker || !bundle.mainModule) {
    throw new Error("Failed to select DuckDB WASM bundle");
  }

  const workerUrl = new URL(bundle.mainWorker, import.meta.url);
  const wasmUrl = new URL(bundle.mainModule, import.meta.url);

  const logger = new ConsoleLogger();
  const worker = new Worker(workerUrl, { type: "module" });

  const db = new AsyncDuckDB(logger, worker);
  await db.instantiate(wasmUrl);

  dbSingleton = db;
  return db;
}
