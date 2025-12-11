import { UploadInference } from "@/components/UploadInference";

export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-b from-white to-gray-50">
      <main className="flex min-h-screen flex-col items-center justify-center p-6 gap-8">
        <div className="text-center space-y-4">
          <p className="rounded-full border border-gray-200 px-4 py-1 text-sm text-gray-600">
            Self-serve sales analytics demo
          </p>
          <h1 className="text-5xl font-semibold tracking-tight">
            Upload messy CSVs. Get insights fast.
          </h1>
          <p className="text-lg text-gray-600 max-w-2xl">
            We infer schema, clean data, and run DuckDB WASM analytics in-browser so you can explore
            sales across products, time, geo, and channels.
          </p>
        </div>

        <div className="w-full max-w-4xl overflow-hidden rounded-2xl border border-gray-200 bg-white shadow-sm p-6">
          <UploadInference />
        </div>
      </main>
    </div>
  );
}
