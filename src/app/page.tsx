import { UploadInference } from "@/components/UploadInference";

export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-b from-white via-slate-50 to-white text-slate-900">
      <main className="relative mx-auto flex min-h-screen w-full max-w-6xl flex-col gap-10 px-4 pb-16 pt-12 md:px-8">
        <header className="flex flex-col gap-4 text-center md:text-left">
          <div className="mx-auto md:mx-0 inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white px-4 py-2 text-xs font-medium text-slate-600 shadow-sm">
            <span className="h-2 w-2 rounded-full bg-emerald-500" /> In-browser AI sales analyst
          </div>
          <h1 className="text-4xl font-semibold leading-tight tracking-tight text-slate-900 md:text-5xl">
            Upload messy CSVs. Get clean insights in seconds.
          </h1>
          <p className="text-base text-slate-600 md:text-lg">
            We infer schema, clean data, and run DuckDB WASM analytics locally—then layer AI insights and chat on top of your sales data.
          </p>
        </header>

        <section className="w-full overflow-hidden rounded-3xl border border-slate-200 bg-white p-6 shadow-xl">
          <UploadInference />
        </section>
      </main>
    </div>
  );
}
