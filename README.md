This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

### Requirements

- Node.js 20.x (installed via Homebrew or nvm)
- npm/yarn/pnpm (npm is used below)
- An [OpenAI API key](https://platform.openai.com/docs/api-reference/authentication) stored in `.env.local`
- DuckDB WASM assets (`public/duckdb/duckdb-*`) are already self-hosted inside the repo
 
### Bootstrapping

1. Install dependencies and the DuckDB peer (`apache-arrow` is required by `@duckdb/duckdb-wasm`):

```bash
npm install
npm install apache-arrow
```

2. Create `.env.local` and add the following values:

```bash
OPENAI_API_KEY=sk-...
NEXT_PUBLIC_ENABLE_LLM=true
```

3. (Optional) If you prefer yarn/pnpm, run the equivalent install commands before the steps below.

### Development

```bash
npm run dev -- --hostname 0.0.0.0 --port 3000
```

The app listens on `http://localhost:3000`. The page automatically reloads after edits.

### Key Flows

- Upload a CSV via the “Choose CSV” button, wait for DuckDB WASM to infer the schema, then click **Run** to materialize aggregates and insights.
- The left-hand **History** panel will save each aggregated upload. Click an entry to reload its dashboard, or use **Delete** to remove just that snapshot.
- Local storage keys (`upload-history-v1`, `upload-cache-v1:<hash>`, `upload-last-session`) mirror what you see in the UI for persistence troubleshooting.

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
