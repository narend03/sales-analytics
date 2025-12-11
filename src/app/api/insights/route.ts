import { NextResponse } from "next/server";
import OpenAI from "openai";

export const runtime = "nodejs";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

function buildPrompt(context: InsightContext) {
  const lines = [
    "You are a sales data analyst. Produce 3-5 concise bullet insights.",
    "Rules: only use provided fields; cite numbers with units; be honest about gaps; no invented fields; keep under 120 words.",
    "If revenue is missing or zero, say 'Insufficient data to generate insights.'",
  ];

  const payload = {
    summary: context.summary,
    topProducts: context.topProducts?.slice(0, 5),
    topChannels: context.topChannels?.slice(0, 5),
    topGeo: context.topGeo?.slice(0, 5),
    anomalies: context.anomalies?.slice(0, 3),
    timeseries: context.timeseries?.slice?.(0, 7) ?? context.timeseries,
  };

  lines.push("Context JSON:");
  lines.push(JSON.stringify(payload));

  lines.push(
    "Respond as a JSON array of strings. Each string is one bullet. Include caveats if geo/channel are absent."
  );

  return lines.join("\n");
}

export async function POST(req: Request) {
  try {
    if (!process.env.OPENAI_API_KEY) {
      return NextResponse.json({ error: "OPENAI_API_KEY not set" }, { status: 500 });
    }

    const body = (await req.json()) as Partial<InsightContext>;
    if (!body.summary || body.summary.totalRevenue == null) {
      return NextResponse.json({ error: "summary.totalRevenue is required" }, { status: 400 });
    }

    const prompt = buildPrompt(body as InsightContext);

    const response = await client.responses.create({
      model: "gpt-4o-mini",
      input: prompt,
      max_output_tokens: 300,
    });

    const content = response.output_text ?? "[]";
    let parsed: string[] = [];
    try {
      parsed = JSON.parse(content);
      if (!Array.isArray(parsed)) throw new Error("not array");
    } catch (err) {
      parsed = [content];
    }

    return NextResponse.json({ insights: parsed });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : "server error";
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

// Types
export type InsightSummary = {
  totalRevenue: number;
  totalQuantity?: number;
  minDate?: string | number | null;
  maxDate?: string | number | null;
  momGrowthPct?: number | null;
  yoyGrowthPct?: number | null;
};

export type InsightRow = {
  name?: string;
  product?: string;
  channel?: string;
  state?: string | null;
  city?: string | null;
  revenue?: number;
  quantity?: number;
};

export type InsightAnomaly = {
  date: string | number;
  revenue: number;
  zscore: number;
};

export type InsightContext = {
  summary: InsightSummary;
  topProducts?: InsightRow[];
  topChannels?: InsightRow[];
  topGeo?: InsightRow[];
  timeseries?: { date: string | number; revenue: number; quantity?: number }[];
  anomalies?: InsightAnomaly[];
};
