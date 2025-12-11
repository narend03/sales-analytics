import { NextResponse } from "next/server";
import OpenAI from "openai";

export const runtime = "nodejs";

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

function buildPrompt(payload: ChatRequest) {
  const { question, summary, table, template } = payload;
  const lines = [
    "You are a sales data analyst. Answer concisely (2-4 sentences).",
    "Rules: only use provided fields; cite the numbers you use; do not invent columns; if table is empty say insufficient data; mention assumptions (e.g., date column ts).",
    `Question: ${question}`,
    "Context:",
    JSON.stringify({ summary, template, table }, null, 2),
  ];
  lines.push("Respond as plain text (no JSON, no bullets).");
  return lines.join("\n");
}

export async function POST(req: Request) {
  try {
    if (!process.env.OPENAI_API_KEY) {
      return NextResponse.json({ error: "OPENAI_API_KEY not set" }, { status: 500 });
    }
    const body = (await req.json()) as ChatRequest;
    if (!body.question) {
      return NextResponse.json({ error: "question is required" }, { status: 400 });
    }
    const prompt = buildPrompt(body);
    const response = await client.responses.create({
      model: "gpt-4o-mini",
      input: prompt,
      max_output_tokens: 200,
    });
    const answer = response.output_text ?? "No answer";
    return NextResponse.json({ answer });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : "server error";
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}

type ChatRequest = {
  question: string;
  template: string;
  summary?: any;
  table?: any;
};
