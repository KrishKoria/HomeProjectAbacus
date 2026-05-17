import { requireAuthorizedSession } from "@/lib/auth-session";
import { databricksFetch } from "@/lib/databricks/client";
import { env } from "@/lib/server/env";
import { z } from "zod";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const messageSchema = z.object({
  role: z.enum(["user", "assistant"]),
  content: z.string().min(1).max(2000),
});

const bodySchema = z.object({
  messages: z.array(messageSchema).min(1).max(12),
  claimContext: z.object({
    claimId: z.string(),
    riskScore: z.number(),
    riskLevel: z.string(),
    narrative: z.string().optional(),
    topReasons: z.array(z.object({
      description: z.string(),
      direction: z.string(),
    })).optional(),
  }),
});

interface LLMResponse {
  choices: Array<{
    message: { content: string };
  }>;
}

export async function POST(
  request: Request,
  { params }: { params: Promise<{ claimId: string }> },
) {
  try {
    await requireAuthorizedSession();
  } catch {
    return Response.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { claimId } = await params;

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const parsed = bodySchema.safeParse(body);
  if (!parsed.success) {
    return Response.json({ error: "Invalid request" }, { status: 400 });
  }

  const { messages, claimContext } = parsed.data;

  if (claimContext.claimId !== claimId) {
    return Response.json({ error: "Claim ID mismatch" }, { status: 400 });
  }

  const scorePercent = Math.round(claimContext.riskScore * 100);
  const reasonsSummary = claimContext.topReasons?.length
    ? claimContext.topReasons
        .slice(0, 5)
        .map((r) => `- ${r.description} (${r.direction === "increases_risk" ? "raises" : "lowers"} risk)`)
        .join("\n")
    : "No detailed findings available.";

  const systemPrompt = [
    "You are a medical billing assistant helping a billing analyst understand a specific claim's denial risk.",
    "Answer questions directly and concisely. Use plain language — no jargon, no policy citations inline.",
    "Do not mention patient names, dates of birth, or any protected health information.",
    "Focus on actionable guidance: what to check, what to fix, what policy to reference.",
    "",
    `CLAIM: ${claimId}`,
    `RISK SCORE: ${scorePercent}% (${claimContext.riskLevel} risk)`,
    "",
    "KEY RISK FACTORS:",
    reasonsSummary,
    claimContext.narrative ? `\nANALYSIS SUMMARY:\n${claimContext.narrative}` : "",
  ]
    .filter(Boolean)
    .join("\n");

  const result = await databricksFetch<LLMResponse>(
    `/serving-endpoints/${env.CLAIMOPS_CHAT_MODEL}/invocations`,
    {
      method: "POST",
      body: JSON.stringify({
        messages: [
          { role: "system", content: systemPrompt },
          ...messages,
        ],
        temperature: 0.2,
        max_tokens: 400,
      }),
    },
  );

  if (!result.ok) {
    console.error("[chat] LLM call failed:", result.message);
    return Response.json({ error: "Chat unavailable" }, { status: 502 });
  }

  const reply = result.data.choices[0]?.message?.content ?? "";
  return Response.json({ reply });
}
