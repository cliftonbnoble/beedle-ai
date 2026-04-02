import {
  assistantChatRequestSchema,
  assistantChatResponseSchema,
  type AssistantChatResponse,
  type SearchResponse
} from "@beedle/shared";
import type { Env } from "../lib/types";
import { search } from "./search";

type AssistantDecision = Pick<
  SearchResponse["results"][number],
  | "documentId"
  | "title"
  | "citation"
  | "authorName"
  | "sourceLink"
  | "primaryAuthorityPassage"
  | "supportingFactPassage"
  | "matchedPassage"
  | "snippet"
  | "score"
>;

function compactWhitespace(value: string): string {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function assistantScopeLabel(indexCodes: string[]): string {
  if (indexCodes.length === 0) return "All decisions";
  if (indexCodes.length === 1) return `Index code ${indexCodes[0] || ""}`;
  return `${indexCodes.length} index codes`;
}

function latestUserQuestion(messages: Array<{ role: "user" | "assistant"; content: string }>): string {
  const latest = [...messages].reverse().find((message) => message.role === "user");
  return compactWhitespace(latest?.content || "");
}

function recentConversation(messages: Array<{ role: "user" | "assistant"; content: string }>) {
  return messages
    .slice(-8)
    .map((message) => ({
      role: message.role,
      content: compactWhitespace(message.content)
    }))
    .filter((message) => message.content.length > 0);
}

function groupTopDecisions(results: SearchResponse["results"], limit: number): AssistantDecision[] {
  const byDocument = new Map<string, AssistantDecision>();
  for (const row of results) {
    const existing = byDocument.get(row.documentId);
    if (!existing || row.score > existing.score) {
      byDocument.set(row.documentId, row);
    }
  }

  return Array.from(byDocument.values())
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
}

async function retrieveDecisions(
  env: Env,
  question: string,
  indexCodes: string[],
  limit: number
): Promise<AssistantDecision[]> {
  const response = await search(env, {
    query: question,
    limit: Math.max(24, limit * 4),
    snippetMaxLength: 360,
    corpusMode: "trusted_plus_provisional",
    filters: {
      fileType: "decision_docx",
      approvedOnly: true,
      indexCodes: indexCodes.length > 0 ? indexCodes : undefined
    }
  });

  return groupTopDecisions(response.results, limit);
}

function decisionContextBlock(decision: AssistantDecision, index: number): string {
  const primary = compactWhitespace(
    decision.primaryAuthorityPassage?.snippet || decision.matchedPassage?.snippet || decision.snippet || ""
  );
  const findings = compactWhitespace(
    decision.supportingFactPassage?.snippet || decision.matchedPassage?.snippet || decision.snippet || ""
  );

  return [
    `Decision ${index + 1}`,
    `Title: ${decision.title}`,
    `Citation: ${decision.citation}`,
    `Judge: ${decision.authorName || "Unknown"}`,
    `Conclusions of Law: ${primary || "None surfaced."}`,
    `Findings of Fact: ${findings || "None surfaced."}`
  ].join("\n");
}

function extractAssistantContent(payload: any): string {
  const choice = payload?.choices?.[0];
  const content = choice?.message?.content;
  if (typeof content === "string") return compactWhitespace(content);
  if (Array.isArray(content)) {
    return compactWhitespace(
      content
        .map((item) => {
          if (typeof item === "string") return item;
          if (typeof item?.text === "string") return item.text;
          return "";
        })
        .join("\n")
    );
  }
  return "";
}

async function callLlm(params: {
  env: Env;
  scopeLabel: string;
  messages: Array<{ role: "user" | "assistant"; content: string }>;
  decisions: AssistantDecision[];
}): Promise<{ answer: string; model: string }> {
  const { env, scopeLabel, messages, decisions } = params;
  if (!env.LLM_API_KEY) {
    throw new Error(
      "LLM_API_KEY is not configured. Add it to apps/api/.dev.vars, then restart the API."
    );
  }

  const model = env.LLM_MODEL || "gpt-4.1-mini";
  const baseUrl = (env.LLM_BASE_URL || "https://api.openai.com/v1").replace(/\/+$/, "");
  const question = latestUserQuestion(messages);
  const conversation = recentConversation(messages);

  const systemPrompt = [
    "You are Beedle, a grounded judicial research assistant for a judge.",
    "Answer in clear, plain English.",
    "Use only the retrieved decisions provided in the context below.",
    "If the retrieved decisions are not enough to support a firm answer, say that plainly.",
    "Start with a direct answer to the current question.",
    "Then explain briefly using the cited decisions.",
    "Do not invent holdings, counts, or percentages that are not supported by the retrieved excerpts.",
    `Current retrieval scope: ${scopeLabel}.`
  ].join(" ");

  const contextBlock = [
    `Current question: ${question}`,
    "",
    "Retrieved decisions:",
    decisions.length > 0 ? decisions.map(decisionContextBlock).join("\n\n") : "No decisions were retrieved for this question."
  ].join("\n");

  const response = await fetch(`${baseUrl}/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${env.LLM_API_KEY}`
    },
    body: JSON.stringify({
      model,
      temperature: 0.2,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "system", content: contextBlock },
        ...conversation.map((message) => ({
          role: message.role,
          content: message.content
        }))
      ]
    })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`LLM request failed (${response.status}): ${text}`);
  }

  const payload = await response.json();
  const answer = extractAssistantContent(payload);
  if (!answer) {
    throw new Error("LLM response did not include an answer.");
  }

  return { answer, model };
}

export async function runAssistantChat(env: Env, input: unknown): Promise<AssistantChatResponse> {
  const parsed = assistantChatRequestSchema.parse(input);
  const question = latestUserQuestion(parsed.messages);
  if (!question) {
    throw new Error("A user question is required.");
  }

  const decisions = await retrieveDecisions(env, question, parsed.indexCodes, parsed.limit);
  const llm = await callLlm({
    env,
    scopeLabel: assistantScopeLabel(parsed.indexCodes),
    messages: parsed.messages,
    decisions
  });

  return assistantChatResponseSchema.parse({
    answer: llm.answer,
    citations: decisions.map((decision) => ({
      documentId: decision.documentId,
      title: decision.title,
      citation: decision.citation,
      authorName: decision.authorName,
      sourceLink: decision.sourceLink,
      primaryAuthorityPassage: decision.primaryAuthorityPassage,
      supportingFactPassage: decision.supportingFactPassage
    })),
    scopeLabel: assistantScopeLabel(parsed.indexCodes),
    model: llm.model,
    retrievedCount: decisions.length
  });
}
