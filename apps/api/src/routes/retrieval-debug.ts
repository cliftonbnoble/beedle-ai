import { searchDebug } from "../services/search";
import { getDecisionRetrievalPreview, getDecisionRetrievalRawDebug } from "../services/retrieval-foundation";
import { json, readJson, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";

export async function handleRetrievalDebug(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await searchDebug(env, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleDecisionRetrievalChunks(request: Request, env: Env, documentId: string): Promise<Response> {
  try {
    const url = new URL(request.url);
    const includeTextRaw = (url.searchParams.get("includeText") || "1").toLowerCase();
    const includeText = !["0", "false", "no"].includes(includeTextRaw);
    const result = await getDecisionRetrievalPreview(env, documentId, { includeText });
    if (!result) return json({ error: "Document not found" }, { status: 404 });
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleDecisionRetrievalChunksDebug(request: Request, env: Env, documentId: string): Promise<Response> {
  try {
    const url = new URL(request.url);
    const includeTextRaw = (url.searchParams.get("includeText") || "1").toLowerCase();
    const includeText = !["0", "false", "no"].includes(includeTextRaw);
    const maxParagraphRows = Number(url.searchParams.get("maxParagraphRows") || "40");
    const result = await getDecisionRetrievalRawDebug(env, documentId, { includeText, maxParagraphRows });
    if (!result) return json({ error: "Document not found" }, { status: 404 });
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}
