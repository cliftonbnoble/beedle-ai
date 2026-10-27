import { searchDebug } from "../services/search";
import { getDecisionRetrievalPreview } from "../services/retrieval-foundation";
import { searchCorpusRevision } from "../services/search-sessions";
import { json, readJson, toErrorResponse, RequestConflictError } from "../lib/http";
import type { Env } from "../lib/types";

export async function handleRetrievalDebug(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await searchDebug(env, payload, {session:true});
    return json(result,{headers:{"cache-control":"private, no-store"}});
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleDecisionRetrievalChunks(request: Request, env: Env, documentId: string, inspection = false): Promise<Response> {
  try {
    const url = new URL(request.url);
    const includeTextRaw = (url.searchParams.get("includeText") || "1").toLowerCase();
    const includeText = !["0", "false", "no"].includes(includeTextRaw);
    const revision = await searchCorpusRevision(env);
    const result = await getDecisionRetrievalPreview(env, documentId, { includeText, inspection });
    if (!result) return json({ error: "Document not found" }, { status: 404 });
    if (await searchCorpusRevision(env) !== revision) throw new RequestConflictError("The corpus changed while loading this decision. Reload the decision.");
    return json({ ...result, corpusRevision: String(revision) }, { headers: { "cache-control": "private, no-store" } });
  } catch (error) {
    return toErrorResponse(error);
  }
}
