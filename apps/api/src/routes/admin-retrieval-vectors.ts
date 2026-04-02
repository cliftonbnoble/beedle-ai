import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse } from "../lib/http";
import { backfillRetrievalVectors } from "../services/retrieval-vector-backfill";

export async function handleBackfillRetrievalVectors(request: Request, env: Env): Promise<Response> {
  try {
    const payload = request.headers.get("content-type")?.includes("application/json") ? await readJson(request) : {};
    const report = await backfillRetrievalVectors(env, payload);
    return json(report);
  } catch (error) {
    return toErrorResponse(error);
  }
}
