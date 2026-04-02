import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse } from "../lib/http";
import { probeRetrievalVectors } from "../services/retrieval-vector-probe";

export async function handleProbeRetrievalVectors(request: Request, env: Env): Promise<Response> {
  try {
    const payload = request.headers.get("content-type")?.includes("application/json") ? await readJson(request) : {};
    const report = await probeRetrievalVectors(env, payload);
    return json(report);
  } catch (error) {
    return toErrorResponse(error);
  }
}
