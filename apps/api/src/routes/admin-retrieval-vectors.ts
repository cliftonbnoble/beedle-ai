import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse } from "../lib/http";
import { createBackfillJob } from "../services/maintenance-jobs";

export async function handleBackfillRetrievalVectors(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const report = await createBackfillJob(env, "vector_backfill", payload);
    return json(report, {status:202});
  } catch (error) {
    return toErrorResponse(error);
  }
}
