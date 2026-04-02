import { runDraftConclusions, runDraftConclusionsDebug } from "../services/draft-conclusions";
import { json, readJson, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";

export async function handleDraftConclusions(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await runDraftConclusions(env, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleDraftConclusionsDebug(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await runDraftConclusionsDebug(env, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}
