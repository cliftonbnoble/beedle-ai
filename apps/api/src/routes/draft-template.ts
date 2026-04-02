import { json, readJson, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";
import { runDraftTemplate } from "../services/draft-template";

export async function handleDraftTemplate(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await runDraftTemplate(env, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}
