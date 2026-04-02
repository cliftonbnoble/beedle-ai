import { runCaseAssistant } from "../services/case-assistant";
import { readJson, json, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";

export async function handleCaseAssistant(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await runCaseAssistant(env, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}
