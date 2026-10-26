import { runAssistantChat } from "../services/assistant-chat";
import { json, readJson, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";

export async function handleAssistantChat(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await runAssistantChat(env, payload, { signal: request.signal });
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}
