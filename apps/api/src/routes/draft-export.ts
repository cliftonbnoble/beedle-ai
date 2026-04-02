import { json, readJson, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";
import { exportDraft } from "../services/draft-export";

export async function handleDraftExport(request: Request, _env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await exportDraft(payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}
