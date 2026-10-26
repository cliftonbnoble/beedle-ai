import { prepareRetrievalActivation } from "../services/activation-plan";
import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse } from "../lib/http";
import { rollbackTrustedRetrievalActivation, writeTrustedRetrievalActivation } from "../services/retrieval-activation";

export async function handleWriteRetrievalActivation(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const report = await writeTrustedRetrievalActivation(env, payload);
    return json(report);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleRollbackRetrievalActivation(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const report = await rollbackTrustedRetrievalActivation(env, payload);
    return json(report);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handlePrepareRetrievalActivation(request: Request, env: Env): Promise<Response> {
  try { return json({plan: await prepareRetrievalActivation(env, await readJson(request))}); }
  catch (error) { return toErrorResponse(error); }
}
