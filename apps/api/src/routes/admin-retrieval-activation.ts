import { createActivationJob } from "../services/maintenance-jobs";
import { prepareRetrievalActivation } from "../services/activation-plan";
import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse } from "../lib/http";
import { rollbackTrustedRetrievalActivation } from "../services/retrieval-activation";

export async function handleWriteRetrievalActivation(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const report = await createActivationJob(env, payload);
    return json(report, {status:"jobId" in report ? 202 : 200});
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
