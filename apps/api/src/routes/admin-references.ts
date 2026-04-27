import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse } from "../lib/http";
import { backfillReferenceValidation, inspectLegalReferences, listRulesCitationInventory, rebuildLegalReferences, verifyCitations } from "../services/legal-references";

export async function handleInspectReferences(_request: Request, env: Env): Promise<Response> {
  try {
    const result = await inspectLegalReferences(env);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleRebuildReferences(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await rebuildLegalReferences(env, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleBackfillReferenceValidation(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request).catch(() => ({}));
    const limit = typeof (payload as { limit?: unknown }).limit === "number" ? Number((payload as { limit?: number }).limit) : 500;
    const offset = typeof (payload as { offset?: unknown }).offset === "number" ? Number((payload as { offset?: number }).offset) : 0;
    const result = await backfillReferenceValidation(env, limit, offset);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleVerifyReferenceCitations(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await verifyCitations(env, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleRulesCitationInventory(request: Request, env: Env): Promise<Response> {
  try {
    const url = new URL(request.url);
    const result = await listRulesCitationInventory(env, {
      citation: url.searchParams.get("citation") ?? undefined,
      normalized: url.searchParams.get("normalized") ?? undefined,
      bare: url.searchParams.get("bare") ?? undefined,
      prefix: url.searchParams.get("prefix") ?? undefined,
      limit: Number(url.searchParams.get("limit") ?? "100")
    });
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}
