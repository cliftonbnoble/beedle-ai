import { createBackfillJob } from "../services/maintenance-jobs";
import { listLegalCatalogVersions, restoreLegalCatalog } from "../services/legal-catalog";
import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse } from "../lib/http";
import { inspectLegalReferences, listRulesCitationInventory, rebuildLegalReferences, verifyCitations } from "../services/legal-references";

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
    const result = await createBackfillJob(env, "reference_backfill", await readJson(request));
    return json(result, {status:202});
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
      limit: Number.isFinite(Number(url.searchParams.get("limit") ?? "100")) ? Number(url.searchParams.get("limit") ?? "100") : 100
    });
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleListCatalogVersions(_request: Request, env: Env): Promise<Response> {
  try {return json(await listLegalCatalogVersions(env));} catch(error){return toErrorResponse(error);}
}
export async function handleRestoreCatalog(request: Request, env: Env): Promise<Response> {
  try {return json(await restoreLegalCatalog(env,await readJson(request)));} catch(error){return toErrorResponse(error);}
}
