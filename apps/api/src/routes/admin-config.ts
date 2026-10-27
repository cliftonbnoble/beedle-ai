import { taxonomyResolveRequestSchema, taxonomyResolveResponseSchema, taxonomyValidateResponseSchema } from "@beedle/shared";
import { json, readJson, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";
import { inspectActiveTaxonomyConfig, resolveCaseTypeTemplate, validateTaxonomyConfig } from "../services/template-config";

export async function handleGetTaxonomyConfig(_request: Request, _env: Env): Promise<Response> {
  return json(inspectActiveTaxonomyConfig());
}

export async function handleResolveTaxonomyCaseType(request: Request, _env: Env): Promise<Response> {
  try {
    const payload = taxonomyResolveRequestSchema.parse(await readJson(request));
    const { resolution } = resolveCaseTypeTemplate(payload.case_type);
    return json(taxonomyResolveResponseSchema.parse(resolution));
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleValidateTaxonomyConfig(request: Request, _env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const config = validateTaxonomyConfig(payload);
    return json(
      taxonomyValidateResponseSchema.parse({
        ok: true,
        config
      })
    );
  } catch (error) {
    const response=toErrorResponse(error);const body=await response.json() as {error:string};
    return json({ok:false,error:body.error},{status:response.status,headers:response.headers});
  }
}
