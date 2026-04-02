import { fileTypeSchema, ingestDocumentSchema } from "@beedle/shared";
import { ingestDocument, approveDecision } from "../services/ingest";
import { readJson, json, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";

export async function handleIngest(request: Request, env: Env, forcedType?: "decision_docx" | "law_pdf"): Promise<Response> {
  try {
    const raw = (await readJson(request)) as Record<string, unknown>;
    const payload = ingestDocumentSchema.parse({
      ...raw,
      performVectorUpsert: raw.performVectorUpsert !== false,
      fileType: forcedType ? fileTypeSchema.parse(forcedType) : raw.fileType
    });
    const result = await ingestDocument(env, payload);
    return json(result, { status: 201 });
  } catch (error) {
    return toErrorResponse(error);
  }
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
  }
  return btoa(binary);
}

export async function handleIngestMultipart(
  request: Request,
  env: Env,
  forcedType?: "decision_docx" | "law_pdf"
): Promise<Response> {
  try {
    const formData = await request.formData();
    const file = formData.get("file");
    if (!(file instanceof File)) {
      return json({ error: "file is required" }, { status: 400 });
    }

    const filename = String(formData.get("filename") || file.name || "").trim() || file.name;
    const mimeType = String(formData.get("mimeType") || file.type || "").trim() || "application/octet-stream";
    const decisionDateRaw = String(formData.get("decisionDate") || "").trim();
    const payload = ingestDocumentSchema.parse({
      jurisdiction: String(formData.get("jurisdiction") || "").trim(),
      title: String(formData.get("title") || filename.replace(/\.[^.]+$/, "")).trim(),
      citation: String(formData.get("citation") || "").trim(),
      decisionDate: decisionDateRaw || undefined,
      performVectorUpsert: String(formData.get("performVectorUpsert") || "true").trim().toLowerCase() !== "false",
      fileType: forcedType ? fileTypeSchema.parse(forcedType) : formData.get("fileType"),
      sourceFile: {
        filename,
        mimeType,
        bytesBase64: bytesToBase64(new Uint8Array(await file.arrayBuffer()))
      }
    });

    const result = await ingestDocument(env, payload);
    return json(result, { status: 201 });
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleDecisionApproval(_request: Request, env: Env, documentId: string): Promise<Response> {
  try {
    const result = await approveDecision(env, documentId);
    return json(result, { status: result.approved ? 200 : 422 });
  } catch (error) {
    return toErrorResponse(error);
  }
}
