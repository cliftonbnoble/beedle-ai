import { fileTypeSchema, ingestDocumentSchema, ingestDocumentMetadataSchema } from "@beedle/shared";
import { ingestDocument, ingestDocumentBytes, approveDecision } from "../services/ingest";
import { readLimitedFormData } from "../lib/multipart";
import { readJson, json, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";

const maxIngestUploadBytes = 15 * 1024 * 1024;
const maxMultipartEnvelopeBytes = maxIngestUploadBytes + 1024 * 1024;
// The JSON ingest body carries the source file as base64 (~4/3 the binary size) plus small
// metadata fields. Cap the envelope so an oversized body is rejected before request.json()
// loads it all into memory. Both JSON and multipart also enforce streamed limits.
const maxJsonIngestEnvelopeBytes = Math.ceil((maxIngestUploadBytes * 4) / 3) + 1024 * 1024;

export async function handleIngest(request: Request, env: Env, forcedType?: "decision_docx" | "law_pdf"): Promise<Response> {
  try {
    const contentLength = Number(request.headers.get("content-length") || "0");
    if (Number.isFinite(contentLength) && contentLength > maxJsonIngestEnvelopeBytes) {
      return json({ error: `Upload is too large. Maximum file size is ${maxIngestUploadBytes} bytes.` }, { status: 413 });
    }

    const raw = (await readJson(request, { maxBytes: maxJsonIngestEnvelopeBytes })) as Record<string, unknown>;
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

export async function handleIngestMultipart(
  request: Request,
  env: Env,
  forcedType?: "decision_docx" | "law_pdf"
): Promise<Response> {
  try {
    const contentLength = Number(request.headers.get("content-length") || "0");
    if (Number.isFinite(contentLength) && contentLength > maxMultipartEnvelopeBytes) {
      return json({ error: `Upload is too large. Maximum file size is ${maxIngestUploadBytes} bytes.` }, { status: 413 });
    }

    const formData = await readLimitedFormData(request, maxMultipartEnvelopeBytes);
    const file = formData.get("file");
    if (!(file instanceof File) || formData.getAll("file").length !== 1) {
      return json({ error: "file is required" }, { status: 400 });
    }
    if (file.size > maxIngestUploadBytes) {
      return json({ error: `Upload is too large. Maximum file size is ${maxIngestUploadBytes} bytes.` }, { status: 413 });
    }

    const fileBuffer = await file.arrayBuffer();
    if (fileBuffer.byteLength > maxIngestUploadBytes) {
      return json({ error: `Upload is too large. Maximum file size is ${maxIngestUploadBytes} bytes.` }, { status: 413 });
    }

    const filename = String(formData.get("filename") || file.name || "").trim() || file.name;
    const mimeType = String(formData.get("mimeType") || file.type || "").trim() || "application/octet-stream";
    const decisionDateRaw = String(formData.get("decisionDate") || "").trim();
    const payload = ingestDocumentMetadataSchema.parse({
      jurisdiction: String(formData.get("jurisdiction") || "").trim(),
      title: String(formData.get("title") || filename.replace(/\.[^.]+$/, "")).trim(),
      citation: String(formData.get("citation") || "").trim(),
      decisionDate: decisionDateRaw || undefined,
      performVectorUpsert: String(formData.get("performVectorUpsert") || "true").trim().toLowerCase() !== "false",
      fileType: forcedType ? fileTypeSchema.parse(forcedType) : formData.get("fileType"),
      sourceFile: {
        filename,
        mimeType
      }
    });

    const result = await ingestDocumentBytes(env, payload, new Uint8Array(fileBuffer));
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
