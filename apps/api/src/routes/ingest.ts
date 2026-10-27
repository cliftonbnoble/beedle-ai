import {z} from "zod";
import { ingestionKeySchema } from "@beedle/shared";
import { IngestionInProgress, ingestionStatus, readIngestionStatus, ingestionRequestId } from "../services/ingestion-requests";
import { documentApprovalSchema } from "@beedle/shared";
import { fileTypeSchema, ingestDocumentSchema, ingestDocumentMetadataSchema } from "@beedle/shared";
import { ingestDocument, ingestDocumentBytes, approveDecision } from "../services/ingest";
import { readLimitedFormData } from "../lib/multipart";
import { readJson, json, toErrorResponse, RequestValidationError } from "../lib/http";
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

    const raw = z.record(z.unknown()).parse(await readJson(request, { maxBytes: maxJsonIngestEnvelopeBytes }));
    if(forcedType&&raw.fileType!==undefined&&raw.fileType!==forcedType)throw new RequestValidationError("fileType does not match this ingestion endpoint");
    const payload = ingestDocumentSchema.parse({
      ...raw,
      idempotencyKey:requestKey(request,raw.idempotencyKey),

      fileType: forcedType ? fileTypeSchema.parse(forcedType) : raw.fileType
    });
    const result = await ingestDocument(env, payload);
    return json(result, { status: 201 });
  } catch (error) {
    if(error instanceof IngestionInProgress)return json(ingestionStatus(error.receipt),{status:202,headers:{"retry-after":"3","location":`/ingest/requests/${error.receipt.request_id}`}});
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

    const allowed=new Set(['file','filename','mimeType','decisionDate','jurisdiction','title','citation','performVectorUpsert','fileType','idempotencyKey','revisionOf']);
    formData.forEach((_value,name)=>{if(!allowed.has(name)||formData.getAll(name).length!==1)throw new RequestValidationError("Unknown or repeated multipart field");});
    const field=(name:string)=>{const value=formData.get(name);if(value!==null&&typeof value!=="string")throw new RequestValidationError(`${name} must be text`);return value?.trim();};
    const filename = field("filename")??file.name;
    const mimeType = field("mimeType")??(file.type||"application/octet-stream");
    const decisionDateRaw = field("decisionDate")??"";
    if(forcedType&&field("fileType")!==undefined&&field("fileType")!==forcedType)throw new RequestValidationError("fileType does not match this ingestion endpoint");
    const payload = ingestDocumentMetadataSchema.parse({
      idempotencyKey:requestKey(request,field("idempotencyKey")),
      ...(field("revisionOf")?{revisionOf:parseRevision(field("revisionOf")!)}:{}),
      jurisdiction: field("jurisdiction")??"",
      title: field("title")??filename.replace(/\.[^.]+$/, ""),
      citation: field("citation")??"",
      decisionDate: decisionDateRaw || undefined,
      performVectorUpsert: z.enum(["true","false"]).parse((field("performVectorUpsert")??"true").toLowerCase())==="true",
      fileType: forcedType ? fileTypeSchema.parse(forcedType) : field("fileType"),
      sourceFile: {
        filename,
        mimeType
      }
    });

    const fileBuffer = await file.arrayBuffer();
    if (fileBuffer.byteLength > maxIngestUploadBytes) {
      return json({ error: `Upload is too large. Maximum file size is ${maxIngestUploadBytes} bytes.` }, { status: 413 });
    }

    const result = await ingestDocumentBytes(env, payload, new Uint8Array(fileBuffer));
    return json(result, { status: 201 });
  } catch (error) {
    if(error instanceof IngestionInProgress)return json(ingestionStatus(error.receipt),{status:202,headers:{"retry-after":"3","location":`/ingest/requests/${error.receipt.request_id}`}});
    return toErrorResponse(error);
  }
}

export async function handleDecisionApproval(request: Request, env: Env, documentId: string): Promise<Response> {
  try {
    const { expectedRevision } = documentApprovalSchema.parse(await readJson(request));
    const result = await approveDecision(env, documentId, expectedRevision);
    return json(result, { status: result.approved ? 200 : result.conflict ? 409 : 422 });
  } catch (error) {
    if(error instanceof IngestionInProgress)return json(ingestionStatus(error.receipt),{status:202,headers:{"retry-after":"3","location":`/ingest/requests/${error.receipt.request_id}`}});
    return toErrorResponse(error);
  }
}

export async function handleIngestionStatus(_request:Request,env:Env,requestId:string){
 try{if(ingestionKeySchema.safeParse(requestId).success){const operation=fileTypeSchema.parse(new URL(_request.url).searchParams.get("operation"));requestId=await ingestionRequestId(operation,requestId);}
 const status=await readIngestionStatus(env,requestId);return status?json(status):json({error:"Ingestion request not found"},{status:404});}
 catch(error){return toErrorResponse(error);}
}

function parseRevision(value:string):unknown{try{return JSON.parse(value);}catch{throw new RequestValidationError("revisionOf must contain valid JSON");}}

function requestKey(request:Request,body:unknown){
 const header=request.headers.get('idempotency-key');
 if(header!==null&&body!==undefined&&ingestionKeySchema.parse(header)!==ingestionKeySchema.parse(body))throw new RequestValidationError('Header and body idempotency keys must match');
 return ingestionKeySchema.parse(header??body);
}
