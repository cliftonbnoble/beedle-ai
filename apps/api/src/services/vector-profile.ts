import { z } from "zod";
import type { Env } from "../lib/types";
import { sha256Bytes } from "../lib/digests";
import { ServiceUnavailableError } from "../lib/http";

// Bump when extraction/chunk selection/query instruction changes. The concrete
// source parser is also retained per entry; the profile versions the pipeline.
export const VECTOR_PIPELINE_VERSION = "legal-text-2-bge-query-1";
export const CHUNKER_VERSION = "section-aware-640-200-900-v1";
export const RETRIEVAL_CHUNKER_VERSION = "retrieval-650-250-980-overlap1-repair1";
const configSchema=z.object({model:z.string().min(1).max(200),dimensions:z.coerce.number().int().min(1).max(4096),base:z.string().min(1).max(40).regex(/^[A-Za-z0-9_.-]+$/)});
export function embeddingDimensions(env:Env) {
  const value=Number(env.AI_EMBEDDING_DIMENSIONS ?? (env.AI_EMBEDDING_MODEL==='@cf/baai/bge-base-en-v1.5'?768:NaN));
  if(!Number.isSafeInteger(value)||value<1||value>4096) throw new ServiceUnavailableError("Embedding dimensions must be configured for this model");
  return value;
}
export async function vectorProfile(env:Env) {
  const parsed=configSchema.safeParse({model:env.AI_EMBEDDING_MODEL,dimensions:embeddingDimensions(env),base:env.VECTOR_NAMESPACE});
  if(!parsed.success) throw new ServiceUnavailableError("Vector profile configuration unavailable");
  const {model,dimensions,base}=parsed.data;
  const id=await sha256Bytes(new TextEncoder().encode(JSON.stringify([base,model,dimensions,VECTOR_PIPELINE_VERSION,CHUNKER_VERSION,RETRIEVAL_CHUNKER_VERSION])));
  return {id,namespace:`${base}-g${id.slice(0,16)}`,model,dimensions,pipelineVersion:`${VECTOR_PIPELINE_VERSION}:${CHUNKER_VERSION}:${RETRIEVAL_CHUNKER_VERSION}`,environment:env.APP_ENV||'local'};
}
export type VectorProfile = Awaited<ReturnType<typeof vectorProfile>>;
