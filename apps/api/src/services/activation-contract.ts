import { z } from "zod";
const identifier = z.string().min(1).max(128).regex(/^[A-Za-z0-9_.:-]+$/);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const selection = z.object({ documentId: identifier, sourceChunkId: identifier }).strict();
export const activationPrepareSchema = z.object({
  schemaVersion: z.literal(2),
  batchId: identifier,
  documentIds: z.array(identifier).min(1).max(5),
  selectedChunks: z.array(selection).min(1).max(128).optional(),
  performVectorUpsert: z.boolean().default(true)
}).strict();
const document = z.object({
  documentId: identifier,
  documentRevision: z.number().int().positive(),
  retrievalRevision: z.number().int().nonnegative(),
  textGeneration: identifier,
  sourceFileRef: z.string().min(1).max(1024),
  sourceSha256: digest,
  title: z.string().min(1).max(2000),
  citation: z.string().min(1).max(500),
  sourceLink: z.string().url().max(2048)
}).strict();
export const activationChunkSchema = z.object({
  chunkId: identifier.max(64),
  sourceChunkId: identifier,
  documentId: identifier,
  sourceText: z.string().min(1).max(20000),
  sectionLabel: z.string().min(1).max(1000),
  chunkType: z.string().min(1).max(100),
  retrievalPriority: z.enum(["high", "medium", "low"]),
  paragraphAnchorStart: z.string().min(1).max(500),
  paragraphAnchorEnd: z.string().min(1).max(500),
  citationAnchorStart: z.string().min(1).max(1000),
  citationAnchorEnd: z.string().min(1).max(1000),
  hasCanonicalReferenceAlignment: z.boolean()
}).strict();
const rollback = z.object({
  batchId: identifier,
  documents: z.array(z.object({
    documentId: identifier,
    previousBatchId: identifier.nullable(),
    previousChunkIds: z.array(identifier).max(128)
  }).strict()).min(1).max(5)
}).strict();
export const activationPlanSchema = z.object({
  schemaVersion: z.literal(2),
  batchId: identifier,
  performVectorUpsert: z.boolean(),
  targetNamespace: z.string().min(1).max(64),
  documents: z.array(document).min(1).max(5),
  chunks: z.array(activationChunkSchema).min(1).max(128),
  rollback,
  digest,
  rollbackDigest: digest
}).strict();
export const activationWriteSchema = z.object({ plan: activationPlanSchema, dryRun: z.boolean().default(false) }).strict();
export const activationRollbackSchema = z.object({
  schemaVersion: z.literal(2), batchId: identifier,
  digest, rollbackDigest: digest, dryRun: z.boolean().default(false)
}).strict();
export type ActivationPlan = z.infer<typeof activationPlanSchema>;
export type ActivationChunk = z.infer<typeof activationChunkSchema>;
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value))
    return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object")
    return `{${Object.entries(value).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(",")}}`;
  return JSON.stringify(value);
}
export async function sha256(value: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(canonicalJson(value));
  return Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes)), byte => byte.toString(16).padStart(2, "0")).join("");
}
