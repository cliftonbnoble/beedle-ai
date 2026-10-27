import { vectorProfile } from "./vector-profile";
import type { Env } from "../lib/types";
import { RequestConflictError, RequestValidationError } from "../lib/http";
import { activationPrepareSchema, activationPlanSchema, canonicalJson, sha256, type ActivationPlan, type ActivationChunk } from "./activation-contract";
import { trustedDocumentSql } from "./publication-policy";
import { assertDocumentRevision } from "./document-revision";
import { getDecisionRetrievalPreview } from "./retrieval-foundation";
import { effectiveSourceLink } from "./storage";
import { sha256Bytes } from "../lib/digests";
import { isLikelyFixtureName } from "./qc-shared";
type SourceDocument = {
  documentId: string;
  documentRevision: number;
  retrievalRevision: number;
  textGeneration: string;
  sourceFileRef: string;
  title: string;
  citation: string;
  sourceLink: string;
  activeBatchId: string | null;
  metadataJson: string;
};
export function requireUnique(values: string[], label: string) {
  if (new Set(values).size !== values.length)
    throw new RequestValidationError(`Duplicate ${label} are not allowed.`);
}
export async function sourceDigest(env: Env, sourceKey: string) {
  const object = await env.SOURCE_BUCKET.get(sourceKey);
  if (!object || object.size > 15 * 1024 * 1024)
    throw new RequestValidationError("Activation source is missing or exceeds the verified intake limit.");
  const bytes = new Uint8Array(await object.arrayBuffer());
  if (bytes.length > 15 * 1024 * 1024)
    throw new RequestValidationError("Activation source exceeds the verified intake limit.");
  return sha256Bytes(bytes);
}
export async function signPlan(plan: Omit<ActivationPlan, "digest" | "rollbackDigest">): Promise<ActivationPlan> {
  const rollbackDigest = await sha256({ documents: plan.documents, rollback: plan.rollback });
  return { ...plan, rollbackDigest, digest: await sha256({ ...plan, rollbackDigest }) };
}
export async function verifyPlanDigest(plan: ActivationPlan) {
  requireUnique(plan.documents.map(row => row.documentId), "document IDs");
  requireUnique(plan.chunks.map(row => row.chunkId), "chunk IDs");
  requireUnique(plan.rollback.documents.map(row => row.documentId), "rollback document IDs");
  const { digest, rollbackDigest, ...unsigned } = plan;
  const expected = await signPlan(unsigned);
  if (expected.digest !== digest || expected.rollbackDigest !== rollbackDigest)
    throw new RequestValidationError("Activation or rollback SHA-256 digest does not match its payload.");
}
// Read-only: old report labels cannot confer trust. The caller chooses documents
// and optional preview chunks; every returned field comes from persisted sources.
export async function prepareRetrievalActivation(env: Env, raw: unknown): Promise<ActivationPlan> {
  const input = activationPrepareSchema.parse(raw);
  requireUnique(input.documentIds, "document IDs");
  if (input.selectedChunks)
    requireUnique(input.selectedChunks.map(row => `${row.documentId}:${row.sourceChunkId}`), "selected chunks");
  const documents: ActivationPlan["documents"] = [];
  const rollback: ActivationPlan["rollback"] = { batchId: input.batchId, documents: [] };
  const chunks: ActivationChunk[] = [];
  const selection = input.selectedChunks ? new Set(input.selectedChunks.map(row => `${row.documentId}:${row.sourceChunkId}`)) : null;
  const matched = new Set<string>();
  for (const documentId of [...input.documentIds].sort()) {
    const row = await env.DB.prepare(`SELECT d.id AS documentId, d.document_revision AS documentRevision,
      d.retrieval_revision AS retrievalRevision, d.active_text_generation AS textGeneration, d.active_retrieval_batch AS activeBatchId,
      d.source_r2_key AS sourceFileRef, d.title, d.citation, d.source_link AS sourceLink, d.metadata_json AS metadataJson
      FROM documents d WHERE d.id=? AND d.file_type='decision_docx' AND ${trustedDocumentSql()}`)
      .bind(documentId).first<SourceDocument>();
    if (!row)
      throw new RequestValidationError(`Document ${documentId} is not a completed, published, approved decision under the current policy.`);
    if (isLikelyFixtureName(`${row.title} ${row.citation} ${row.sourceFileRef}`))
      throw new RequestValidationError("Fixture documents cannot be activated.");
    const { activeBatchId, metadataJson, ...document } = row;
    const sourceSha256 = await sourceDigest(env, row.sourceFileRef);
    const capturedHash = row.textGeneration === "initial" ? JSON.parse(metadataJson || "{}").sourceSha256 :
      (await env.DB.prepare("SELECT source_sha256 FROM document_generations WHERE id=? AND document_id=?").bind(row.textGeneration, documentId).first<{
        source_sha256: string;
      }>())?.source_sha256;
    if (!capturedHash || !JSON.parse(metadataJson || "{}").sourceSha256)
      throw new RequestValidationError("Source has no captured digest. Reprocess and review this document before activation.");
    if (capturedHash !== sourceSha256 || JSON.parse(metadataJson || "{}").sourceSha256 !== sourceSha256)
      throw new RequestValidationError("Source digest differs from the captured source; activation was stopped.");
    documents.push({ ...document, sourceLink: effectiveSourceLink(env, documentId, row.sourceLink), sourceSha256 });
    const previous = (await env.DB.prepare("SELECT chunk_id, batch_id FROM retrieval_search_chunks WHERE document_id=? AND active=1 ORDER BY chunk_id").bind(documentId)
      .all<{
        chunk_id: string;
        batch_id: string;
      }>()).results || [];
    if (previous.some(item => item.batch_id !== activeBatchId))
      throw new RequestConflictError("Legacy or inconsistent active retrieval rows require explicit reconciliation before preparing a new activation.");
    if (activeBatchId) {
      const oldPlan = await env.DB.prepare("SELECT state, plan_json FROM retrieval_activation_plans WHERE batch_id=?").bind(activeBatchId).first<{
        state: string;
        plan_json: string;
      }>();
      if (oldPlan?.state !== "published")
        throw new RequestConflictError("Previous activation has no verified published plan.");
      const previousPlan = activationPlanSchema.parse(JSON.parse(oldPlan.plan_json));
      await verifyPlanDigest(previousPlan);
      const expectedIds = previousPlan.chunks.filter(chunk => chunk.documentId === documentId).map(chunk => chunk.chunkId).sort();
      if (canonicalJson(expectedIds) !== canonicalJson(previous.map(item => item.chunk_id)))
        throw new RequestConflictError("Previous activation projection is incomplete; reconcile it before preparing a replacement.");
      const previousDocument = previousPlan.documents.find(item => item.documentId === documentId);
      if (!previousDocument || previousDocument.documentRevision !== row.documentRevision || previousDocument.textGeneration !== row.textGeneration || previousDocument.sourceFileRef !== row.sourceFileRef || previousDocument.sourceSha256 !== sourceSha256) {
        throw new RequestConflictError("Previous activation belongs to an older document revision and cannot be used as a rollback target.");
      }
    }
    rollback.documents.push({ documentId, previousBatchId: activeBatchId, previousChunkIds: previous.map(item => item.chunk_id) });
    const preview = await getDecisionRetrievalPreview(env, documentId, { includeText: true });
    if (!preview)
      throw new RequestConflictError("Document publication changed while preparing activation.");
    let selected = 0;
    for (const chunk of preview.chunks) {
      const key = `${documentId}:${chunk.chunkId}`;
      if (selection && !selection.has(key))
        continue;
      matched.add(key);
      selected++;
      const content = {
        documentId, sourceChunkId: chunk.chunkId, sourceText: chunk.sourceText, sectionLabel: chunk.sectionLabel,
        chunkType: chunk.chunkType, retrievalPriority: chunk.retrievalPriority, paragraphAnchorStart: chunk.paragraphAnchorStart,
        paragraphAnchorEnd: chunk.paragraphAnchorEnd, citationAnchorStart: chunk.citationAnchorStart, citationAnchorEnd: chunk.citationAnchorEnd,
        hasCanonicalReferenceAlignment: chunk.hasCanonicalReferenceAlignment
      };
      // 63 ASCII bytes fits Vectorize's 64-byte ID limit. Batch-specific IDs keep
      // pending upserts from overwriting any currently visible generation.
      const chunkId = `ar_${(await sha256({ batchId: input.batchId, namespace: env.VECTOR_NAMESPACE, revision: row.documentRevision, content })).slice(0, 60)}`;
      chunks.push({ ...content, chunkId });
    }
    if (!selected)
      throw new RequestValidationError(`No selected source chunks belong to document ${documentId}.`);
    await assertDocumentRevision(env, documentId, row.documentRevision);
    const activation = await env.DB.prepare("SELECT retrieval_revision FROM documents WHERE id=?").bind(documentId).first<{
      retrieval_revision: number;
    }>();
    if (activation?.retrieval_revision !== row.retrievalRevision)
      throw new RequestConflictError("Retrieval changed while preparing activation.");
  }
  if (selection && matched.size !== selection.size)
    throw new RequestValidationError("Selected chunks do not match the current persisted document revisions.");
  chunks.sort((a, b) => a.chunkId < b.chunkId ? -1 : a.chunkId > b.chunkId ? 1 : 0);
  const plan = await signPlan({ schemaVersion: 2, batchId: input.batchId, performVectorUpsert: input.performVectorUpsert, targetNamespace: env.VECTOR_NAMESPACE, ...(input.performVectorUpsert?{embeddingProfile:await vectorProfile(env)}:{}), documents, chunks, rollback });
  if (new TextEncoder().encode(canonicalJson(plan)).length > 900000)
    throw new RequestValidationError("Activation plan exceeds the request/storage budget; prepare a smaller document or chunk set.");
  return activationPlanSchema.parse(plan);
}
export async function verifyCurrentActivationPlan(env: Env, plan: ActivationPlan) {
  await verifyPlanDigest(plan);
  const expected = await prepareRetrievalActivation(env, {
    schemaVersion: 2, batchId: plan.batchId, documentIds: plan.documents.map(row => row.documentId),
    selectedChunks: plan.chunks.map(row => ({ documentId: row.documentId, sourceChunkId: row.sourceChunkId })), performVectorUpsert: plan.performVectorUpsert
  });
  if (canonicalJson(expected) !== canonicalJson(plan))
    throw new RequestConflictError("Activation payload, source revision, or rollback target differs from the current verified plan. Prepare and review it again.");
}
