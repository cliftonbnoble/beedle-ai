import { readVectorManifest, vectorOutboxStatement } from "./vector-job-manifest";
import { sha256Bytes } from "../lib/digests";
import type { AuthoredSection, Env } from "../lib/types";
import { RequestConflictError, RequestValidationError, RequestNotFoundError } from "../lib/http";
import { buildDocumentTextArtifactStatements, executeTextArtifactStatementBatches } from "./ingest";
import { buildDocumentReferenceValidationStatements, type ReferenceValidationPlan } from "./legal-references";
import { generationTables } from "./generation-tables";
import { withDocumentRevision } from "./document-revision";
import { storeSourceFile } from "./storage";
import type { SourceFormat } from "./source-format";
import { enqueueVectorJob } from "./vector-jobs";

type Snapshot = Record<string, string | number | null>;
type Table = keyof typeof generationTables;
type Manifest = Record<Table, { count: number; sha256: string }>;
type Generation = {
  id: string; document_id: string; base_revision: number; status: string; source_key: string;
  source_sha256: string; manifest_version: number; snapshot_json: string; manifest_json: string
};
const tables = Object.keys(generationTables) as Table[];
const restoredFields = ["file_type", "jurisdiction", "title", "citation", "case_number", "decision_date", "author_name", "outcome_label",
  "index_codes_json", "rules_sections_json", "ordinance_sections_json", "extraction_confidence", "extraction_warnings_json",
  "metadata_json", "reviewer_overrides_json", "reference_catalog_version", "reference_edition_context_json", "qc_has_index_codes", "qc_has_rules_section", "qc_has_ordinance_section", "qc_passed"];

export { sha256Bytes } from "../lib/digests";
function whereDocument(table: Table) {
  return table === "section_paragraphs" ? "section_id IN (SELECT id FROM document_sections WHERE document_id = ?)" : "document_id = ?";
}
async function manifest(env: Env, documentId: string, generationId?: string, manifestVersion = 3): Promise<Manifest> {
  const result = {} as Manifest;
  for (const table of tables) {
    const columns = generationTables[table].filter(column => (manifestVersion >= 2 || column !== "catalog_version") && (manifestVersion >= 3 || !["edition_id","edition_provision_id","edition_body_sha256","applicability_status"].includes(column))).join(",");
    const rows = await env.DB.prepare(`SELECT ${columns} FROM ${generationId ? "staged_" : ""}${table}
      WHERE ${generationId ? "generation_id = ?" : whereDocument(table)} ORDER BY id`).bind(generationId || documentId).all();
    const serialized = JSON.stringify(rows.results || []);
    if (serialized.length > 8 * 1024 * 1024 || (rows.results || []).length > 10000) {
      throw new RequestValidationError("Generation exceeds the bounded revision workflow; current content was preserved.");
    }
    result[table] = { count: (rows.results || []).length, sha256: await sha256Bytes(new TextEncoder().encode(serialized)) };
  }
  return result;
}
async function readSnapshot(env: Env, documentId: string, expectedRevision: number) {
  const row = await env.DB.prepare("SELECT * FROM documents WHERE id = ?").bind(documentId).first<Snapshot>();
  if (!row || row.document_revision !== expectedRevision) throw new RequestConflictError("Document changed; reload before reprocessing.");
  return row;
}
async function verifySource(env: Env, generation: Pick<Generation, "source_key" | "source_sha256">) {
  const object = await env.SOURCE_BUCKET.get(generation.source_key);
  if (!object || object.size > 15 * 1024 * 1024) throw new RequestValidationError("Revision source is missing or exceeds the source limit.");
  const bytes = new Uint8Array(await object.arrayBuffer());
  if (bytes.length > 15 * 1024 * 1024 || await sha256Bytes(bytes) !== generation.source_sha256) {
    throw new RequestValidationError("Revision source digest does not match; publication was stopped.");
  }
}
function snapshotStatements(env: Env, documentId: string, generationId: string) {
  return tables.map(table => {
    const columns = generationTables[table].join(",");
    return env.DB.prepare(`INSERT INTO staged_${table} (generation_id,${columns})
      SELECT ?,${columns} FROM ${table} WHERE ${whereDocument(table)}`).bind(generationId, documentId);
  });
}
function projectStatements(env: Env, generationId: string) {
  return tables.map(table => {
    const columns = generationTables[table].join(",");
    return env.DB.prepare(`INSERT INTO ${table} (${columns}) SELECT ${columns} FROM staged_${table} WHERE generation_id = ?`).bind(generationId);
  });
}
async function publish(env: Env, generation: Generation, previousSnapshot: Snapshot, previousManifest: Manifest, previousSource: { key: string; sha256: string }) {
  const nextManifest = JSON.parse(generation.manifest_json) as Manifest;
  if (!nextManifest.document_chunks?.count || !nextManifest.section_paragraphs?.count || !nextManifest.document_sections?.count) {
    throw new RequestValidationError("Revision has no complete text artifacts.");
  }
  if (JSON.stringify(await manifest(env, generation.document_id, generation.id, generation.manifest_version)) !== JSON.stringify(nextManifest)) {
    throw new RequestValidationError("Staged revision digest/count mismatch; current content was preserved.");
  }
  await verifySource(env, generation);
  const vectorManifest = await readVectorManifest(env, generation.document_id, generation.id);
  const previousId = crypto.randomUUID();
  const now = new Date().toISOString();
  const documentId = generation.document_id;
  const snapshot = JSON.parse(generation.snapshot_json) as Snapshot;
  const changes: D1PreparedStatement[] = [
    env.DB.prepare(`INSERT INTO document_generations (id,document_id,base_revision,status,source_key,source_sha256,snapshot_json,manifest_json,created_at,manifest_version)
      VALUES (?,?,?,'retained',?,?,?,?,?,3)`).bind(previousId, documentId, generation.base_revision, previousSource.key,
      previousSource.sha256, JSON.stringify(previousSnapshot), JSON.stringify(previousManifest), now),
    ...snapshotStatements(env, documentId, previousId),
    // This state is transaction-local. Public readers see either the old complete
    // projection or the new complete one; no intermediate delete is committed.
    env.DB.prepare("UPDATE documents SET ingestion_state = 'preparing' WHERE id = ?").bind(documentId),
    env.DB.prepare("DELETE FROM document_chunks WHERE document_id = ?").bind(documentId),
    env.DB.prepare("DELETE FROM section_paragraphs WHERE section_id IN (SELECT id FROM document_sections WHERE document_id = ?)").bind(documentId),
    env.DB.prepare("DELETE FROM document_sections WHERE document_id = ?").bind(documentId),
    env.DB.prepare("DELETE FROM document_reference_links WHERE document_id = ?").bind(documentId),
    env.DB.prepare("DELETE FROM document_reference_issues WHERE document_id = ?").bind(documentId),
    ...projectStatements(env, generation.id),
    env.DB.prepare(`UPDATE documents SET ${restoredFields.map(field => `${field} = ?`).join(",")}, source_r2_key = ?,
      active_text_generation = ?, ingestion_state = 'ready', ingestion_completed_at = ?, expected_chunk_count = ?,
      approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, qc_required_confirmed = 0, qc_confirmed_at = NULL,
      searchable_at = NULL, published_revision = NULL, updated_at = ? WHERE id = ?`)
      .bind(...restoredFields.map(field => snapshot[field] ?? (field === "reference_catalog_version" ? "unverified" : field === "reference_edition_context_json" ? '{"status":"unknown","reason":"legacy_unassessed"}' : null)), generation.source_key, generation.id, now, nextManifest.document_chunks.count, now, documentId),
    env.DB.prepare("UPDATE retrieval_search_chunks SET active = 0 WHERE document_id = ? AND active = 1").bind(documentId),
    vectorOutboxStatement(env, {documentId,generationId:generation.id,sourceKey:generation.source_key,sourceSha256:generation.source_sha256,
      count:vectorManifest.count,digest:vectorManifest.digest,timestamp:now}),
    env.DB.prepare("UPDATE document_generations SET status = 'retained' WHERE document_id = ? AND status = 'published'").bind(documentId),
    env.DB.prepare("UPDATE document_generations SET status = 'published', previous_generation = ?, published_at = ? WHERE id = ? AND status = 'ready'")
      .bind(previousId, now, generation.id)
  ];
  await withDocumentRevision(env, documentId, generation.base_revision, changes);
  // The verified outbox and complete text generation commit together before delivery.
  try { await enqueueVectorJob(env, documentId, generation.id); }
  catch (error) { env.TELEMETRY?.tag("job_state","dispatch_deferred"); }
  return { generationId: generation.id, previousGenerationId: previousId, manifest: nextManifest };
}

export async function stageDocumentGeneration(env: Env, input: {
  documentId: string; expectedRevision: number; sourceBytes: Uint8Array;
  sourceFormat: SourceFormat; documentChanges: Snapshot; sections: AuthoredSection[];
  referencePlan?:ReferenceValidationPlan;
  references: { indexCodes: string[]; rulesSections: string[]; ordinanceSections: string[] }
}) {
  const previousSnapshot = await readSnapshot(env, input.documentId, input.expectedRevision);
  const previousManifest = await manifest(env, input.documentId);
  const generationId = crypto.randomUUID();
  const sourceKey = `revisions/${input.documentId}/${generationId}/source.${input.sourceFormat === "markdown" ? "md" : input.sourceFormat}`;
  const sourceHash = await sha256Bytes(input.sourceBytes);
  const previousHash = JSON.parse(String(previousSnapshot.metadata_json || "{}")).sourceSha256;
  if (previousHash && previousHash !== sourceHash) throw new RequestValidationError("Source digest changed; the current revision was preserved.");
  const artifacts = buildDocumentTextArtifactStatements(env, { documentId: input.documentId, citation: String(previousSnapshot.citation), sections: input.sections, generationId });
  if (!artifacts.chunkCount || artifacts.chunkCount > 1000 || artifacts.paragraphCount > 5000) {
    throw new RequestValidationError("Reprocessing requires 1–1000 chunks and at most 5000 paragraphs; current content was preserved.");
  }
  const nextSnapshot={...previousSnapshot,...input.documentChanges};
  const generation: Generation = {
id: generationId, document_id: input.documentId, base_revision: input.expectedRevision, status: "staging",
    source_key: sourceKey, source_sha256: sourceHash, manifest_version: 3, snapshot_json: JSON.stringify(nextSnapshot), manifest_json: "{}"
};
  await storeSourceFile(env, sourceKey, input.sourceBytes, input.sourceFormat);
  await env.DB.prepare(`INSERT INTO document_generations (id,document_id,base_revision,status,source_key,source_sha256,snapshot_json,created_at,manifest_version)
    VALUES (?,?,?,'staging',?,?,?,?,3)`).bind(generationId, input.documentId, input.expectedRevision, sourceKey, sourceHash, generation.snapshot_json, new Date().toISOString()).run();
  try {
    const references = await buildDocumentReferenceValidationStatements(env, input.documentId, input.references, undefined, generationId,{jurisdiction:String(nextSnapshot.jurisdiction||""),as_of_date:typeof nextSnapshot.decision_date==="string"?nextSnapshot.decision_date:null},input.referencePlan);
    await executeTextArtifactStatementBatches(env, [...artifacts.statements, ...references]);
    const verified = await manifest(env, input.documentId, generationId);
    if (verified.document_chunks.count !== artifacts.chunkCount || verified.section_paragraphs.count !== artifacts.paragraphCount || verified.document_sections.count !== artifacts.sectionCount) {
      throw new Error("Staging artifact count mismatch");
    }
    generation.snapshot_json = (await env.DB.prepare("SELECT snapshot_json FROM document_generations WHERE id=?").bind(generationId).first<{snapshot_json:string}>())!.snapshot_json;
    generation.manifest_json = JSON.stringify(verified);
    await env.DB.prepare("UPDATE document_generations SET status='ready', manifest_json=? WHERE id=? AND status='staging'").bind(generation.manifest_json, generationId).run();
    return await publish(env, generation, previousSnapshot, previousManifest, { key: sourceKey, sha256: sourceHash });
  } catch (error) {
    // Preserve sources and staged artifacts on uncertain outcomes. A committed
    // publication is never marked failed or deleted by response-path cleanup.
    await env.DB.prepare("UPDATE document_generations SET status='failed', error=? WHERE id=? AND status IN ('staging','ready')")
      .bind(error instanceof RequestValidationError || error instanceof RequestConflictError ? error.message : "Revision staging/publication failed", generationId).run().catch(() => { });
    throw error;
  }
}

export async function listDocumentGenerations(env: Env, documentId: string) {
  return (await env.DB.prepare(`SELECT id, base_revision AS baseRevision, status, source_sha256 AS sourceSha256,
    manifest_json AS manifestJson, previous_generation AS previousGenerationId, created_at AS createdAt, published_at AS publishedAt, error
    FROM document_generations WHERE document_id=? ORDER BY created_at DESC LIMIT 100`).bind(documentId).all()).results || [];
}

export async function restoreDocumentGeneration(env: Env, documentId: string, generationId: string, expectedRevision: number) {
  const source = await env.DB.prepare("SELECT * FROM document_generations WHERE id=? AND document_id=? AND status IN ('retained','published')")
    .bind(generationId, documentId).first<Generation>();
  if (!source) throw new RequestNotFoundError("Retained generation not found for this document.");
  const current = await readSnapshot(env, documentId, expectedRevision);
  await verifySource(env, source);
  const currentSource = await env.SOURCE_BUCKET.get(String(current.source_r2_key));
  if (!currentSource || currentSource.size > 15 * 1024 * 1024) throw new RequestValidationError("Current source unavailable for rollback retention.");
  const currentBytes = new Uint8Array(await currentSource.arrayBuffer());
  const currentHash = await sha256Bytes(currentBytes);
  const capturedHash = JSON.parse(String(current.metadata_json || "{}")).sourceSha256;
  if (currentBytes.length > 15 * 1024 * 1024 || (capturedHash && capturedHash !== currentHash)) {
    throw new RequestValidationError("Current source digest changed; rollback retention was stopped.");
  }
  const previousManifest = await manifest(env, documentId);
  const id = crypto.randomUUID();
  const clone = { ...source, id, base_revision: expectedRevision, status: "ready" };
  const copied = [env.DB.prepare(`INSERT INTO document_generations (id,document_id,base_revision,status,source_key,source_sha256,snapshot_json,manifest_json,created_at,manifest_version)
    VALUES (?,?,?,'ready',?,?,?,?,?,?)`).bind(id, documentId, expectedRevision, source.source_key, source.source_sha256, source.snapshot_json, source.manifest_json, new Date().toISOString(), source.manifest_version),
  ...tables.map(table => {
const columns = generationTables[table].join(","); return env.DB.prepare(`INSERT INTO staged_${table} (generation_id,${columns})
      SELECT ?,${columns} FROM staged_${table} WHERE generation_id=?`).bind(id, generationId);
})];
  await env.DB.batch(copied);
  return publish(env, clone, current, previousManifest, { key: String(current.source_r2_key), sha256: currentHash });
}
