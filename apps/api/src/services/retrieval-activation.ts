import type { Env } from "../lib/types";
import { embed } from "./embeddings";

interface TrustedEmbeddingPayloadRow {
  embeddingId: string;
  documentId: string;
  chunkId: string;
  sourceText: string;
  chunkType: string;
  sectionLabel?: string;
  retrievalPriority: string;
  hasCanonicalReferenceAlignment: boolean;
  paragraphAnchorStart?: string;
  paragraphAnchorEnd?: string;
  citationAnchorStart: string;
  citationAnchorEnd: string;
  sourceLink: string;
}

interface TrustedSearchPayloadRow {
  searchId: string;
  documentId: string;
  chunkId: string;
  title: string;
  chunkType: string;
  sectionLabel?: string;
  retrievalPriority: string;
  paragraphAnchorStart?: string;
  paragraphAnchorEnd?: string;
  citationAnchorStart: string;
  citationAnchorEnd: string;
  sourceLink: string;
  hasCanonicalReferenceAlignment: boolean;
}

interface ActivationManifest {
  activationBatchIds: string[];
  documentsToActivate: string[];
  chunksToActivate: string[];
  baselineAdmittedDocIds?: string[];
  promotedEnrichmentDocIds?: string[];
  integrity?: {
    activationChecksum?: string;
  };
}

interface RollbackManifest {
  rollbackBatchIds: string[];
  activationBatchIdsReversed: string[];
  documentsToRemove: string[];
  chunksToRemove: string[];
  integrity?: {
    rollbackChecksum?: string;
  };
}

interface ActivationWriteInput {
  embeddingPayload: {
    rows: TrustedEmbeddingPayloadRow[];
  };
  searchPayload: {
    rows: TrustedSearchPayloadRow[];
  };
  activationManifest: ActivationManifest;
  rollbackManifest: RollbackManifest;
  dryRun?: boolean;
  performVectorUpsert?: boolean;
}

interface RollbackWriteInput {
  rollbackManifest: RollbackManifest;
  rollbackBatchId?: string;
  dryRun?: boolean;
}

interface RollbackTableState {
  activationBatchRecordsCount: number;
  retrievalSearchChunksRowsForManifest: number;
  retrievalSearchChunksActiveRowsForManifest: number;
  retrievalSearchRowsRowsForManifest: number;
  retrievalSearchRowsRowsForManifestInRollbackBatches: number;
  retrievalEmbeddingRowsRowsForManifest: number;
  retrievalEmbeddingRowsRowsForManifestInRollbackBatches: number;
  retrievalActivationChunksRowsForManifest: number;
  retrievalActivationChunksRowsForManifestInRollbackBatches: number;
  retrievalActivationDocumentsRowsForManifest: number;
  retrievalActivationDocumentsRowsForManifestInRollbackBatches: number;
}

interface DocumentDbRow {
  id: string;
  title: string;
  citation: string;
  fileType: string;
  sourceFileRef: string;
  sourceLink: string;
  qcPassed: number;
  rejectedAt: string | null;
}

const SQLITE_BIND_LIMIT = 200;

function parseInput(raw: unknown): ActivationWriteInput {
  const input = (raw || {}) as Partial<ActivationWriteInput>;
  const embeddingRows = Array.isArray(input.embeddingPayload?.rows) ? input.embeddingPayload?.rows : [];
  const searchRows = Array.isArray(input.searchPayload?.rows) ? input.searchPayload?.rows : [];
  const activationManifest = (input.activationManifest || {}) as ActivationManifest;
  const rollbackManifest = (input.rollbackManifest || {}) as RollbackManifest;

  if (!Array.isArray(activationManifest.activationBatchIds) || activationManifest.activationBatchIds.length === 0) {
    throw new Error("activationManifest.activationBatchIds is required");
  }
  if (!Array.isArray(activationManifest.documentsToActivate) || !Array.isArray(activationManifest.chunksToActivate)) {
    throw new Error("activationManifest.documentsToActivate and chunksToActivate are required");
  }
  if (!Array.isArray(rollbackManifest.documentsToRemove) || !Array.isArray(rollbackManifest.chunksToRemove)) {
    throw new Error("rollbackManifest.documentsToRemove and chunksToRemove are required");
  }

  return {
    embeddingPayload: { rows: embeddingRows },
    searchPayload: { rows: searchRows },
    activationManifest,
    rollbackManifest,
    dryRun: Boolean(input.dryRun),
    performVectorUpsert: input.performVectorUpsert !== false
  };
}

function parseRollbackInput(raw: unknown): RollbackWriteInput {
  const input = (raw || {}) as Partial<RollbackWriteInput>;
  const rollbackManifest = (input.rollbackManifest || {}) as RollbackManifest;
  if (!Array.isArray(rollbackManifest.documentsToRemove) || !Array.isArray(rollbackManifest.chunksToRemove)) {
    throw new Error("rollbackManifest.documentsToRemove and rollbackManifest.chunksToRemove are required");
  }
  return {
    rollbackManifest,
    rollbackBatchId: input.rollbackBatchId ? String(input.rollbackBatchId) : undefined,
    dryRun: Boolean(input.dryRun)
  };
}

function uniqueSorted(values: string[]): string[] {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function chunkValues<T>(values: T[], size = SQLITE_BIND_LIMIT): T[][] {
  if (!Array.isArray(values) || values.length === 0) return [];
  const chunkSize = Math.max(1, size);
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }
  return chunks;
}

function countBy(values: string[]) {
  const out: Record<string, number> = {};
  for (const v of values || []) {
    const key = String(v || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function isLikelyFixtureDoc(params: { title: string; citation: string; sourceFileRef: string }) {
  const joined = `${params.title} ${params.citation} ${params.sourceFileRef}`.toLowerCase();
  return /harness|fixture|seed|decision_pass|decision_fail|decision_invalid|law_sample|bee-harness/.test(joined);
}

function isProvenanceComplete(row: {
  documentId?: string;
  chunkId?: string;
  paragraphAnchorStart?: string;
  citationAnchorStart?: string;
  citationAnchorEnd?: string;
  sourceLink?: string;
}) {
  return Boolean(
    row.documentId &&
      row.chunkId &&
      row.paragraphAnchorStart &&
      row.citationAnchorStart &&
      row.citationAnchorEnd &&
      row.sourceLink
  );
}

async function ensureActivationTables(env: Env) {
  const statements = [
    `CREATE TABLE IF NOT EXISTS retrieval_activation_batches (
      batch_id TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      trusted_document_count INTEGER NOT NULL,
      trusted_chunk_count INTEGER NOT NULL,
      activation_checksum TEXT NOT NULL,
      rollback_checksum TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS retrieval_activation_documents (
      batch_id TEXT NOT NULL,
      document_id TEXT NOT NULL,
      trust_source TEXT NOT NULL,
      write_status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (batch_id, document_id)
    )`,
    `CREATE TABLE IF NOT EXISTS retrieval_activation_chunks (
      batch_id TEXT NOT NULL,
      chunk_id TEXT NOT NULL,
      document_id TEXT NOT NULL,
      chunk_type TEXT NOT NULL,
      embedding_write_status TEXT NOT NULL,
      search_write_status TEXT NOT NULL,
      provenance_complete INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      PRIMARY KEY (batch_id, chunk_id)
    )`,
    `CREATE TABLE IF NOT EXISTS retrieval_embedding_rows (
      embedding_id TEXT PRIMARY KEY,
      batch_id TEXT NOT NULL,
      document_id TEXT NOT NULL,
      chunk_id TEXT NOT NULL,
      chunk_type TEXT NOT NULL,
      retrieval_priority TEXT NOT NULL,
      citation_anchor_start TEXT NOT NULL,
      citation_anchor_end TEXT NOT NULL,
      has_canonical_reference_alignment INTEGER NOT NULL DEFAULT 0,
      source_link TEXT NOT NULL,
      source_text TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS retrieval_search_rows (
      search_id TEXT PRIMARY KEY,
      batch_id TEXT NOT NULL,
      document_id TEXT NOT NULL,
      chunk_id TEXT NOT NULL,
      title TEXT NOT NULL,
      chunk_type TEXT NOT NULL,
      retrieval_priority TEXT NOT NULL,
      citation_anchor_start TEXT NOT NULL,
      citation_anchor_end TEXT NOT NULL,
      has_canonical_reference_alignment INTEGER NOT NULL DEFAULT 0,
      source_link TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
    `CREATE TABLE IF NOT EXISTS retrieval_search_chunks (
      chunk_id TEXT PRIMARY KEY,
      batch_id TEXT NOT NULL,
      document_id TEXT NOT NULL,
      title TEXT NOT NULL,
      citation TEXT NOT NULL,
      source_file_ref TEXT NOT NULL,
      source_link TEXT NOT NULL,
      section_label TEXT NOT NULL,
      paragraph_anchor TEXT NOT NULL,
      citation_anchor TEXT NOT NULL,
      chunk_text TEXT NOT NULL,
      chunk_type TEXT NOT NULL,
      retrieval_priority TEXT NOT NULL,
      has_canonical_reference_alignment INTEGER NOT NULL DEFAULT 0,
      active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL
    )`,
    `CREATE INDEX IF NOT EXISTS idx_retrieval_activation_documents_doc
      ON retrieval_activation_documents (document_id, batch_id)`,
    `CREATE INDEX IF NOT EXISTS idx_retrieval_activation_chunks_doc
      ON retrieval_activation_chunks (document_id, batch_id)`,
    `CREATE INDEX IF NOT EXISTS idx_retrieval_embedding_rows_doc
      ON retrieval_embedding_rows (document_id, batch_id)`,
    `CREATE INDEX IF NOT EXISTS idx_retrieval_search_rows_doc
      ON retrieval_search_rows (document_id, batch_id)`,
    `CREATE INDEX IF NOT EXISTS idx_retrieval_search_chunks_doc
      ON retrieval_search_chunks (document_id, active, batch_id)`
  ];

  for (const sql of statements) {
    await env.DB.prepare(sql).run();
  }
}

async function readActivationMigrationStatus(env: Env) {
  const expected = [
    "retrieval_activation_batches",
    "retrieval_activation_documents",
    "retrieval_activation_chunks",
    "retrieval_embedding_rows",
    "retrieval_search_rows",
    "retrieval_search_chunks"
  ];
  const placeholders = expected.map(() => "?").join(",");
  const rows = await env.DB.prepare(
    `SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (${placeholders})`
  )
    .bind(...expected)
    .all<{ name: string }>();
  const found = new Set((rows.results || []).map((row) => row.name));
  return {
    expectedTables: expected,
    foundTables: Array.from(found).sort((a, b) => a.localeCompare(b)),
    missingTables: expected.filter((name) => !found.has(name)),
    allTablesPresent: expected.every((name) => found.has(name))
  };
}

async function fetchDocumentsByIds(env: Env, ids: string[]): Promise<DocumentDbRow[]> {
  if (!ids.length) return [];
  const rows: DocumentDbRow[] = [];
  for (const chunk of chunkValues(ids)) {
    const placeholders = chunk.map(() => "?").join(",");
    const result = await env.DB.prepare(
      `SELECT
        id,
        title,
        citation,
        file_type as fileType,
        source_r2_key as sourceFileRef,
        source_link as sourceLink,
        qc_passed as qcPassed,
        rejected_at as rejectedAt
       FROM documents
       WHERE id IN (${placeholders})`
    )
      .bind(...chunk)
      .all<DocumentDbRow>();

    rows.push(...(result.results || []));
  }

  return rows;
}

function stableHash(value: unknown): string {
  const input = typeof value === "string" ? value : JSON.stringify(value);
  let hash = 0x811c9dc5;
  const text = String(input || "");
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

async function verifyQueryableCount(env: Env, batchId: string) {
  const row = await env.DB.prepare(
    `SELECT COUNT(1) as count
     FROM retrieval_search_chunks
     WHERE batch_id = ? AND active = 1`
  )
    .bind(batchId)
    .first<{ count: number }>();
  return Number(row?.count || 0);
}

function chunkIds<T extends { chunkId: string }>(rows: T[]): string[] {
  return uniqueSorted(rows.map((row) => row.chunkId));
}

function documentIds<T extends { documentId: string }>(rows: T[]): string[] {
  return uniqueSorted(rows.map((row) => row.documentId));
}

async function queryCount(env: Env, sql: string, binds: unknown[]) {
  const row = await env.DB.prepare(sql)
    .bind(...binds)
    .first<{ count: number }>();
  return Number(row?.count || 0);
}

async function queryCountForIdBatches(
  env: Env,
  ids: string[],
  buildSql: (idPlaceholders: string, batchPlaceholders: string) => string,
  batchIds: string[] = []
) {
  if (!ids.length) return 0;
  const chunkSize = Math.max(1, SQLITE_BIND_LIMIT - batchIds.length);
  let total = 0;
  for (const chunk of chunkValues(ids, chunkSize)) {
    const idPlaceholders = chunk.map(() => "?").join(",");
    const batchPlaceholders = batchIds.map(() => "?").join(",");
    total += await queryCount(env, buildSql(idPlaceholders, batchPlaceholders), [...chunk, ...batchIds]);
  }
  return total;
}

async function selectRowsForIdBatches<T>(
  env: Env,
  ids: string[],
  buildSql: (idPlaceholders: string, batchPlaceholders: string) => string,
  batchIds: string[] = []
) {
  if (!ids.length) return [];
  const chunkSize = Math.max(1, SQLITE_BIND_LIMIT - batchIds.length);
  const rows: T[] = [];
  for (const chunk of chunkValues(ids, chunkSize)) {
    const idPlaceholders = chunk.map(() => "?").join(",");
    const batchPlaceholders = batchIds.map(() => "?").join(",");
    const result = await env.DB.prepare(buildSql(idPlaceholders, batchPlaceholders))
      .bind(...chunk, ...batchIds)
      .all<T>();
    rows.push(...(result.results || []));
  }
  return rows;
}

async function readRollbackTableState(env: Env, params: {
  manifestDocIds: string[];
  manifestChunkIds: string[];
  rollbackBatchIds: string[];
}): Promise<RollbackTableState> {
  const { manifestDocIds, manifestChunkIds, rollbackBatchIds } = params;
  const batchPlaceholders = rollbackBatchIds.map(() => "?").join(",");
  const hasChunkIds = manifestChunkIds.length > 0;
  const hasDocIds = manifestDocIds.length > 0;
  const hasBatchIds = rollbackBatchIds.length > 0;

  const activationBatchRecordsCount = hasBatchIds
    ? await queryCount(
        env,
        `SELECT COUNT(1) as count FROM retrieval_activation_batches WHERE batch_id IN (${batchPlaceholders})`,
        rollbackBatchIds
      )
    : 0;

  const retrievalSearchChunksRowsForManifest = hasChunkIds
    ? await queryCountForIdBatches(
        env,
        manifestChunkIds,
        (chunkIdPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_search_chunks WHERE chunk_id IN (${chunkIdPlaceholders})`
      )
    : 0;
  const retrievalSearchChunksActiveRowsForManifest = hasChunkIds
    ? await queryCountForIdBatches(
        env,
        manifestChunkIds,
        (chunkIdPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_search_chunks WHERE chunk_id IN (${chunkIdPlaceholders}) AND active = 1`
      )
    : 0;
  const retrievalSearchRowsRowsForManifest = hasChunkIds
    ? await queryCountForIdBatches(
        env,
        manifestChunkIds,
        (chunkIdPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_search_rows WHERE chunk_id IN (${chunkIdPlaceholders})`
      )
    : 0;
  const retrievalSearchRowsRowsForManifestInRollbackBatches = hasChunkIds
    ? await queryCountForIdBatches(
        env,
        manifestChunkIds,
        (chunkIdPlaceholders, rollbackBatchPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_search_rows WHERE chunk_id IN (${chunkIdPlaceholders})${hasBatchIds ? ` AND batch_id IN (${rollbackBatchPlaceholders})` : ""}`,
        rollbackBatchIds
      )
    : 0;
  const retrievalEmbeddingRowsRowsForManifest = hasChunkIds
    ? await queryCountForIdBatches(
        env,
        manifestChunkIds,
        (chunkIdPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_embedding_rows WHERE chunk_id IN (${chunkIdPlaceholders})`
      )
    : 0;
  const retrievalEmbeddingRowsRowsForManifestInRollbackBatches = hasChunkIds
    ? await queryCountForIdBatches(
        env,
        manifestChunkIds,
        (chunkIdPlaceholders, rollbackBatchPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_embedding_rows WHERE chunk_id IN (${chunkIdPlaceholders})${hasBatchIds ? ` AND batch_id IN (${rollbackBatchPlaceholders})` : ""}`,
        rollbackBatchIds
      )
    : 0;
  const retrievalActivationChunksRowsForManifest = hasChunkIds
    ? await queryCountForIdBatches(
        env,
        manifestChunkIds,
        (chunkIdPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_activation_chunks WHERE chunk_id IN (${chunkIdPlaceholders})`
      )
    : 0;
  const retrievalActivationChunksRowsForManifestInRollbackBatches = hasChunkIds
    ? await queryCountForIdBatches(
        env,
        manifestChunkIds,
        (chunkIdPlaceholders, rollbackBatchPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_activation_chunks WHERE chunk_id IN (${chunkIdPlaceholders})${hasBatchIds ? ` AND batch_id IN (${rollbackBatchPlaceholders})` : ""}`,
        rollbackBatchIds
      )
    : 0;
  const retrievalActivationDocumentsRowsForManifest = hasDocIds
    ? await queryCountForIdBatches(
        env,
        manifestDocIds,
        (docIdPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_activation_documents WHERE document_id IN (${docIdPlaceholders})`
      )
    : 0;
  const retrievalActivationDocumentsRowsForManifestInRollbackBatches = hasDocIds
    ? await queryCountForIdBatches(
        env,
        manifestDocIds,
        (docIdPlaceholders, rollbackBatchPlaceholders) =>
          `SELECT COUNT(1) as count FROM retrieval_activation_documents WHERE document_id IN (${docIdPlaceholders})${hasBatchIds ? ` AND batch_id IN (${rollbackBatchPlaceholders})` : ""}`,
        rollbackBatchIds
      )
    : 0;

  return {
    activationBatchRecordsCount,
    retrievalSearchChunksRowsForManifest,
    retrievalSearchChunksActiveRowsForManifest,
    retrievalSearchRowsRowsForManifest,
    retrievalSearchRowsRowsForManifestInRollbackBatches,
    retrievalEmbeddingRowsRowsForManifest,
    retrievalEmbeddingRowsRowsForManifestInRollbackBatches,
    retrievalActivationChunksRowsForManifest,
    retrievalActivationChunksRowsForManifestInRollbackBatches,
    retrievalActivationDocumentsRowsForManifest,
    retrievalActivationDocumentsRowsForManifestInRollbackBatches
  };
}

export async function writeTrustedRetrievalActivation(env: Env, rawInput: unknown) {
  const input = parseInput(rawInput);
  const now = new Date().toISOString();
  const activationBatchId = String(input.activationManifest.activationBatchIds[0]);

  await ensureActivationTables(env);
  const migrationStatus = await readActivationMigrationStatus(env);

  const manifestDocIds = uniqueSorted(input.activationManifest.documentsToActivate || []);
  const manifestChunkIds = uniqueSorted(input.activationManifest.chunksToActivate || []);
  const baselineDocIds = uniqueSorted(input.activationManifest.baselineAdmittedDocIds || []);
  const promotedDocIds = uniqueSorted(input.activationManifest.promotedEnrichmentDocIds || []);
  const trustedDocIds = uniqueSorted([...baselineDocIds, ...promotedDocIds]);

  const rollbackDocIds = uniqueSorted(input.rollbackManifest.documentsToRemove || []);
  const rollbackChunkIds = uniqueSorted(input.rollbackManifest.chunksToRemove || []);

  const rollbackMatchesWriteSet =
    JSON.stringify(manifestDocIds) === JSON.stringify(rollbackDocIds) &&
    JSON.stringify(manifestChunkIds) === JSON.stringify(rollbackChunkIds);

  const embeddingRows = (input.embeddingPayload.rows || []).filter(
    (row) => manifestDocIds.includes(row.documentId) && manifestChunkIds.includes(row.chunkId)
  );
  const searchRows = (input.searchPayload.rows || []).filter(
    (row) => manifestDocIds.includes(row.documentId) && manifestChunkIds.includes(row.chunkId)
  );

  const embeddingMapByChunkId = new Map(embeddingRows.map((row) => [row.chunkId, row]));
  const searchMapByChunkId = new Map(searchRows.map((row) => [row.chunkId, row]));

  const docs = await fetchDocumentsByIds(env, manifestDocIds);
  const docsById = new Map(docs.map((doc) => [doc.id, doc]));

  const documentsActivated: Array<{
    documentId: string;
    title: string;
    trustSource: "baseline_admit_now" | "promoted_after_enrichment";
    activationEligible: boolean;
    writeStatus: string;
  }> = [];
  const documentsRejectedFromWrite: Array<{ documentId: string; reason: string }> = [];

  let heldDocsWrittenCount = 0;
  let excludedDocsWrittenCount = 0;
  let fixtureDocsWrittenCount = 0;

  for (const docId of manifestDocIds) {
    const doc = docsById.get(docId);
    const trustSource = baselineDocIds.includes(docId) ? "baseline_admit_now" : "promoted_after_enrichment";

    if (!doc) {
      documentsRejectedFromWrite.push({ documentId: docId, reason: "document_not_found" });
      continue;
    }

    if (doc.fileType !== "decision_docx") {
      documentsRejectedFromWrite.push({ documentId: docId, reason: "non_decision_docx" });
      continue;
    }

    if (doc.rejectedAt) {
      documentsRejectedFromWrite.push({ documentId: docId, reason: "document_rejected" });
      continue;
    }

    if (!trustedDocIds.includes(docId)) {
      documentsRejectedFromWrite.push({ documentId: docId, reason: "outside_trusted_manifest_bundle" });
      if (trustSource === "baseline_admit_now") heldDocsWrittenCount += 1;
      else excludedDocsWrittenCount += 1;
      continue;
    }

    if (isLikelyFixtureDoc({ title: doc.title, citation: doc.citation, sourceFileRef: doc.sourceFileRef })) {
      documentsRejectedFromWrite.push({ documentId: docId, reason: "fixture_detected" });
      fixtureDocsWrittenCount += 1;
      continue;
    }

    documentsActivated.push({
      documentId: docId,
      title: doc.title,
      trustSource,
      activationEligible: true,
      writeStatus: input.dryRun ? "dry_run_validated" : "written"
    });
  }

  const allowedDocSet = new Set(documentsActivated.map((row) => row.documentId));

  const chunksActivated: Array<{
    chunkId: string;
    documentId: string;
    chunkType: string;
    embeddingWriteStatus: string;
    searchWriteStatus: string;
    provenanceComplete: boolean;
    vectorWriteStatus?: string;
  }> = [];
  const chunksRejectedFromWrite: Array<{ chunkId: string; documentId: string; reason: string }> = [];

  let provenanceFailuresCount = 0;

  for (const chunkId of manifestChunkIds) {
    const embeddingRow = embeddingMapByChunkId.get(chunkId);
    const searchRow = searchMapByChunkId.get(chunkId);
    const docId = embeddingRow?.documentId || searchRow?.documentId || "";

    if (!docId || !allowedDocSet.has(docId)) {
      chunksRejectedFromWrite.push({ chunkId, documentId: docId || "", reason: "chunk_document_not_activated" });
      continue;
    }

    if (!embeddingRow) {
      chunksRejectedFromWrite.push({ chunkId, documentId: docId, reason: "missing_embedding_payload_row" });
      continue;
    }
    if (!searchRow) {
      chunksRejectedFromWrite.push({ chunkId, documentId: docId, reason: "missing_search_payload_row" });
      continue;
    }

    const provenanceComplete =
      isProvenanceComplete(embeddingRow) &&
      isProvenanceComplete(searchRow) &&
      Boolean(embeddingRow.sourceText && embeddingRow.sourceText.trim().length > 0);

    if (!provenanceComplete) {
      provenanceFailuresCount += 1;
      chunksRejectedFromWrite.push({ chunkId, documentId: docId, reason: "provenance_incomplete" });
      continue;
    }

    chunksActivated.push({
      chunkId,
      documentId: docId,
      chunkType: searchRow.chunkType,
      embeddingWriteStatus: input.dryRun ? "dry_run_validated" : "written",
      searchWriteStatus: input.dryRun ? "dry_run_validated" : "written",
      provenanceComplete
    });
  }

  const documentsRejectedCount = documentsRejectedFromWrite.length;
  const chunksRejectedCount = chunksRejectedFromWrite.length;
  const rejectionReasonCounts = {
    documentReasons: countBy(documentsRejectedFromWrite.map((row) => row.reason)),
    chunkReasons: countBy(chunksRejectedFromWrite.map((row) => row.reason))
  };

  if (!input.dryRun) {
    await env.DB.prepare(
      `INSERT OR REPLACE INTO retrieval_activation_batches
       (batch_id, created_at, trusted_document_count, trusted_chunk_count, activation_checksum, rollback_checksum)
       VALUES (?, ?, ?, ?, ?, ?)`
    )
      .bind(
        activationBatchId,
        now,
        documentsActivated.length,
        chunksActivated.length,
        input.activationManifest.integrity?.activationChecksum || stableHash(manifestChunkIds.join("|")),
        input.rollbackManifest.integrity?.rollbackChecksum || stableHash(rollbackChunkIds.join("|"))
      )
      .run();

    for (const row of documentsActivated) {
      await env.DB.prepare(
        `INSERT OR REPLACE INTO retrieval_activation_documents
         (batch_id, document_id, trust_source, write_status, created_at)
         VALUES (?, ?, ?, ?, ?)`
      )
        .bind(activationBatchId, row.documentId, row.trustSource, row.writeStatus, now)
        .run();

      await env.DB.prepare(
        `UPDATE documents
         SET searchable_at = COALESCE(searchable_at, ?), updated_at = ?
         WHERE id = ?`
      )
        .bind(now, now, row.documentId)
        .run();
    }

    for (const row of chunksActivated) {
      const embeddingRow = embeddingMapByChunkId.get(row.chunkId);
      const searchRow = searchMapByChunkId.get(row.chunkId);
      const doc = docsById.get(row.documentId);
      if (!embeddingRow || !searchRow || !doc) continue;

      await env.DB.prepare(
        `INSERT OR REPLACE INTO retrieval_embedding_rows
         (embedding_id, batch_id, document_id, chunk_id, chunk_type, retrieval_priority, citation_anchor_start, citation_anchor_end,
          has_canonical_reference_alignment, source_link, source_text, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
        .bind(
          embeddingRow.embeddingId,
          activationBatchId,
          row.documentId,
          row.chunkId,
          embeddingRow.chunkType,
          embeddingRow.retrievalPriority,
          embeddingRow.citationAnchorStart,
          embeddingRow.citationAnchorEnd,
          embeddingRow.hasCanonicalReferenceAlignment ? 1 : 0,
          embeddingRow.sourceLink,
          embeddingRow.sourceText,
          now
        )
        .run();

      await env.DB.prepare(
        `INSERT OR REPLACE INTO retrieval_search_rows
         (search_id, batch_id, document_id, chunk_id, title, chunk_type, retrieval_priority, citation_anchor_start, citation_anchor_end,
          has_canonical_reference_alignment, source_link, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
        .bind(
          searchRow.searchId,
          activationBatchId,
          row.documentId,
          row.chunkId,
          searchRow.title,
          searchRow.chunkType,
          searchRow.retrievalPriority,
          searchRow.citationAnchorStart,
          searchRow.citationAnchorEnd,
          searchRow.hasCanonicalReferenceAlignment ? 1 : 0,
          searchRow.sourceLink,
          now
        )
        .run();

      await env.DB.prepare(
        `INSERT OR REPLACE INTO retrieval_search_chunks
         (chunk_id, batch_id, document_id, title, citation, source_file_ref, source_link, section_label, paragraph_anchor, citation_anchor,
          chunk_text, chunk_type, retrieval_priority, has_canonical_reference_alignment, active, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`
      )
        .bind(
          row.chunkId,
          activationBatchId,
          row.documentId,
          doc.title,
          doc.citation,
          doc.sourceFileRef,
          searchRow.sourceLink,
          searchRow.sectionLabel || embeddingRow.sectionLabel || searchRow.chunkType,
          searchRow.paragraphAnchorStart || embeddingRow.paragraphAnchorStart || embeddingRow.citationAnchorStart,
          embeddingRow.citationAnchorStart,
          embeddingRow.sourceText,
          searchRow.chunkType,
          searchRow.retrievalPriority,
          searchRow.hasCanonicalReferenceAlignment ? 1 : 0,
          now
        )
        .run();

      let vectorWriteStatus = "db_only";
      if (input.performVectorUpsert) {
        try {
          const vector = await embed(env, embeddingRow.sourceText);
          if (vector) {
            await env.VECTOR_INDEX.upsert([
              {
                id: row.chunkId,
                values: vector,
                namespace: env.VECTOR_NAMESPACE,
                metadata: {
                  documentId: row.documentId,
                  citationAnchor: embeddingRow.citationAnchorStart,
                  sectionLabel: searchRow.sectionLabel || embeddingRow.sectionLabel || searchRow.chunkType
                } as Record<string, VectorizeVectorMetadata>
              }
            ]);
            vectorWriteStatus = "vector_upserted";
          } else {
            vectorWriteStatus = "vector_unavailable";
          }
        } catch {
          vectorWriteStatus = "vector_upsert_failed";
        }
      }

      row.embeddingWriteStatus = vectorWriteStatus === "vector_upserted" ? "written_with_vector" : "written";
      row.vectorWriteStatus = vectorWriteStatus;

      await env.DB.prepare(
        `INSERT OR REPLACE INTO retrieval_activation_chunks
         (batch_id, chunk_id, document_id, chunk_type, embedding_write_status, search_write_status, provenance_complete, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      )
        .bind(
          activationBatchId,
          row.chunkId,
          row.documentId,
          row.chunkType,
          row.embeddingWriteStatus,
          row.searchWriteStatus,
          row.provenanceComplete ? 1 : 0,
          now
        )
        .run();
    }
  }

  const expectedDocumentSet = uniqueSorted(documentsActivated.map((row) => row.documentId));
  const expectedChunkSet = uniqueSorted(chunksActivated.map((row) => row.chunkId));

  const queryableChunkCount = input.dryRun ? expectedChunkSet.length : await verifyQueryableCount(env, activationBatchId);
  const activationVerificationPassed =
    manifestDocIds.length > 0 &&
    manifestChunkIds.length > 0 &&
    documentsActivated.length > 0 &&
    chunksActivated.length > 0 &&
    rollbackMatchesWriteSet &&
    heldDocsWrittenCount === 0 &&
    excludedDocsWrittenCount === 0 &&
    fixtureDocsWrittenCount === 0 &&
    migrationStatus.allTablesPresent &&
    provenanceFailuresCount === 0 &&
    queryableChunkCount === expectedChunkSet.length;

  const rollbackVerificationPassed =
    rollbackMatchesWriteSet &&
    JSON.stringify(rollbackDocIds) === JSON.stringify(expectedDocumentSet) &&
    JSON.stringify(rollbackChunkIds) === JSON.stringify(expectedChunkSet);

  const writeCounts = {
    attemptedTrustedDocumentCount: manifestDocIds.length,
    attemptedTrustedChunkCount: manifestChunkIds.length,
    writtenEmbeddingRowCount: chunksActivated.length,
    writtenSearchRowCount: chunksActivated.length,
    activatedDocumentCount: documentsActivated.length,
    activatedChunkCount: chunksActivated.length,
    documentsRejectedCount,
    chunksRejectedCount,
    heldDocsWrittenCount,
    excludedDocsWrittenCount,
    fixtureDocsWrittenCount,
    provenanceFailuresCount
  };

  const summary = {
    ...writeCounts,
    activationVerificationPassed,
    rollbackVerificationPassed,
    activationBatchId
  };

  const verificationSummary = {
    onlyTrustedDocsWritten: documentsActivated.every((row) => trustedDocIds.includes(row.documentId)),
    noHeldDocsWritten: heldDocsWrittenCount === 0,
    noExcludedDocsWritten: excludedDocsWrittenCount === 0,
    noFixtureDocsWritten: fixtureDocsWrittenCount === 0,
    provenanceIntact: provenanceFailuresCount === 0,
    countsMatchManifestExpectations:
      documentsActivated.length + documentsRejectedCount === manifestDocIds.length &&
      chunksActivated.length + chunksRejectedCount === manifestChunkIds.length,
    activatedDocsQueryable: queryableChunkCount === chunksActivated.length
  };

  const activationBatchSummary = {
    activationBatchId,
    trustedDocumentCount: documentsActivated.length,
    trustedChunkCount: chunksActivated.length,
    trustSourceCounts: countBy(documentsActivated.map((row) => row.trustSource))
  };

  const rollbackVerificationSummary = {
    rollbackManifestMatchesActivationSet: rollbackVerificationPassed,
    rollbackDocumentCount: rollbackDocIds.length,
    rollbackChunkCount: rollbackChunkIds.length
  };

  return {
    readOnly: Boolean(input.dryRun),
    summary,
    writeCounts,
    verificationSummary,
    activationBatchSummary,
    rollbackVerificationSummary,
    rejectionReasonCounts,
    migrationStatus,
    manifestValidationStatus: {
      rollbackMatchesWriteSet,
      attemptedManifestDocumentCount: manifestDocIds.length,
      attemptedManifestChunkCount: manifestChunkIds.length,
      trustedManifestDocumentCount: trustedDocIds.length
    },
    writePathValidationStatus: {
      documentsActivatedCount: documentsActivated.length,
      chunksActivatedCount: chunksActivated.length,
      documentsRejectedCount,
      chunksRejectedCount,
      provenanceFailuresCount
    },
    rollbackValidationStatus: {
      rollbackVerificationPassed,
      rollbackMatchesWriteSet,
      rollbackDocumentCount: rollbackDocIds.length,
      rollbackChunkCount: rollbackChunkIds.length
    },
    documentsActivated,
    chunksActivated,
    documentsRejectedFromWrite,
    chunksRejectedFromWrite,
    verification: {
      queryableChunkCount,
      expectedChunkCount: chunksActivated.length,
      expectedDocumentCount: documentsActivated.length,
      expectedDocumentIds: expectedDocumentSet,
      expectedChunkIds: expectedChunkSet,
      rollbackDocumentIds: rollbackDocIds,
      rollbackChunkIds: rollbackChunkIds
    }
  };
}

export async function rollbackTrustedRetrievalActivation(env: Env, rawInput: unknown) {
  const input = parseRollbackInput(rawInput);
  const now = new Date().toISOString();
  const rollbackManifest = input.rollbackManifest;
  const rollbackBatchIds = uniqueSorted(rollbackManifest.activationBatchIdsReversed || []);
  const rollbackBatchId = String(input.rollbackBatchId || rollbackManifest.rollbackBatchIds?.[0] || `rollback_${stableHash(now).slice(0, 12)}`);

  await ensureActivationTables(env);
  const migrationStatus = await readActivationMigrationStatus(env);

  const manifestDocIds = uniqueSorted(rollbackManifest.documentsToRemove || []);
  const manifestChunkIds = uniqueSorted(rollbackManifest.chunksToRemove || []);
  if (!manifestDocIds.length && !manifestChunkIds.length) {
    throw new Error("Rollback manifest has no documents/chunks to remove");
  }

  const chunkPlaceholders = manifestChunkIds.map(() => "?").join(",");
  const docPlaceholders = manifestDocIds.map(() => "?").join(",");
  const batchPlaceholders = rollbackBatchIds.map(() => "?").join(",");

  const batchChunkClause = rollbackBatchIds.length ? ` AND batch_id IN (${batchPlaceholders})` : "";
  const batchDocClause = rollbackBatchIds.length ? ` AND batch_id IN (${batchPlaceholders})` : "";

  const tableStateBefore = await readRollbackTableState(env, {
    manifestDocIds,
    manifestChunkIds,
    rollbackBatchIds
  });

  const chunkRows = manifestChunkIds.length
    ? await env.DB.prepare(
        `SELECT chunk_id as chunkId, document_id as documentId, batch_id as batchId
         FROM retrieval_search_chunks
         WHERE chunk_id IN (${chunkPlaceholders})${batchChunkClause}`
      )
        .bind(...manifestChunkIds, ...(rollbackBatchIds.length ? rollbackBatchIds : []))
        .all<{ chunkId: string; documentId: string; batchId: string }>()
    : { results: [] as Array<{ chunkId: string; documentId: string; batchId: string }> };

  const docRows = manifestDocIds.length
    ? await env.DB.prepare(
        `SELECT document_id as documentId, batch_id as batchId, trust_source as trustSource
         FROM retrieval_activation_documents
         WHERE document_id IN (${docPlaceholders})${batchDocClause}`
      )
        .bind(...manifestDocIds, ...(rollbackBatchIds.length ? rollbackBatchIds : []))
        .all<{ documentId: string; batchId: string; trustSource: string }>()
    : { results: [] as Array<{ documentId: string; batchId: string; trustSource: string }> };

  const matchedChunkRows = chunkRows.results || [];
  const matchedDocRows = docRows.results || [];
  const matchedChunkIds = chunkIds(matchedChunkRows);
  const matchedDocIds = documentIds(matchedDocRows);

  const chunksMissingFromRollbackTarget = manifestChunkIds.filter((chunkId) => !matchedChunkIds.includes(chunkId));
  const docsMissingFromRollbackTarget = manifestDocIds.filter((docId) => !matchedDocIds.includes(docId));

  let removedChunkCount = 0;
  let removedDocumentCount = 0;

  if (!input.dryRun) {
    if (manifestChunkIds.length) {
      await env.DB.prepare(
        `UPDATE retrieval_search_chunks
         SET active = 0
         WHERE chunk_id IN (${chunkPlaceholders})${batchChunkClause}`
      )
        .bind(...manifestChunkIds, ...(rollbackBatchIds.length ? rollbackBatchIds : []))
        .run();

      await env.DB.prepare(
        `DELETE FROM retrieval_search_rows
         WHERE chunk_id IN (${chunkPlaceholders})${batchChunkClause}`
      )
        .bind(...manifestChunkIds, ...(rollbackBatchIds.length ? rollbackBatchIds : []))
        .run();

      await env.DB.prepare(
        `DELETE FROM retrieval_embedding_rows
         WHERE chunk_id IN (${chunkPlaceholders})${batchChunkClause}`
      )
        .bind(...manifestChunkIds, ...(rollbackBatchIds.length ? rollbackBatchIds : []))
        .run();

      await env.DB.prepare(
        `DELETE FROM retrieval_activation_chunks
         WHERE chunk_id IN (${chunkPlaceholders})${batchChunkClause}`
      )
        .bind(...manifestChunkIds, ...(rollbackBatchIds.length ? rollbackBatchIds : []))
        .run();
    }

    if (manifestDocIds.length) {
      await env.DB.prepare(
        `DELETE FROM retrieval_activation_documents
         WHERE document_id IN (${docPlaceholders})${batchDocClause}`
      )
        .bind(...manifestDocIds, ...(rollbackBatchIds.length ? rollbackBatchIds : []))
        .run();
    }
  }

  const activeChunkRows = manifestChunkIds.length
    ? await env.DB.prepare(
        `SELECT chunk_id as chunkId
         FROM retrieval_search_chunks
         WHERE chunk_id IN (${chunkPlaceholders}) AND active = 1`
      )
        .bind(...manifestChunkIds)
        .all<{ chunkId: string }>()
    : { results: [] as Array<{ chunkId: string }> };

  const remainingActiveChunkIds = uniqueSorted((activeChunkRows.results || []).map((row) => row.chunkId));
  removedChunkCount = manifestChunkIds.length - remainingActiveChunkIds.length;

  const remainingDocRows = manifestDocIds.length
    ? await env.DB.prepare(
        `SELECT document_id as documentId
         FROM retrieval_activation_documents
         WHERE document_id IN (${docPlaceholders})${batchDocClause}`
      )
        .bind(...manifestDocIds, ...(rollbackBatchIds.length ? rollbackBatchIds : []))
        .all<{ documentId: string }>()
    : { results: [] as Array<{ documentId: string }> };
  const remainingDocIds = uniqueSorted((remainingDocRows.results || []).map((row) => row.documentId));
  removedDocumentCount = manifestDocIds.length - remainingDocIds.length;

  const remainingDocRowsAnyBatch = manifestDocIds.length
    ? await env.DB.prepare(
        `SELECT document_id as documentId
         FROM retrieval_activation_documents
         WHERE document_id IN (${docPlaceholders})`
      )
        .bind(...manifestDocIds)
        .all<{ documentId: string }>()
    : { results: [] as Array<{ documentId: string }> };
  const remainingDocIdsAnyBatch = uniqueSorted((remainingDocRowsAnyBatch.results || []).map((row) => row.documentId));

  const tableStateAfter = await readRollbackTableState(env, {
    manifestDocIds,
    manifestChunkIds,
    rollbackBatchIds
  });

  const rollbackVerificationPassed = remainingActiveChunkIds.length === 0 && remainingDocIds.length === 0;

  const summary = {
    rollbackBatchId,
    attemptedDocumentCount: manifestDocIds.length,
    attemptedChunkCount: manifestChunkIds.length,
    removedDocumentCount,
    removedChunkCount,
    docsMissingFromRollbackTargetCount: docsMissingFromRollbackTarget.length,
    chunksMissingFromRollbackTargetCount: chunksMissingFromRollbackTarget.length,
    rollbackVerificationPassed
  };

  return {
    readOnly: Boolean(input.dryRun),
    generatedAt: now,
    summary,
    migrationStatus,
    rollbackManifestSummary: {
      rollbackBatchIds,
      rollbackBatchId,
      rollbackChecksum: rollbackManifest.integrity?.rollbackChecksum || "",
      activationBatchIdsReversed: rollbackManifest.activationBatchIdsReversed || []
    },
    tableStateBefore,
    tableStateAfter,
    removalDetails: {
      matchedDocumentIds: matchedDocIds,
      matchedChunkIds: matchedChunkIds,
      docsMissingFromRollbackTarget,
      chunksMissingFromRollbackTarget,
      remainingActiveChunkIds,
      remainingDocumentIds: remainingDocIds,
      remainingDocumentIdsAnyBatch: remainingDocIdsAnyBatch
    },
    rollbackVerificationDiagnostics: {
      rollbackTargetsCleared: remainingActiveChunkIds.length === 0 && remainingDocIds.length === 0,
      idempotentReplayObserved:
        (docsMissingFromRollbackTarget.length > 0 || chunksMissingFromRollbackTarget.length > 0) &&
        remainingActiveChunkIds.length === 0 &&
        remainingDocIds.length === 0
    }
  };
}
