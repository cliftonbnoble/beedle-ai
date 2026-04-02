import { z } from "zod";
import type { Env } from "../lib/types";
import { embed } from "./embeddings";

const backfillRequestSchema = z.object({
  batchSize: z.number().int().min(1).max(100).default(25),
  limit: z.number().int().min(1).max(5000).optional(),
  includeDocumentChunks: z.boolean().default(true),
  includeTrustedChunks: z.boolean().default(true),
  dryRun: z.boolean().default(false)
});

type BackfillRow = {
  chunkId: string;
  documentId: string;
  sourceText: string;
  sectionLabel: string;
  paragraphAnchor: string;
  citationAnchor: string;
  sourceKind: "document_chunk" | "trusted_chunk";
};

type FailedBackfillRow = {
  chunkId: string;
  documentId: string;
  sectionLabel: string;
  paragraphAnchor: string;
  citationAnchor: string;
  sourceKind: "document_chunk" | "trusted_chunk";
  reason: string;
};

function uniqByChunk(rows: BackfillRow[]): BackfillRow[] {
  const seen = new Set<string>();
  const out: BackfillRow[] = [];
  for (const row of rows) {
    if (seen.has(row.chunkId)) continue;
    seen.add(row.chunkId);
    out.push(row);
  }
  return out;
}

async function loadDocumentChunkRows(env: Env, limit?: number): Promise<BackfillRow[]> {
  const query = `
    SELECT
      c.id as chunkId,
      c.document_id as documentId,
      c.chunk_text as sourceText,
      c.section_label as sectionLabel,
      c.paragraph_anchor as paragraphAnchor,
      c.citation_anchor as citationAnchor
    FROM document_chunks c
    JOIN documents d ON d.id = c.document_id
    WHERE d.searchable_at IS NOT NULL
      AND d.rejected_at IS NULL
    ORDER BY d.searchable_at DESC, c.chunk_order ASC
    ${typeof limit === "number" ? "LIMIT ?" : ""}
  `;
  const stmt = env.DB.prepare(query);
  const result = typeof limit === "number" ? await stmt.bind(limit).all<BackfillRow>() : await stmt.all<BackfillRow>();
  return (result.results || []).map((row) => ({ ...row, sourceKind: "document_chunk" as const }));
}

async function loadTrustedChunkRows(env: Env, limit?: number): Promise<BackfillRow[]> {
  const query = `
    SELECT
      rs.chunk_id as chunkId,
      rs.document_id as documentId,
      rs.chunk_text as sourceText,
      rs.section_label as sectionLabel,
      rs.paragraph_anchor as paragraphAnchor,
      rs.citation_anchor as citationAnchor
    FROM retrieval_search_chunks rs
    JOIN documents d ON d.id = rs.document_id
    WHERE rs.active = 1
      AND d.rejected_at IS NULL
    ORDER BY rs.created_at DESC
    ${typeof limit === "number" ? "LIMIT ?" : ""}
  `;
  const stmt = env.DB.prepare(query);
  const result = typeof limit === "number" ? await stmt.bind(limit).all<BackfillRow>() : await stmt.all<BackfillRow>();
  return (result.results || []).map((row) => ({ ...row, sourceKind: "trusted_chunk" as const }));
}

export async function backfillRetrievalVectors(env: Env, input: unknown) {
  const parsed = backfillRequestSchema.parse(input || {});
  const aiAvailable = Boolean(env.AI);
  const vectorBindingPresent = Boolean(env.VECTOR_INDEX);

  const sourceRows = uniqByChunk([
    ...(parsed.includeDocumentChunks ? await loadDocumentChunkRows(env, parsed.limit) : []),
    ...(parsed.includeTrustedChunks ? await loadTrustedChunkRows(env, parsed.limit) : [])
  ]);

  const batches: Array<{
    batchIndex: number;
    rowCount: number;
    embeddedCount: number;
    upsertedCount: number;
    failedCount: number;
  }> = [];

  let processedCount = 0;
  let embeddedCount = 0;
  let upsertedCount = 0;
  let failedCount = 0;
  const failedRows: FailedBackfillRow[] = [];

  for (let start = 0; start < sourceRows.length; start += parsed.batchSize) {
    const batchRows = sourceRows.slice(start, start + parsed.batchSize);
    const payload: VectorizeVector[] = [];
    let batchEmbedded = 0;
    let batchFailed = 0;

    for (const row of batchRows) {
      processedCount += 1;
      try {
        const vector = await embed(env, row.sourceText);
        if (!vector) {
          batchFailed += 1;
          failedCount += 1;
          failedRows.push({
            chunkId: row.chunkId,
            documentId: row.documentId,
            sectionLabel: row.sectionLabel,
            paragraphAnchor: row.paragraphAnchor,
            citationAnchor: row.citationAnchor,
            sourceKind: row.sourceKind,
            reason: "empty_embedding"
          });
          continue;
        }
        batchEmbedded += 1;
        embeddedCount += 1;
        payload.push({
          id: row.chunkId,
          values: vector,
          namespace: env.VECTOR_NAMESPACE,
          metadata: {
            documentId: row.documentId,
            paragraphAnchor: row.paragraphAnchor,
            citationAnchor: row.citationAnchor,
            sectionLabel: row.sectionLabel,
            sourceKind: row.sourceKind
          } as Record<string, VectorizeVectorMetadata>
        });
      } catch (error) {
        batchFailed += 1;
        failedCount += 1;
        failedRows.push({
          chunkId: row.chunkId,
          documentId: row.documentId,
          sectionLabel: row.sectionLabel,
          paragraphAnchor: row.paragraphAnchor,
          citationAnchor: row.citationAnchor,
          sourceKind: row.sourceKind,
          reason: error instanceof Error ? error.message : String(error)
        });
      }
    }

    if (!parsed.dryRun && payload.length > 0) {
      await env.VECTOR_INDEX.upsert(payload);
      upsertedCount += payload.length;
    }

    batches.push({
      batchIndex: batches.length + 1,
      rowCount: batchRows.length,
      embeddedCount: batchEmbedded,
      upsertedCount: parsed.dryRun ? 0 : payload.length,
      failedCount: batchFailed
    });
  }

  return {
    generatedAt: new Date().toISOString(),
    dryRun: parsed.dryRun,
    aiAvailable,
    vectorBindingPresent,
    vectorNamespace: env.VECTOR_NAMESPACE,
    embeddingModel: env.AI_EMBEDDING_MODEL,
    counts: {
      discoveredChunkCount: sourceRows.length,
      processedCount,
      embeddedCount,
      upsertedCount,
      failedCount
    },
    sourceCounts: {
      documentChunkCount: sourceRows.filter((row) => row.sourceKind === "document_chunk").length,
      trustedChunkCount: sourceRows.filter((row) => row.sourceKind === "trusted_chunk").length
    },
    batches,
    failedRows
  };
}
