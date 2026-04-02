import { z } from "zod";
import type { Env } from "../lib/types";
import { embed } from "./embeddings";

const probeRequestSchema = z.object({
  chunkId: z.string().min(1).optional(),
  queryText: z.string().min(1).optional(),
  topK: z.number().int().min(1).max(25).default(5),
  useNamespace: z.boolean().default(true)
}).refine((value) => Boolean(value.chunkId || value.queryText), {
  message: "chunkId or queryText is required"
});

type ProbeSourceRow = {
  chunkId: string;
  documentId: string;
  chunkText: string;
  sectionLabel: string;
  citationAnchor: string;
};

async function loadChunkSource(env: Env, chunkId: string): Promise<ProbeSourceRow | null> {
  const direct = await env.DB.prepare(
    `SELECT
      c.id as chunkId,
      c.document_id as documentId,
      c.chunk_text as chunkText,
      c.section_label as sectionLabel,
      c.citation_anchor as citationAnchor
     FROM document_chunks c
     WHERE c.id = ?`
  )
    .bind(chunkId)
    .first<ProbeSourceRow>();
  if (direct) return direct;

  const trusted = await env.DB.prepare(
    `SELECT
      rs.chunk_id as chunkId,
      rs.document_id as documentId,
      rs.chunk_text as chunkText,
      rs.section_label as sectionLabel,
      rs.citation_anchor as citationAnchor
     FROM retrieval_search_chunks rs
     WHERE rs.chunk_id = ?`
  )
    .bind(chunkId)
    .first<ProbeSourceRow>();
  return trusted || null;
}

export async function probeRetrievalVectors(env: Env, input: unknown) {
  const parsed = probeRequestSchema.parse(input || {});
  const sourceRow = parsed.chunkId ? await loadChunkSource(env, parsed.chunkId) : null;
  const queryText = parsed.queryText || sourceRow?.chunkText || "";
  const vector = await embed(env, queryText);

  if (!vector) {
    return {
      ok: false,
      aiAvailable: Boolean(env.AI),
      vectorNamespace: env.VECTOR_NAMESPACE,
      reason: "embedding_unavailable"
    };
  }

  const result = await env.VECTOR_INDEX.query(vector, {
    topK: parsed.topK,
    ...(parsed.useNamespace ? { namespace: env.VECTOR_NAMESPACE } : {}),
    returnMetadata: true
  });

  return {
    ok: true,
    aiAvailable: Boolean(env.AI),
    vectorNamespace: env.VECTOR_NAMESPACE,
    useNamespace: parsed.useNamespace,
    queryTextPreview: queryText.slice(0, 220),
    sourceRow: sourceRow
      ? {
          chunkId: sourceRow.chunkId,
          documentId: sourceRow.documentId,
          sectionLabel: sourceRow.sectionLabel,
          citationAnchor: sourceRow.citationAnchor
        }
      : null,
    matchCount: result.matches.length,
    matches: result.matches.map((match) => ({
      id: match.id,
      score: match.score,
      metadata: match.metadata || null
    }))
  };
}
