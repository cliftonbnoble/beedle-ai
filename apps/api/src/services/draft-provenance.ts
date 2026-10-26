import type { CaseAssistantResponse, DraftConclusionsResponse } from "@beedle/shared";
import type { Env } from "../lib/types";

export async function captureDraftProvenance(env: Env, authorities: CaseAssistantResponse["similar_cases"]): Promise<NonNullable<DraftConclusionsResponse["provenance"]>> {
  const ids = [...new Set(authorities.filter(row => row.source_kind !== "provision_candidate").map(row => row.document_id))];
  const revisions = new Map<string, { revision: string; parser: string | null }>();
  if (ids.length && env.DB) {
    const rows = await env.DB.prepare(`SELECT id, updated_at AS revision, metadata_json AS metadata FROM documents WHERE id IN (${ids.map(() => "?").join(",")})`)
      .bind(...ids).all<{ id: string; revision: string; metadata: string | null }>();
    for (const row of rows.results || []) {
      let parser: string | null = null;
      try { const value = JSON.parse(row.metadata || "{}"); if (typeof value.extraction?.parser === "string") parser = value.extraction.parser; } catch { /* Legacy metadata has no extraction provenance. */ }
      revisions.set(row.id, { revision: row.revision, parser });
    }
  }
  return { corpus_version: null, sources: await Promise.all(authorities.map(async authority => ({
    citation_id: authority.citation_id, document_id: authority.document_id,
    document_revision: authority.source_revision || revisions.get(authority.document_id)?.revision || null,
    extraction_version: revisions.get(authority.document_id)?.parser || null,
    revision_status: authority.source_revision ? "captured_with_excerpt" as const : revisions.has(authority.document_id) ? "observed_after_retrieval" as const : "unavailable" as const,
    excerpt_sha256: Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(authority.snippet)))).map(byte => byte.toString(16).padStart(2, "0")).join("")
  }))) };
}
