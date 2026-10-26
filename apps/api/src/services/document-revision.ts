import type { Env } from "../lib/types";
import { RequestConflictError, RequestValidationError } from "../lib/http";

// The guard trigger aborts the entire D1 batch on a stale read. Its assertion and
// all writes share a transaction; no durable lock or cleanup after failure is needed.
export async function withDocumentRevision(env: Env, documentId: string, expectedRevision: number, statements: D1PreparedStatement[]) {
  if (statements.length > 90) throw new RequestValidationError("This document requires a staged revision update; too many reference mutations for an atomic review update.");
  const token = crypto.randomUUID();
  try {
    return await env.DB.batch([
      env.DB.prepare("INSERT INTO document_revision_guards (token, document_id, expected_revision) VALUES (?, ?, ?)").bind(token, documentId, expectedRevision),
      ...statements,
      env.DB.prepare("DELETE FROM document_revision_guards WHERE token = ?").bind(token)
    ]);
  } catch (error) {
    if (String(error).includes("document_revision_conflict")) throw new RequestConflictError("Document changed; reload and review its current revision.");
    throw error;
  }
}

export async function assertDocumentRevision(env: Env, documentId: string, expectedRevision: number) {
  const current = await env.DB.prepare("SELECT document_revision AS revision FROM documents WHERE id = ?").bind(documentId).first<{revision: number}>();
  if (current?.revision !== expectedRevision) throw new RequestConflictError("Document changed while loading; reload its current revision.");
}
