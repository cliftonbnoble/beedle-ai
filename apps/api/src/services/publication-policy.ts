import { APPROVAL_POLICY_VERSION } from "./approval-policy";
import type { SearchRequest } from "@beedle/shared";

// SQL aliases are internal constants, never user input. Readiness, publication,
// and human approval are separate; vector activation cannot confer trust.
export function readyDocumentSql(alias = "d"): string {
  return `(${alias}.ingestion_state = 'ready' AND ${alias}.ingestion_completed_at IS NOT NULL
    AND ${alias}.expected_chunk_count > 0 AND ${alias}.rejected_at IS NULL
    AND COALESCE(${alias}.source_r2_key, '') != ''
    AND NOT EXISTS(SELECT 1 FROM document_source_revisions sr JOIN source_records rec ON rec.record_id=sr.record_id
      WHERE sr.document_id=${alias}.id AND rec.published_source_revision>sr.source_revision))`;
}
export function publishedDocumentSql(alias = "d"): string {
  return `(${readyDocumentSql(alias)} AND ${alias}.searchable_at IS NOT NULL
    AND ${alias}.published_revision = ${alias}.document_revision)`;
}
export function currentReferenceCatalogSql(alias = "d"): string {
  return `(${alias}.reference_catalog_version = (SELECT active_version FROM legal_catalog_state WHERE singleton=1))`;
}
export function trustedDocumentSql(alias = "d"): string {
  return `(${publishedDocumentSql(alias)} AND ${alias}.approved_at IS NOT NULL
    AND ${alias}.approved_revision = ${alias}.document_revision AND ${alias}.qc_required_confirmed = 1
    AND ${alias}.approval_policy_version = '${APPROVAL_POLICY_VERSION}' AND ${currentReferenceCatalogSql(alias)})`;
}
export function searchPublicationSql(input: Pick<SearchRequest, "corpusMode" | "filters">, alias = "d"): string {
  return input.corpusMode === "trusted_only" || input.filters.approvedOnly ? trustedDocumentSql(alias) : publishedDocumentSql(alias);
}
export function trustLabelSql(alias = "d"): string {
  return `CASE WHEN ${trustedDocumentSql(alias)} THEN 1 ELSE 0 END`;
}
