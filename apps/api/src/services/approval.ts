import type { Env } from "../lib/types";
import { computeApprovalReadiness, APPROVAL_POLICY_VERSION } from "./approval-policy";
import { readyDocumentSql, currentReferenceCatalogSql } from "./publication-policy";
import { detectCriticalReferenceExceptions } from "./qc-shared";

export async function approveDocumentRevision(env: Env, documentId: string, expectedRevision: number) {
  // One database snapshot for all policy inputs. Reference and evidence triggers
  // advance the revision if any input changes before the conditional write.
  const row = await env.DB.prepare(`SELECT d.file_type AS fileType, d.document_revision AS documentRevision,
    ${readyDocumentSql()} AS ingestionReady, ${currentReferenceCatalogSql()} AS referenceCatalogCurrent, d.approved_at AS approvedAt, d.rejected_at AS rejectedAt,
    (d.qc_passed AND d.qc_has_index_codes AND d.qc_has_rules_section AND d.qc_has_ordinance_section) AS qcPassed,
    d.qc_required_confirmed AS qcRequiredConfirmed, d.extraction_confidence AS extractionConfidence,
    COALESCE(json_array_length(d.extraction_warnings_json), 0) AS warningCount,
    (SELECT COUNT(*) FROM document_reference_issues i WHERE i.document_id = d.id) AS unresolvedReferenceCount,
    (SELECT COUNT(DISTINCT normalized_value) FROM document_reference_links r WHERE r.document_id = d.id
      AND normalized_value IN ('37.2(g)', '37.15', '10.10(c)(3)')) AS criticalExceptionCount,
    d.rules_sections_json AS rulesJson, d.ordinance_sections_json AS ordinanceJson,
    NOT d.qc_has_rules_section AS missingRulesDetection, NOT d.qc_has_ordinance_section AS missingOrdinanceDetection
    FROM documents d WHERE d.id = ?`).bind(documentId).first<{
      fileType: "decision_docx" | "law_pdf"; documentRevision: number; ingestionReady: number; referenceCatalogCurrent: number;
      approvedAt: string | null; rejectedAt: string | null; qcPassed: number; qcRequiredConfirmed: number;
      extractionConfidence: number; warningCount: number; unresolvedReferenceCount: number;
      criticalExceptionCount: number; rulesJson: string; ordinanceJson: string;
      missingRulesDetection: number; missingOrdinanceDetection: number;
    }>();
  if (!row) return { approved: false, reason: "Decision document not found", conflict: false };
  if (!Number.isSafeInteger(expectedRevision) || expectedRevision !== row.documentRevision) {
    return { approved: false, reason: "Document changed; reload and review the current revision", conflict: true };
  }
  const readiness = computeApprovalReadiness({ ...row,
    criticalExceptionCount: Math.max(row.criticalExceptionCount, detectCriticalReferenceExceptions({
      rules: JSON.parse(row.rulesJson), ordinance: JSON.parse(row.ordinanceJson)
    }).length),
    missingRulesDetection: Boolean(row.missingRulesDetection), missingOrdinanceDetection: Boolean(row.missingOrdinanceDetection),
    lowConfidenceTaxonomy: false
  });
  if (!readiness.eligible) return { approved: false, reason: `Approval blocked: ${readiness.blockers.join(", ")}`, readiness, conflict: false };
  const now = new Date().toISOString();
  const result = await env.DB.prepare(`UPDATE documents SET approved_at = ?, approved_revision = document_revision,
    approval_policy_version = ?, published_revision = document_revision, searchable_at = COALESCE(searchable_at, ?), updated_at = ?
    WHERE id = ? AND document_revision = ? AND ${readyDocumentSql("documents")}
      AND approved_at IS NULL AND qc_required_confirmed = 1 AND ${currentReferenceCatalogSql("documents")} RETURNING id`)
    .bind(now, APPROVAL_POLICY_VERSION, now, now, documentId, expectedRevision).first<{id:string}>();
  return result?.id === documentId
    ? { approved: true, documentRevision: expectedRevision, policyVersion: APPROVAL_POLICY_VERSION, conflict: false }
    : { approved: false, reason: "Document changed during approval; reload and review it", conflict: true };
}
