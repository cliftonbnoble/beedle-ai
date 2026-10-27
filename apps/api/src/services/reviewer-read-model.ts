// Bounded reviewer SQL hydration and detail assembly using the shared pure policy.
import { knownCalendarDate } from "@beedle/shared";
import { referenceEditionBindingSchema } from "@beedle/shared";
import { readReviewerOverrides } from "./reviewer-overrides";
import { RequestValidationError } from "../lib/http";
import { computeApprovalReadiness } from "./approval-policy";
import { assertDocumentRevision } from "./document-revision";
import { readyDocumentSql, currentReferenceCatalogSql } from "./publication-policy";
import type { Env } from "../lib/types";
import { reviewSourceLink } from "./storage";
import { detectCriticalReferenceExceptions } from "./qc-shared";
import { storedTaxonomySuggestion } from "./taxonomy-inference";
import { asObject, computeBlocked37xReview, computeReviewerReadiness, computeRuntimeReviewerPolicy, computeUnresolvedTriage, isLikelyFixtureDoc, parseJsonArray, parseJsonObject, qcFailedRequirements } from './reviewer-policy';
import type { UnresolvedIssueLite } from './reviewer-policy';



export class IngestionListBuildError extends Error {
  operation: string;
  subOperation: string;
  selectedDocCount: number;
  chunkingEnabled: boolean;
  chunkSize: number;
  chunkCount: number;
  currentChunkIndex: number;
  idsInCurrentChunk: number;
  queryKind: string;
  causeMessage: string;

  constructor(params: {
    operation: string;
    subOperation: string;
    selectedDocCount: number;
    chunkingEnabled: boolean;
    chunkSize: number;
    chunkCount: number;
    currentChunkIndex: number;
    idsInCurrentChunk: number;
    queryKind: string;
    cause: unknown;
  }) {
    const causeMessage = params.cause instanceof Error ? params.cause.message : String(params.cause ?? "unknown");
    super(`${params.operation}/${params.subOperation} failed: ${causeMessage}`);
    this.name = "IngestionListBuildError";
    this.operation = params.operation;
    this.subOperation = params.subOperation;
    this.selectedDocCount = params.selectedDocCount;
    this.chunkingEnabled = params.chunkingEnabled;
    this.chunkSize = params.chunkSize;
    this.chunkCount = params.chunkCount;
    this.currentChunkIndex = params.currentChunkIndex;
    this.idsInCurrentChunk = params.idsInCurrentChunk;
    this.queryKind = params.queryKind;
    this.causeMessage = causeMessage;
  }
}




const LIST_DOCS_IN_QUERY_CHUNK = 75;

function chunkArray<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}


// Materialization and detail use the same policy functions; list SQL never approximates them.
export async function buildReviewerRows(env: Env, documentIds: string[]) {
  if (documentIds.length > 25) throw new RequestValidationError("Reviewer materialization is limited to 25 documents per step.");
  const metadataSource="CASE WHEN json_valid(d.metadata_json) THEN d.metadata_json ELSE '{}' END";
  const queueMetadata=`json_object('taxonomy',json_object('caseTypeId',json_extract(${metadataSource},'$.taxonomy.caseTypeId'),
    'caseTypeLabel',json_extract(${metadataSource},'$.taxonomy.caseTypeLabel'),'confidence',CASE WHEN json_type(${metadataSource},'$.taxonomy.confidence') IN ('integer','real') THEN json_extract(${metadataSource},'$.taxonomy.confidence') ELSE NULL END,
    'fallback',json_extract(${metadataSource},'$.taxonomy.fallback'),
    'inferenceVersion',json_extract(${metadataSource},'$.taxonomy.inferenceVersion'),'configVersion',json_extract(${metadataSource},'$.taxonomy.configVersion'),
    'reviewStatus',json_extract(${metadataSource},'$.taxonomy.reviewStatus')),'originalFilename',json_extract(${metadataSource},'$.originalFilename'))`;
  const rowBytes=["d.title","d.citation","d.case_number","d.decision_date","d.extraction_warnings_json","d.index_codes_json","d.rules_sections_json","d.ordinance_sections_json",queueMetadata]
    .map(column=>`COALESCE(length(CAST(${column} AS BLOB)),0)`).join('+');
  let rows;
  try {
    rows = await env.DB.prepare(
      `WITH issue_counts AS (
        SELECT document_id,COUNT(*) AS total,SUM(length(CAST(COALESCE(raw_value,'') AS BLOB))+length(CAST(COALESCE(normalized_value,'') AS BLOB))+length(CAST(COALESCE(message,'') AS BLOB))) AS bytes,
          SUM(reference_type='rules_section') AS rules,SUM(reference_type='ordinance_section') AS ordinance,
          SUM(reference_type='index_code') AS index_count,
          SUM(reference_type='ordinance_section' AND normalized_value IN ('ordinance37.3','ordinance37.7','ordinance37.9')) AS unsafe
        FROM document_reference_issues WHERE document_id IN (SELECT value FROM json_each(?1)) GROUP BY document_id
      ), link_counts AS (
        SELECT document_id,SUM(reference_type='index_code' AND is_valid=1) AS index_count,
          SUM(reference_type='rules_section' AND is_valid=1) AS rules,SUM(reference_type='ordinance_section' AND is_valid=1) AS ordinance,
          COUNT(DISTINCT CASE WHEN normalized_value IN ('37.2(g)','37.15','10.10(c)(3)') THEN normalized_value END) AS critical
        FROM document_reference_links WHERE document_id IN (SELECT value FROM json_each(?1)) GROUP BY document_id
      ) SELECT d.id,d.jurisdiction,d.file_type AS fileType,d.title,d.citation,d.case_number AS caseNumber,d.decision_date AS decisionDate,d.created_at AS createdAt,
        d.approved_at AS approvedAt,d.document_revision AS documentRevision,${readyDocumentSql()} AS ingestionReady,
        ${currentReferenceCatalogSql()} AS referenceCatalogCurrent,d.rejected_at AS rejectedAt,d.searchable_at AS searchableAt,
        d.qc_passed AS qcPassed,d.qc_has_index_codes AS qcHasIndexCodes,d.qc_has_rules_section AS qcHasRulesSection,
        d.qc_has_ordinance_section AS qcHasOrdinanceSection,d.qc_required_confirmed AS qcRequiredConfirmed,
        d.extraction_confidence AS extractionConfidence,d.extraction_warnings_json AS extractionWarningsJson,
        d.index_codes_json AS indexCodesJson,d.rules_sections_json AS rulesSectionsJson,d.ordinance_sections_json AS ordinanceSectionsJson,
        ${queueMetadata} AS metadataJson,COALESCE(json_array_length(d.extraction_warnings_json),0) AS warningCount,
        COALESCE(i.total,0) AS unresolvedReferenceCount,COALESCE(i.bytes,0) AS unresolvedBytes,COALESCE(i.rules,0) AS unresolvedRulesCount,
        COALESCE(i.ordinance,0) AS unresolvedOrdinanceCount,COALESCE(i.index_count,0) AS unresolvedIndexCount,
        COALESCE(i.unsafe,0) AS unresolvedUnsafe37xCount,COALESCE(l.index_count,0) AS validIndexRefCount,
        COALESCE(l.rules,0) AS validRulesRefCount,COALESCE(l.ordinance,0) AS validOrdinanceRefCount,COALESCE(l.critical,0) AS criticalExceptionCount
        FROM documents d LEFT JOIN issue_counts i ON i.document_id=d.id LEFT JOIN link_counts l ON l.document_id=d.id
        WHERE d.id IN (SELECT value FROM json_each(?1)) AND (${rowBytes})<=65536 ORDER BY d.id`
    ).bind(JSON.stringify(documentIds))
      .all<{
    id: string;
    jurisdiction:string;
    fileType: "decision_docx" | "law_pdf";
    title: string;
    citation: string;
    caseNumber: string | null;
    decisionDate: string | null;
    createdAt: string;
    documentRevision: number;
    ingestionReady: number; referenceCatalogCurrent: number;
    approvedAt: string | null;
    rejectedAt: string | null;
    searchableAt: string | null;
    qcPassed: number;
    qcHasIndexCodes: number;
    qcHasRulesSection: number;
    qcHasOrdinanceSection: number;
    qcRequiredConfirmed: number;
    extractionConfidence: number;
    extractionWarningsJson: string;
    indexCodesJson: string; rulesSectionsJson: string; ordinanceSectionsJson: string;
    metadataJson: string | null;
    warningCount: number;
    unresolvedReferenceCount: number;
    unresolvedBytes: number;
    unresolvedRulesCount: number;
    unresolvedOrdinanceCount: number;
    unresolvedIndexCount: number;
    unresolvedUnsafe37xCount: number;
    validIndexRefCount: number;
    validRulesRefCount: number;
    validOrdinanceRefCount: number;
      criticalExceptionCount: number;
    }>();
  } catch (error) {
    throw new IngestionListBuildError({
      operation: "list_ingestion_documents",
      subOperation: "select_documents",
      selectedDocCount: 0,
      chunkingEnabled: false,
      chunkSize: 0,
      chunkCount: 0,
      currentChunkIndex: 0,
      idsInCurrentChunk: 0,
      queryKind: "documents_select",
      cause: error
    });
  }

  const candidateRows = rows.results ?? [];
  if(candidateRows.length!==documentIds.length)throw new RequestValidationError("Reviewer rows changed or exceed the 64 KiB policy input limit; inspect metadata before retrying.");
  if(candidateRows.reduce((sum,row)=>sum+row.unresolvedReferenceCount,0)>2000 || candidateRows.reduce((sum,row)=>sum+row.unresolvedBytes,0)>1024*1024) {
    throw new RequestValidationError("Reviewer reference details exceed the 2,000-row/1 MiB step limit; inspect the source issue records before rebuilding.");
  }
  const docIds = candidateRows.map((row) => row.id);
  const unresolvedByDoc = new Map<string, UnresolvedIssueLite[]>();
  if (docIds.length > 0) {
    const chunks = chunkArray(docIds, LIST_DOCS_IN_QUERY_CHUNK);
    for (let i = 0; i < chunks.length; i += 1) {
      const chunk = chunks[i]!;
      const placeholders = chunk.map(() => "?").join(",");
      let issues;
      try {
        issues = await env.DB.prepare(
          `SELECT document_id as documentId,
                reference_type as referenceType,
                raw_value as rawValue,
                normalized_value as normalizedValue,
                message,
                severity
         FROM document_reference_issues
         WHERE document_id IN (${placeholders})
         ORDER BY created_at DESC`
        )
          .bind(...chunk)
          .all<{
          documentId: string;
          referenceType: string;
          rawValue: string;
          normalizedValue: string;
          message: string;
          severity: string;
          }>();
      } catch (error) {
        throw new IngestionListBuildError({
          operation: "list_ingestion_documents",
          subOperation: "load_unresolved_issues_by_chunk",
          selectedDocCount: docIds.length,
          chunkingEnabled: true,
          chunkSize: LIST_DOCS_IN_QUERY_CHUNK,
          chunkCount: chunks.length,
          currentChunkIndex: i,
          idsInCurrentChunk: chunk.length,
          queryKind: "document_reference_issues_by_doc_ids",
          cause: error
        });
      }
      for (const issue of issues.results ?? []) {
        const list = unresolvedByDoc.get(issue.documentId) ?? [];
        list.push({
          referenceType: issue.referenceType,
          rawValue: issue.rawValue,
          normalizedValue: issue.normalizedValue,
          message: issue.message,
          severity: issue.severity
        });
        unresolvedByDoc.set(issue.documentId, list);
      }
    }
  }

  const documents = candidateRows.map((row) => {
    const metadata = parseJsonObject(row.metadataJson);
    const taxonomy = storedTaxonomySuggestion(asObject(metadata.taxonomy),undefined,row.jurisdiction);
    const extractionWarnings = parseJsonArray(row.extractionWarningsJson);
    const filteredNoiseCount = extractionWarnings.filter((warning) => warning.toLowerCase().includes("extraction noise filtered")).length;
    const taxonomyConfidence = typeof taxonomy.confidence === "number" ? taxonomy.confidence : null;
    const taxonomyFallback = taxonomy.fallback;
    const doc = {
      ...row,
      decisionDate:knownCalendarDate(row.decisionDate),
      unverifiedDecisionDate:row.decisionDate&&!knownCalendarDate(row.decisionDate)?row.decisionDate:null,
      extractionWarnings,
      indexCodes: parseJsonArray(row.indexCodesJson),
      taxonomySuggestion: taxonomy,
      warningCount: row.warningCount || 0,
      filteredNoiseCount,
      unresolvedReferenceCount: row.unresolvedReferenceCount || 0,
      criticalExceptionCount: Math.max(row.criticalExceptionCount || 0, detectCriticalReferenceExceptions({ rules: parseJsonArray(row.rulesSectionsJson), ordinance: parseJsonArray(row.ordinanceSectionsJson) }).length),
      missingRulesDetection: row.qcHasRulesSection === 0,
      missingOrdinanceDetection: row.qcHasOrdinanceSection === 0,
      failedQcRequirements: qcFailedRequirements({
        qcHasIndexCodes: row.qcHasIndexCodes,
        qcHasRulesSection: row.qcHasRulesSection,
        qcHasOrdinanceSection: row.qcHasOrdinanceSection
      }),
      lowConfidenceTaxonomy: taxonomyFallback || (taxonomyConfidence !== null && taxonomyConfidence < 0.45),
      isLikelyFixture: isLikelyFixtureDoc({ title: row.title, citation: row.citation, metadata }),
      status: row.rejectedAt ? "rejected" : row.approvedAt ? "approved" : row.searchableAt ? "searchable" : "staged"
    };
    const approvalReadiness = computeApprovalReadiness({
      fileType: doc.fileType,
      ingestionReady: doc.ingestionReady,
      referenceCatalogCurrent: doc.referenceCatalogCurrent,
      approvedAt: doc.approvedAt,
      rejectedAt: doc.rejectedAt,
      qcPassed: doc.qcPassed && doc.qcHasIndexCodes && doc.qcHasRulesSection && doc.qcHasOrdinanceSection,
      qcRequiredConfirmed: doc.qcRequiredConfirmed,
      warningCount: doc.warningCount,
      unresolvedReferenceCount: doc.unresolvedReferenceCount,
      criticalExceptionCount: doc.criticalExceptionCount,
      extractionConfidence: doc.extractionConfidence || 0,
      missingRulesDetection: doc.missingRulesDetection,
      missingOrdinanceDetection: doc.missingOrdinanceDetection,
      lowConfidenceTaxonomy: doc.lowConfidenceTaxonomy
    });
    const reviewer = computeReviewerReadiness({
      isLikelyFixture: doc.isLikelyFixture,
      blockers: approvalReadiness.blockers,
      qcPassed: doc.qcPassed && doc.qcHasIndexCodes && doc.qcHasRulesSection && doc.qcHasOrdinanceSection,
      qcRequiredConfirmed: doc.qcRequiredConfirmed,
      failedQcRequirements: doc.failedQcRequirements,
      unresolvedReferenceCount: doc.unresolvedReferenceCount,
      unresolvedRulesCount: row.unresolvedRulesCount || 0,
      unresolvedOrdinanceCount: row.unresolvedOrdinanceCount || 0,
      unresolvedIndexCount: row.unresolvedIndexCount || 0,
      unresolvedUnsafe37xCount: row.unresolvedUnsafe37xCount || 0,
      criticalExceptionCount: doc.criticalExceptionCount,
      extractionConfidence: doc.extractionConfidence || 0,
      validIndexRefCount: row.validIndexRefCount || 0,
      validRulesRefCount: row.validRulesRefCount || 0,
      validOrdinanceRefCount: row.validOrdinanceRefCount || 0,
      missingRulesDetection: doc.missingRulesDetection,
      missingOrdinanceDetection: doc.missingOrdinanceDetection,
      approvalThresholds: { maxUnresolvedReferences: approvalReadiness.thresholds.maxUnresolvedReferences }
    });
    const triage = computeUnresolvedTriage(unresolvedByDoc.get(row.id) ?? []);
    const blocked37x = computeBlocked37xReview(unresolvedByDoc.get(row.id) ?? []);
    const runtimePolicy = computeRuntimeReviewerPolicy({
      triage,
      blocked37xReferences: blocked37x.blocked37xReferences,
      unresolvedReferenceCount: doc.unresolvedReferenceCount,
      documentId: doc.id,
      title: doc.title,
      unresolvedBuckets: triage.unresolvedBuckets,
      recurringCitationFamilies: triage.recurringCitationFamilies
    });
    return {
      ...doc,
      approvalReadiness,
      reviewerReady: reviewer.reviewerReady,
      reviewerReadyReasons: reviewer.reviewerReadyReasons,
      reviewerRequiredActions: reviewer.reviewerRequiredActions,
      reviewerRiskLevel: reviewer.reviewerRiskLevel,
      metadataConfirmationWouldUnlock: reviewer.metadataConfirmationWouldUnlock,
      unresolvedBlockersAfterConfirmation: reviewer.unresolvedBlockersAfterConfirmation,
      unresolvedBuckets: triage.unresolvedBuckets,
      topRecommendedReviewerAction: runtimePolicy.runtimeSurfaceForManualReview
        ? triage.topRecommendedReviewerAction
        : runtimePolicy.runtimePolicyReason,
      estimatedReviewerEffort: triage.estimatedReviewerEffort,
      candidateManualFixes: runtimePolicy.runtimeSurfaceForManualReview ? triage.candidateManualFixes : [],
      recurringCitationFamilies: triage.recurringCitationFamilies,
      runtimeDisposition: runtimePolicy.runtimeDisposition,
      runtimeManualReasonCode: runtimePolicy.runtimeManualReasonCode,
      runtimeManualReasonSummary: runtimePolicy.runtimeManualReasonSummary,
      runtimeSuggestedOperatorAction: runtimePolicy.runtimeSuggestedOperatorAction,
      runtimeOperatorReviewSummary: runtimePolicy.runtimeOperatorReviewSummary,
      runtimeReviewDiagnostic: runtimePolicy.runtimeReviewDiagnostic,
      runtimePolicyReason: runtimePolicy.runtimePolicyReason,
      runtimeSurfaceForManualReview: runtimePolicy.runtimeSurfaceForManualReview,
      runtimeManualReviewRequired: runtimePolicy.runtimeManualReviewRequired,
      runtimeDoNotAutoApply: runtimePolicy.runtimeDoNotAutoApply,
      blocked37xReferences: blocked37x.blocked37xReferences,
      blocked37xReason: blocked37x.blocked37xReason,
      blocked37xReviewerHint: blocked37x.blocked37xReviewerHint,
      blocked37xSafeToBatchReview: blocked37x.blocked37xSafeToBatchReview,
      blocked37xBatchKey: blocked37x.blocked37xBatchKey
    };
  });

  return documents;
}


export async function getIngestionDocumentDetail(env: Env, documentId: string) {
  const document = await env.DB.prepare(
    `SELECT
      id,
      file_type as fileType,
      jurisdiction,
      title,
      citation,
      decision_date as decisionDate,
      case_number as caseNumber,
      author_name as authorName,
      outcome_label as outcomeLabel,
      source_r2_key as sourceFileRef,
      source_link as sourceLink,
      qc_has_index_codes as qcHasIndexCodes,
      qc_has_rules_section as qcHasRulesSection,
      qc_has_ordinance_section as qcHasOrdinanceSection,
      qc_passed as qcPassed,
      qc_required_confirmed as qcRequiredConfirmed,
      approved_at as approvedAt,
      active_text_generation as activeTextGeneration, reviewer_overrides_json as reviewerOverridesJson,
      document_revision as documentRevision,
      ${readyDocumentSql("documents")} as ingestionReady,
      ${currentReferenceCatalogSql("documents")} as referenceCatalogCurrent,
      searchable_at as searchableAt,
      rejected_at as rejectedAt,
      rejected_reason as rejectedReason,
      extraction_confidence as extractionConfidence,
      extraction_warnings_json as extractionWarningsJson,
      index_codes_json as indexCodesJson,
      rules_sections_json as rulesSectionsJson,
      ordinance_sections_json as ordinanceSectionsJson,
      metadata_json as metadataJson,reference_edition_context_json as referenceEditionContextJson,(SELECT epoch FROM legal_catalog_state WHERE singleton=1) AS referenceRegistryEpoch,
      created_at as createdAt,
      updated_at as updatedAt
     FROM documents
     WHERE id = ?`
  )
    .bind(documentId)
    .first<{
      id: string;
      fileType: "decision_docx" | "law_pdf";
      jurisdiction: string;
      title: string;
      citation: string;
      decisionDate: string | null;
      caseNumber: string | null;
      authorName: string | null;
      outcomeLabel: string | null;
      sourceFileRef: string;
      sourceLink: string;
      qcHasIndexCodes: number;
      qcHasRulesSection: number;
      qcHasOrdinanceSection: number;
      qcPassed: number;
      qcRequiredConfirmed: number;
      activeTextGeneration: string; reviewerOverridesJson: string;
      documentRevision: number;
      ingestionReady: number; referenceCatalogCurrent: number;
      approvedAt: string | null;
      searchableAt: string | null;
      rejectedAt: string | null;
      rejectedReason: string | null;
      extractionConfidence: number;
      extractionWarningsJson: string;
      indexCodesJson: string;
      rulesSectionsJson: string;
      ordinanceSectionsJson: string;
      metadataJson: string | null;referenceEditionContextJson:string;referenceRegistryEpoch:number;
      createdAt: string;
      updatedAt: string;
    }>();

  if (!document) {
    return null;
  }

  const sections = await env.DB.prepare(
    `SELECT
      s.id,
      s.heading,
      s.canonical_key as canonicalKey,
      s.section_order as sectionOrder,
      p.id as paragraphId,
      p.anchor,
      p.paragraph_order as paragraphOrder,
      p.text
     FROM document_sections s
     LEFT JOIN section_paragraphs p ON p.section_id = s.id
     WHERE s.document_id = ?
     ORDER BY s.section_order ASC, p.paragraph_order ASC`
  )
    .bind(documentId)
    .all<{
      id: string;
      heading: string;
      canonicalKey: string;
      sectionOrder: number;
      paragraphId: string | null;
      anchor: string | null;
      paragraphOrder: number | null;
      text: string | null;
    }>();

  const chunks = await env.DB.prepare(
    `SELECT
      id,
      section_id as sectionId,
      paragraph_anchor as paragraphAnchor,
      paragraph_anchor_end as paragraphAnchorEnd,
      citation_anchor as citationAnchor,
      section_label as sectionLabel,
      chunk_order as chunkOrder,
      chunk_text as chunkText,
      token_estimate as tokenEstimate,
      chunk_warnings_json as chunkWarningsJson
     FROM document_chunks
     WHERE document_id = ?
     ORDER BY chunk_order ASC`
  )
    .bind(documentId)
    .all<{
      id: string;
      sectionId: string;
      paragraphAnchor: string;
      paragraphAnchorEnd: string | null;
      citationAnchor: string;
      sectionLabel: string;
      chunkOrder: number;
      chunkText: string;
      tokenEstimate: number;
      chunkWarningsJson: string;
    }>();

  const referenceIssues = await env.DB.prepare(
    `SELECT reference_type as referenceType, raw_value as rawValue, normalized_value as normalizedValue, message, severity, created_at as createdAt
     FROM document_reference_issues
     WHERE document_id = ?
     ORDER BY created_at DESC`
  )
    .bind(documentId)
    .all<{
      referenceType: string;
      rawValue: string;
      normalizedValue: string;
      message: string;
      severity: string;
      createdAt: string;
    }>();

  const validReferenceLinks = await env.DB.prepare(
    `SELECT id AS referenceLinkId,reference_type as referenceType, canonical_value as canonicalValue,edition_id AS editionId,edition_provision_id AS editionProvisionId,edition_body_sha256 AS editionBodySha256,applicability_status AS applicabilityStatus
     FROM document_reference_links
     WHERE document_id = ? AND is_valid = 1 AND canonical_value IS NOT NULL
     ORDER BY reference_type ASC, canonical_value ASC`
  )
    .bind(documentId)
    .all<{ referenceLinkId:string;referenceType: "index_code" | "rules_section" | "ordinance_section"; canonicalValue: string;editionId:string|null;editionProvisionId:string|null;editionBodySha256:string|null;applicabilityStatus:string }>();

  const criticalExceptionRefs = await env.DB.prepare(
    `SELECT DISTINCT normalized_value as normalizedValue
     FROM document_reference_links
     WHERE document_id = ?
       AND normalized_value IN ('37.2(g)', '37.15', '10.10(c)(3)')
     ORDER BY normalized_value ASC`
  )
    .bind(documentId)
    .all<{ normalizedValue: string }>();

  const sectionMap = new Map<string, {
    id: string;
    heading: string;
    canonicalKey: string;
    sectionOrder: number;
    paragraphs: Array<{ id: string; anchor: string; paragraphOrder: number; text: string }>;
  }>();

  for (const row of sections.results ?? []) {
    const section = sectionMap.get(row.id) ?? {
      id: row.id,
      heading: row.heading,
      canonicalKey: row.canonicalKey,
      sectionOrder: row.sectionOrder,
      paragraphs: []
    };

    if (row.paragraphId && row.anchor && row.paragraphOrder !== null && row.text) {
      section.paragraphs.push({
        id: row.paragraphId,
        anchor: row.anchor,
        paragraphOrder: row.paragraphOrder,
        text: row.text
      });
    }

    sectionMap.set(row.id, section);
  }

  await assertDocumentRevision(env, documentId, document.documentRevision);
  const metadata = parseJsonObject(document.metadataJson);
  const taxonomy = storedTaxonomySuggestion(asObject(metadata.taxonomy),undefined,document.jurisdiction);

  const capturedEditionParsed=referenceEditionBindingSchema.safeParse(parseJsonObject(document.referenceEditionContextJson));
  const capturedEditionContext=capturedEditionParsed.success?capturedEditionParsed.data:referenceEditionBindingSchema.parse({edition_id:null,catalog_version:null,descriptor_sha256:null,status:"unknown",reason:"legacy_or_unavailable_edition_context",jurisdiction:document.jurisdiction||null,as_of_date:document.decisionDate||null,effective_from:null,effective_to:null});
  const editionContextStale=capturedEditionContext.registry_epoch!==document.referenceRegistryEpoch || (capturedEditionContext.date_basis==="decision_date_proxy" && (capturedEditionContext.as_of_date!==document.decisionDate || capturedEditionContext.jurisdiction!==document.jurisdiction));
  const detail = {
    ...document,
    decisionDate:knownCalendarDate(document.decisionDate),
    unverifiedDecisionDate:document.decisionDate&&!knownCalendarDate(document.decisionDate)?document.decisionDate:null,
    referenceEditionContext:editionContextStale?{...capturedEditionContext,status:"unknown",reason:"captured_edition_context_needs_revalidation"}:capturedEditionContext,
    referenceEditionBindings:validReferenceLinks.results,
    reviewerOverrides: readReviewerOverrides(document.reviewerOverridesJson),
    // The persisted source_link is the example.invalid sentinel; rewrite to the proxy URL so this
    // response never hands a caller a dead link (ARCH-02).
    sourceLink: reviewSourceLink(env, documentId),
    extractionWarnings: parseJsonArray(document.extractionWarningsJson),
    indexCodes: parseJsonArray(document.indexCodesJson),
    rulesSections: parseJsonArray(document.rulesSectionsJson),
    ordinanceSections: parseJsonArray(document.ordinanceSectionsJson),
    taxonomySuggestion: taxonomy,
    referenceIssues: referenceIssues.results ?? [],
    unresolvedReferenceCount: (referenceIssues.results ?? []).length,
    criticalExceptionReferences: (criticalExceptionRefs.results ?? []).map((row) => row.normalizedValue),
    filteredNoiseCount: parseJsonArray(document.extractionWarningsJson).filter((warning) =>
      warning.toLowerCase().includes("extraction noise filtered")
    ).length,
    missingRulesDetection: document.qcHasRulesSection === 0,
    missingOrdinanceDetection: document.qcHasOrdinanceSection === 0,
    lowConfidenceTaxonomy:
      Boolean(taxonomy.fallback) || (typeof taxonomy.confidence === "number" ? taxonomy.confidence < 0.45 : false),
    failedQcRequirements: qcFailedRequirements({
      qcHasIndexCodes: document.qcHasIndexCodes,
      qcHasRulesSection: document.qcHasRulesSection,
      qcHasOrdinanceSection: document.qcHasOrdinanceSection
    }),
    qcGateDiagnostics: {
      hasIndexCodes: Boolean(document.qcHasIndexCodes),
      hasRulesSection: Boolean(document.qcHasRulesSection),
      hasOrdinanceSection: Boolean(document.qcHasOrdinanceSection),
      passed: Boolean(document.qcPassed)
    },
    validReferences: {
      indexCodes: Array.from(
        new Set(
          (validReferenceLinks.results ?? [])
            .filter((row) => row.referenceType === "index_code")
            .map((row) => row.canonicalValue)
        )
      ),
      rulesSections: Array.from(
        new Set(
          (validReferenceLinks.results ?? [])
            .filter((row) => row.referenceType === "rules_section")
            .map((row) => row.canonicalValue)
        )
      ),
      ordinanceSections: Array.from(
        new Set(
          (validReferenceLinks.results ?? [])
            .filter((row) => row.referenceType === "ordinance_section")
            .map((row) => row.canonicalValue)
        )
      )
    },
    isLikelyFixture: isLikelyFixtureDoc({ title: document.title, citation: document.citation, metadata }),
    sections: Array.from(sectionMap.values()),
    chunks: (chunks.results ?? []).map((chunk) => ({
      ...chunk,
      paragraphAnchorEnd: chunk.paragraphAnchorEnd ?? chunk.paragraphAnchor,
      chunkWarnings: parseJsonArray(chunk.chunkWarningsJson)
    }))
  };
  return {
    ...detail,
    ...(function () {
      const approvalReadiness = computeApprovalReadiness({
      fileType: detail.fileType,
      ingestionReady: detail.ingestionReady,
      referenceCatalogCurrent: detail.referenceCatalogCurrent,
      approvedAt: detail.approvedAt,
      rejectedAt: detail.rejectedAt,
      qcPassed: detail.qcPassed && detail.qcHasIndexCodes && detail.qcHasRulesSection && detail.qcHasOrdinanceSection,
      qcRequiredConfirmed: detail.qcRequiredConfirmed,
      warningCount: (detail.extractionWarnings || []).length,
      unresolvedReferenceCount: detail.unresolvedReferenceCount || 0,
      criticalExceptionCount: Math.max((detail.criticalExceptionReferences || []).length, detectCriticalReferenceExceptions({ rules: detail.rulesSections, ordinance: detail.ordinanceSections }).length),
      extractionConfidence: detail.extractionConfidence || 0,
      missingRulesDetection: detail.missingRulesDetection,
      missingOrdinanceDetection: detail.missingOrdinanceDetection,
      lowConfidenceTaxonomy: detail.lowConfidenceTaxonomy
      });
      const unresolvedRulesCount = (detail.referenceIssues || []).filter((row) => row.referenceType === "rules_section").length;
      const unresolvedOrdinanceCount = (detail.referenceIssues || []).filter((row) => row.referenceType === "ordinance_section").length;
      const unresolvedIndexCount = (detail.referenceIssues || []).filter((row) => row.referenceType === "index_code").length;
      const unresolvedUnsafe37xCount = (detail.referenceIssues || []).filter(
        (row) => row.referenceType === "ordinance_section" && ["ordinance37.3", "ordinance37.7", "ordinance37.9"].includes(row.normalizedValue)
      ).length;
      const reviewer = computeReviewerReadiness({
        isLikelyFixture: detail.isLikelyFixture,
        blockers: approvalReadiness.blockers,
        qcPassed: detail.qcPassed && detail.qcHasIndexCodes && detail.qcHasRulesSection && detail.qcHasOrdinanceSection,
        qcRequiredConfirmed: detail.qcRequiredConfirmed,
        failedQcRequirements: detail.failedQcRequirements || [],
        unresolvedReferenceCount: detail.unresolvedReferenceCount || 0,
        unresolvedRulesCount,
        unresolvedOrdinanceCount,
        unresolvedIndexCount,
        unresolvedUnsafe37xCount,
        criticalExceptionCount: Math.max((detail.criticalExceptionReferences || []).length, detectCriticalReferenceExceptions({ rules: detail.rulesSections, ordinance: detail.ordinanceSections }).length),
        extractionConfidence: detail.extractionConfidence || 0,
        validIndexRefCount: (detail.validReferences?.indexCodes || []).length,
        validRulesRefCount: (detail.validReferences?.rulesSections || []).length,
        validOrdinanceRefCount: (detail.validReferences?.ordinanceSections || []).length,
        missingRulesDetection: detail.missingRulesDetection,
        missingOrdinanceDetection: detail.missingOrdinanceDetection,
        approvalThresholds: { maxUnresolvedReferences: approvalReadiness.thresholds.maxUnresolvedReferences }
      });
      const triage = computeUnresolvedTriage((detail.referenceIssues || []) as UnresolvedIssueLite[]);
      const blocked37x = computeBlocked37xReview((detail.referenceIssues || []) as UnresolvedIssueLite[]);
      const runtimePolicy = computeRuntimeReviewerPolicy({
        triage,
        blocked37xReferences: blocked37x.blocked37xReferences,
        unresolvedReferenceCount: detail.unresolvedReferenceCount || 0,
        documentId: detail.id,
        title: detail.title,
        unresolvedBuckets: triage.unresolvedBuckets,
        recurringCitationFamilies: triage.recurringCitationFamilies
      });
      return {
        approvalReadiness,
        reviewerReady: reviewer.reviewerReady,
        reviewerReadyReasons: reviewer.reviewerReadyReasons,
        reviewerRequiredActions: reviewer.reviewerRequiredActions,
        reviewerRiskLevel: reviewer.reviewerRiskLevel,
        metadataConfirmationWouldUnlock: reviewer.metadataConfirmationWouldUnlock,
        unresolvedBlockersAfterConfirmation: reviewer.unresolvedBlockersAfterConfirmation,
        unresolvedBuckets: triage.unresolvedBuckets,
        topRecommendedReviewerAction: runtimePolicy.runtimeSurfaceForManualReview
          ? triage.topRecommendedReviewerAction
          : runtimePolicy.runtimePolicyReason,
        estimatedReviewerEffort: triage.estimatedReviewerEffort,
        candidateManualFixes: runtimePolicy.runtimeSurfaceForManualReview ? triage.candidateManualFixes : [],
        recurringCitationFamilies: triage.recurringCitationFamilies,
        runtimeDisposition: runtimePolicy.runtimeDisposition,
        runtimeManualReasonCode: runtimePolicy.runtimeManualReasonCode,
        runtimeManualReasonSummary: runtimePolicy.runtimeManualReasonSummary,
        runtimeSuggestedOperatorAction: runtimePolicy.runtimeSuggestedOperatorAction,
        runtimeOperatorReviewSummary: runtimePolicy.runtimeOperatorReviewSummary,
        runtimeReviewDiagnostic: runtimePolicy.runtimeReviewDiagnostic,
        runtimePolicyReason: runtimePolicy.runtimePolicyReason,
        runtimeSurfaceForManualReview: runtimePolicy.runtimeSurfaceForManualReview,
        runtimeManualReviewRequired: runtimePolicy.runtimeManualReviewRequired,
        runtimeDoNotAutoApply: runtimePolicy.runtimeDoNotAutoApply,
        blocked37xReferences: blocked37x.blocked37xReferences,
        blocked37xReason: blocked37x.blocked37xReason,
        blocked37xReviewerHint: blocked37x.blocked37xReviewerHint,
        blocked37xSafeToBatchReview: blocked37x.blocked37xSafeToBatchReview,
        blocked37xBatchKey: blocked37x.blocked37xBatchKey
      };
    })()
  };
}
