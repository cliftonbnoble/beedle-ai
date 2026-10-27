// Reviewer mutation and reprocessing commands; read models and policy are separate dependencies.
import { knownCalendarDate } from "@beedle/shared";
import { advanceReviewerQueue } from "./reviewer-queue-index";
import { sha256Bytes } from "../lib/digests";
import { ExtractionReviewRequired } from "./text-extraction";
import { stageDocumentGeneration } from "./document-generations";
import { preserveReviewedMetadata, readReviewerOverrides, reviewerPayloadFields } from "./reviewer-overrides";
import { RequestConflictError, RequestValidationError } from "../lib/http";
import { reviewMutation, reviewerSnapshotSql } from "./reviewer-mutations";
import { readyDocumentSql } from "./publication-policy";
import { adminIngestionMetadataUpdateSchema, adminIngestionRejectSchema } from "@beedle/shared";
import type { Env, ParsedDocument } from "../lib/types";
import { approveDecision } from "./ingest";
import { computeQcFlags, detectCriticalReferenceExceptions } from "./qc-shared";
import { parseDocument, parseMarkdownDocument } from "./parser";
import { detectSourceFormat } from "./source-format";
import { inferTaxonomySuggestion } from "./taxonomy-inference";
import { buildDocumentReferenceValidationStatements, inferIndexCodesFromReferences, validateReferencesAgainstNormalized, prepareReferenceValidation } from "./legal-references";
import { buildReviewerRows, getIngestionDocumentDetail } from './reviewer-read-model';
import { boolish, parseJsonArray, parseJsonObject } from './reviewer-policy';
export { IngestionListBuildError, buildReviewerRows, getIngestionDocumentDetail } from './reviewer-read-model';
export type { ListIngestionDocumentsOptions } from './reviewer-contracts';
export { listIngestionDocuments } from "./reviewer-queue";


export async function advanceReviewerQueueIndex(env:Env){return advanceReviewerQueue(env,buildReviewerRows);}


export async function updateIngestionMetadata(env: Env, documentId: string, payload: unknown) {
  const parsed = adminIngestionMetadataUpdateSchema.parse(payload);
  const operation = await reviewMutation(env, documentId, "metadata", parsed);
  if (operation.existing) {
    const detail = await getIngestionDocumentDetail(env,documentId);
    return detail ? {...detail,mutationReceipt:operation.existing} : null;
  }
  const row = await env.DB.prepare(
    `SELECT id,jurisdiction, document_revision as documentRevision, reviewer_overrides_json as reviewerOverridesJson,
      index_codes_json as indexCodesJson, rules_sections_json as rulesSectionsJson,
      ordinance_sections_json as ordinanceSectionsJson, case_number as caseNumber, decision_date as decisionDate,
      author_name as authorName, outcome_label as outcomeLabel, ${reviewerSnapshotSql} AS beforeJson
      FROM documents WHERE id = ?`
  ).bind(documentId).first<{
    id: string; jurisdiction:string; documentRevision: number; reviewerOverridesJson: string; beforeJson: string;
    indexCodesJson: string; rulesSectionsJson: string; ordinanceSectionsJson: string;
    caseNumber: string | null; decisionDate: string | null; authorName: string | null; outcomeLabel: string | null;
  }>();
  if (!row) return null;
  if (row.documentRevision !== parsed.expectedRevision) {
    const receipt = await operation.replay();
    if (receipt) {
      const detail = await getIngestionDocumentDetail(env,documentId);
      return detail ? {...detail,mutationReceipt:receipt} : null;
    }
    throw new RequestConflictError("Document changed; reload and review its current revision.");
  }

  const updateIndexCodes = parsed.index_codes;
  const updateRules = parsed.rules_sections;
  const updateOrdinance = parsed.ordinance_sections;
  const persistedIndexCodes = updateIndexCodes ?? parseJsonArray(row.indexCodesJson);
  const persistedRules = updateRules ?? parseJsonArray(row.rulesSectionsJson);
  const persistedOrdinance = updateOrdinance ?? parseJsonArray(row.ordinanceSectionsJson);
  const hasIndexCodes = persistedIndexCodes.some(value=>value.trim().length>0);
  const hasRules = persistedRules.some(value=>value.trim().length>0);
  const hasOrdinance = persistedOrdinance.some(value=>value.trim().length>0);
  if (parsed.confirm_required_metadata === true && !(hasIndexCodes && hasRules && hasOrdinance)) {
    throw new RequestValidationError("Required metadata cannot be confirmed until all three reference groups contain a value.");
  }
  const now = new Date().toISOString();
  const overrides = readReviewerOverrides(row.reviewerOverridesJson);
  let overridesChanged = false;
  for (const [key, field] of Object.entries(reviewerPayloadFields)) {
    const value = parsed[key as keyof typeof reviewerPayloadFields];
    if (value !== undefined && (overrides[field]?.provenance !== "reviewer" || JSON.stringify(overrides[field]?.value) !== JSON.stringify(value))) {
      overrides[field] = {value,provenance:"reviewer",recordedAt:now,basedOnRevision:parsed.expectedRevision};
      overridesChanged = true;
    }
  }
  // Bind the entire resulting record. Undefined retains; explicit null clears.
  const retained = <T>(value:T|undefined,current:T) => value === undefined ? current : value;
  // Omission and a semantic no-op must not become evidence edits solely because
  // an older writer used different JSON whitespace. Preserve those stored bytes.
  const retainedReferences = (value:string[]|undefined,current:string) => {
    if(value===undefined)return current;
    const encoded=JSON.stringify(value);
    try { if(JSON.stringify(JSON.parse(current))===encoded)return current; } catch { /* An explicit replacement repairs invalid stored JSON. */ }
    return encoded;
  };
  const nextIndexJson=retainedReferences(updateIndexCodes,row.indexCodesJson);
  const nextRulesJson=retainedReferences(updateRules,row.rulesSectionsJson);
  const nextOrdinanceJson=retainedReferences(updateOrdinance,row.ordinanceSectionsJson);
  const documentUpdateStatement = env.DB.prepare(`UPDATE documents SET reviewer_overrides_json = ?,
    index_codes_json = ?, rules_sections_json = ?, ordinance_sections_json = ?,
    case_number = ?, decision_date = ?, author_name = ?, outcome_label = ?,
    qc_has_index_codes = ?, qc_has_rules_section = ?, qc_has_ordinance_section = ?, qc_passed = ?, updated_at = ?
    WHERE id = ?`).bind(overridesChanged?JSON.stringify(overrides):row.reviewerOverridesJson,nextIndexJson,nextRulesJson,nextOrdinanceJson,
      retained(parsed.case_number,row.caseNumber),retained(parsed.decision_date,row.decisionDate),retained(parsed.author_name,row.authorName),
      retained(parsed.outcome_label,row.outcomeLabel),boolish(hasIndexCodes),boolish(hasRules),boolish(hasOrdinance),
      boolish(hasIndexCodes && hasRules && hasOrdinance),now,documentId);
  const referencesChanged = nextIndexJson !== row.indexCodesJson || nextRulesJson !== row.rulesSectionsJson || nextOrdinanceJson !== row.ordinanceSectionsJson;
  const referenceValidationStatements = referencesChanged || (parsed.decision_date!==undefined&&parsed.decision_date!==row.decisionDate) || parsed.confirm_required_metadata === true
    ? await buildDocumentReferenceValidationStatements(env, documentId, {
      indexCodes:persistedIndexCodes,rulesSections:persistedRules,ordinanceSections:persistedOrdinance
    },undefined,undefined,{jurisdiction:row.jurisdiction,as_of_date:retained(parsed.decision_date,row.decisionDate)}) : [];
  const mutations = [documentUpdateStatement,...referenceValidationStatements];
  // Run after evidence/reference triggers, which revoke earlier confirmation and approval.
  if (parsed.confirm_required_metadata !== undefined) {
    mutations.push(env.DB.prepare(`UPDATE documents SET qc_required_confirmed = ?, qc_confirmed_at = ? WHERE id = ?`)
      .bind(boolish(parsed.confirm_required_metadata),parsed.confirm_required_metadata ? now : null,documentId));
  }
  const mutationReceipt = await operation.commit(row.beforeJson,mutations);
  const detail = await getIngestionDocumentDetail(env,documentId);
  return detail ? {...detail,mutationReceipt} : null;
}


export async function reprocessIngestionDocument(env: Env, documentId: string) {
  const row = await env.DB.prepare(
    `SELECT
      id, document_revision as documentRevision, reviewer_overrides_json as reviewerOverridesJson,
      case_number as caseNumber, author_name as authorName, outcome_label as outcomeLabel,
      index_codes_json as indexCodesJson, rules_sections_json as rulesSectionsJson, ordinance_sections_json as ordinanceSectionsJson,
      jurisdiction,file_type as fileType,
      source_r2_key as sourceKey,
      title,
      citation,
      decision_date as decisionDate,
      metadata_json as metadataJson,
      (SELECT COUNT(*) FROM document_sections WHERE document_id = documents.id) as sectionCount,
      (SELECT COUNT(*) FROM section_paragraphs WHERE section_id IN (
        SELECT id FROM document_sections WHERE document_id = documents.id
      )) as paragraphCount,
      (SELECT COUNT(*) FROM document_chunks WHERE document_id = documents.id) as chunkCount
     FROM documents
     WHERE id = ?`
  )
    .bind(documentId)
    .first<{
      id: string; documentRevision: number; reviewerOverridesJson: string;
      caseNumber: string | null; authorName: string | null; outcomeLabel: string | null;
      indexCodesJson: string; rulesSectionsJson: string; ordinanceSectionsJson: string;
      jurisdiction:string;fileType: "decision_docx" | "law_pdf";
      sourceKey: string;
      title: string;
      citation: string;
      decisionDate: string | null;
      metadataJson: string | null;
      sectionCount: number;
      paragraphCount: number;
      chunkCount: number;
    }>();

  if (!row) {
    return null;
  }

  const object = await env.SOURCE_BUCKET.get(row.sourceKey);
  if (!object) {
    throw new RequestConflictError("Captured source is missing; restore it before reprocessing");
  }
  if (object.size > 15 * 1024 * 1024) throw new RequestValidationError("Source exceeds the reprocessing limit; current content was preserved.");
  const bytes = new Uint8Array(await object.arrayBuffer());
  if (bytes.length > 15 * 1024 * 1024) throw new RequestValidationError("Source exceeds the reprocessing limit; current content was preserved.");
  const sourceSha256 = await sha256Bytes(bytes);
  const capturedHash = parseJsonObject(row.metadataJson).sourceSha256;
  if (capturedHash && capturedHash !== sourceSha256) throw new RequestValidationError("Source digest differs from the captured source; the current revision was preserved.");
  const originalFilename = parseJsonObject(row.metadataJson).originalFilename;
  const format = detectSourceFormat(bytes, typeof originalFilename === "string" ? originalFilename : row.sourceKey, row.fileType);
  let parsed: ParsedDocument;
  try { parsed = format === "markdown" ? parseMarkdownDocument(bytes) : await parseDocument(bytes, row.fileType); }
  catch (error) {
    if (error instanceof ExtractionReviewRequired) throw new RequestValidationError("Reprocessing requires manual extraction review; the current revision was preserved.");
    throw error;
  }
  const existingMetadata = parseJsonObject(row.metadataJson);
  const preserved = preserveReviewedMetadata(parsed.extractedMetadata, {
    indexCodes: parseJsonArray(row.indexCodesJson), rulesSections: parseJsonArray(row.rulesSectionsJson),
    ordinanceSections: parseJsonArray(row.ordinanceSectionsJson), caseNumber: row.caseNumber,
    decisionDate: row.decisionDate, author: row.authorName, outcomeLabel: row.outcomeLabel
  }, existingMetadata.extractedMetadata as Record<string, unknown> | undefined, readReviewerOverrides(row.reviewerOverridesJson), row.documentRevision);
  const metadata = preserved.metadata;
  const warnings = [...parsed.warnings];
  if(metadata.decisionDate&&!knownCalendarDate(metadata.decisionDate)){metadata.unverifiedDecisionDate=metadata.decisionDate;metadata.decisionDate=null;warnings.push("Retained invalid calendar date is unknown; review the original source before assigning a date");}

  const validationContext={jurisdiction:row.jurisdiction,as_of_date:metadata.decisionDate||null};
  const referenceValues={indexCodes:metadata.indexCodes,rulesSections:metadata.rulesSections,ordinanceSections:metadata.ordinanceSections};
  const referencePlan=await prepareReferenceValidation(env,referenceValues,validationContext);
  const referenceValidation=await validateReferencesAgainstNormalized(env,referenceValues,validationContext,referencePlan);
  const normalizedIndexCount = await env.DB.prepare(`SELECT COUNT(*) as count FROM legal_index_codes WHERE active = 1`).first<{ count: number }>();
  if (!preserved.overrides.indexCodes && (normalizedIndexCount?.count ?? 0) > 0 && referenceValidation.unknownIndexCodes.length > 0) {
    const unknownSet = new Set(referenceValidation.unknownIndexCodes);
    const kept = metadata.indexCodes.filter((value) => !unknownSet.has(value));
    const dropped = metadata.indexCodes.filter((value) => unknownSet.has(value));
    metadata.indexCodes = kept;
    if (dropped.length > 0) {
      warnings.push(`Extraction noise filtered (index codes): ${dropped.join(", ")}`);
    }
  }
  if (!preserved.overrides.indexCodes && metadata.indexCodes.length === 0) {
    const inferred = await inferIndexCodesFromReferences(env, {
      rulesSections: metadata.rulesSections,
      ordinanceSections: metadata.ordinanceSections
    });
    if (inferred.inferredIndexCodes.length > 0) {
      metadata.indexCodes = inferred.inferredIndexCodes;
      warnings.push(
        `Index codes inferred from validated references: ${inferred.inferredIndexCodes.join(", ")} (${inferred.evidence.join("; ")})`
      );
    }
  }

  if (referenceValidation.unknownIndexCodes.length > 0) {
    warnings.push(`Unknown index codes (manual review): ${referenceValidation.unknownIndexCodes.join(", ")}`);
  }
  if (referenceValidation.unknownRules.length > 0) {
    warnings.push(`Unknown rules references (manual review): ${referenceValidation.unknownRules.join(", ")}`);
  }
  if (referenceValidation.unknownOrdinance.length > 0) {
    warnings.push(`Unknown ordinance references (manual review): ${referenceValidation.unknownOrdinance.join(", ")}`);
  }
  const criticalExceptions = detectCriticalReferenceExceptions({
    rules: metadata.rulesSections,
    ordinance: metadata.ordinanceSections
  });
  for (const citation of criticalExceptions) {
    if (citation === "37.15") {
      warnings.push("Critical reference exception: 37.15 may be cross-context ambiguous (ordinance vs rules); manual QC required");
      continue;
    }
    if (citation === "37.2(g)" || citation === "10.10(c)(3)") {
      warnings.push(`Critical reference exception: ${citation} currently classed as parent_or_related_only; manual QC required`);
      continue;
    }
    warnings.push(`Critical reference exception: ${citation}; manual QC required`);
  }

  const headings = parsed.sections.map((section) => section.heading || "");
  const qcFlags = computeQcFlags(headings, metadata);
  const taxonomySuggestion = inferTaxonomySuggestion({
    jurisdiction:row.jurisdiction,
    title: row.title,
    citation: row.citation,
    sections: parsed.sections,
    metadata
  });
  if (taxonomySuggestion.fallback || taxonomySuggestion.confidence < 0.45) {
    warnings.push("Taxonomy suggestion is low-confidence; review case type during QC");
  }

  const published = await stageDocumentGeneration(env, {
    referencePlan,documentId, expectedRevision: row.documentRevision, sourceBytes: bytes, sourceFormat: format, sections: parsed.sections,
    references: {indexCodes:metadata.indexCodes, rulesSections:metadata.rulesSections, ordinanceSections:metadata.ordinanceSections},
    documentChanges: {
      qc_has_index_codes: boolish(qcFlags.hasIndexCodes), qc_has_rules_section: boolish(qcFlags.hasRulesSection),
      qc_has_ordinance_section: boolish(qcFlags.hasOrdinanceSection),
      qc_passed: boolish(qcFlags.hasIndexCodes && qcFlags.hasRulesSection && qcFlags.hasOrdinanceSection),
      index_codes_json: JSON.stringify(metadata.indexCodes), rules_sections_json: JSON.stringify(metadata.rulesSections),
      ordinance_sections_json: JSON.stringify(metadata.ordinanceSections), case_number: metadata.caseNumber,
      decision_date: metadata.decisionDate, author_name: metadata.author, outcome_label: metadata.outcomeLabel,
      extraction_confidence: metadata.extractionConfidence, extraction_warnings_json: JSON.stringify(Array.from(new Set(warnings))),
      reviewer_overrides_json: JSON.stringify(preserved.overrides),
      metadata_json: JSON.stringify({...existingMetadata, sourceSha256, plainTextLength:parsed.plainText.length, extraction:parsed.extraction,
        extractedMetadata:parsed.extractedMetadata, citationEvidence:parsed.citations, taxonomy:taxonomySuggestion, referenceValidation})
    }
  });
  const detail = await getIngestionDocumentDetail(env, documentId);
  if (!detail) return null;
  return {...detail, reprocessArtifacts: {rebuilt:true, generationId:published.generationId, previousGenerationId:published.previousGenerationId,
    sectionCountBefore:Number(row.sectionCount), paragraphCountBefore:Number(row.paragraphCount), chunkCountBefore:Number(row.chunkCount),
    sectionCountAfter:published.manifest.document_sections.count, paragraphCountAfter:published.manifest.section_paragraphs.count,
    chunkCountAfter:published.manifest.document_chunks.count}};
}


export async function rejectIngestionDocument(env: Env, documentId: string, payload: unknown) {
  const parsed = adminIngestionRejectSchema.parse(payload);
  const operation = await reviewMutation(env,documentId,"reject",parsed);
  if (operation.existing) return {rejected:true,documentId,reason:parsed.reason,mutationReceipt:operation.existing};
  const row = await env.DB.prepare(`SELECT document_revision AS revision,${reviewerSnapshotSql} AS beforeJson FROM documents WHERE id=?`)
    .bind(documentId).first<{revision:number;beforeJson:string}>();
  if (!row) return null;
  if (row.revision !== parsed.expectedRevision) {
    const receipt = await operation.replay();
    if (receipt) return {rejected:true,documentId,reason:parsed.reason,mutationReceipt:receipt};
    throw new RequestConflictError("Document changed; reload and review its current revision.");
  }
  const now = new Date().toISOString();
  const mutationReceipt = await operation.commit(row.beforeJson,[env.DB.prepare(`UPDATE documents SET rejected_at = ?, rejected_reason = ?,
    approved_at = NULL, approved_revision = NULL, approval_policy_version = NULL, searchable_at = NULL,
    qc_required_confirmed = 0, qc_confirmed_at = NULL, updated_at = ? WHERE id = ?`).bind(now,parsed.reason,now,documentId)]);
  return {rejected:true,documentId,reason:parsed.reason,mutationReceipt};
}


export async function approveIngestionDocument(env: Env, documentId: string, expectedRevision: number) {
  return approveDecision(env, documentId, expectedRevision);
}


export type BulkSearchabilityCandidateMode =
  | "qcPassed"
  | "missingIndexOnlyTextReady"
  | "singleContextTextReady"
  | "decisionLikeTextReady";


function buildRealDecisionFilterSql() {
  return `
    d.file_type = 'decision_docx'
    AND ${readyDocumentSql()}
    AND d.rejected_at IS NULL
    AND COALESCE(d.title, '') NOT LIKE 'Harness %'
    AND COALESCE(d.title, '') NOT LIKE 'Retrieval %'
    AND COALESCE(d.title, '') NOT LIKE 'R5 %'
    AND COALESCE(d.citation, '') NOT LIKE 'BEE-%'
    AND lower(COALESCE(d.source_r2_key, '')) NOT LIKE '%harness%'
    AND lower(COALESCE(d.source_r2_key, '')) NOT LIKE '%fixture%'
  `;
}


export async function listBulkSearchabilityCandidates(
  env: Env,
  options?: { limit?: number; realOnly?: boolean; mode?: BulkSearchabilityCandidateMode }
) {
  const limit = Math.max(1, Math.min(1000, Number(options?.limit || 200)));
  const mode: BulkSearchabilityCandidateMode =
    options?.mode === "missingIndexOnlyTextReady" ||
    options?.mode === "singleContextTextReady" ||
    options?.mode === "decisionLikeTextReady"
      ? options.mode
      : "qcPassed";
  const where = [
    "d.file_type = 'decision_docx'",
    "d.searchable_at IS NULL",
    readyDocumentSql(),
    "d.rejected_at IS NULL",
    "COALESCE(d.source_r2_key, '') != ''",
    "COALESCE(d.title, '') != ''"
  ];
  if (mode === "missingIndexOnlyTextReady") {
    where.push("d.qc_has_index_codes = 0");
    where.push("d.qc_has_rules_section = 1");
    where.push("d.qc_has_ordinance_section = 1");
  } else if (mode === "singleContextTextReady") {
    where.push("d.qc_passed = 0");
    where.push("COALESCE(d.qc_required_confirmed, 0) = 0");
    where.push(
      "((COALESCE(d.qc_has_rules_section, 0) = 1 AND COALESCE(d.qc_has_ordinance_section, 0) = 0) OR (COALESCE(d.qc_has_rules_section, 0) = 0 AND COALESCE(d.qc_has_ordinance_section, 0) = 1))"
    );
    where.push("EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id)");
    where.push("COALESCE(CAST(json_extract(d.metadata_json, '$.plainTextLength') AS INTEGER), 0) >= 1000");
    where.push("COALESCE(d.extraction_warnings_json, '[]') NOT LIKE '%Unknown index codes (manual review):%'");
    where.push("COALESCE(d.citation, '') NOT LIKE 'UNK-REF-%'");
    where.push("COALESCE(d.title, '') NOT LIKE 'Unknown Reference %'");
  } else if (mode === "decisionLikeTextReady") {
    where.push("d.qc_passed = 0");
    where.push("COALESCE(d.qc_required_confirmed, 0) = 0");
    where.push("COALESCE(d.qc_has_index_codes, 0) = 0");
    where.push("COALESCE(d.qc_has_rules_section, 0) = 0");
    where.push("COALESCE(d.qc_has_ordinance_section, 0) = 0");
    where.push("EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id)");
    where.push("COALESCE(CAST(json_extract(d.metadata_json, '$.plainTextLength') AS INTEGER), 0) >= 1000");
    where.push("COALESCE(d.extraction_warnings_json, '[]') NOT LIKE '%Unknown index codes (manual review):%'");
    where.push("COALESCE(d.citation, '') NOT LIKE 'UNK-REF-%'");
    where.push("COALESCE(d.title, '') NOT LIKE 'Unknown Reference %'");
    where.push(`
      (
        lower(COALESCE(d.title, '')) LIKE '%decision%'
        OR lower(COALESCE(d.title, '')) LIKE '%dismissal%'
        OR lower(COALESCE(d.title, '')) LIKE '%order%'
        OR lower(COALESCE(d.title, '')) LIKE '%remand%'
        OR lower(COALESCE(d.title, '')) LIKE '%appeal%'
        OR lower(COALESCE(d.title, '')) LIKE '%hearing%'
        OR upper(COALESCE(d.citation, '')) LIKE '%DECISION%'
        OR upper(COALESCE(d.citation, '')) LIKE '%DISMISSAL%'
        OR upper(COALESCE(d.citation, '')) LIKE '%ORDER%'
        OR upper(COALESCE(d.citation, '')) LIKE '%REMAND%'
      )
    `);
  } else {
    where.push("d.qc_passed = 1");
  }
  if (options?.realOnly !== false) {
    where.push(buildRealDecisionFilterSql());
  }

  const sql = `
    WITH ranked AS (
      SELECT
        d.id,
        d.title,
        d.citation,
        d.decision_date as decisionDate,
        d.created_at as createdAt,
        d.updated_at as updatedAt,
        d.source_r2_key as sourceR2Key,
        d.qc_has_index_codes as qcHasIndexCodes,
        d.qc_has_rules_section as qcHasRulesSection,
        d.qc_has_ordinance_section as qcHasOrdinanceSection,
        ROW_NUMBER() OVER (
          PARTITION BY d.citation
          ORDER BY d.updated_at DESC, d.created_at DESC, d.id DESC
        ) as citation_rank
      FROM documents d
      WHERE ${where.join("\n        AND ")}
    )
    SELECT *
    FROM ranked
    WHERE citation_rank = 1
    ORDER BY decisionDate DESC, updatedAt DESC
    LIMIT ?
  `;

  const result = await env.DB.prepare(sql).bind(limit).all<{
    id: string;
    title: string;
    citation: string;
    decisionDate: string | null;
    createdAt: string;
    updatedAt: string;
    sourceR2Key: string;
    qcHasIndexCodes: number;
    qcHasRulesSection: number;
    qcHasOrdinanceSection: number;
  }>();

  const candidates = (result.results || []).map((row) => ({
    documentId: row.id,
    title: row.title,
    citation: row.citation,
    decisionDate: row.decisionDate,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    sourceR2Key: row.sourceR2Key,
    qc: {
      hasIndexCodes: Boolean(row.qcHasIndexCodes),
      hasRulesSection: Boolean(row.qcHasRulesSection),
      hasOrdinanceSection: Boolean(row.qcHasOrdinanceSection)
    }
  }));

  const totals = await env.DB.prepare(
    `
      SELECT
        COUNT(*) as qcPassedUnsearchableCount,
        COUNT(DISTINCT citation) as qcPassedUnsearchableDistinctCitationCount
      FROM documents d
      WHERE ${where.join("\n        AND ")}
    `
  ).first<{ qcPassedUnsearchableCount: number; qcPassedUnsearchableDistinctCitationCount: number }>();

  return {
    summary: {
      limit,
      mode,
      realOnly: options?.realOnly !== false,
      qcPassedUnsearchableCount: Number(totals?.qcPassedUnsearchableCount || 0),
      qcPassedUnsearchableDistinctCitationCount: Number(totals?.qcPassedUnsearchableDistinctCitationCount || 0),
      candidateCount: candidates.length
    },
    candidates: candidates.map((candidate) => ({
      ...candidate,
      searchabilityMode: mode
    }))
  };
}


export async function bulkEnableSearchability(
  env: Env,
  options?: { limit?: number; realOnly?: boolean; dryRun?: boolean; mode?: BulkSearchabilityCandidateMode }
) {
  const candidateReport = await listBulkSearchabilityCandidates(env, options);
  const candidates = candidateReport.candidates || [];
  const documentIds = candidates.map((row) => row.documentId);
  const now = new Date().toISOString();
  const maxSearchabilityUpdateBatchSize = 25;

  let enabledCount = 0;
  if (!options?.dryRun && documentIds.length > 0) {
    const updateStatements: D1PreparedStatement[] = [];
    for (let index = 0; index < documentIds.length; index += maxSearchabilityUpdateBatchSize) {
      const batch = documentIds.slice(index, index + maxSearchabilityUpdateBatchSize);
      const placeholders = batch.map(() => "?").join(",");
      updateStatements.push(
        env.DB.prepare(
          `UPDATE documents
           SET searchable_at = COALESCE(searchable_at, ?),
               published_revision = document_revision,
               updated_at = ?
           WHERE id IN (${placeholders}) AND ${readyDocumentSql("documents")} RETURNING id`
        ).bind(now, now, ...batch)
      );
    }
    if (updateStatements.length > 0) {
      const updates = await env.DB.batch(updateStatements);
      enabledCount = updates.reduce((count, result) => count + result.results.length, 0);
    }
  }

  return {
    generatedAt: now,
    dryRun: Boolean(options?.dryRun),
    summary: {
      ...candidateReport.summary,
      enabledCount
    },
    documentIds,
    candidates
  };
}
