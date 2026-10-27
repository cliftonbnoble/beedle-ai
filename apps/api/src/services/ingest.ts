import { beginIngestion, releaseIngestion, parseIngestionReceiptResult, type IngestionAttempt } from "./ingestion-requests";
import { readVectorManifest, verifyVectorSource, vectorOutboxStatement } from "./vector-job-manifest";
import { sha256Bytes } from "../lib/digests";
import { approveDocumentRevision } from "./approval";
import { ingestDocumentSchema, ingestDocumentMetadataSchema, type FileType } from "@beedle/shared";
import { RequestBodyTooLargeError, RequestValidationError, RequestConflictError } from "../lib/http";
import type { AuthoredSection, Env, ParsedDocument } from "../lib/types";
import { parseDocument, parseMarkdownDocument } from "./parser";
import { reviewSourceLink, sourceLink, storeSourceFile } from "./storage";
import { computeQcFlags, detectCriticalReferenceExceptions } from "./qc-shared";
import { inferTaxonomySuggestion } from "./taxonomy-inference";
import { enqueueVectorJob } from "./vector-jobs";
import { ExtractionReviewRequired } from "./text-extraction";
import { detectSourceFormat } from "./source-format";
import {
  buildDocumentReferenceValidationStatements,
  inferIndexCodesFromReferences,
  validateReferencesAgainstNormalized, prepareReferenceValidation
} from "./legal-references";

export type PersistResult = import("@beedle/shared").IngestionReceiptResultV1;

interface PersistParagraphRow {
  paragraphId: string;
  sectionId: string;
  sectionHeading: string;
  paragraphAnchor: string;
  paragraphOrder: number;
  sectionOrder: number;
  paragraphText: string;
}

interface ChunkRow {
  id: string;
  paragraphId: string;
  sectionId: string;
  sectionLabel: string;
  paragraphAnchor: string;
  paragraphAnchorEnd: string;
  citationAnchor: string;
  chunkOrder: number;
  chunkText: string;
  tokenEstimate: number;
  warnings: string[];
}

function id(prefix: string): string {
  return `${prefix}_${crypto.randomUUID()}`;
}

const maxIngestSourceBytes = 15 * 1024 * 1024;

function decodeBase64(input: string): Uint8Array {
  if (input.length > Math.ceil(maxIngestSourceBytes * 4 / 3)) throw new RequestBodyTooLargeError(maxIngestSourceBytes);
  let raw: string;
  try { raw = atob(input); }
  catch { throw new RequestValidationError("Source file must contain valid base64."); }
  if (raw.length > maxIngestSourceBytes) {
    throw new RequestBodyTooLargeError(maxIngestSourceBytes);
  }
  const bytes = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) {
    bytes[i] = raw.charCodeAt(i);
  }
  return bytes;
}

function estimateTokens(text: string): number {
  return Math.max(1, Math.ceil(text.length / 4));
}

function buildSectionAwareChunks(params: { citation: string; paragraphs: PersistParagraphRow[] }): ChunkRow[] {
  const targetChars = 640;
  const minChars = 200;
  const maxChars = 900;

  const out: ChunkRow[] = [];
  let chunkOrder = 0;

  const grouped = new Map<string, PersistParagraphRow[]>();
  for (const row of params.paragraphs.sort((a, b) => (a.sectionOrder - b.sectionOrder) || (a.paragraphOrder - b.paragraphOrder))) {
    const list = grouped.get(row.sectionId) ?? [];
    list.push(row);
    grouped.set(row.sectionId, list);
  }

  for (const rows of grouped.values()) {
    let active: PersistParagraphRow[] = [];
    let length = 0;

    const flush = () => {
      if (active.length === 0) return;

      const text = active.map((row) => row.paragraphText).join("\n\n").trim();
      const start = active[0];
      const end = active[active.length - 1];
      if (!start || !end || !text) {
        active = [];
        length = 0;
        return;
      }

      const warnings: string[] = [];
      if (text.length < minChars) warnings.push("small_chunk");
      if (text.length > maxChars) warnings.push("large_chunk");

      out.push({
        id: id("chk"),
        paragraphId: start.paragraphId,
        sectionId: start.sectionId,
        sectionLabel: start.sectionHeading,
        paragraphAnchor: start.paragraphAnchor,
        paragraphAnchorEnd: end.paragraphAnchor,
        citationAnchor: `${params.citation}#${start.paragraphAnchor}`,
        chunkOrder,
        chunkText: text,
        tokenEstimate: estimateTokens(text),
        warnings
      });

      chunkOrder += 1;
      active = [];
      length = 0;
    };

    for (const row of rows) {
      const nextLength = length + row.paragraphText.length;
      if (active.length > 0 && nextLength > maxChars && length >= minChars) {
        flush();
      }

      active.push(row);
      length += row.paragraphText.length;

      if (length >= targetChars) {
        flush();
      }
    }

    flush();
  }

  return out;
}

const textArtifactBatchSize = 50;

export async function executeTextArtifactStatementBatches(env: Env, statements: D1PreparedStatement[]) {
  for (let i = 0; i < statements.length; i += textArtifactBatchSize) {
    const batch = statements.slice(i, i + textArtifactBatchSize);
    if (batch.length > 0) {
      try{await env.DB.batch(batch);}catch(error){if(String(error).includes("catalog_revision_conflict"))throw new RequestConflictError("Legal catalog changed; reload and retry validation");throw error;}
    }
  }
}

function buildSectionAndParagraphStatements(env: Env, documentId: string, sections: AuthoredSection[], generationId?: string) {
  const generation = generationId ? [generationId] : [];
  const paragraphRows: PersistParagraphRow[] = [];
  const statements: D1PreparedStatement[] = [];

  for (const section of sections) {
    const sectionId = id("sec");
    const sectionText = section.paragraphs.map((paragraph) => paragraph.text).join("\n\n");

    statements.push(
      env.DB.prepare(
        `INSERT INTO ${generationId ? "staged_document_sections" : "document_sections"} (${generationId ? "generation_id," : ""} id, document_id, canonical_key, heading, section_order, section_text)
         VALUES (${generationId ? "?," : ""} ?, ?, ?, ?, ?, ?)`
      ).bind(...generation, sectionId, documentId, section.canonicalKey, section.heading, section.order, sectionText)
    );

    for (const paragraph of section.paragraphs) {
      const paragraphId = id("par");
      statements.push(
        env.DB.prepare(
          `INSERT INTO ${generationId ? "staged_section_paragraphs" : "section_paragraphs"} (${generationId ? "generation_id," : ""} id, section_id, anchor, paragraph_order, text)
           VALUES (${generationId ? "?," : ""} ?, ?, ?, ?, ?)`
        ).bind(...generation, paragraphId, sectionId, paragraph.anchor, paragraph.order, paragraph.text)
      );

      paragraphRows.push({
        paragraphId,
        sectionId,
        sectionHeading: section.heading,
        paragraphAnchor: paragraph.anchor,
        paragraphOrder: paragraph.order,
        sectionOrder: section.order,
        paragraphText: paragraph.text
      });
    }
  }

  return { paragraphRows, statements };
}

function buildDeleteDocumentTextArtifactStatements(env: Env, documentId: string) {
  return [
    env.DB.prepare(`DELETE FROM document_chunks WHERE document_id = ?`).bind(documentId),
    env.DB.prepare(
      `DELETE FROM section_paragraphs
       WHERE section_id IN (SELECT id FROM document_sections WHERE document_id = ?)`
    ).bind(documentId),
    env.DB.prepare(`DELETE FROM document_sections WHERE document_id = ?`).bind(documentId)
  ];
}

export function buildDocumentTextArtifactStatements(
  env: Env,
  params: {
    documentId: string;
    citation: string;
    sections: AuthoredSection[];
    generationId?: string;
  }
) {
  const now = new Date().toISOString();
  const generationId = params.generationId;
  const generation = generationId ? [generationId] : [];
  const deleteStatements = generationId ? [] : buildDeleteDocumentTextArtifactStatements(env, params.documentId);
  const { paragraphRows, statements: sectionStatements } = buildSectionAndParagraphStatements(env, params.documentId, params.sections, generationId);
  const chunks = buildSectionAwareChunks({ citation: params.citation, paragraphs: paragraphRows });
  const chunkStatements: D1PreparedStatement[] = [];

  for (const chunk of chunks) {
    chunkStatements.push(
      env.DB.prepare(
        `INSERT INTO ${generationId ? "staged_document_chunks" : "document_chunks"} (
          ${generationId ? "generation_id," : ""} id, document_id, section_id, paragraph_id, paragraph_anchor,
          paragraph_anchor_end, citation_anchor, section_label, chunk_order, chunk_text,
          token_estimate, chunk_warnings_json, created_at
        ) VALUES (${generationId ? "?," : ""} ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).bind(
        ...generation, chunk.id,
        params.documentId,
        chunk.sectionId,
        chunk.paragraphId,
        chunk.paragraphAnchor,
        chunk.paragraphAnchorEnd,
        chunk.citationAnchor,
        chunk.sectionLabel,
        chunk.chunkOrder,
        chunk.chunkText,
        chunk.tokenEstimate,
        JSON.stringify(chunk.warnings),
        now
      )
    );
  }

  return {
    statements: [...deleteStatements, ...sectionStatements, ...chunkStatements],
    deleteStatements,
    chunks,
    sectionCount: params.sections.length,
    paragraphCount: paragraphRows.length,
    chunkCount: chunks.length
  };
}

// Reprocessing uses unpublished generation tables and an atomic projection swap.
function qcPassed(flags: ParsedDocument["qcFlags"]): boolean {
  return flags.hasIndexCodes && flags.hasRulesSection && flags.hasOrdinanceSection;
}

function recomputeQcFlags(sections: AuthoredSection[], metadata: ParsedDocument["extractedMetadata"]): ParsedDocument["qcFlags"] {
  return computeQcFlags(sections.map((section) => section.heading || ""), metadata);
}

function shouldBeSearchable(fileType: FileType): boolean {
  return fileType === "law_pdf";
}

// The uploaded filename is user input and becomes an R2 key segment plus part of the persisted source
// URL. Restrict it to a safe charset (dots survive, so extension-based type detection keeps working) and
// cap the length — `/`, `..`, `?`, `#`, `%`, control chars, or multi-KB names would otherwise escape the
// `fileType/date/` key layout and persist permanently-broken links. Mirrors safeFilename in routes/source.
function sanitizeSourceFilenameSegment(filename: string): string {
  return (
    String(filename || "source")
      .replace(/[^a-zA-Z0-9._-]+/g, "_")
      .replace(/\.{2,}/g, ".")
      .replace(/^[._]+|[._]+$/g, "")
      .slice(0, 120) || "source"
  );
}

export async function ingestDocument(env: Env, input: unknown): Promise<PersistResult> {
  const parsedInput = ingestDocumentSchema.parse(input);
  const bytes = decodeBase64(parsedInput.sourceFile.bytesBase64);
  return ingestDocumentBytes(env, {...parsedInput,sourceFile:{filename:parsedInput.sourceFile.filename,mimeType:parsedInput.sourceFile.mimeType}}, bytes);
}

export async function ingestDocumentBytes(env: Env, input: unknown, bytes: Uint8Array): Promise<PersistResult> {
  const parsedInput = ingestDocumentMetadataSchema.parse(input);
  if(parsedInput.revisionOf&&!parsedInput.idempotencyKey)throw new RequestValidationError("Source revisions require an idempotency key");
  if (bytes.byteLength > maxIngestSourceBytes) throw new RequestBodyTooLargeError(maxIngestSourceBytes);
  const sourceFormat = detectSourceFormat(bytes, parsedInput.sourceFile.filename, parsedInput.fileType);
  const sourceSha256 = await sha256Bytes(bytes);
  if(!parsedInput.idempotencyKey)return persistIngestion(env,parsedInput,bytes,sourceFormat,sourceSha256);
  const attempt=await beginIngestion(env,parsedInput,sourceSha256);
  if(attempt.receipt.result_json)return parseIngestionReceiptResult(attempt.receipt.result_json,attempt.receipt);
  try {
    await persistIngestion(attempt.env,parsedInput,bytes,sourceFormat,sourceSha256,attempt,env);
  } catch(error) {
    // Inspect the durable receipt before releasing ownership or returning failure.
    // A publication commit can succeed even when its acknowledgment is lost.
    const stored=await env.DB.prepare('SELECT result_json FROM ingestion_requests WHERE request_id=?').bind(attempt.receipt.request_id).first<{result_json:string|null}>();
    if(stored?.result_json)return parseIngestionReceiptResult(stored.result_json,attempt.receipt);
    if(await releaseIngestion(env,attempt.receipt))throw new RequestConflictError("Revision predecessor changed during ingestion; review it and use a new request key");
    throw error;
  }
  const stored=await env.DB.prepare('SELECT result_json FROM ingestion_requests WHERE request_id=?').bind(attempt.receipt.request_id).first<{result_json:string}>();
  if(!stored?.result_json)throw new Error('Missing committed ingestion receipt');
  return parseIngestionReceiptResult(stored.result_json,attempt.receipt);
}
async function persistIngestion(env:Env,parsedInput:ReturnType<typeof ingestDocumentMetadataSchema.parse>,bytes:Uint8Array,
 sourceFormat:ReturnType<typeof detectSourceFormat>,sourceSha256:string,attempt?:IngestionAttempt,dispatchEnv:Env=env):Promise<PersistResult> {
  const documentId = attempt?.receipt.document_id ?? id("doc");
  const sourceKey = attempt?.receipt.source_key ?? `${parsedInput.fileType}/${new Date().toISOString().slice(0,10)}/${crypto.randomUUID()}-${sanitizeSourceFilenameSegment(parsedInput.sourceFile.filename)}`;
  const identical=attempt?await env.DB.prepare(`SELECT COUNT(*) OVER() AS total,document_id FROM document_source_revisions WHERE source_sha256=? AND identity_origin='verified_upload' ORDER BY document_id LIMIT 10`).bind(sourceSha256).all<{total:number;document_id:string}>():null;
  const sourceIdentity=attempt?{sha256:sourceSha256,recordId:attempt.receipt.record_id,sourceRevision:attempt.receipt.source_revision,
   predecessorDocumentId:attempt.receipt.predecessor_document_id,identicalSourceCount:identical?.results[0]?.total??0,identicalSourceDocumentIds:identical?.results.map(row=>row.document_id)??[]}:undefined;
  let extracted: ParsedDocument;
  try {
    extracted = sourceFormat === "markdown"
      ? parseMarkdownDocument(bytes)
      : await parseDocument(bytes, parsedInput.fileType);
  } catch (error) {
    if (!(error instanceof ExtractionReviewRequired)) throw error;
    // Retain the original for manual/OCR review, with no searchable artifacts or
    // vector job. Extraction failure must never turn binary syntax into authority.
    const now = new Date().toISOString();
    await storeSourceFile(env, sourceKey, bytes, sourceFormat);
    try {
      const insert=env.DB.prepare(`INSERT INTO documents (
        id, file_type, jurisdiction, title, citation, decision_date, source_r2_key, source_link,
        metadata_json, extraction_confidence, extraction_warnings_json,
        qc_has_index_codes, qc_has_rules_section, qc_has_ordinance_section, qc_passed,
        qc_required_confirmed, approved_at, searchable_at, ingestion_state, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 0, 0, 0, 0, 0, NULL, NULL, 'needs_review', ?, ?)`)
        .bind(documentId, parsedInput.fileType, parsedInput.jurisdiction, parsedInput.title,
          parsedInput.citation, parsedInput.decisionDate ?? null, sourceKey, sourceLink(env, sourceKey),
          JSON.stringify({ sourceSha256, originalFilename: parsedInput.sourceFile.filename, extraction: {
            status: "needs_review", format: sourceFormat, reason: error.reason, pipeline: "extraction/2"
          } }), JSON.stringify([error.message]), now, now);
      const result:PersistResult={documentId,qc:{hasIndexCodes:false,hasRulesSection:false,hasOrdinanceSection:false},sourceLink:reviewSourceLink(env,documentId),chunkCount:0,searchable:false,warnings:[error.message],extractionConfidence:0,extractionStatus:"needs_review",vectorJobStatus:"not_requested",...(sourceIdentity?{sourceIdentity,ingestionRequestId:attempt!.receipt.request_id}:{})};
      if(attempt)await env.DB.batch([insert,...attempt.completion(result)]);else await insert.run();
      return result;
    } catch (writeError) {
      await env.SOURCE_BUCKET.delete(sourceKey);
      throw writeError;
    }

  }
  env.TELEMETRY?.count('ingest_warnings',extracted.warnings.length);

  const now = new Date().toISOString();
  const searchable = shouldBeSearchable(parsedInput.fileType);
  const qcConfirmed = 0; // Extraction is not human metadata confirmation.
  const extractedMetadata: ParsedDocument["extractedMetadata"] = {
    ...extracted.extractedMetadata,
    indexCodes: [...extracted.extractedMetadata.indexCodes],
    rulesSections: [...extracted.extractedMetadata.rulesSections],
    ordinanceSections: [...extracted.extractedMetadata.ordinanceSections]
  };
  const warnings = [...extracted.warnings];
  const normalizedDecisionDate = parsedInput.decisionDate ?? extractedMetadata.decisionDate;
  const validationContext={jurisdiction:parsedInput.jurisdiction,as_of_date:normalizedDecisionDate||null};
  const referenceValues={indexCodes:extractedMetadata.indexCodes,rulesSections:extractedMetadata.rulesSections,ordinanceSections:extractedMetadata.ordinanceSections};
  const referencePlan=await prepareReferenceValidation(env,referenceValues,validationContext);
  const referenceValidation=await validateReferencesAgainstNormalized(env,referenceValues,validationContext,referencePlan);
  const normalizedIndexCount = await env.DB.prepare(`SELECT COUNT(*) as count FROM legal_index_codes WHERE active = 1`).first<{ count: number }>();
  if ((normalizedIndexCount?.count ?? 0) > 0 && referenceValidation.unknownIndexCodes.length > 0) {
    const unknownSet = new Set(referenceValidation.unknownIndexCodes);
    const kept = extractedMetadata.indexCodes.filter((value) => !unknownSet.has(value));
    const dropped = extractedMetadata.indexCodes.filter((value) => unknownSet.has(value));
    extractedMetadata.indexCodes = kept;
    if (dropped.length > 0) {
      warnings.push(`Extraction noise filtered (index codes): ${dropped.join(", ")}`);
    }
  }
  if (extractedMetadata.indexCodes.length === 0) {
    const inferred = await inferIndexCodesFromReferences(env, {
      rulesSections: extractedMetadata.rulesSections,
      ordinanceSections: extractedMetadata.ordinanceSections
    });
    if (inferred.inferredIndexCodes.length > 0) {
      extractedMetadata.indexCodes = inferred.inferredIndexCodes;
      warnings.push(
        `Index codes inferred from validated references: ${inferred.inferredIndexCodes.join(", ")} (${inferred.evidence.join("; ")})`
      );
    }
  }
  const qcFlags = recomputeQcFlags(extracted.sections, extractedMetadata);
  const passed = qcPassed(qcFlags);
  const taxonomySuggestion = inferTaxonomySuggestion({
    jurisdiction:parsedInput.jurisdiction,
    title: parsedInput.title,
    citation: parsedInput.citation,
    sections: extracted.sections,
    metadata: extractedMetadata
  });
  if (taxonomySuggestion.fallback || taxonomySuggestion.confidence < 0.45) {
    warnings.push("Taxonomy suggestion is low-confidence; review case type during QC");
  }
  const indexCodesJson = JSON.stringify(extractedMetadata.indexCodes);
  const rulesSectionsJson = JSON.stringify(extractedMetadata.rulesSections);
  const ordinanceSectionsJson = JSON.stringify(extractedMetadata.ordinanceSections);
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
    rules: extractedMetadata.rulesSections,
    ordinance: extractedMetadata.ordinanceSections
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
  const warningsJson = JSON.stringify(Array.from(new Set(warnings)));

  // A document is published to search only after its source and all derived DB rows have been
  // stored. D1 cannot span the multiple batches needed for long decisions, so the surrounding
  // compensation block removes any partial DB state and R2 object if a later operation fails.
  const documentInsertStatement = env.DB.prepare(
    `INSERT INTO documents (
      id, file_type, jurisdiction, title, citation, decision_date,
      source_r2_key, source_link, qc_has_index_codes, qc_has_rules_section,
      qc_has_ordinance_section, qc_passed, approved_at, searchable_at,
      metadata_json, case_number, author_name, outcome_label,
      index_codes_json, rules_sections_json, ordinance_sections_json,
      extraction_confidence, extraction_warnings_json,
      qc_required_confirmed, qc_confirmed_at, rejected_at, rejected_reason,
      created_at, updated_at, ingestion_state
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 'preparing')`
  )
    .bind(
      documentId,
      parsedInput.fileType,
      parsedInput.jurisdiction,
      parsedInput.title,
      parsedInput.citation,
      normalizedDecisionDate,
      sourceKey,
      sourceLink(env, sourceKey),
      qcFlags.hasIndexCodes ? 1 : 0,
      qcFlags.hasRulesSection ? 1 : 0,
      qcFlags.hasOrdinanceSection ? 1 : 0,
      passed ? 1 : 0,
      null,
      JSON.stringify({
        sourceSha256,
        originalFilename: parsedInput.sourceFile.filename,
        mimeType: parsedInput.sourceFile.mimeType,
        plainTextLength: extracted.plainText.length,
        extractedMetadata,
        extraction: extracted.extraction,
        citationEvidence: extracted.citations,
        taxonomy: taxonomySuggestion,
        referenceValidation
      }),
      extractedMetadata.caseNumber,
      extractedMetadata.author,
      extractedMetadata.outcomeLabel,
      indexCodesJson,
      rulesSectionsJson,
      ordinanceSectionsJson,
      extractedMetadata.extractionConfidence,
      warningsJson,
      qcConfirmed,
      qcConfirmed ? now : null,
      now,
      now
    );

  const referenceValidationStatements = await buildDocumentReferenceValidationStatements(env, documentId, {
    indexCodes: extractedMetadata.indexCodes,
    rulesSections: extractedMetadata.rulesSections,
    ordinanceSections: extractedMetadata.ordinanceSections
  },undefined,undefined,validationContext,referencePlan);
  const artifacts = buildDocumentTextArtifactStatements(env, {
    documentId,
    citation: parsedInput.citation,
    sections: extracted.sections
  });
  const durableResult:PersistResult={documentId,qc:qcFlags,sourceLink:reviewSourceLink(env,documentId),chunkCount:artifacts.chunkCount,searchable,
   warnings:Array.from(new Set(warnings)),extractionConfidence:extractedMetadata.extractionConfidence,extractionStatus:"extracted",
   vectorJobStatus:parsedInput.performVectorUpsert?"queued":"not_requested",...(sourceIdentity?{sourceIdentity,ingestionRequestId:attempt!.receipt.request_id}:{})};
  let sourceStored = false;
  let preparedVectorDigest: string | null = null;
  try {
    await executeTextArtifactStatementBatches(env, [
      documentInsertStatement,
      ...referenceValidationStatements,
      ...artifacts.statements
    ]);
    await storeSourceFile(env, sourceKey, bytes, sourceFormat);
    sourceStored = true;

    const vectorManifest = parsedInput.performVectorUpsert ? await readVectorManifest(env, documentId) : null;
    if (vectorManifest) {
      preparedVectorDigest = vectorManifest.digest;
      if (vectorManifest.count !== artifacts.chunkCount) throw new Error("Vector manifest count mismatch");
      await verifyVectorSource(env, sourceKey, sourceSha256);
    }
    const completedStatement = env.DB.prepare(`UPDATE documents SET ingestion_state = 'ready', ingestion_completed_at = ?,
      expected_chunk_count = ?, searchable_at = ?, published_revision = ?, updated_at = ?
      WHERE id = ? AND ingestion_state = 'preparing' AND document_revision=1
        AND (SELECT COUNT(*) FROM document_chunks WHERE document_id = documents.id) = ? RETURNING id`)
      .bind(now, artifacts.chunkCount, searchable ? now : null, searchable ? 1 : null, now, documentId, artifacts.chunkCount);
    const receiptStatements=attempt?.completion(durableResult)??[];
    const published = await env.DB.batch([...receiptStatements,completedStatement, ...(vectorManifest ? [vectorOutboxStatement(env, {
      documentId, generationId:"initial", sourceKey, sourceSha256, count:vectorManifest.count, digest:vectorManifest.digest, timestamp:now
    })] : [])]);
    if (published.slice(receiptStatements.length).some(result => result.results.length !== 1)) throw new Error("Ingestion completeness check failed");
  } catch (error) {
    if(attempt){
      const committed=await dispatchEnv.DB.prepare('SELECT result_json FROM ingestion_requests WHERE request_id=?').bind(attempt.receipt.request_id).first<{result_json:string|null}>();
      // The outer wrapper resolves a committed receipt. Do not let a delayed
      // losing invocation dispatch the newly completed owner's job again.
      if(committed?.result_json)throw error;
    }
    // An acknowledgment can be lost after the publication transaction commits.
    // Inspect before compensating, and never delete a ready or concurrently edited import.
    const stored = await env.DB.prepare("SELECT ingestion_state,source_r2_key FROM documents WHERE id=?").bind(documentId)
      .first<{ingestion_state:string;source_r2_key:string}>();
    if (stored?.ingestion_state !== "ready" || stored.source_r2_key !== sourceKey) {
      const removed = await env.DB.prepare("DELETE FROM documents WHERE id=? AND ingestion_state='preparing' AND document_revision=1 RETURNING id")
        .bind(documentId).first<{id:string}>();
      if (sourceStored && (removed?.id === documentId || !stored)) await env.SOURCE_BUCKET.delete(sourceKey);
      throw error;
    }
    if (parsedInput.performVectorUpsert) {
      const receipt = await env.DB.prepare(`SELECT document_id FROM document_vector_jobs WHERE document_id=? AND generation_id='initial'
        AND outbox_ready_at IS NOT NULL AND source_key=? AND source_sha256=? AND expected_chunk_count=? AND chunk_manifest_sha256=?`)
        .bind(documentId,sourceKey,sourceSha256,artifacts.chunkCount,preparedVectorDigest).first();
      if (!receipt) throw error;
    }
  }

  let vectorJobStatus: PersistResult["vectorJobStatus"] = "not_requested";
  if (parsedInput.performVectorUpsert) {
    try {
      await enqueueVectorJob(dispatchEnv, documentId);
      vectorJobStatus = "queued";
    } catch (error) {
      // The durable job row remains queued and the scheduled sweep will re-enqueue it. Do not turn
      // an otherwise committed upload into a failed client request just because the queue is down.
      vectorJobStatus = "queue_unavailable";
      warnings.push("Document stored; vector job is queued for scheduled recovery.");
      env.TELEMETRY?.tag("job_state","dispatch_deferred");
    }
  }

  return {
    documentId,
    qc: qcFlags,
    // The persisted link is the example.invalid sentinel; the response must hand back the working
    // proxy URL, not the raw sentinel (ARCH-02).
    sourceLink: reviewSourceLink(env, documentId),
    chunkCount: artifacts.chunkCount,
    searchable,
    warnings: Array.from(new Set(warnings)),
    extractionConfidence: extractedMetadata.extractionConfidence,
    extractionStatus: "extracted",
    vectorJobStatus
  };
}

export async function approveDecision(env: Env, documentId: string, expectedRevision: number) {
  return approveDocumentRevision(env, documentId, expectedRevision);
}
