import { ingestDocumentSchema, type FileType } from "@beedle/shared";
import type { AuthoredSection, Env, ParsedDocument } from "../lib/types";
import { parseDocument, parseMarkdownDocument } from "./parser";
import { embed } from "./embeddings";
import { sourceLink, storeSourceFile } from "./storage";
import { inferTaxonomySuggestion } from "./taxonomy-inference";
import { inferIndexCodesFromReferences, refreshDocumentReferenceValidation, validateReferencesAgainstNormalized } from "./legal-references";

interface PersistResult {
  documentId: string;
  qc: ParsedDocument["qcFlags"];
  sourceLink: string;
  chunkCount: number;
  searchable: boolean;
  warnings: string[];
  extractionConfidence: number;
}

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

function decodeBase64(input: string): Uint8Array {
  const raw = atob(input);
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

async function insertChunkVectors(env: Env, documentId: string, chunks: ChunkRow[]) {
  const payload: VectorizeVector[] = [];

  for (const chunk of chunks) {
    const vector = await embed(env, chunk.chunkText);
    if (!vector) {
      continue;
    }
    payload.push({
      id: chunk.id,
      values: vector,
      namespace: env.VECTOR_NAMESPACE,
      metadata: {
        documentId,
        paragraphAnchor: chunk.paragraphAnchor,
        paragraphAnchorEnd: chunk.paragraphAnchorEnd,
        citationAnchor: chunk.citationAnchor,
        sectionLabel: chunk.sectionLabel
      } as Record<string, VectorizeVectorMetadata>
    });
  }

  if (payload.length === 0) {
    return;
  }

  try {
    await env.VECTOR_INDEX.upsert(payload);
  } catch {
    // Local vector bindings can be unavailable; lexical search remains functional.
  }
}

async function insertSectionsAndParagraphs(env: Env, documentId: string, sections: AuthoredSection[]) {
  const paragraphRows: PersistParagraphRow[] = [];

  for (const section of sections) {
    const sectionId = id("sec");
    const sectionText = section.paragraphs.map((paragraph) => paragraph.text).join("\n\n");

    await env.DB.prepare(
      `INSERT INTO document_sections (id, document_id, canonical_key, heading, section_order, section_text)
       VALUES (?, ?, ?, ?, ?, ?)`
    )
      .bind(sectionId, documentId, section.canonicalKey, section.heading, section.order, sectionText)
      .run();

    for (const paragraph of section.paragraphs) {
      const paragraphId = id("par");
      await env.DB.prepare(
        `INSERT INTO section_paragraphs (id, section_id, anchor, paragraph_order, text)
         VALUES (?, ?, ?, ?, ?)`
      )
        .bind(paragraphId, sectionId, paragraph.anchor, paragraph.order, paragraph.text)
        .run();

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

  return paragraphRows;
}

async function deleteDocumentTextArtifacts(env: Env, documentId: string) {
  await env.DB.prepare(`DELETE FROM document_chunks WHERE document_id = ?`).bind(documentId).run();
  await env.DB.prepare(
    `DELETE FROM section_paragraphs
     WHERE section_id IN (SELECT id FROM document_sections WHERE document_id = ?)`
  )
    .bind(documentId)
    .run();
  await env.DB.prepare(`DELETE FROM document_sections WHERE document_id = ?`).bind(documentId).run();
}

export async function rebuildDocumentTextArtifacts(
  env: Env,
  params: {
    documentId: string;
    citation: string;
    sections: AuthoredSection[];
    performVectorUpsert?: boolean;
  }
) {
  const now = new Date().toISOString();
  await deleteDocumentTextArtifacts(env, params.documentId);

  const paragraphRows = await insertSectionsAndParagraphs(env, params.documentId, params.sections);
  const chunks = buildSectionAwareChunks({ citation: params.citation, paragraphs: paragraphRows });

  for (const chunk of chunks) {
    await env.DB.prepare(
      `INSERT INTO document_chunks (
        id, document_id, section_id, paragraph_id, paragraph_anchor,
        paragraph_anchor_end, citation_anchor, section_label, chunk_order, chunk_text,
        token_estimate, chunk_warnings_json, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
      .bind(
        chunk.id,
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
      .run();
  }

  if (params.performVectorUpsert) {
    await insertChunkVectors(env, params.documentId, chunks);
  }

  return {
    sectionCount: params.sections.length,
    paragraphCount: paragraphRows.length,
    chunkCount: chunks.length
  };
}

function qcPassed(flags: ParsedDocument["qcFlags"]): boolean {
  return flags.hasIndexCodes && flags.hasRulesSection && flags.hasOrdinanceSection;
}

function recomputeQcFlags(sections: AuthoredSection[], metadata: ParsedDocument["extractedMetadata"]): ParsedDocument["qcFlags"] {
  const headings = sections.map((section) => section.heading || "");
  return {
    hasIndexCodes: metadata.indexCodes.length > 0 || headings.some((heading) => /index\s+codes?/i.test(heading)),
    hasRulesSection: metadata.rulesSections.length > 0 || headings.some((heading) => /^rules?$/i.test(heading)),
    hasOrdinanceSection: metadata.ordinanceSections.length > 0 || headings.some((heading) => /^ordinance(s)?$/i.test(heading))
  };
}

function shouldBeSearchable(fileType: FileType): boolean {
  return fileType === "law_pdf";
}

function normalizeCitationToken(input: string): string {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/^section/, "")
    .replace(/^sec\.?/, "")
    .replace(/^rule/, "")
    .replace(/^part[0-9a-z.\-]+\-/, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

function isMarkdownSourceFile(input: { filename: string; mimeType: string }) {
  const name = String(input.filename || "").toLowerCase();
  const mime = String(input.mimeType || "").toLowerCase();
  return mime.includes("markdown") || name.endsWith(".md") || name.endsWith(".markdown");
}

function detectCriticalReferenceExceptions(values: { rules: string[]; ordinance: string[] }) {
  const refs = [...values.rules, ...values.ordinance].map(normalizeCitationToken);
  const hits: string[] = [];
  if (refs.includes("37.2(g)")) hits.push("37.2(g)");
  if (refs.includes("37.15")) hits.push("37.15");
  if (refs.includes("10.10(c)(3)")) hits.push("10.10(c)(3)");
  return Array.from(new Set(hits));
}

export async function ingestDocument(env: Env, input: unknown): Promise<PersistResult> {
  const parsedInput = ingestDocumentSchema.parse(input);
  const bytes = decodeBase64(parsedInput.sourceFile.bytesBase64);
  const sourceKey = `${parsedInput.fileType}/${new Date().toISOString().slice(0, 10)}/${crypto.randomUUID()}-${parsedInput.sourceFile.filename}`;

  await storeSourceFile(env, sourceKey, bytes, parsedInput.sourceFile.mimeType);
  const extracted = isMarkdownSourceFile(parsedInput.sourceFile)
    ? parseMarkdownDocument(bytes)
    : parseDocument(bytes, parsedInput.fileType);
  if (extracted.warnings.length > 0) {
    console.warn(
      JSON.stringify({
        event: "ingest_warnings",
        filename: parsedInput.sourceFile.filename,
        fileType: parsedInput.fileType,
        warnings: extracted.warnings
      })
    );
  }

  const documentId = id("doc");
  const now = new Date().toISOString();
  const searchable = shouldBeSearchable(parsedInput.fileType);
  const qcConfirmed = parsedInput.fileType === "law_pdf" ? 1 : 0;
  const extractedMetadata: ParsedDocument["extractedMetadata"] = {
    ...extracted.extractedMetadata,
    indexCodes: [...extracted.extractedMetadata.indexCodes],
    rulesSections: [...extracted.extractedMetadata.rulesSections],
    ordinanceSections: [...extracted.extractedMetadata.ordinanceSections]
  };
  const warnings = [...extracted.warnings];
  const referenceValidation = await validateReferencesAgainstNormalized(env, {
    indexCodes: extractedMetadata.indexCodes,
    rulesSections: extractedMetadata.rulesSections,
    ordinanceSections: extractedMetadata.ordinanceSections
  });
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
  const normalizedDecisionDate = parsedInput.decisionDate ?? extractedMetadata.decisionDate;
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

  await env.DB.prepare(
    `INSERT INTO documents (
      id, file_type, jurisdiction, title, citation, decision_date,
      source_r2_key, source_link, qc_has_index_codes, qc_has_rules_section,
      qc_has_ordinance_section, qc_passed, approved_at, searchable_at,
      metadata_json, case_number, author_name, outcome_label,
      index_codes_json, rules_sections_json, ordinance_sections_json,
      extraction_confidence, extraction_warnings_json,
      qc_required_confirmed, qc_confirmed_at, rejected_at, rejected_reason,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)`
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
      searchable ? now : null,
      JSON.stringify({
        originalFilename: parsedInput.sourceFile.filename,
        mimeType: parsedInput.sourceFile.mimeType,
        plainTextLength: extracted.plainText.length,
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
    )
    .run();

  await refreshDocumentReferenceValidation(env, documentId, {
    indexCodes: extractedMetadata.indexCodes,
    rulesSections: extractedMetadata.rulesSections,
    ordinanceSections: extractedMetadata.ordinanceSections
  });

  const artifacts = await rebuildDocumentTextArtifacts(env, {
    documentId,
    citation: parsedInput.citation,
    sections: extracted.sections,
    performVectorUpsert: parsedInput.performVectorUpsert
  });

  return {
    documentId,
    qc: qcFlags,
    sourceLink: sourceLink(env, sourceKey),
    chunkCount: artifacts.chunkCount,
    searchable,
    warnings: Array.from(new Set(warnings)),
    extractionConfidence: extractedMetadata.extractionConfidence
  };
}

export async function approveDecision(env: Env, documentId: string): Promise<{ approved: boolean; reason?: string }> {
  const row = await env.DB.prepare(
    `SELECT
      qc_has_index_codes as hasIndexCodes,
      qc_has_rules_section as hasRules,
      qc_has_ordinance_section as hasOrdinance,
      qc_required_confirmed as qcConfirmed,
      rejected_at as rejectedAt
     FROM documents WHERE id = ? AND file_type = ?`
  )
    .bind(documentId, "decision_docx" satisfies FileType)
    .first<{ hasIndexCodes: number; hasRules: number; hasOrdinance: number; qcConfirmed: number; rejectedAt: string | null }>();

  if (!row) {
    return { approved: false, reason: "Decision document not found" };
  }

  if (row.rejectedAt) {
    return { approved: false, reason: "Decision was rejected in QC review" };
  }

  if (!(row.hasIndexCodes && row.hasRules && row.hasOrdinance)) {
    return {
      approved: false,
      reason: "QC gate blocked approval: required Index Codes, Rules, and Ordinance sections are missing"
    };
  }

  if (!row.qcConfirmed) {
    return {
      approved: false,
      reason: "QC gate blocked approval: required metadata must be manually confirmed in admin review"
    };
  }

  const now = new Date().toISOString();
  await env.DB.prepare(
    `UPDATE documents
     SET approved_at = ?, searchable_at = COALESCE(searchable_at, ?), updated_at = ?
     WHERE id = ?`
  )
    .bind(now, now, now, documentId)
    .run();

  return { approved: true };
}
