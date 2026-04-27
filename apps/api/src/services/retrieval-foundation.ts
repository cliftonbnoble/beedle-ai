import type { Env } from "../lib/types";
import { effectiveSourceLink } from "./storage";

export type RetrievalChunkType =
  | "caption_title"
  | "procedural_history"
  | "facts_background"
  | "issue_statement"
  | "authority_discussion"
  | "analysis_reasoning"
  | "findings"
  | "holding_disposition"
  | "general_body";

export type ChunkClassificationReason =
  | "heading_match"
  | "paragraph_window_fallback"
  | "authority_reference_density"
  | "finding_section_match"
  | "disposition_language_match"
  | "issue_language_match"
  | "analysis_language_match"
  | "caption_signal_match"
  | "procedural_language_match"
  | "facts_background_language_match";

export type RetrievalPriority = "high" | "medium" | "low";
export type ChunkRepairStrategy =
  | "none"
  | "low_structure_discourse_split"
  | "citation_density_boundary_split"
  | "micro_heading_normalization"
  | "disposition_tail_split";

export interface DecisionRetrievalDocument {
  documentId: string;
  title: string;
  citation: string;
  jurisdiction: string;
  authorName: string | null;
  decisionDate: string | null;
  sourceFileRef: string;
  sourceLink: string;
  fileType: "decision_docx";
  sections: Array<{
    sectionId: string;
    canonicalKey: string;
    heading: string;
    sectionOrder: number;
    paragraphCount: number;
  }>;
  validReferences: {
    indexCodes: string[];
    rulesSections: string[];
    ordinanceSections: string[];
  };
}

export interface DecisionRetrievalChunk {
  chunkId: string;
  documentId: string;
  title: string;
  citation: string;
  chunkType: RetrievalChunkType;
  chunkTypeConfidence: number;
  chunkClassificationReason: ChunkClassificationReason;
  chunkOrdinal: number;
  sectionLabel: string;
  sectionCanonicalKey: string;
  headingPath: string[];
  paragraphAnchorStart: string;
  paragraphAnchorEnd: string;
  citationAnchorStart: string;
  citationAnchorEnd: string;
  sourceText: string;
  textLength: number;
  charStart: number | null;
  charEnd: number | null;
  tokenEstimate: number;
  containsFindings: boolean;
  containsProceduralHistory: boolean;
  containsAuthorityDiscussion: boolean;
  containsDispositionLanguage: boolean;
  referenceDensity: number;
  citationFamilies: string[];
  ordinanceReferences: string[];
  rulesReferences: string[];
  indexCodeReferences: string[];
  canonicalOrdinanceReferences: string[];
  canonicalRulesReferences: string[];
  canonicalIndexCodes: string[];
  retrievalPriority: RetrievalPriority;
  retrievalPriorityReason: string;
  questionFitSignals: {
    fitsIssueQuery: boolean;
    fitsAuthorityQuery: boolean;
    fitsFindingsQuery: boolean;
    fitsDispositionQuery: boolean;
  };
  hasCitationAnchorCoverage: boolean;
  hasCanonicalReferenceAlignment: boolean;
  segmentQualityFlags: string[];
  chunkRepairApplied: boolean;
  chunkRepairStrategy: ChunkRepairStrategy;
  chunkRepairNotes: string[];
  provenance: {
    sourceFileRef: string;
    sourceLink: string;
    sectionId: string;
    sectionCanonicalKey: string;
    sectionLabel: string;
    paragraphAnchorStart: string;
    paragraphAnchorEnd: string;
    citationAnchorStart: string;
    citationAnchorEnd: string;
    chunkOrdinal: number;
  };
}

interface ParagraphRow {
  sectionId: string;
  canonicalKey: string;
  heading: string;
  sectionOrder: number;
  paragraphOrder: number;
  anchor: string;
  text: string;
}

interface RetrievalPreviewOptions {
  includeText?: boolean;
}

interface RetrievalRawDebugOptions {
  includeText?: boolean;
  maxParagraphRows?: number;
}

async function fetchSectionParagraphRows(env: Env, documentId: string): Promise<{ rows: ParagraphRow[]; fallbackUsed: boolean }> {
  const primarySql = `SELECT s.id as sectionId, s.canonical_key as canonicalKey, s.heading, s.section_order as sectionOrder,
            p.paragraph_order as paragraphOrder, p.anchor, COALESCE(CAST(p.text AS TEXT), '') as text
       FROM document_sections s
       JOIN section_paragraphs p ON p.section_id = s.id
      WHERE s.document_id = ?
      ORDER BY s.section_order ASC, p.paragraph_order ASC`;

  try {
    const sections = await env.DB.prepare(primarySql).bind(documentId).all<ParagraphRow>();
    return { rows: sections.results ?? [], fallbackUsed: false };
  } catch {
    const fallbackSql = `SELECT s.id as sectionId, s.canonical_key as canonicalKey, s.heading, s.section_order as sectionOrder,
            p.paragraph_order as paragraphOrder, p.anchor, SUBSTR(COALESCE(CAST(p.text AS TEXT), ''), 1, 12000) as text
       FROM document_sections s
       JOIN section_paragraphs p ON p.section_id = s.id
      WHERE s.document_id = ?
      ORDER BY s.section_order ASC, p.paragraph_order ASC`;
    const fallback = await env.DB.prepare(fallbackSql).bind(documentId).all<ParagraphRow>();
    return { rows: fallback.results ?? [], fallbackUsed: true };
  }
}

interface ChunkWindowPlan {
  start: number;
  end: number;
  repairStrategy: ChunkRepairStrategy;
  repairNotes: string[];
}

const DEFAULT_CHUNK_TARGET = 650;
const DEFAULT_CHUNK_MIN = 250;
const DEFAULT_CHUNK_MAX = 980;
const OVERLAP_PARAGRAPHS = 1;

function normalizeWhitespace(input: string): string {
  return String(input || "")
    .replace(/\r/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function normalizeForMatch(input: string): string {
  return normalizeWhitespace(input).toLowerCase();
}

function estimateTokens(text: string): number {
  return Math.max(1, Math.ceil(text.length / 4));
}

function fnv1a(input: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

function uniqueSorted(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

function normalizeCompactReference(input: string): string {
  return normalizeWhitespace(input)
    .toUpperCase()
    .replace(/[^A-Z0-9.()\-]+/g, "");
}

function headingToType(heading: string): { type: RetrievalChunkType | null; normalizedLabel: string } {
  const h = normalizeForMatch(heading);
  if (!h || h === "body") return { type: null, normalizedLabel: "body" };

  if (/caption|appearance|parties|before the|case\s+no\.?|case\s+number/.test(h)) {
    return { type: "caption_title", normalizedLabel: "caption_parties" };
  }
  if (/procedural|history|background and procedural|chronology|prior proceedings|hearing history/.test(h)) {
    return { type: "procedural_history", normalizedLabel: "procedural_history" };
  }
  if (/facts|background|factual background|statement of facts|record background/.test(h)) {
    return { type: "facts_background", normalizedLabel: "facts_background" };
  }
  if (/issues? presented|issues?|question presented|question/.test(h)) {
    return { type: "issue_statement", normalizedLabel: "issue_statement" };
  }
  if (/conclusions? of law|authority|legal standard|rules?|ordinance/.test(h)) {
    return { type: "authority_discussion", normalizedLabel: "authority_discussion" };
  }
  if (/analysis|discussion|reasoning|application of law/.test(h)) {
    return { type: "analysis_reasoning", normalizedLabel: "analysis_reasoning" };
  }
  if (/findings?( of fact)?/.test(h)) {
    return { type: "findings", normalizedLabel: "findings" };
  }
  if (/holding|order|disposition|decision|relief|conclusion/.test(h)) {
    return { type: "holding_disposition", normalizedLabel: "holding_disposition" };
  }
  return { type: null, normalizedLabel: "unknown_heading" };
}

function detectSignals(text: string) {
  const families = new Set<string>();
  const ordinance = new Set<string>();
  const rules = new Set<string>();
  const indexCodes = new Set<string>();
  const lowered = normalizeForMatch(text);

  const pushRuleCitation = (rawValue: string) => {
    const citation = normalizeWhitespace(rawValue).replace(/^section\s+/i, "");
    if (!citation) return;
    rules.add(citation.toLowerCase().startsWith("rule ") ? citation.replace(/^rule\s+/i, "Rule ") : "Rule " + citation);
    const family = citation.match(/^([0-9]+\.[0-9]+)/)?.[1] ?? citation;
    families.add(family);
  };

  const pushIndexCode = (rawValue: string) => {
    const token = normalizeCompactReference(rawValue);
    if (!token) return;
    if (!/^(?:IC-\d{1,4}[A-Z]?|[A-Z]{1,3}\d{1,4}(?:\.\d+)*(?:[A-Z])?|[A-Z]{1,4}-\d{1,4}[A-Z]?)$/.test(token)) {
      return;
    }
    indexCodes.add(token);
  };

  const ordinanceMatches = Array.from(text.matchAll(/\bordinance\s+([0-9]+(?:\.[0-9]+)*(?:\([a-z0-9]+\))*)/gi));
  for (const match of ordinanceMatches) {
    const citation = String(match[1] || "").trim();
    if (!citation) continue;
    ordinance.add("Ordinance " + citation);
    const family = citation.match(/^([0-9]+\.[0-9]+)/)?.[1] ?? citation;
    families.add(family);
  }

  const rulesMatches = Array.from(
    text.matchAll(/\brules?(?:\s+and\s+regulations?)?\s+sections?\s+([0-9]+(?:\.[0-9A-Z]+)*(?:\([a-z0-9]+\))*)/gi)
  );
  for (const match of rulesMatches) {
    pushRuleCitation(String(match[1] || ""));
  }

  const rulesListMatches = Array.from(text.matchAll(/\brules?(?:\s+and\s+regulations?)?\s+sections?:\s*([^\n]+)/gi));
  for (const match of rulesListMatches) {
    for (const token of String(match[1] || "").split(/[;,]/)) {
      pushRuleCitation(token);
    }
  }

  const indexListMatches = Array.from(text.matchAll(/\bindex codes?:\s*([^\n]+)/gi));
  for (const match of indexListMatches) {
    for (const token of String(match[1] || "").split(/[;,]/)) {
      pushIndexCode(token);
    }
  }

  const indexMatches = Array.from(
    text.matchAll(/\b(?:IC-\d{1,4}[A-Z]?|[A-Z]{1,3}\d{1,4}(?:\.\d+)*(?:[A-Z])?|[A-Z]{1,4}-\d{1,4}[A-Z]?)\b/g)
  );
  for (const match of indexMatches) {
    pushIndexCode(String(match[0] || ""));
  }

  const totalRefs = ordinance.size + rules.size + indexCodes.size;
  const density = text.length > 0 ? Number(((totalRefs / text.length) * 1000).toFixed(4)) : 0;

  const containsFindings = /\bfindings?\b|found that|finding of fact|credibility/.test(lowered);
  const containsProceduralHistory = /procedural|history|hearing|petition filed|notice of hearing|continuance|prior order/.test(lowered);
  const containsAuthorityDiscussion =
    /ordinance\s+[0-9]|rules?(?:\s+and\s+regulations?)?\s+sections?|authority|conclusions? of law|legal standard|index codes?/.test(
      lowered
    );
  const containsDispositionLanguage = /ordered|granted|denied|dismissed|disposition|relief|affirmed|reversed/.test(lowered);
  const containsIssueLanguage = /\bissue\b|issues? presented|question presented|whether/.test(lowered);
  const containsAnalysisLanguage = /analysis|therefore|because|reasoning|we conclude|balanc/.test(lowered);
  const containsCaptionSignal = /case\s+no\.?|petitioner|respondent|before the|department/.test(lowered);
  const containsFactsLanguage = /facts?|background|record shows|testimony|evidence/.test(lowered);

  return {
    citationFamilies: uniqueSorted(Array.from(families)),
    ordinanceReferences: uniqueSorted(Array.from(ordinance)),
    rulesReferences: uniqueSorted(Array.from(rules)),
    indexCodeReferences: uniqueSorted(Array.from(indexCodes)),
    referenceDensity: density,
    containsFindings,
    containsProceduralHistory,
    containsAuthorityDiscussion,
    containsDispositionLanguage,
    containsIssueLanguage,
    containsAnalysisLanguage,
    containsCaptionSignal,
    containsFactsLanguage
  };
}

function determineChunkClassification(params: {
  heading: string;
  text: string;
  fallback: boolean;
  referenceDensity: number;
}) {
  const heading = headingToType(params.heading);
  const s = detectSignals(params.text);

  const withFallbackReason = (
    value: {
      chunkType: RetrievalChunkType;
      chunkTypeConfidence: number;
      chunkClassificationReason: ChunkClassificationReason;
    }
  ) =>
    params.fallback && heading.type === null
      ? {
          ...value,
          chunkClassificationReason: "paragraph_window_fallback" as const,
          chunkTypeConfidence: Math.min(value.chunkTypeConfidence, 0.64)
        }
      : value;

  if (heading.type) {
    const byHeadingReason: ChunkClassificationReason =
      heading.type === "findings"
        ? "finding_section_match"
        : heading.type === "authority_discussion"
          ? "authority_reference_density"
          : "heading_match";
    return {
      ...withFallbackReason({
        chunkType: heading.type,
        chunkTypeConfidence: heading.type === "findings" ? 0.93 : 0.9,
        chunkClassificationReason: byHeadingReason
      }),
      headingNormalizedLabel: heading.normalizedLabel,
      signals: s
    };
  }

  if (s.containsDispositionLanguage) {
    return {
      ...withFallbackReason({
        chunkType: "holding_disposition",
        chunkTypeConfidence: 0.78,
        chunkClassificationReason: "disposition_language_match"
      }),
      headingNormalizedLabel: heading.normalizedLabel,
      signals: s
    };
  }
  if (s.containsFindings) {
    return {
      ...withFallbackReason({
        chunkType: "findings",
        chunkTypeConfidence: 0.76,
        chunkClassificationReason: "finding_section_match"
      }),
      headingNormalizedLabel: heading.normalizedLabel,
      signals: s
    };
  }
  if (s.containsProceduralHistory) {
    return {
      ...withFallbackReason({
        chunkType: "procedural_history",
        chunkTypeConfidence: 0.7,
        chunkClassificationReason: "procedural_language_match"
      }),
      headingNormalizedLabel: heading.normalizedLabel,
      signals: s
    };
  }
  if (s.containsFactsLanguage && !s.containsAuthorityDiscussion) {
    return {
      ...withFallbackReason({
        chunkType: "facts_background",
        chunkTypeConfidence: 0.68,
        chunkClassificationReason: "facts_background_language_match"
      }),
      headingNormalizedLabel: heading.normalizedLabel,
      signals: s
    };
  }
  if (s.containsIssueLanguage) {
    return {
      ...withFallbackReason({
        chunkType: "issue_statement",
        chunkTypeConfidence: 0.66,
        chunkClassificationReason: "issue_language_match"
      }),
      headingNormalizedLabel: heading.normalizedLabel,
      signals: s
    };
  }
  if (s.containsAnalysisLanguage) {
    return {
      ...withFallbackReason({
        chunkType: "analysis_reasoning",
        chunkTypeConfidence: 0.64,
        chunkClassificationReason: "analysis_language_match"
      }),
      headingNormalizedLabel: heading.normalizedLabel,
      signals: s
    };
  }
  if (s.containsAuthorityDiscussion || params.referenceDensity >= 2.0) {
    return {
      ...withFallbackReason({
        chunkType: "authority_discussion",
        chunkTypeConfidence: params.referenceDensity >= 2.6 ? 0.72 : 0.66,
        chunkClassificationReason: "authority_reference_density"
      }),
      headingNormalizedLabel: heading.normalizedLabel,
      signals: s
    };
  }
  if (s.containsCaptionSignal) {
    return {
      ...withFallbackReason({
        chunkType: "caption_title",
        chunkTypeConfidence: 0.62,
        chunkClassificationReason: "caption_signal_match"
      }),
      headingNormalizedLabel: heading.normalizedLabel,
      signals: s
    };
  }

  return {
    ...withFallbackReason({
      chunkType: "general_body",
      chunkTypeConfidence: params.fallback ? 0.57 : 0.53,
      chunkClassificationReason: params.fallback ? "paragraph_window_fallback" : "heading_match"
    }),
    headingNormalizedLabel: heading.normalizedLabel,
    signals: s
  };
}

function detectMicroHeading(text: string): boolean {
  const normalized = normalizeWhitespace(text);
  if (!normalized || normalized.length > 110) return false;
  if (!/^[A-Za-z0-9/&().,\-:;'\s]+$/.test(normalized)) return false;
  return /(^[A-Z0-9][A-Z0-9\s/&().,\-:;']{3,}$)|(^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,6}:?$)/.test(normalized);
}

function detectParagraphProfile(text: string) {
  const normalized = normalizeWhitespace(text);
  const signals = detectSignals(normalized);
  const classified = determineChunkClassification({
    heading: "",
    text: normalized,
    fallback: true,
    referenceDensity: signals.referenceDensity
  });
  return {
    text: normalized,
    signals,
    classifiedType: classified.chunkType,
    referenceDensity: signals.referenceDensity,
    isMicroHeading: detectMicroHeading(normalized)
  };
}

function buildDefaultChunkWindows(rows: ParagraphRow[]): ChunkWindowPlan[] {
  const windows: ChunkWindowPlan[] = [];
  let cursor = 0;
  while (cursor < rows.length) {
    let end = cursor;
    let length = 0;

    while (end < rows.length) {
      const paraLength = normalizeWhitespace(rows[end].text).length;
      const tentative = length + paraLength;
      if (tentative > DEFAULT_CHUNK_MAX && length >= DEFAULT_CHUNK_MIN) break;
      length = tentative;
      end += 1;
      if (length >= DEFAULT_CHUNK_TARGET) break;
    }

    if (end <= cursor) end = Math.min(rows.length, cursor + 1);

    windows.push({
      start: cursor,
      end,
      repairStrategy: "none",
      repairNotes: []
    });

    const nextCursor = end - OVERLAP_PARAGRAPHS;
    cursor = nextCursor <= cursor ? end : nextCursor;
  }
  return windows;
}

function buildLowStructureRepairWindows(rows: ParagraphRow[]): ChunkWindowPlan[] {
  if (rows.length <= 1) {
    return [{ start: 0, end: rows.length, repairStrategy: "none", repairNotes: [] }];
  }

  const profiles = rows.map((row) => detectParagraphProfile(row.text));
  const windows: ChunkWindowPlan[] = [];
  let start = 0;
  let startStrategy: ChunkRepairStrategy = "none";
  let startNotes: string[] = [];
  let runningLength = profiles[0]?.text.length || 0;

  const shouldSplitAt = (index: number): { split: boolean; reason: ChunkRepairStrategy; note: string } => {
    if (index <= start || index >= rows.length) return { split: false, reason: "none", note: "" };
    const prev = profiles[index - 1];
    const current = profiles[index];
    const remaining = rows.length - index;
    const minLenForSemanticSplit = Math.max(120, Math.floor(DEFAULT_CHUNK_MIN * 0.55));
    const minLenForTailSplit = Math.max(90, Math.floor(DEFAULT_CHUNK_MIN * 0.4));

    if (current.isMicroHeading && runningLength >= minLenForSemanticSplit) {
      return {
        split: true,
        reason: "micro_heading_normalization",
        note: `micro_heading@${rows[index].anchor}`
      };
    }

    if (
      current.classifiedType === "holding_disposition" &&
      remaining <= 2 &&
      runningLength >= minLenForTailSplit
    ) {
      return {
        split: true,
        reason: "disposition_tail_split",
        note: `disposition_tail@${rows[index].anchor}`
      };
    }

    const prevDensity = Math.max(prev.referenceDensity, 0.0001);
    const currentDensity = Math.max(current.referenceDensity, 0.0001);
    const densityRatio = Math.max(prevDensity, currentDensity) / Math.min(prevDensity, currentDensity);
    const densityDiff = Math.abs(prev.referenceDensity - current.referenceDensity);
    if (densityRatio >= 1.8 && densityDiff >= 0.25 && runningLength >= minLenForSemanticSplit) {
      return {
        split: true,
        reason: "citation_density_boundary_split",
        note: `density_shift:${prev.referenceDensity.toFixed(2)}->${current.referenceDensity.toFixed(2)}`
      };
    }

    const typeChanged =
      prev.classifiedType !== current.classifiedType &&
      prev.classifiedType !== "general_body" &&
      current.classifiedType !== "general_body";
    if (typeChanged && runningLength >= minLenForSemanticSplit) {
      return {
        split: true,
        reason: "low_structure_discourse_split",
        note: `type_shift:${prev.classifiedType}->${current.classifiedType}`
      };
    }

    return { split: false, reason: "none", note: "" };
  };

  for (let i = 1; i < rows.length; i += 1) {
    const decision = shouldSplitAt(i);
    const currentLength = profiles[i]?.text.length || 0;
    const wouldExceedMax = runningLength + currentLength > DEFAULT_CHUNK_MAX;

    if (decision.split || (wouldExceedMax && runningLength >= DEFAULT_CHUNK_MIN)) {
      windows.push({
        start,
        end: i,
        repairStrategy: startStrategy,
        repairNotes: startNotes
      });
      start = i;
      if (decision.split) {
        startStrategy = decision.reason;
        startNotes = decision.note ? [decision.note] : [];
      } else {
        startStrategy = "none";
        startNotes = [];
      }
      runningLength = currentLength;
      continue;
    }

    runningLength += currentLength;
  }

  windows.push({
    start,
    end: rows.length,
    repairStrategy: startStrategy,
    repairNotes: startNotes
  });

  return windows.filter((window) => window.end > window.start);
}

function buildDocumentTextOffsets(paragraphs: ParagraphRow[]) {
  const orderSorted = [...paragraphs].sort((a, b) => a.sectionOrder - b.sectionOrder || a.paragraphOrder - b.paragraphOrder);
  const byAnchor = new Map<string, { start: number; end: number }>();
  const blocks: string[] = [];
  let cursor = 0;

  for (const row of orderSorted) {
    const text = normalizeWhitespace(row.text);
    if (!text) continue;
    const start = cursor;
    const end = cursor + text.length;
    byAnchor.set(row.anchor, { start, end });
    blocks.push(text);
    cursor = end + 2;
  }

  return {
    plainText: blocks.join("\n\n"),
    offsetsByAnchor: byAnchor
  };
}

function alignCanonicalReferences(params: {
  detected: { ordinanceReferences: string[]; rulesReferences: string[]; indexCodeReferences: string[] };
  valid: DecisionRetrievalDocument["validReferences"];
  text: string;
}) {
  const lowered = normalizeForMatch(params.text);
  const canonicalOrdinanceReferences = params.valid.ordinanceSections.filter((value) => lowered.includes(normalizeForMatch(value)));
  const canonicalRulesReferences = params.valid.rulesSections.filter((value) => lowered.includes(normalizeForMatch(value)));
  const canonicalIndexCodes = params.valid.indexCodes.filter((value) => lowered.includes(normalizeForMatch(value)));
  const validIndexCodesByNormalized = new Map(
    params.valid.indexCodes.map((value) => [normalizeCompactReference(value), value])
  );

  if (!canonicalOrdinanceReferences.length) {
    for (const raw of params.detected.ordinanceReferences) {
      const token = raw.replace(/^Ordinance\s+/i, "").trim();
      const matched = params.valid.ordinanceSections.find((value) => normalizeForMatch(value).includes(normalizeForMatch(token)));
      if (matched) canonicalOrdinanceReferences.push(matched);
    }
  }

  if (!canonicalRulesReferences.length) {
    for (const raw of params.detected.rulesReferences) {
      const token = raw.replace(/^Rule\s+/i, "").trim();
      const matched = params.valid.rulesSections.find((value) => normalizeForMatch(value).includes(normalizeForMatch(token)));
      if (matched) canonicalRulesReferences.push(matched);
    }
  }

  if (!canonicalIndexCodes.length) {
    for (const raw of params.detected.indexCodeReferences) {
      const matched = validIndexCodesByNormalized.get(normalizeCompactReference(raw));
      if (matched) canonicalIndexCodes.push(matched);
    }
  }

  return {
    canonicalOrdinanceReferences: uniqueSorted(canonicalOrdinanceReferences),
    canonicalRulesReferences: uniqueSorted(canonicalRulesReferences),
    canonicalIndexCodes: uniqueSorted(canonicalIndexCodes)
  };
}

function classifyRetrievalPriority(params: {
  chunkType: RetrievalChunkType;
  confidence: number;
  hasCanonicalReferenceAlignment: boolean;
  containsFindings: boolean;
  containsDispositionLanguage: boolean;
  containsAuthorityDiscussion: boolean;
}) {
  if (
    params.chunkType === "holding_disposition" ||
    params.chunkType === "findings" ||
    (params.chunkType === "analysis_reasoning" && params.confidence >= 0.66) ||
    (params.chunkType === "authority_discussion" && params.hasCanonicalReferenceAlignment)
  ) {
    return {
      retrievalPriority: "high" as const,
      retrievalPriorityReason: "core_merits_or_authority_signal"
    };
  }

  if (
    params.chunkType === "procedural_history" ||
    params.chunkType === "facts_background" ||
    params.chunkType === "issue_statement" ||
    params.containsAuthorityDiscussion ||
    params.containsFindings ||
    params.containsDispositionLanguage
  ) {
    return {
      retrievalPriority: "medium" as const,
      retrievalPriorityReason: "contextual_support_signal"
    };
  }

  return {
    retrievalPriority: "low" as const,
    retrievalPriorityReason: "low_specificity_signal"
  };
}

function buildSegmentQualityFlags(params: {
  textLength: number;
  paragraphCount: number;
  headingType: RetrievalChunkType | null;
  fallbackChunking: boolean;
  hasCanonicalReferenceAlignment: boolean;
  hasDetectedReferences: boolean;
  topicSignals: {
    containsFindings: boolean;
    containsProceduralHistory: boolean;
    containsAuthorityDiscussion: boolean;
    containsDispositionLanguage: boolean;
  };
}) {
  const flags: string[] = [];
  if (params.textLength > DEFAULT_CHUNK_MAX) flags.push("overlong_chunk");
  if (params.textLength > DEFAULT_CHUNK_TARGET * 1.35 || params.paragraphCount >= 6) flags.push("undersegmented_chunk");
  if (!params.headingType) flags.push("weak_heading_signal");
  if (params.fallbackChunking) flags.push("fallback_only");
  if (params.hasDetectedReferences && !params.hasCanonicalReferenceAlignment) flags.push("low_reference_alignment");

  const topicCount = [
    params.topicSignals.containsFindings,
    params.topicSignals.containsProceduralHistory,
    params.topicSignals.containsAuthorityDiscussion,
    params.topicSignals.containsDispositionLanguage
  ].filter(Boolean).length;
  if (topicCount >= 3) flags.push("mixed_topic_chunk");

  return uniqueSorted(flags);
}

function buildChunksFromRows(params: {
  doc: DecisionRetrievalDocument;
  rows: ParagraphRow[];
  offsetsByAnchor: Map<string, { start: number; end: number }>;
  fallbackChunking: boolean;
  applyLowStructureRepair: boolean;
}): DecisionRetrievalChunk[] {
  const sorted = [...params.rows].sort((a, b) => a.sectionOrder - b.sectionOrder || a.paragraphOrder - b.paragraphOrder);
  const out: DecisionRetrievalChunk[] = [];
  if (!sorted.length) return out;

  const windows =
    params.applyLowStructureRepair && params.fallbackChunking
      ? buildLowStructureRepairWindows(sorted)
      : buildDefaultChunkWindows(sorted);

  for (const window of windows) {
    const chunkRows = sorted.slice(window.start, window.end);
    if (!chunkRows.length) continue;
    const startRow = chunkRows[0];
    const endRow = chunkRows[chunkRows.length - 1];
    const sourceText = normalizeWhitespace(chunkRows.map((row) => normalizeWhitespace(row.text)).join("\n\n"));

    const classified = determineChunkClassification({
      heading: startRow.heading,
      text: sourceText,
      fallback: params.fallbackChunking,
      referenceDensity: detectSignals(sourceText).referenceDensity
    });

    const detected = classified.signals;
    const canonical = alignCanonicalReferences({
      detected,
      valid: params.doc.validReferences,
      text: sourceText
    });

    const hasCanonicalReferenceAlignment =
      canonical.canonicalOrdinanceReferences.length > 0 ||
      canonical.canonicalRulesReferences.length > 0 ||
      canonical.canonicalIndexCodes.length > 0;

    const priority = classifyRetrievalPriority({
      chunkType: classified.chunkType,
      confidence: classified.chunkTypeConfidence,
      hasCanonicalReferenceAlignment,
      containsFindings: detected.containsFindings,
      containsDispositionLanguage: detected.containsDispositionLanguage,
      containsAuthorityDiscussion: detected.containsAuthorityDiscussion
    });

    const headingResolved = headingToType(startRow.heading);
    const segmentQualityFlags = buildSegmentQualityFlags({
      textLength: sourceText.length,
      paragraphCount: chunkRows.length,
      headingType: headingResolved.type,
      fallbackChunking: params.fallbackChunking,
      hasCanonicalReferenceAlignment,
      hasDetectedReferences:
        detected.ordinanceReferences.length > 0 || detected.rulesReferences.length > 0 || detected.indexCodeReferences.length > 0,
      topicSignals: {
        containsFindings: detected.containsFindings,
        containsProceduralHistory: detected.containsProceduralHistory,
        containsAuthorityDiscussion: detected.containsAuthorityDiscussion,
        containsDispositionLanguage: detected.containsDispositionLanguage
      }
    });

    const offsetsStart = params.offsetsByAnchor.get(startRow.anchor) ?? null;
    const offsetsEnd = params.offsetsByAnchor.get(endRow.anchor) ?? null;
    const stableSeed = [
      params.doc.documentId,
      classified.chunkType,
      classified.chunkClassificationReason,
      String(startRow.sectionOrder),
      startRow.anchor,
      endRow.anchor,
      sourceText.slice(0, 180)
    ].join("|");

    const chunkId = `drchk_${fnv1a(stableSeed)}`;
    const citationAnchorStart = `${params.doc.citation}#${startRow.anchor}`;
    const citationAnchorEnd = `${params.doc.citation}#${endRow.anchor}`;
    const chunkOrdinal = out.length;

    out.push({
      chunkId,
      documentId: params.doc.documentId,
      title: params.doc.title,
      citation: params.doc.citation,
      chunkType: classified.chunkType,
      chunkTypeConfidence: classified.chunkTypeConfidence,
      chunkClassificationReason: classified.chunkClassificationReason,
      chunkOrdinal,
      sectionLabel: startRow.heading,
      sectionCanonicalKey: startRow.canonicalKey,
      headingPath: [startRow.heading, classified.headingNormalizedLabel],
      paragraphAnchorStart: startRow.anchor,
      paragraphAnchorEnd: endRow.anchor,
      citationAnchorStart,
      citationAnchorEnd,
      sourceText,
      textLength: sourceText.length,
      charStart: offsetsStart?.start ?? null,
      charEnd: offsetsEnd?.end ?? null,
      tokenEstimate: estimateTokens(sourceText),
      containsFindings: detected.containsFindings,
      containsProceduralHistory: detected.containsProceduralHistory,
      containsAuthorityDiscussion: detected.containsAuthorityDiscussion,
      containsDispositionLanguage: detected.containsDispositionLanguage,
      referenceDensity: detected.referenceDensity,
      citationFamilies: detected.citationFamilies,
      ordinanceReferences: detected.ordinanceReferences,
      rulesReferences: detected.rulesReferences,
      indexCodeReferences: detected.indexCodeReferences,
      canonicalOrdinanceReferences: canonical.canonicalOrdinanceReferences,
      canonicalRulesReferences: canonical.canonicalRulesReferences,
      canonicalIndexCodes: canonical.canonicalIndexCodes,
      retrievalPriority: priority.retrievalPriority,
      retrievalPriorityReason: priority.retrievalPriorityReason,
      questionFitSignals: {
        fitsIssueQuery: detected.containsIssueLanguage,
        fitsAuthorityQuery: detected.containsAuthorityDiscussion,
        fitsFindingsQuery: detected.containsFindings,
        fitsDispositionQuery: detected.containsDispositionLanguage
      },
      hasCitationAnchorCoverage: Boolean(citationAnchorStart && citationAnchorEnd),
      hasCanonicalReferenceAlignment,
      segmentQualityFlags,
      chunkRepairApplied: window.repairStrategy !== "none",
      chunkRepairStrategy: window.repairStrategy,
      chunkRepairNotes: uniqueSorted(window.repairNotes),
      provenance: {
        sourceFileRef: params.doc.sourceFileRef,
        sourceLink: params.doc.sourceLink,
        sectionId: startRow.sectionId,
        sectionCanonicalKey: startRow.canonicalKey,
        sectionLabel: startRow.heading,
        paragraphAnchorStart: startRow.anchor,
        paragraphAnchorEnd: endRow.anchor,
        citationAnchorStart,
        citationAnchorEnd,
        chunkOrdinal
      }
    });
  }

  return out.map((chunk, idx) => ({
    ...chunk,
    chunkOrdinal: idx,
    provenance: { ...chunk.provenance, chunkOrdinal: idx }
  }));
}

async function fetchDecisionRows(env: Env, documentId: string) {
  const doc = await env.DB.prepare(
    `SELECT id, title, citation, jurisdiction, author_name as authorName, decision_date as decisionDate,
            source_r2_key as sourceFileRef, source_link as sourceLink, file_type as fileType
       FROM documents
      WHERE id = ?`
  )
    .bind(documentId)
    .first<{
      id: string;
      title: string;
      citation: string;
      jurisdiction: string;
      authorName: string | null;
      decisionDate: string | null;
      sourceFileRef: string;
      sourceLink: string;
      fileType: "decision_docx" | "law_pdf";
    }>();

  if (!doc) return null;
  if (doc.fileType !== "decision_docx") {
    throw new Error(`retrieval foundation only supports decision_docx in this phase; received ${doc.fileType}`);
  }

  const sections = await fetchSectionParagraphRows(env, documentId);

  const references = await env.DB.prepare(
    `SELECT reference_type as referenceType, canonical_value as canonicalValue
       FROM document_reference_links
      WHERE document_id = ?
        AND is_valid = 1`
  )
    .bind(documentId)
    .all<{ referenceType: "index_code" | "rules_section" | "ordinance_section"; canonicalValue: string }>();

  return {
    doc,
    paragraphs: sections.rows ?? [],
    references: references.results ?? []
  };
}

export async function getDecisionRetrievalRawDebug(
  env: Env,
  documentId: string,
  options: RetrievalRawDebugOptions = {}
) {
  const includeText = options.includeText !== false;
  const maxParagraphRows = Math.max(1, Math.min(200, Number(options.maxParagraphRows || 40)));

  const doc = await env.DB.prepare(
    `SELECT id, title, citation, jurisdiction, decision_date as decisionDate,
            source_r2_key as sourceFileRef, source_link as sourceLink, file_type as fileType
       FROM documents
      WHERE id = ?`
  )
    .bind(documentId)
    .first<{
      id: string;
      title: string;
      citation: string;
      jurisdiction: string;
      decisionDate: string | null;
      sourceFileRef: string;
      sourceLink: string;
      fileType: "decision_docx" | "law_pdf";
    }>();

  if (!doc) return null;

  const sectionRows = await fetchSectionParagraphRows(env, documentId);

  const referenceRows = await env.DB.prepare(
    `SELECT reference_type as referenceType, canonical_value as canonicalValue, is_valid as isValid
       FROM document_reference_links
      WHERE document_id = ?
      ORDER BY canonical_value ASC`
  )
    .bind(documentId)
    .all<{ referenceType: string; canonicalValue: string; isValid: number }>();

  const paragraphs = (sectionRows.rows ?? []).slice(0, maxParagraphRows).map((row) => {
    const text = String(row.text || "");
    return {
      sectionId: row.sectionId,
      canonicalKey: row.canonicalKey,
      heading: row.heading,
      sectionOrder: row.sectionOrder,
      paragraphOrder: row.paragraphOrder,
      anchor: row.anchor,
      textPreview: includeText ? text.slice(0, 220) : "",
      textLength: text.length
    };
  });

  return {
    documentId,
    includeText,
    maxParagraphRows,
    doc,
    sectionParagraphCount: Number(sectionRows.rows?.length || 0),
    sectionParagraphFallbackUsed: Boolean(sectionRows.fallbackUsed),
    referenceCount: Number(referenceRows.results?.length || 0),
    paragraphRows: paragraphs,
    references: (referenceRows.results || []).map((row) => ({
      referenceType: row.referenceType,
      canonicalValue: row.canonicalValue,
      isValid: Number(row.isValid || 0)
    }))
  };
}

function shouldUseFallbackChunking(sections: DecisionRetrievalDocument["sections"]) {
  const meaningful = sections.filter((section) => {
    const resolved = headingToType(section.heading);
    return Boolean(resolved.type);
  });
  return meaningful.length <= 1;
}

function countBy(values: string[]) {
  const out: Record<string, number> = {};
  for (const value of values) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

export async function getDecisionRetrievalPreview(env: Env, documentId: string, options: RetrievalPreviewOptions = {}) {
  const loaded = await fetchDecisionRows(env, documentId);
  if (!loaded) return null;

  const sectionMap = new Map<string, { sectionId: string; canonicalKey: string; heading: string; sectionOrder: number; paragraphCount: number }>();
  for (const row of loaded.paragraphs) {
    const entry = sectionMap.get(row.sectionId) ?? {
      sectionId: row.sectionId,
      canonicalKey: row.canonicalKey,
      heading: row.heading,
      sectionOrder: row.sectionOrder,
      paragraphCount: 0
    };
    entry.paragraphCount += 1;
    sectionMap.set(row.sectionId, entry);
  }

  const validReferences = {
    indexCodes: uniqueSorted(loaded.references.filter((row) => row.referenceType === "index_code").map((row) => row.canonicalValue)),
    rulesSections: uniqueSorted(loaded.references.filter((row) => row.referenceType === "rules_section").map((row) => row.canonicalValue)),
    ordinanceSections: uniqueSorted(loaded.references.filter((row) => row.referenceType === "ordinance_section").map((row) => row.canonicalValue))
  };

  const sourceLink = effectiveSourceLink(env, loaded.doc.sourceFileRef, loaded.doc.sourceLink);
  const retrievalDoc: DecisionRetrievalDocument = {
    documentId: loaded.doc.id,
    title: loaded.doc.title,
    citation: loaded.doc.citation,
    jurisdiction: loaded.doc.jurisdiction,
    authorName: loaded.doc.authorName ?? null,
    decisionDate: loaded.doc.decisionDate,
    sourceFileRef: loaded.doc.sourceFileRef,
    sourceLink,
    fileType: "decision_docx",
    sections: Array.from(sectionMap.values()).sort((a, b) => a.sectionOrder - b.sectionOrder),
    validReferences
  };

  const { offsetsByAnchor, plainText } = buildDocumentTextOffsets(loaded.paragraphs);
  const useFallbackChunking = shouldUseFallbackChunking(retrievalDoc.sections);
  const rowsBySection = new Map<string, ParagraphRow[]>();
  for (const row of loaded.paragraphs) {
    const list = rowsBySection.get(row.sectionId) ?? [];
    list.push(row);
    rowsBySection.set(row.sectionId, list);
  }

  const buildAllChunks = (applyLowStructureRepair: boolean): DecisionRetrievalChunk[] => {
    const built: DecisionRetrievalChunk[] = [];
    if (applyLowStructureRepair && useFallbackChunking) {
      const allRows = [...loaded.paragraphs].sort((a, b) => a.sectionOrder - b.sectionOrder || a.paragraphOrder - b.paragraphOrder);
      built.push(
        ...buildChunksFromRows({
          doc: retrievalDoc,
          rows: allRows,
          offsetsByAnchor,
          fallbackChunking: useFallbackChunking,
          applyLowStructureRepair: true
        })
      );
      return built.map((chunk, idx) => ({
        ...chunk,
        chunkOrdinal: idx,
        provenance: { ...chunk.provenance, chunkOrdinal: idx }
      }));
    }

    for (const section of retrievalDoc.sections) {
      const sectionRows = rowsBySection.get(section.sectionId) ?? [];
      if (!sectionRows.length) continue;
      built.push(
        ...buildChunksFromRows({
          doc: retrievalDoc,
          rows: sectionRows,
          offsetsByAnchor,
          fallbackChunking: useFallbackChunking,
          applyLowStructureRepair
        })
      );
    }
    return built.map((chunk, idx) => ({
      ...chunk,
      chunkOrdinal: idx,
      provenance: { ...chunk.provenance, chunkOrdinal: idx }
    }));
  };

  const preRepairChunks = buildAllChunks(false);
  const preRepairChunkTypeCounts = countBy(preRepairChunks.map((chunk) => chunk.chunkType));
  const preRepairTypeSpread = Object.keys(preRepairChunkTypeCounts).length;
  const preRepairMixedTopicCount = preRepairChunks.filter((chunk) => chunk.segmentQualityFlags.includes("mixed_topic_chunk")).length;
  const preRepairWeakHeadingCount = preRepairChunks.filter((chunk) => chunk.segmentQualityFlags.includes("weak_heading_signal")).length;
  const preRepairAlignedCount = preRepairChunks.filter((chunk) => chunk.hasCanonicalReferenceAlignment).length;

  const lowStructureRepairCandidate =
    useFallbackChunking &&
    (preRepairChunks.length <= 3 ||
      preRepairTypeSpread <= 2 ||
      preRepairMixedTopicCount > 0 ||
      preRepairWeakHeadingCount > 0 ||
      retrievalDoc.sections.length <= 2 ||
      loaded.paragraphs.length >= 4 ||
      (preRepairChunks.length > 0 && preRepairAlignedCount / preRepairChunks.length < 0.25));

  let finalChunks = preRepairChunks;
  if (lowStructureRepairCandidate) {
    const repairedChunks = buildAllChunks(true);
    const repairedTypeSpread = Object.keys(countBy(repairedChunks.map((chunk) => chunk.chunkType))).length;
    const repairedMixedTopicCount = repairedChunks.filter((chunk) => chunk.segmentQualityFlags.includes("mixed_topic_chunk")).length;
    const repairedWeakHeadingCount = repairedChunks.filter((chunk) => chunk.segmentQualityFlags.includes("weak_heading_signal")).length;
    const repairedAlignedCount = repairedChunks.filter((chunk) => chunk.hasCanonicalReferenceAlignment).length;
    const repairedHasStrategies = repairedChunks.some((chunk) => chunk.chunkRepairApplied);
    const boundaryChanged =
      repairedChunks.length !== preRepairChunks.length ||
      repairedChunks.map((chunk) => chunk.chunkId).join("|") !== preRepairChunks.map((chunk) => chunk.chunkId).join("|");
    const alignmentImproved =
      preRepairChunks.length > 0 &&
      repairedChunks.length > 0 &&
      repairedAlignedCount / repairedChunks.length > preRepairAlignedCount / preRepairChunks.length;
    const improved =
      repairedHasStrategies &&
      (boundaryChanged ||
        repairedChunks.length > preRepairChunks.length ||
        repairedTypeSpread > preRepairTypeSpread ||
        repairedMixedTopicCount < preRepairMixedTopicCount ||
        repairedWeakHeadingCount < preRepairWeakHeadingCount ||
        alignmentImproved);
    if (improved) {
      finalChunks = repairedChunks;
    }
  }

  finalChunks = finalChunks.map((chunk, idx) => ({
    ...chunk,
    chunkOrdinal: idx,
    provenance: { ...chunk.provenance, chunkOrdinal: idx }
  }));

  const chunkTypeCounts = countBy(finalChunks.map((chunk) => chunk.chunkType));
  const chunkClassificationReasonCounts = countBy(finalChunks.map((chunk) => chunk.chunkClassificationReason));
  const retrievalPriorityCounts = countBy(finalChunks.map((chunk) => chunk.retrievalPriority));
  const repairStrategyCounts = countBy(finalChunks.filter((chunk) => chunk.chunkRepairApplied).map((chunk) => chunk.chunkRepairStrategy));
  const preRepairChunkClassificationReasonCounts = countBy(preRepairChunks.map((chunk) => chunk.chunkClassificationReason));
  const preRepairChunkTypeSpread = Object.keys(preRepairChunkTypeCounts).length;
  const preRepairOverlongCount = preRepairChunks.filter((chunk) => chunk.segmentQualityFlags.includes("overlong_chunk")).length;
  const preRepairAvgLength =
    preRepairChunks.length > 0
      ? Math.round(preRepairChunks.reduce((sum, chunk) => sum + chunk.textLength, 0) / preRepairChunks.length)
      : 0;
  const preRepairReferenceDensityAvg =
    preRepairChunks.length > 0
      ? Number((preRepairChunks.reduce((sum, chunk) => sum + chunk.referenceDensity, 0) / preRepairChunks.length).toFixed(4))
      : 0;
  const repairApplied = finalChunks.some((chunk) => chunk.chunkRepairApplied);
  const repairedChunkCount = finalChunks.filter((chunk) => chunk.chunkRepairApplied).length;

  return {
    document: retrievalDoc,
    stats: {
      sectionCount: retrievalDoc.sections.length,
      headingCount: retrievalDoc.sections.filter((section) => headingToType(section.heading).type !== null).length,
      paragraphCount: loaded.paragraphs.length,
      chunkCount: finalChunks.length,
      chunkTypeSpread: Object.keys(chunkTypeCounts).length,
      chunkTypes: uniqueSorted(finalChunks.map((chunk) => chunk.chunkType)),
      chunkTypeCounts,
      chunkClassificationReasonCounts,
      preRepairChunkClassificationReasonCounts,
      retrievalPriorityCounts,
      avgChunkLength: finalChunks.length > 0 ? Math.round(finalChunks.reduce((sum, chunk) => sum + chunk.textLength, 0) / finalChunks.length) : 0,
      maxChunkLength: finalChunks.length > 0 ? Math.max(...finalChunks.map((chunk) => chunk.textLength)) : 0,
      minChunkLength: finalChunks.length > 0 ? Math.min(...finalChunks.map((chunk) => chunk.textLength)) : 0,
      usedFallbackChunking: useFallbackChunking,
      repairApplied,
      repairStrategyCounts,
      repairedChunkCount,
      preRepairChunkCount: preRepairChunks.length,
      postRepairChunkCount: finalChunks.length,
      preRepairChunkTypeSpread,
      preRepairChunkTypeCounts,
      preRepairChunksFlaggedOverlong: preRepairOverlongCount,
      preRepairChunksFlaggedMixedTopic: preRepairMixedTopicCount,
      preRepairChunksWithWeakHeadingSignal: preRepairWeakHeadingCount,
      preRepairChunksWithCanonicalReferenceAlignment: preRepairAlignedCount,
      preRepairAvgChunkLength: preRepairAvgLength,
      preRepairReferenceDensityAvg,
      chunksFlaggedOverlong: finalChunks.filter((chunk) => chunk.segmentQualityFlags.includes("overlong_chunk")).length,
      chunksFlaggedMixedTopic: finalChunks.filter((chunk) => chunk.segmentQualityFlags.includes("mixed_topic_chunk")).length,
      chunksWithWeakHeadingSignal: finalChunks.filter((chunk) => chunk.segmentQualityFlags.includes("weak_heading_signal")).length,
      chunksWithCanonicalReferenceAlignment: finalChunks.filter((chunk) => chunk.hasCanonicalReferenceAlignment).length,
      referenceDensityStats: {
        min: finalChunks.length > 0 ? Math.min(...finalChunks.map((chunk) => chunk.referenceDensity)) : 0,
        max: finalChunks.length > 0 ? Math.max(...finalChunks.map((chunk) => chunk.referenceDensity)) : 0,
        avg: finalChunks.length > 0 ? Number((finalChunks.reduce((sum, chunk) => sum + chunk.referenceDensity, 0) / finalChunks.length).toFixed(4)) : 0
      },
      parsingGaps: [
        retrievalDoc.sections.length === 0 ? "no_sections" : null,
        loaded.paragraphs.length === 0 ? "no_paragraphs" : null,
        finalChunks.length === 0 ? "no_chunks" : null,
        finalChunks.length <= 2 ? "low_chunk_count" : null,
        Object.keys(chunkTypeCounts).length <= 2 ? "low_type_diversity" : null
      ].filter((item): item is string => Boolean(item)),
      plainTextLength: plainText.length
    },
    chunks: options.includeText === false ? finalChunks.map((chunk) => ({ ...chunk, sourceText: "" })) : finalChunks
  };
}
