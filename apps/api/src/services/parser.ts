import { extractCitations, citationLabel } from "./legal-citations";
import { extractDocxText, extractPdfText, requireReadableText, type ExtractionDiagnostics } from "./text-extraction";
import type { FileType } from "@beedle/shared";
import type { AuthoredSection, ParsedDocument } from "../lib/types";
import { canonicalizeJudgeName, extractCanonicalJudgeNamesFromText } from "./judges";

const REQUIRED_SECTION_LABELS = {
  indexCodes: /index\s+codes?/i,
  rules: /^rules?$/i,
  ordinance: /^ordinance(s)?$/i
} as const;

const KNOWN_HEADINGS = [
  "findings of fact",
  "conclusions of law",
  "conclusion",
  "decision",
  "order",
  "background",
  "discussion",
  "analysis",
  "facts",
  "rules",
  "ordinance",
  "index codes"
];

const EMBEDDED_HEADING_SPLITS = [
  "FINDINGS OF FACT",
  "CONCLUSIONS OF LAW",
  "ORDER",
  "DECISION",
  "BACKGROUND",
  "DISCUSSION",
  "ANALYSIS",
  "RULES",
  "ORDINANCE",
  "INDEX CODES"
];

const CAPTION_HEADING_PATTERNS = [
  /^city and county of /i,
  /^case no\.?/i,
  /^(tenant|landlord)\s+(petitioner|respondent|appellant|appellee)[,.]?$/i,
  /^in re\b/i,
  /^from [a-z0-9]/i,
  /^[a-z0-9 .,'&/-]+\s+llc[,.]?$/i,
  /^[a-z0-9 .,'&/-]+\s+inc[,.]?$/i,
  /^[a-z0-9 .,'&/-]+\s+corporation[,.]?$/i,
  /^[a-z0-9 .,'&/-]+\s+management[,.]?$/i
];

function normalizeWhitespace(input: string): string {
  return input
    .replace(/\r/g, "")
    .replace(/\u00A0/g, " ")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function isCaptionHeadingNoise(line: string): boolean {
  const clean = normalizeWhitespace(String(line || "").replace(/^#+\s*/, "").replace(/:$/, ""));
  if (!clean) {
    return false;
  }

  if (CAPTION_HEADING_PATTERNS.some((pattern) => pattern.test(clean))) {
    return true;
  }

  if (/^([A-Z][A-Z0-9.'#&/,-]*\s+){1,6}(LLC|INC|L\.?P\.?|LP|LTD|CORP\.?|CORPORATION)[,.]?$/i.test(clean)) {
    return true;
  }

  if (/^[A-Z][A-Z0-9.'#&/,-]*(\s+[A-Z][A-Z0-9.'#&/,-]*)*\s*\(#?\d{1,6}\)\s*,?$/i.test(clean)) {
    return true;
  }

  return false;
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function canonicalKey(heading: string): string {
  return heading.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_|_$/g, "");
}

function splitLongParagraph(text: string): string[] {
  if (text.length < 900) {
    return [text];
  }

  const out: string[] = [];
  const sentences = text.split(/(?<=[.!?])\s+/);
  let current = "";

  for (const sentence of sentences) {
    const candidate = current ? `${current} ${sentence}` : sentence;
    if (candidate.length > 520 && current) {
      out.push(current);
      current = sentence;
    } else {
      current = candidate;
    }
  }

  if (current) {
    out.push(current);
  }

  return out.filter(Boolean);
}

function expandEmbeddedHeadings(text: string): string[] {
  let working = String(text || "");
  for (const heading of EMBEDDED_HEADING_SPLITS) {
    const pattern = new RegExp(`\\s*${escapeRegex(heading)}\\s*`, "g");
    working = working.replace(pattern, `\n\n${heading}\n\n`);
  }
  return working
    .split(/\n{2,}/)
    .map((part) => normalizeWhitespace(part))
    .filter(Boolean);
}

function normalizeInputParagraphs(paragraphs: string[]): { paragraphs: string[]; warnings: string[] } {
  const warnings: string[] = [];
  const out: string[] = [];

  for (const paragraph of paragraphs) {
    const expanded = expandEmbeddedHeadings(paragraph);
    if (expanded.length > 1) {
      warnings.push(`Recovered ${expanded.length - 1} embedded heading boundaries from paragraph`);
    }

    for (const expandedParagraph of expanded.length > 0 ? expanded : [paragraph]) {
      const clean = normalizeWhitespace(expandedParagraph);
      if (!clean) {
        continue;
      }
      if (/^<\/?w:[^>]+>$/i.test(clean) || /^w:[a-z]/i.test(clean)) {
        warnings.push("Dropped pure DOCX XML artifact paragraph");
        continue;
      }

      const split = splitLongParagraph(clean);
      if (split.length > 1) {
        warnings.push(`Split long merged paragraph into ${split.length} parts`);
      }
      out.push(...split);
    }
  }

  return { paragraphs: out, warnings };
}

function stripStandaloneMarkdownDecoration(line: string): string {
  let clean = String(line || "").trim();
  if (!clean) {
    return "";
  }

  clean = clean.replace(/^#{1,6}\s+/, "");
  clean = clean.replace(/^\*\*(.+)\*\*$/, "$1");
  clean = clean.replace(/^__(.+)__$/, "$1");
  clean = clean.replace(/^\*(.+)\*$/, "$1");
  clean = clean.replace(/^_(.+)_$/, "$1");
  return normalizeWhitespace(clean);
}

function isMarkdownTableDivider(line: string): boolean {
  const trimmed = String(line || "").trim();
  if (!trimmed) {
    return false;
  }
  return /^\|?(?:\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\|?$/.test(trimmed);
}

function extractMarkdownParagraphs(bytes: Uint8Array): string[] {
  const lines = new TextDecoder().decode(bytes).replace(/\r/g, "").split("\n");
  const out: string[] = [];
  let current: string[] = [];

  const flush = () => {
    if (current.length === 0) {
      return;
    }
    const paragraph = normalizeWhitespace(current.join(" "));
    if (paragraph) {
      out.push(paragraph);
    }
    current = [];
  };

  for (const rawLine of lines) {
    const trimmed = rawLine.trim();
    if (!trimmed) {
      flush();
      continue;
    }

    if (isMarkdownTableDivider(trimmed)) {
      continue;
    }

    if (/^\|.*\|$/.test(trimmed)) {
      flush();
      const cells = trimmed
        .split("|")
        .map((cell) => stripStandaloneMarkdownDecoration(cell))
        .filter(Boolean);
      if (cells.length > 0) {
        out.push(normalizeWhitespace(cells.join(" | ")));
      }
      continue;
    }

    const cleaned = stripStandaloneMarkdownDecoration(trimmed);
    if (!cleaned) {
      flush();
      continue;
    }

    if (/^#{1,6}\s+/.test(trimmed)) {
      flush();
      out.push(cleaned);
      continue;
    }

    if (cleaned !== trimmed && looksLikeHeading(cleaned)) {
      flush();
      out.push(cleaned);
      continue;
    }

    current.push(cleaned);
  }

  flush();
  return out.filter(Boolean);
}

async function loadParagraphs(bytes: Uint8Array, fileType: FileType) {
  if (fileType === "decision_docx") {
    return { paragraphs: extractDocxText(bytes), warnings: [], extraction: { format: "docx" as const, parser: "sax/1.6.1 docx/2", pages: [] } };
  }
  const result = await extractPdfText(bytes);
  return { ...result, warnings: [], extraction: { format: "pdf" as const, parser: result.parser, pages: result.pages } };
}

function looksLikeHeading(line: string): boolean {
  const clean = normalizeWhitespace(line.replace(/^#+\s*/, ""));
  if (!clean || clean.length < 3 || clean.length > 120) {
    return false;
  }

  if (isCaptionHeadingNoise(clean)) {
    return false;
  }

  if (/^(?:[A-Z]{1,4}-?\d{1,4}[A-Z]?(?:\s*,\s*)?)+$/.test(clean)) {
    return false;
  }

  const lower = clean.toLowerCase();
  if (KNOWN_HEADINGS.some((entry) => lower === entry)) {
    return true;
  }

  if (/^[A-Z0-9\s()\-.,']+$/.test(clean) && /[A-Z]/.test(clean) && clean.length <= 80) {
    return true;
  }

  if (/^(\d+(?:\.\d+)*|[IVXLCM]+)\.?\s+[A-Za-z].{0,80}$/.test(clean)) {
    return true;
  }

  if (/^[A-Za-z][A-Za-z\s]{2,80}:$/.test(clean)) {
    return true;
  }

  return false;
}

function toSections(paragraphs: string[]): { sections: AuthoredSection[]; warnings: string[] } {
  const warnings: string[] = [];
  if (paragraphs.length === 0) {
    return { sections: [], warnings: ["No text paragraphs detected"] };
  }

  const sections: AuthoredSection[] = [];
  let currentHeading = "Body";
  let currentParagraphs: string[] = [];
  let sectionOrder = 0;

  function flushCurrent() {
    if (currentParagraphs.length === 0) {
      return;
    }

    const keyBase = canonicalKey(currentHeading) || `section_${sectionOrder + 1}`;
    const key = sections.some((section) => section.canonicalKey === keyBase) ? `${keyBase}_${sectionOrder + 1}` : keyBase;

    sections.push({
      canonicalKey: key,
      heading: currentHeading,
      order: sectionOrder,
      paragraphs: currentParagraphs.map((text, idx) => ({
        anchor: `${key}-p${idx + 1}`,
        order: idx,
        text
      }))
    });

    sectionOrder += 1;
    currentParagraphs = [];
  }

  for (const paragraph of paragraphs) {
    if (looksLikeHeading(paragraph)) {
      flushCurrent();
      currentHeading = normalizeWhitespace(paragraph.replace(/^#+\s*/, "").replace(/:$/, ""));
      continue;
    }
    currentParagraphs.push(paragraph);
  }

  flushCurrent();

  if (sections.length === 0) {
    sections.push({
      canonicalKey: "body",
      heading: "Body",
      order: 0,
      paragraphs: paragraphs.map((text, idx) => ({
        anchor: `body-p${idx + 1}`,
        order: idx,
        text
      }))
    });
  }

  if (!sections.some((section) => section.heading !== "Body")) {
    warnings.push("No explicit section headings detected; parsed as Body");
  }

  return { sections, warnings };
}

function extractIndexCodes(text: string, sections: AuthoredSection[]): string[] {
  const candidates = new Set<string>();
  const prefixedLinePattern = /\b(?:index\s*codes?|index\s*code|ic)\s*[:#-]?\s*([^\n\r]{0,220})/gi;
  const genericInIndexSectionPattern =
    /\b(?:[A-Z]{1,4}\s*-?\s*(?:\d\s*){1,4}(?:\.\s*(?:\d\s*){1,2})?[A-Z]?|\d{1,4}(?:\.\d{1,2})?[A-Z]?)\b/g;

  const normalizeCandidate = (value: string) =>
    value
      .replace(/\s+/g, "")
      .replace(/^[^A-Z0-9]+/, "")
      .replace(/[^A-Z0-9-]+$/, "")
      .toUpperCase();

  const isNoise = (value: string) => {
    const compact = value.replace(/-/g, "");
    if (!compact) return true;
    if (/^(19|20)\d{2}$/.test(compact)) return true;
    if (/^\d{6,}$/.test(compact)) return true;
    if (/^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(value)) return true;
    if (/^\d{1,2}-\d{1,2}-\d{2,4}$/.test(value)) return true;
    if (/^\d{1,3}(?:,\d{3})+$/.test(value)) return true;
    if (/^\d+$/.test(compact) && (compact.length < 2 || compact.length > 4)) return true;
    return false;
  };

  for (const match of text.matchAll(prefixedLinePattern)) {
    const line = match[1] || "";
    for (const tokenMatch of line.matchAll(genericInIndexSectionPattern)) {
      const code = normalizeCandidate(tokenMatch[0] || "");
      if (!code || isNoise(code)) continue;
      candidates.add(code);
    }
  }

  const indexSection = sections.find((section) => REQUIRED_SECTION_LABELS.indexCodes.test(section.heading));
  if (indexSection) {
    for (const paragraph of indexSection.paragraphs) {
      for (const match of paragraph.text.matchAll(genericInIndexSectionPattern)) {
        const code = normalizeCandidate(match[0] || "");
        if (!code || isNoise(code)) continue;
        candidates.add(code);
      }
    }
  }

  return Array.from(candidates).slice(0, 20);
}

function parseIsoDate(text: string): string | null {
  const direct = text.match(/\b(20\d{2}|19\d{2})-(\d{2})-(\d{2})\b/);
  if (direct) {
    return `${direct[1]}-${direct[2]}-${direct[3]}`;
  }

  const monthMatch = text.match(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(20\d{2}|19\d{2})\b/i
  );
  if (!monthMatch) {
    return null;
  }

  const months = {
    january: "01",
    february: "02",
    march: "03",
    april: "04",
    may: "05",
    june: "06",
    july: "07",
    august: "08",
    september: "09",
    october: "10",
    november: "11",
    december: "12"
  } as const;

  const monthKey = monthMatch[1]?.toLowerCase() as keyof typeof months;
  const month = months[monthKey];
  const dayRaw = monthMatch[2];
  const year = monthMatch[3];
  if (!month || !dayRaw || !year) {
    return null;
  }
  const day = dayRaw.padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function extractCaseNumber(text: string): string | null {
  const match = text.match(/\b(?:case\s*(?:no\.?|number|#)|docket\s*(?:no\.?|#))\s*[:#-]?\s*([A-Z0-9\-\/]{3,30})\b/i);
  return match?.[1] ?? null;
}

function extractAuthor(text: string): string | null {
  const byLabel = text.match(/\b(?:judge|alj|hearing officer|author)\s*[:\-]\s*([A-Z][A-Za-z .,'-]{2,80})\b/i);
  if (byLabel?.[1]) {
    return canonicalizeJudgeName(normalizeWhitespace(byLabel[1]));
  }

  const signature = text.match(/\n\/?s\/?\s*([A-Z][A-Za-z .,'-]{3,80})\n/i);
  if (signature?.[1]) {
    return canonicalizeJudgeName(normalizeWhitespace(signature[1]));
  }

  const canonicalMatches = extractCanonicalJudgeNamesFromText(text);
  if (canonicalMatches.length === 1) {
    return canonicalMatches[0] || null;
  }

  return null;
}

function extractOutcomeLabel(text: string): "grant" | "deny" | "partial" | "unclear" {
  const lowered = text.toLowerCase();
  const grantHits = (lowered.match(/\b(grant|approved?|allow(?:ed)?|sustain(?:ed)?)\b/g) || []).length;
  const denyHits = (lowered.match(/\b(deny|denied|reject(?:ed)?|dismiss(?:ed)?)\b/g) || []).length;
  const partialHits = (lowered.match(/\b(partial|partially|limited grant|modified)\b/g) || []).length;

  if (partialHits >= Math.max(grantHits, denyHits) && partialHits > 0) {
    return "partial";
  }
  if (grantHits > denyHits) {
    return "grant";
  }
  if (denyHits > grantHits) {
    return "deny";
  }
  return "unclear";
}

function inferMetadata(text: string, sections: AuthoredSection[]) {
  const indexCodes = extractIndexCodes(text, sections);
  const citations = extractCitations(text);
  const rulesSections = Array.from(new Set(citations.filter(c => c.context === "rule" && c.canonical).map(citationLabel))).slice(0, 30);
  const ordinanceSections = Array.from(new Set(citations.filter(c => c.context === "ordinance" && c.canonical).map(citationLabel))).slice(0, 30);
  const caseNumber = extractCaseNumber(text);
  const decisionDate = parseIsoDate(text);
  const author = extractAuthor(text);
  const outcomeLabel = extractOutcomeLabel(text);

  let confidence = 0.3;
  if (indexCodes.length > 0) confidence += 0.15;
  if (rulesSections.length > 0) confidence += 0.15;
  if (ordinanceSections.length > 0) confidence += 0.15;
  if (caseNumber) confidence += 0.1;
  if (decisionDate) confidence += 0.1;
  if (author) confidence += 0.05;

  return {
    indexCodes,
    rulesSections,
    ordinanceSections,
    caseNumber,
    decisionDate,
    author,
    outcomeLabel,
    extractionConfidence: Number(Math.min(0.98, confidence).toFixed(2))
  };
}

function inferQcFlags(sections: AuthoredSection[], metadata: ParsedDocument["extractedMetadata"]) {
  const headings = sections.map((section) => section.heading);
  return {
    hasIndexCodes:
      metadata.indexCodes.length > 0 || headings.some((heading) => REQUIRED_SECTION_LABELS.indexCodes.test(heading)),
    hasRulesSection:
      metadata.rulesSections.length > 0 || headings.some((heading) => REQUIRED_SECTION_LABELS.rules.test(heading)),
    hasOrdinanceSection:
      metadata.ordinanceSections.length > 0 || headings.some((heading) => REQUIRED_SECTION_LABELS.ordinance.test(heading))
  };
}

function buildParsedDocument(loaded: { paragraphs: string[]; warnings: string[]; extraction: Omit<ExtractionDiagnostics, "status" | "characterCount"> }): ParsedDocument {
  const characterCount = requireReadableText(loaded.paragraphs);
  const normalized = normalizeInputParagraphs(loaded.paragraphs);
  const sectionParse = toSections(normalized.paragraphs);

  const plainText = sectionParse.sections
    .map((section) => `## ${section.heading}\n${section.paragraphs.map((paragraph) => paragraph.text).join("\n\n")}`)
    .join("\n\n");

  const extractedMetadata = inferMetadata(plainText, sectionParse.sections);
  const qcFlags = inferQcFlags(sectionParse.sections, extractedMetadata);

  const warnings = [...loaded.warnings, ...normalized.warnings, ...sectionParse.warnings];
  if (sectionParse.sections.length < 2) {
    warnings.push("Low section segmentation confidence due to limited heading structure");
  }
  if (extractCitations(plainText).some(citation => citation.resolution === "ambiguous")) {
    warnings.push("Ambiguous citation context: review raw citation evidence before assigning rules or ordinance metadata");
  }
  if (!qcFlags.hasIndexCodes) warnings.push("Index Codes not detected");
  if (!qcFlags.hasRulesSection) warnings.push("Rules section not detected");
  if (!qcFlags.hasOrdinanceSection) warnings.push("Ordinance section not detected");

  return {
    plainText,
    extraction: { ...loaded.extraction, status: "extracted", characterCount },
    citations: extractCitations(plainText),
    sections: sectionParse.sections,
    qcFlags,
    extractedMetadata,
    warnings: Array.from(new Set(warnings))
  };
}

export function parseMarkdownDocument(bytes: ArrayBufferLike | Uint8Array): ParsedDocument {
  const typed = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  return buildParsedDocument({
    paragraphs: extractMarkdownParagraphs(typed),
    warnings: [],
    extraction: { format: "markdown", parser: "markdown/2", pages: [] }
  });
}

export async function parseDocument(bytes: ArrayBufferLike | Uint8Array, fileType: FileType): Promise<ParsedDocument> {
  const typed = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  return buildParsedDocument(await loadParagraphs(typed, fileType));
}
