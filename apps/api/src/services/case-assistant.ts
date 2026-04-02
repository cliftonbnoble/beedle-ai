import {
  caseAssistantRequestSchema,
  caseAssistantResponseSchema,
  type CaseAssistantResponse,
  type SearchResponse
} from "@beedle/shared";
import { CASE_ASSISTANT_GUARDRAILS, CASE_SECTION_PRIORITY, OUTCOME_KEYWORDS } from "@beedle/prompts";
import type { Env } from "../lib/types";
import { search } from "./search";
import { effectiveSourceLink } from "./storage";

type SupportType = "explicit" | "inference";
type Direction = "grant" | "deny" | "partial" | "unclear";

type Authority = CaseAssistantResponse["similar_cases"][number];
type Citation = CaseAssistantResponse["citations"][number];

interface Candidate {
  authority: Authority;
  citation: Citation;
  score: number;
  reasons: string[];
}

function normalize(input: string): string {
  return input.toLowerCase().replace(/\s+/g, " ").trim();
}

function tokenize(input: string): string[] {
  return normalize(input)
    .split(/[^a-z0-9_:-]+/)
    .map((token) => token.trim())
    .filter((token) => token.length > 2);
}

function uniq<T>(values: T[]): T[] {
  return Array.from(new Set(values));
}

function compact(text: string, max = 320): string {
  return text.length <= max ? text : `${text.slice(0, max - 3)}...`;
}

function buildQueries(input: ReturnType<typeof caseAssistantRequestSchema.parse>): string[] {
  const findingsTerms = tokenize(input.findings_text).slice(0, 10).join(" ");
  const lawTerms = tokenize(input.law_text).slice(0, 8).join(" ");
  const issueTerms = input.issue_tags.join(" ");

  const base = [findingsTerms, lawTerms, issueTerms].filter(Boolean).join(" ").trim();
  const scoped = [
    ...input.index_codes,
    ...input.rules_sections,
    ...input.ordinance_sections,
    ...input.issue_tags
  ]
    .map((part) => part.trim())
    .filter(Boolean)
    .slice(0, 8);

  return uniq([base, ...scoped].filter((item) => item.length > 0)).slice(0, 8);
}

async function runSearches(
  env: Env,
  queries: string[],
  fileType: "decision_docx" | "law_pdf",
  input: ReturnType<typeof caseAssistantRequestSchema.parse>
): Promise<SearchResponse["results"]> {
  const merged = new Map<string, SearchResponse["results"][number]>();

  for (const query of queries) {
    const response = await search(env, {
      query,
      limit: 12,
      filters: {
        fileType,
        approvedOnly: fileType === "decision_docx",
        indexCode: fileType === "decision_docx" ? input.index_codes[0] : undefined,
        rulesSection: fileType === "decision_docx" ? input.rules_sections[0] : undefined,
        ordinanceSection: fileType === "decision_docx" ? input.ordinance_sections[0] : undefined
      }
    });

    for (const result of response.results) {
      const existing = merged.get(result.chunkId);
      if (!existing || result.score > existing.score) {
        merged.set(result.chunkId, result);
      }
    }
  }

  return Array.from(merged.values());
}

async function fetchUploadedDocResults(env: Env, uploadedDocIds: string[]): Promise<SearchResponse["results"]> {
  if (uploadedDocIds.length === 0) {
    return [];
  }

  const placeholders = uploadedDocIds.map(() => "?").join(",");
  const rows = await env.DB.prepare(
    `SELECT
      c.id as chunkId,
      d.id as documentId,
      d.title,
      d.citation,
      d.author_name as authorName,
      d.decision_date as decisionDate,
      d.file_type as fileType,
      c.chunk_text as snippet,
      c.section_label as sectionLabel,
      d.source_r2_key as sourceFileRef,
      d.source_link as sourceLink,
      c.citation_anchor as citationAnchor,
      c.paragraph_anchor as paragraphAnchor
     FROM document_chunks c
     JOIN documents d ON d.id = c.document_id
     WHERE d.id IN (${placeholders})
     ORDER BY d.updated_at DESC, c.chunk_order ASC
     LIMIT 20`
  )
    .bind(...uploadedDocIds)
    .all<{
      chunkId: string;
      documentId: string;
      title: string;
      citation: string;
      authorName: string | null;
      decisionDate: string | null;
      fileType: "decision_docx" | "law_pdf";
      snippet: string;
      sectionLabel: string;
      sourceFileRef: string;
      sourceLink: string;
      citationAnchor: string;
      paragraphAnchor: string;
    }>();

  return (rows.results ?? []).map((row) => ({
    ...row,
    sourceLink: effectiveSourceLink(env, row.documentId, row.sourceLink),
    sectionHeading: row.sectionLabel,
    corpusTier: "trusted",
    chunkType: String(row.sectionLabel || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, ""),
    retrievalReason: ["uploaded_doc_context"],
    score: 0.58,
    lexicalScore: 0.58,
    vectorScore: 0
  }));
}

function inferSupportType(
  snippet: string,
  tokens: string[],
  indexCodes: string[],
  rulesSections: string[],
  ordinanceSections: string[]
): SupportType {
  const target = normalize(snippet);
  const supportTokens = [...tokens, ...indexCodes, ...rulesSections, ...ordinanceSections]
    .map((item) => normalize(item))
    .filter(Boolean);

  if (supportTokens.some((token) => target.includes(token))) {
    return "explicit";
  }
  return "inference";
}

function buildReasons(
  row: SearchResponse["results"][number],
  input: ReturnType<typeof caseAssistantRequestSchema.parse>,
  tokenSet: Set<string>
): string[] {
  const reasons: string[] = [];
  const snippet = normalize(row.snippet);
  const section = normalize(row.sectionLabel);

  const matchingIndex = input.index_codes.filter((code) => snippet.includes(normalize(code)));
  if (matchingIndex.length > 0) {
    reasons.push(`Matches Index Code(s): ${matchingIndex.join(", ")}`);
  }

  const matchingRules = input.rules_sections.filter((rule) => snippet.includes(normalize(rule)) || section.includes(normalize(rule)));
  if (matchingRules.length > 0 || section.includes("rule")) {
    reasons.push("Overlaps with requested rules sections");
  }

  const matchingOrdinances = input.ordinance_sections.filter((ord) => snippet.includes(normalize(ord)) || section.includes(normalize(ord)));
  if (matchingOrdinances.length > 0 || section.includes("ordinance")) {
    reasons.push("Overlaps with ordinance-focused analysis");
  }

  if (input.issue_tags.some((tag) => snippet.includes(normalize(tag)))) {
    reasons.push("Shares issue-tag pattern with the case facts");
  }

  if (tokenize(row.snippet).some((term) => tokenSet.has(term))) {
    reasons.push("Contains overlapping factual pattern language");
  }

  if (CASE_SECTION_PRIORITY.some((key: string) => section.includes(key))) {
    reasons.push("Comes from a high-signal reasoning section");
  }

  return uniq(reasons);
}

function scoreCandidate(
  row: SearchResponse["results"][number],
  reasons: string[],
  uploadedDocIds: string[]
): number {
  // TODO(case-assistant): replace heuristic weighting with calibrated ranking/eval metrics.
  let score = row.score + row.lexicalScore * 0.2 + row.vectorScore * 0.15;
  score += Math.min(0.36, reasons.length * 0.06);

  if (uploadedDocIds.includes(row.documentId)) {
    score += 0.2;
  }

  if (/conclusions? of law|reasoning|findings/i.test(row.sectionLabel)) {
    score += 0.08;
  }

  return Number(Math.min(1.5, score).toFixed(6));
}

function toAuthority(row: SearchResponse["results"][number], reasons: string[], supportType: SupportType): Authority {
  return {
    document_id: row.documentId,
    title: row.title,
    citation: row.citation,
    why_it_matches: reasons.length > 0 ? reasons.join("; ") : "Semantically similar fact/law pattern",
    snippet: row.snippet,
    section_label: row.sectionLabel,
    citation_anchor: row.citationAnchor,
    source_link: row.sourceLink,
    source_file_ref: row.sourceFileRef,
    support_type: supportType,
    citation_id: `${row.documentId}:${row.citationAnchor}`
  };
}

function toCitation(authority: Authority): Citation {
  return {
    id: authority.citation_id,
    title: authority.title,
    citation: authority.citation,
    citation_anchor: authority.citation_anchor,
    source_link: authority.source_link,
    snippet: authority.snippet,
    support_type: authority.support_type
  };
}

function dedupeCandidates(rows: Candidate[], limit: number): Candidate[] {
  const byCitation = new Map<string, Candidate>();
  for (const row of rows) {
    const existing = byCitation.get(row.authority.citation_id);
    if (!existing || row.score > existing.score) {
      byCitation.set(row.authority.citation_id, row);
    }
  }

  return Array.from(byCitation.values())
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
}

function chooseDirection(authorities: Authority[]): { direction: Direction; rationale: string } {
  const counts = { grant: 0, deny: 0, partial: 0 };

  for (const authority of authorities) {
    const text = normalize(`${authority.snippet} ${authority.section_label} ${authority.why_it_matches}`);
    if (OUTCOME_KEYWORDS.grant.some((word: string) => text.includes(word))) counts.grant += 1;
    if (OUTCOME_KEYWORDS.deny.some((word: string) => text.includes(word))) counts.deny += 1;
    if (OUTCOME_KEYWORDS.partial.some((word: string) => text.includes(word))) counts.partial += 1;
  }

  const total = counts.grant + counts.deny + counts.partial;
  if (total === 0) {
    return {
      direction: "unclear",
      rationale: "Retrieved authorities do not contain strong directional language; treat this as exploratory guidance."
    };
  }

  if (Math.abs(counts.grant - counts.deny) <= 1 && counts.grant > 0 && counts.deny > 0) {
    return {
      direction: counts.partial > 0 ? "partial" : "unclear",
      rationale: "Authorities cut in different directions; potential for mixed or split disposition should be considered."
    };
  }

  if (counts.partial >= counts.grant && counts.partial >= counts.deny) {
    return {
      direction: "partial",
      rationale: "Partial-relief indicators appear repeatedly across retrieved authorities."
    };
  }

  if (counts.grant > counts.deny) {
    return {
      direction: "grant",
      rationale: "Retrieved precedents contain more grant/approval language than denial language."
    };
  }

  return {
    direction: "deny",
    rationale: "Retrieved precedents contain more denial/rejection language than grant language."
  };
}

function buildThemes(authorities: Authority[]): CaseAssistantResponse["reasoning_themes"] {
  const bySection = new Map<string, Authority[]>();
  for (const authority of authorities) {
    const key = authority.section_label;
    const list = bySection.get(key) ?? [];
    list.push(authority);
    bySection.set(key, list);
  }

  return Array.from(bySection.entries())
    .sort((a, b) => b[1].length - a[1].length)
    .slice(0, 3)
    .map(([section, items]) => ({
      theme: `Theme: ${section}`,
      explanation: `Multiple authorities rely on ${section} language for similar fact/law pattern analysis.`,
      citation_ids: uniq(items.map((item) => item.citation_id)).slice(0, 4)
    }));
}

function buildVulnerabilities(
  input: ReturnType<typeof caseAssistantRequestSchema.parse>,
  authorities: Authority[],
  direction: Direction
): CaseAssistantResponse["vulnerabilities"] {
  const issues: CaseAssistantResponse["vulnerabilities"] = [];

  if (authorities.length < 3) {
    issues.push({
      issue: "Thin precedent support",
      impact: "Low authority volume raises appeal risk because analogs are limited or weakly matched.",
      citation_ids: authorities.slice(0, 2).map((item) => item.citation_id)
    });
  }

  if (direction === "unclear" || direction === "partial") {
    issues.push({
      issue: "Mixed directional authority",
      impact: "Retrieved authorities support both sides, so conclusions are less stable on review.",
      citation_ids: authorities.slice(0, 3).map((item) => item.citation_id)
    });
  }

  if (input.index_codes.length === 0 || input.rules_sections.length === 0 || input.ordinance_sections.length === 0) {
    issues.push({
      issue: "Missing structured legal hooks in input",
      impact: "Absent Index Codes/Rules/Ordinance references reduce alignment precision with precedents.",
      citation_ids: authorities.slice(0, 2).map((item) => item.citation_id)
    });
  }

  return issues.slice(0, 4);
}

function buildStrengtheningSuggestions(
  input: ReturnType<typeof caseAssistantRequestSchema.parse>,
  authorities: Authority[],
  confidence: "low" | "medium" | "high"
): CaseAssistantResponse["strengthening_suggestions"] {
  const suggestions: CaseAssistantResponse["strengthening_suggestions"] = [];

  if (input.index_codes.length === 0) {
    suggestions.push({
      suggestion: "Add explicit Index Codes in the Findings of Fact section.",
      why: "Index Code alignment is a strong retrieval/rationale signal in comparable decisions.",
      citation_ids: authorities.slice(0, 2).map((item) => item.citation_id)
    });
  }

  if (input.rules_sections.length === 0 || input.ordinance_sections.length === 0) {
    suggestions.push({
      suggestion: "Identify exact Rules and Ordinance sections supporting each requested relief item.",
      why: "Section-level legal mapping reduces ambiguity and improves defensibility.",
      citation_ids: authorities.slice(0, 3).map((item) => item.citation_id)
    });
  }

  if (confidence !== "high") {
    suggestions.push({
      suggestion: "Add concrete factual findings tied to causation, timing, and evidentiary support.",
      why: "Higher-specificity findings improve overlap with stronger precedents.",
      citation_ids: authorities.slice(0, 2).map((item) => item.citation_id)
    });
  }

  suggestions.push({
    suggestion: "Address likely counterarguments directly in Conclusions of Law.",
    why: "Preemptive reasoning reduces appeal vulnerability.",
    citation_ids: authorities.slice(0, 3).map((item) => item.citation_id)
  });

  return suggestions.slice(0, 5);
}

function chooseConfidence(
  similarCases: Authority[],
  relevantLaw: Authority[],
  direction: Direction
): "low" | "medium" | "high" {
  // TODO(case-assistant): calibrate confidence using held-out benchmark cases and statement-level groundedness checks.
  const total = similarCases.length + relevantLaw.length;
  const explicit = [...similarCases, ...relevantLaw].filter((item) => item.support_type === "explicit").length;
  const explicitRatio = total > 0 ? explicit / total : 0;

  let score = 0;
  score += Math.min(0.45, similarCases.length * 0.08);
  score += Math.min(0.25, relevantLaw.length * 0.06);
  score += explicitRatio * 0.2;
  score += direction === "unclear" ? 0 : 0.15;

  if (score >= 0.72) return "high";
  if (score >= 0.45) return "medium";
  return "low";
}

function buildSummary(input: ReturnType<typeof caseAssistantRequestSchema.parse>): string {
  const issueSummary = input.issue_tags.length > 0 ? ` Issue tags: ${input.issue_tags.join(", ")}.` : "";
  const indexSummary = input.index_codes.length > 0 ? ` Index Codes: ${input.index_codes.join(", ")}.` : "";
  return compact(
    `Findings focus: ${compact(input.findings_text, 180)} Law focus: ${compact(input.law_text, 180)}.${issueSummary}${indexSummary}`,
    420
  );
}

function toFinalResponse(params: {
  input: ReturnType<typeof caseAssistantRequestSchema.parse>;
  similarCases: Candidate[];
  relevantLaw: Candidate[];
}): CaseAssistantResponse {
  const similarCases = params.similarCases.map((item) => item.authority);
  const relevantLaw = params.relevantLaw.map((item) => item.authority);
  const allAuthorities = [...similarCases, ...relevantLaw];

  const direction = chooseDirection(similarCases);
  const confidence = chooseConfidence(similarCases, relevantLaw, direction.direction);
  const citations = uniq(allAuthorities.map((item) => item.citation_id))
    .map((id) => {
      const authority = allAuthorities.find((item) => item.citation_id === id);
      return authority ? toCitation(authority) : null;
    })
    .filter((item): item is Citation => item !== null)
    .slice(0, 20);

  return caseAssistantResponseSchema.parse({
    query_summary: buildSummary(params.input),
    similar_cases: similarCases,
    relevant_law: relevantLaw,
    outcome_guidance: direction,
    reasoning_themes: buildThemes(allAuthorities),
    vulnerabilities: buildVulnerabilities(params.input, allAuthorities, direction.direction),
    strengthening_suggestions: buildStrengtheningSuggestions(params.input, allAuthorities, confidence),
    confidence,
    citations,
    guardrails: [...CASE_ASSISTANT_GUARDRAILS]
  });
}

export async function runCaseAssistant(env: Env, input: unknown): Promise<CaseAssistantResponse> {
  const parsed = caseAssistantRequestSchema.parse(input);
  const tokenSet = new Set(tokenize(`${parsed.findings_text} ${parsed.law_text} ${parsed.issue_tags.join(" ")}`));
  const queries = buildQueries(parsed);

  const [caseRows, lawRows, uploadedRows] = await Promise.all([
    runSearches(env, queries, "decision_docx", parsed),
    runSearches(env, queries, "law_pdf", parsed),
    fetchUploadedDocResults(env, parsed.uploaded_doc_ids)
  ]);

  const uploadedByChunk = new Map(uploadedRows.map((row) => [row.chunkId, row]));

  const allCases = uniq([...caseRows, ...uploadedRows.filter((row) => row.fileType === "decision_docx")].map((row) => row.chunkId))
    .map((chunkId) => uploadedByChunk.get(chunkId) ?? caseRows.find((row) => row.chunkId === chunkId))
    .filter((row): row is SearchResponse["results"][number] => Boolean(row));

  const allLaw = uniq([...lawRows, ...uploadedRows.filter((row) => row.fileType === "law_pdf")].map((row) => row.chunkId))
    .map((chunkId) => uploadedByChunk.get(chunkId) ?? lawRows.find((row) => row.chunkId === chunkId))
    .filter((row): row is SearchResponse["results"][number] => Boolean(row));

  const caseCandidates = dedupeCandidates(
    allCases.map((row) => {
      const reasons = buildReasons(row, parsed, tokenSet);
      const supportType = inferSupportType(
        row.snippet,
        Array.from(tokenSet),
        parsed.index_codes,
        parsed.rules_sections,
        parsed.ordinance_sections
      );
      const authority = toAuthority(row, reasons, supportType);
      return {
        authority,
        citation: toCitation(authority),
        score: scoreCandidate(row, reasons, parsed.uploaded_doc_ids),
        reasons
      };
    }),
    6
  );

  const lawCandidates = dedupeCandidates(
    allLaw.map((row) => {
      const reasons = buildReasons(row, parsed, tokenSet);
      const supportType = inferSupportType(
        row.snippet,
        Array.from(tokenSet),
        parsed.index_codes,
        parsed.rules_sections,
        parsed.ordinance_sections
      );
      const authority = toAuthority(row, reasons, supportType);
      return {
        authority,
        citation: toCitation(authority),
        score: scoreCandidate(row, reasons, parsed.uploaded_doc_ids),
        reasons
      };
    }),
    6
  );

  return toFinalResponse({
    input: parsed,
    similarCases: caseCandidates,
    relevantLaw: lawCandidates
  });
}
