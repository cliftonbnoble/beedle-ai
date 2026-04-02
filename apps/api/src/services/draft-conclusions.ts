import {
  caseAssistantResponseSchema,
  draftConclusionsDebugResponseSchema,
  draftConclusionsRequestSchema,
  draftConclusionsResponseSchema,
  type CaseAssistantResponse,
  type DraftConclusionsDebugResponse,
  type DraftConclusionsRequest,
  type DraftConclusionsResponse,
  type SearchResponse
} from "@beedle/shared";
import { DRAFT_CONCLUSIONS_GUARDRAILS, OUTCOME_KEYWORDS } from "@beedle/prompts";
import type { Env } from "../lib/types";
import { search } from "./search";

type Authority = CaseAssistantResponse["similar_cases"][number];
type SupportLevel = DraftConclusionsResponse["paragraph_support"][number]["support_level"];
type SearchRow = SearchResponse["results"][number];

interface ConfidenceSignals {
  retrieval_strength: number;
  authority_count: number;
  direct_conclusions_count: number;
  explicit_support_ratio: number;
  findings_coverage: number;
  law_coverage: number;
  conflict_index: number;
  paragraph_support_ratio: number;
  confidence_score: number;
}

function uniq<T>(values: T[]): T[] {
  return Array.from(new Set(values));
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

function clamp(input: number): number {
  return Number(Math.max(0, Math.min(1, input)).toFixed(4));
}

function compact(text: string, max = 240): string {
  return text.length <= max ? text : `${text.slice(0, max - 3)}...`;
}

function compactWhitespace(value: string): string {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function topCitationIds(authorities: Authority[], count = 3): string[] {
  return authorities.slice(0, count).map((item) => item.citation_id);
}

function makePassageFromRow(row: SearchRow) {
  return {
    chunkId: row.chunkId,
    snippet: row.snippet,
    sectionLabel: row.sectionLabel,
    sectionHeading: row.sectionHeading,
    citationAnchor: row.citationAnchor,
    paragraphAnchor: row.paragraphAnchor,
    chunkType: row.chunkType,
    score: row.score
  };
}

function primaryPassage(row: SearchRow) {
  return row.primaryAuthorityPassage || row.matchedPassage || makePassageFromRow(row);
}

function findingsPassage(row: SearchRow) {
  return row.supportingFactPassage || row.matchedPassage || makePassageFromRow(row);
}

function buildDraftQueries(parsed: DraftConclusionsRequest): string[] {
  const findingsTerms = uniq(tokenize(parsed.findings_text)).slice(0, 10);
  const lawTerms = uniq(tokenize(parsed.law_text)).slice(0, 6);
  const issueTerms = uniq(parsed.issue_tags.map((item) => compactWhitespace(item))).slice(0, 4);

  const primary = [...parsed.index_codes.slice(0, 4), ...findingsTerms, ...lawTerms, ...issueTerms].filter(Boolean).join(" ").trim();
  const findingsFocused = [...parsed.index_codes.slice(0, 3), ...findingsTerms.slice(0, 8)].filter(Boolean).join(" ").trim();
  const lawFocused = [...lawTerms.slice(0, 5), ...findingsTerms.slice(0, 5)].filter(Boolean).join(" ").trim();

  return uniq([primary, findingsFocused, lawFocused].filter((item) => item.length > 0)).slice(0, 3);
}

function groupTopDecisionRows(rows: SearchRow[], limit: number): SearchRow[] {
  const byDocument = new Map<string, SearchRow>();
  for (const row of rows) {
    const existing = byDocument.get(row.documentId);
    const effectiveScore =
      row.score +
      (row.primaryAuthorityPassage ? 0.08 : 0) +
      (row.supportingFactPassage ? 0.06 : 0) +
      (/conclusions? of law/i.test(row.sectionLabel) ? 0.06 : 0) +
      (/findings? of fact/i.test(row.sectionLabel) ? 0.04 : 0);
    const existingScore =
      existing == null
        ? -1
        : existing.score +
          (existing.primaryAuthorityPassage ? 0.08 : 0) +
          (existing.supportingFactPassage ? 0.06 : 0) +
          (/conclusions? of law/i.test(existing.sectionLabel) ? 0.06 : 0) +
          (/findings? of fact/i.test(existing.sectionLabel) ? 0.04 : 0);
    if (!existing || effectiveScore > existingScore) {
      byDocument.set(row.documentId, row);
    }
  }

  return Array.from(byDocument.values())
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
}

function buildAuthorityFromPassage(params: {
  row: SearchRow;
  passage: ReturnType<typeof primaryPassage>;
  whyItMatches: string;
  supportType: "explicit" | "inference";
}): Authority {
  const { row, passage, whyItMatches, supportType } = params;
  return {
    document_id: row.documentId,
    title: row.title,
    citation: row.citation,
    why_it_matches: whyItMatches,
    snippet: passage.snippet,
    section_label: passage.sectionLabel,
    citation_anchor: passage.citationAnchor,
    source_link: row.sourceLink,
    source_file_ref: row.sourceFileRef,
    support_type: supportType,
    citation_id: `${row.documentId}:${passage.citationAnchor}`
  };
}

function buildDraftQuerySummary(parsed: DraftConclusionsRequest): string {
  const indexSummary = parsed.index_codes.length > 0 ? ` Index Codes: ${parsed.index_codes.join(", ")}.` : "";
  const lawSummary = parsed.law_text.trim() ? ` Relevant law: ${compact(parsed.law_text, 160)}` : " Relevant law: none supplied.";
  return compact(`Findings focus: ${compact(parsed.findings_text, 180)}.${lawSummary}${indexSummary}`, 420);
}

async function retrieveDraftResearch(env: Env, parsed: DraftConclusionsRequest): Promise<CaseAssistantResponse> {
  const queries = buildDraftQueries(parsed);
  const merged = new Map<string, SearchRow>();

  for (const query of queries) {
    const response = await search(env, {
      query,
      limit: 18,
      snippetMaxLength: 360,
      corpusMode: "trusted_plus_provisional",
      filters: {
        fileType: "decision_docx",
        approvedOnly: true,
        indexCodes: parsed.index_codes.length > 0 ? parsed.index_codes : undefined
      }
    });

    for (const row of response.results) {
      const existing = merged.get(row.chunkId);
      if (!existing || row.score > existing.score) {
        merged.set(row.chunkId, row);
      }
    }
  }

  const topRows = groupTopDecisionRows(Array.from(merged.values()), 6);
  const tokenSet = new Set(tokenize(`${parsed.findings_text} ${parsed.law_text} ${parsed.issue_tags.join(" ")}`));

  const similarCases = topRows.map((row) => {
    const passage = primaryPassage(row);
    const supportType =
      Array.from(tokenSet).some((token) => normalize(passage.snippet).includes(token)) || parsed.index_codes.some((code) => normalize(passage.snippet).includes(normalize(code)))
        ? "explicit"
        : "inference";
    return buildAuthorityFromPassage({
      row,
      passage,
      whyItMatches:
        "Retrieved as a similar decision with a strong conclusions-style passage aligned to the submitted findings and selected index codes.",
      supportType
    });
  });

  const relevantLaw = topRows
    .map((row) => {
      const passage = findingsPassage(row);
      const supportType =
        Array.from(tokenSet).some((token) => normalize(passage.snippet).includes(token)) || parsed.index_codes.some((code) => normalize(passage.snippet).includes(normalize(code)))
          ? "explicit"
          : "inference";
      return buildAuthorityFromPassage({
        row,
        passage,
        whyItMatches:
          "Retrieved as a similar decision with a findings passage that overlaps the submitted factual pattern.",
        supportType
      });
    })
    .filter((authority, index, list) => list.findIndex((item) => item.citation_id === authority.citation_id) === index);

  const allAuthorities = [...similarCases, ...relevantLaw];
  const outcomeGuidance = inferDraftDirection(similarCases);
  const confidence = inferDraftConfidence(similarCases, relevantLaw, outcomeGuidance.direction);

  return caseAssistantResponseSchema.parse({
    query_summary: buildDraftQuerySummary(parsed),
    similar_cases: similarCases,
    relevant_law: relevantLaw,
    outcome_guidance: outcomeGuidance,
    reasoning_themes: buildDraftThemes(allAuthorities),
    vulnerabilities: [],
    strengthening_suggestions: [],
    confidence,
    citations: uniq(allAuthorities.map((item) => item.citation_id))
      .map((id) => allAuthorities.find((item) => item.citation_id === id))
      .filter((item): item is Authority => Boolean(item))
      .slice(0, 12)
      .map((item) => ({
        id: item.citation_id,
        title: item.title,
        citation: item.citation,
        citation_anchor: item.citation_anchor,
        source_link: item.source_link,
        snippet: item.snippet,
        support_type: item.support_type
      })),
    guardrails: [...DRAFT_CONCLUSIONS_GUARDRAILS]
  });
}

function mapAuthorityByCitationId(result: CaseAssistantResponse): Map<string, Authority> {
  return new Map([...result.similar_cases, ...result.relevant_law].map((item) => [item.citation_id, item]));
}

function extractAssistantContent(payload: any): string {
  const choice = payload?.choices?.[0];
  const content = choice?.message?.content;
  if (typeof content === "string") return compactWhitespace(content);
  if (Array.isArray(content)) {
    return compactWhitespace(
      content
        .map((item) => {
          if (typeof item === "string") return item;
          if (typeof item?.text === "string") return item.text;
          return "";
        })
        .join("\n")
    );
  }
  return "";
}

function authorityContextBlock(authority: Authority, index: number): string {
  return [
    `Authority ${index + 1}`,
    `Decision: ${authority.title}`,
    `Citation: ${authority.citation}`,
    `Section: ${authority.section_label}`,
    `Why it matches: ${compactWhitespace(authority.why_it_matches)}`,
    `Excerpt: ${compactWhitespace(authority.snippet)}`
  ].join("\n");
}

async function callDraftLlm(params: {
  env: Env;
  parsed: DraftConclusionsRequest;
  caseAssistant: CaseAssistantResponse;
}): Promise<{ draftText: string; model: string }> {
  const { env, parsed, caseAssistant } = params;
  if (!env.LLM_API_KEY) {
    throw new Error("LLM_API_KEY is not configured.");
  }

  const model = env.LLM_MODEL || "gpt-4.1-mini";
  const baseUrl = (env.LLM_BASE_URL || "https://api.openai.com/v1").replace(/\/+$/, "");
  const authorities = [...caseAssistant.similar_cases, ...caseAssistant.relevant_law].slice(0, 8);

  const systemPrompt = [
    "You draft Conclusions of Law for San Francisco Rent Board style decisions.",
    "Use the provided Findings of Fact, optional law text, index codes, and retrieved similar decisions.",
    "Match the tone and structure of formal administrative decisions.",
    "Return only the draft Conclusions of Law section text.",
    "Use numbered paragraphs.",
    "Do not invent facts, holdings, ordinance sections, or rules not grounded in the provided materials.",
    "Do not mention retrieved decision titles or citations in the output; use them only as stylistic and reasoning guides.",
    "If support is mixed or limited, use appropriately cautious language."
  ].join(" ");

  const contextBlock = [
    "Drafting inputs:",
    `Findings of Fact:\n${parsed.findings_text.trim()}`,
    parsed.law_text.trim() ? `Relevant Law / Citations:\n${parsed.law_text.trim()}` : "Relevant Law / Citations:\n<none provided>",
    parsed.index_codes.length > 0 ? `Selected Index Codes: ${parsed.index_codes.join(", ")}` : "Selected Index Codes: <none>",
    "",
    `Retrieved query summary: ${caseAssistant.query_summary}`,
    `Outcome guidance: ${caseAssistant.outcome_guidance.direction.toUpperCase()} - ${caseAssistant.outcome_guidance.rationale}`,
    "",
    "Retrieved authorities:",
    authorities.length > 0 ? authorities.map(authorityContextBlock).join("\n\n") : "No retrieved authorities available."
  ].join("\n");

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort("draft-llm-timeout"), 18000);
  let response: Response;
  try {
    response = await fetch(`${baseUrl}/chat/completions`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${env.LLM_API_KEY}`
      },
      body: JSON.stringify({
        model,
        temperature: 0.15,
        max_tokens: 900,
        messages: [
          { role: "system", content: systemPrompt },
          { role: "system", content: contextBlock },
          {
            role: "user",
            content:
              "Draft the Conclusions of Law now. Keep it ready for a judge to review and edit. Return only the draft conclusions text, with numbered paragraphs and no markdown fences."
          }
        ]
      }),
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`LLM request failed (${response.status}): ${text}`);
  }

  const payload = await response.json();
  const draftText = extractAssistantContent(payload);
  if (!draftText) throw new Error("LLM response did not include a conclusions draft.");
  return { draftText, model };
}

function splitDraftParagraphs(draftText: string): string[] {
  const normalized = String(draftText || "").replace(/\r/g, "").trim();
  if (!normalized) return [];
  const blankLineParts = normalized
    .split(/\n\s*\n+/)
    .map((part) => part.trim())
    .filter(Boolean);
  if (blankLineParts.length > 1) return blankLineParts;
  return normalized
    .split(/\n(?=\s*(?:\d+[\.\)]|[A-Z][a-z]+\s+\d+[\.\)]?))/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function scoreAuthorityForParagraph(paragraph: string, authority: Authority): number {
  const paragraphTokens = uniq(tokenize(paragraph)).slice(0, 28);
  if (paragraphTokens.length === 0) return 0;
  const haystack = normalize(`${authority.snippet} ${authority.why_it_matches} ${authority.section_label}`);
  const tokenHits = paragraphTokens.filter((token) => haystack.includes(token)).length;
  let score = tokenHits / paragraphTokens.length;
  if (/conclusions? of law|reasoning|analysis/i.test(authority.section_label)) score += 0.14;
  if (authority.support_type === "explicit") score += 0.08;
  return Number(score.toFixed(6));
}

function keywordCoverage(text: string, authorities: Authority[]): number {
  const tokens = uniq(tokenize(text)).slice(0, 30);
  if (tokens.length === 0 || authorities.length === 0) return 0;
  const corpus = normalize(authorities.map((item) => `${item.snippet} ${item.why_it_matches}`).join(" "));
  const hits = tokens.filter((token) => corpus.includes(token)).length;
  return clamp(hits / tokens.length);
}

function computeConflictIndex(authorities: Authority[]): number {
  const counts = { grant: 0, deny: 0, partial: 0 };
  for (const authority of authorities) {
    const text = normalize(`${authority.snippet} ${authority.section_label} ${authority.why_it_matches}`);
    if (OUTCOME_KEYWORDS.grant.some((token: string) => text.includes(token))) counts.grant += 1;
    if (OUTCOME_KEYWORDS.deny.some((token: string) => text.includes(token))) counts.deny += 1;
    if (OUTCOME_KEYWORDS.partial.some((token: string) => text.includes(token))) counts.partial += 1;
  }

  const dominant = Math.max(counts.grant, counts.deny, counts.partial);
  const opposing = counts.grant + counts.deny + counts.partial - dominant;
  if (dominant === 0) return 0.7;
  return clamp(opposing / dominant);
}

function inferDraftDirection(authorities: Authority[]): { direction: "grant" | "deny" | "partial" | "unclear"; rationale: string } {
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
      rationale: "Retrieved similar decisions do not converge on a single direction yet."
    };
  }

  if (counts.partial >= counts.grant && counts.partial >= counts.deny) {
    return {
      direction: "partial",
      rationale: "Retrieved similar decisions include recurring partial-relief language."
    };
  }

  if (counts.grant > counts.deny) {
    return {
      direction: "grant",
      rationale: "Retrieved similar decisions lean more toward relief than denial."
    };
  }

  if (counts.deny > counts.grant) {
    return {
      direction: "deny",
      rationale: "Retrieved similar decisions lean more toward denial than relief."
    };
  }

  return {
    direction: "unclear",
    rationale: "Retrieved similar decisions point in mixed directions."
  };
}

function inferDraftConfidence(similarCases: Authority[], relevantLaw: Authority[], direction: "grant" | "deny" | "partial" | "unclear") {
  const total = similarCases.length + relevantLaw.length;
  const explicit = [...similarCases, ...relevantLaw].filter((item) => item.support_type === "explicit").length;
  const explicitRatio = total > 0 ? explicit / total : 0;

  let score = 0;
  score += Math.min(0.45, similarCases.length * 0.08);
  score += Math.min(0.2, relevantLaw.length * 0.05);
  score += explicitRatio * 0.2;
  score += direction === "unclear" ? 0.05 : 0.15;

  if (score >= 0.72) return "high" as const;
  if (score >= 0.45) return "medium" as const;
  return "low" as const;
}

function buildDraftThemes(authorities: Authority[]): CaseAssistantResponse["reasoning_themes"] {
  const bySection = new Map<string, Authority[]>();
  for (const authority of authorities) {
    const key = authority.section_label || "Unlabeled section";
    const list = bySection.get(key) ?? [];
    list.push(authority);
    bySection.set(key, list);
  }

  return Array.from(bySection.entries())
    .sort((a, b) => b[1].length - a[1].length)
    .slice(0, 3)
    .map(([section, items]) => ({
      theme: `Theme: ${section}`,
      explanation: `Multiple retrieved decisions rely on ${section} language for similar reasoning or facts.`,
      citation_ids: uniq(items.map((item) => item.citation_id)).slice(0, 4)
    }));
}

function buildFallbackScaffoldSections(result: CaseAssistantResponse): DraftConclusionsResponse["draft_sections"] {
  const combined = [...result.similar_cases, ...result.relevant_law];
  const strongest = combined.slice(0, 6);
  const primary = strongest[0];

  const legalAuthorities = result.relevant_law.length > 0 ? result.relevant_law : strongest;
  const caseAuthorities = result.similar_cases.length > 0 ? result.similar_cases : strongest;

  const lowConfidence = result.confidence === "low";

  return [
    {
      id: "legal_framework",
      heading: "Legal Framework",
      text:
        "The applicable legal standards are grounded in the cited rule and ordinance authorities retrieved for this record. The Conclusions of Law should apply those standards to established findings without adding facts not expressly supported.",
      citation_ids: topCitationIds(legalAuthorities, 3)
    },
    {
      id: "application_to_findings",
      heading: "Application to Findings",
      text:
        "Applying the cited standards to the Findings of Fact, the disposition should turn on whether notice, compliance, and ordinance constraints are affirmatively established in the record. The analysis should prioritize directly supported factual predicates and avoid inferential leaps.",
      citation_ids: topCitationIds(caseAuthorities, 3)
    },
    {
      id: "direction_and_limits",
      heading: "Direction and Limits",
      text: `The retrieved authority presently points toward ${result.outcome_guidance.direction.toUpperCase()}. ${compact(
        result.outcome_guidance.rationale
      )} ${
        lowConfidence
          ? "Support remains limited or mixed, so this draft should be treated as preliminary pending additional findings or authority."
          : "Where authorities diverge, the final conclusion should acknowledge the tension and explain the selected interpretation."
      }`,
      citation_ids: topCitationIds(strongest, 4)
    },
    ...(primary
      ? [
          {
            id: "primary_anchor",
            heading: "Primary Supporting Anchor",
            text: `A key supporting anchor is ${primary.citation} (${primary.citation_anchor}) from ${primary.section_label}, which provides the most directly aligned reasoning for the presented issue set.`,
            citation_ids: [primary.citation_id]
          }
        ]
      : [])
  ];
}

function buildDraftSections(
  draftText: string,
  result: CaseAssistantResponse
): DraftConclusionsResponse["draft_sections"] {
  const combined = [...result.similar_cases, ...result.relevant_law];
  return [
    {
      id: "conclusions_of_law",
      heading: "Conclusions of Law",
      text: draftText,
      citation_ids: topCitationIds(combined, 6)
    }
  ];
}

function supportLevelForParagraph(citations: Authority[]): SupportLevel {
  if (citations.length === 0) return "unsupported";
  const explicitCount = citations.filter((item) => item.support_type === "explicit").length;
  if (explicitCount >= 2) return "strong";
  if (explicitCount >= 1 && citations.length >= 2) return "mixed";
  if (explicitCount >= 1) return "weak";
  return citations.length >= 2 ? "mixed" : "weak";
}

function paragraphSupportScore(level: SupportLevel): number {
  if (level === "strong") return 1;
  if (level === "mixed") return 0.7;
  if (level === "weak") return 0.4;
  return 0;
}

function buildParagraphSupport(
  draftText: string,
  authorityByCitationId: Map<string, Authority>
): DraftConclusionsResponse["paragraph_support"] {
  const authorities = Array.from(authorityByCitationId.values());
  const paragraphs = splitDraftParagraphs(draftText);

  return paragraphs.map((paragraph, idx) => {
    const supportingAuthorities = authorities
      .map((authority) => ({ authority, score: scoreAuthorityForParagraph(paragraph, authority) }))
      .filter((item) => item.score > 0.08)
      .sort((a, b) => b.score - a.score)
      .slice(0, 3)
      .map((item) => item.authority);

    const supportLevel = supportLevelForParagraph(supportingAuthorities);
    const notes: string[] = [];
    if (supportingAuthorities.some((item) => /conclusions? of law/i.test(item.section_label))) {
      notes.push("Includes directly relevant Conclusions of Law authority");
    }
    if (supportingAuthorities.some((item) => /reasoning|analysis/i.test(item.section_label))) {
      notes.push("Includes reasoning-focused support");
    }
    const explicitCount = supportingAuthorities.filter((item) => item.support_type === "explicit").length;
    if (supportingAuthorities.length > 0) {
      notes.push(`${explicitCount}/${supportingAuthorities.length} supports are explicit`);
    } else {
      notes.push("No supporting authorities mapped");
    }

    return {
      paragraph_id: `p${idx + 1}`,
      section_id: "conclusions_of_law",
      text: paragraph,
      support_level: supportLevel,
      citation_ids: supportingAuthorities.map((item) => item.citation_id),
      support_notes: notes
    };
  });
}

function computeConfidenceSignals(params: {
  parsed: DraftConclusionsRequest;
  caseAssistant: CaseAssistantResponse;
  paragraphSupport: DraftConclusionsResponse["paragraph_support"];
}): ConfidenceSignals {
  // TODO(drafting-grounding): calibrate these weights against adjudicator-reviewed gold labels before production.
  const authorities = [...params.caseAssistant.similar_cases, ...params.caseAssistant.relevant_law];
  const authorityCount = authorities.length;
  const directConclusionsCount = authorities.filter((item) => /conclusions? of law/i.test(item.section_label)).length;
  const explicitSupportRatio =
    authorityCount > 0 ? authorities.filter((item) => item.support_type === "explicit").length / authorityCount : 0;
  const findingsCoverage = keywordCoverage(params.parsed.findings_text, authorities);
  const lawCoverage = keywordCoverage(params.parsed.law_text, authorities);
  const conflictIndex = computeConflictIndex(params.caseAssistant.similar_cases);

  const paragraphSupportRatio =
    params.paragraphSupport.length > 0
      ? params.paragraphSupport.reduce((sum, item) => sum + paragraphSupportScore(item.support_level), 0) / params.paragraphSupport.length
      : 0;

  const retrievalStrength = clamp(
    Math.min(1, authorityCount / 8) * 0.35 +
      Math.min(1, directConclusionsCount / 3) * 0.2 +
      explicitSupportRatio * 0.2 +
      findingsCoverage * 0.12 +
      lawCoverage * 0.13
  );

  const confidenceScore = clamp(
    retrievalStrength * 0.5 +
      paragraphSupportRatio * 0.25 +
      (1 - conflictIndex) * 0.15 +
      (directConclusionsCount > 0 ? 0.1 : 0)
  );

  return {
    retrieval_strength: retrievalStrength,
    authority_count: authorityCount,
    direct_conclusions_count: directConclusionsCount,
    explicit_support_ratio: clamp(explicitSupportRatio),
    findings_coverage: findingsCoverage,
    law_coverage: lawCoverage,
    conflict_index: conflictIndex,
    paragraph_support_ratio: clamp(paragraphSupportRatio),
    confidence_score: confidenceScore
  };
}

function chooseConfidence(signals: ConfidenceSignals): DraftConclusionsResponse["confidence"] {
  if (signals.paragraph_support_ratio < 0.45 || signals.authority_count < 2) {
    return "low";
  }
  if (signals.conflict_index > 0.75) {
    return "low";
  }
  if (signals.confidence_score >= 0.74 && signals.conflict_index < 0.35 && signals.authority_count >= 4) {
    return "high";
  }
  if (signals.confidence_score >= 0.48) {
    return "medium";
  }
  return "low";
}

function buildLimitations(
  parsed: DraftConclusionsRequest,
  signals: ConfidenceSignals,
  paragraphSupport: DraftConclusionsResponse["paragraph_support"],
  caseAssistant: CaseAssistantResponse
): string[] {
  // TODO(drafting-grounding): add a human-review-required flag when unsupported paragraphs are present.
  const limitations: string[] = [];
  const unsupported = paragraphSupport.filter((item) => item.support_level === "unsupported");
  const weak = paragraphSupport.filter((item) => item.support_level === "weak");

  if (signals.authority_count < 3) {
    limitations.push("Sparse authority retrieval limits the reliability of Conclusions of Law guidance.");
  }
  if (signals.direct_conclusions_count === 0) {
    limitations.push("No directly relevant Conclusions of Law sections were retrieved; support is primarily indirect.");
  }
  if (signals.conflict_index >= 0.45) {
    limitations.push("Authorities show meaningful directional conflict; competing interpretations should be addressed explicitly.");
  }
  if (signals.findings_coverage < 0.35) {
    limitations.push("Findings-to-authority coverage is limited; additional factual specificity is recommended.");
  }
  if (signals.law_coverage < 0.3 || tokenize(parsed.law_text).length < 6) {
    limitations.push("Provided law/citation detail appears thin; legal support may be incomplete.");
  }
  if (weak.length > 0) {
    limitations.push("One or more draft paragraphs rely on weak support and should be reviewed before use.");
  }
  if (unsupported.length > 0) {
    limitations.push("At least one paragraph lacks mapped supporting anchors and should not be treated as settled.");
  }
  if (caseAssistant.outcome_guidance.direction === "unclear" || caseAssistant.outcome_guidance.direction === "partial") {
    limitations.push("Retrieved authorities do not converge on a single direction; treat the draft as non-determinative.");
  }

  return uniq([...limitations, ...DRAFT_CONCLUSIONS_GUARDRAILS]).slice(0, 10);
}

function buildReasoningNotes(
  caseAssistant: CaseAssistantResponse,
  signals: ConfidenceSignals
): string[] {
  const notes = [
    ...caseAssistant.reasoning_themes.map((item) => `${item.theme}: ${item.explanation}`),
    ...caseAssistant.vulnerabilities.map((item) => `Risk - ${item.issue}: ${item.impact}`),
    `Signal - retrieval strength: ${(signals.retrieval_strength * 100).toFixed(0)}%`,
    `Signal - findings coverage: ${(signals.findings_coverage * 100).toFixed(0)}%`,
    `Signal - law coverage: ${(signals.law_coverage * 100).toFixed(0)}%`,
    `Signal - conflict index: ${(signals.conflict_index * 100).toFixed(0)}%`
  ];
  return notes.slice(0, 10);
}

function buildHeuristicDraftText(sections: DraftConclusionsResponse["draft_sections"]): string {
  return sections.map((section, idx) => `${idx + 1}. ${section.heading}\n${section.text}`).join("\n\n");
}

async function assembleDraft(parsed: DraftConclusionsRequest, caseAssistant: CaseAssistantResponse, env: Env) {
  const authorityByCitationId = mapAuthorityByCitationId(caseAssistant);
  const fallbackSections = buildFallbackScaffoldSections(caseAssistant);
  let draftText = buildHeuristicDraftText(fallbackSections);

  try {
    const llm = await callDraftLlm({ env, parsed, caseAssistant });
    draftText = llm.draftText;
  } catch {
    // Fall back to the prior heuristic draft when no LLM is configured or the call fails.
  }

  const sections = buildDraftSections(draftText, caseAssistant);
  const paragraphSupport = buildParagraphSupport(draftText, authorityByCitationId);
  const signals = computeConfidenceSignals({ parsed, caseAssistant, paragraphSupport });
  const confidence = chooseConfidence(signals);
  const limitations = buildLimitations(parsed, signals, paragraphSupport, caseAssistant);

  const draft = {
    query_summary: caseAssistant.query_summary,
    draft_text: draftText,
    draft_sections: sections,
    paragraph_support: paragraphSupport,
    supporting_authorities: [...caseAssistant.similar_cases, ...caseAssistant.relevant_law].slice(0, 12),
    reasoning_notes: buildReasoningNotes(caseAssistant, signals),
    confidence,
    confidence_signals: signals,
    limitations,
    citations: caseAssistant.citations
  };

  return {
    draft: draftConclusionsResponseSchema.parse(draft),
    debug: {
      paragraph_support: paragraphSupport,
      confidence_signals: signals,
      triggered_limitations: limitations,
      chosen_citation_ids: uniq(sections.flatMap((item) => item.citation_ids)),
      unsupported_paragraphs: paragraphSupport.filter((item) => item.support_level === "unsupported").map((item) => item.paragraph_id)
    }
  };
}

export async function runDraftConclusions(env: Env, input: unknown): Promise<DraftConclusionsResponse> {
  const parsed = draftConclusionsRequestSchema.parse(input);
  const caseAssistant = await retrieveDraftResearch(env, parsed);
  return (await assembleDraft(parsed, caseAssistant, env)).draft;
}

export async function runDraftConclusionsDebug(env: Env, input: unknown): Promise<DraftConclusionsDebugResponse> {
  const parsed = draftConclusionsRequestSchema.parse(input);
  const caseAssistant = await retrieveDraftResearch(env, parsed);
  const assembled = await assembleDraft(parsed, caseAssistant, env);

  return draftConclusionsDebugResponseSchema.parse({
    request: parsed,
    draft: assembled.draft,
    debug: assembled.debug
  });
}
