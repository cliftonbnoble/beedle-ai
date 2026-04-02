import type { SearchResponse } from "@beedle/shared";

export type SearchResultRow = SearchResponse["results"][number];

export type MatchBadge = {
  label: string;
  tone: "gold" | "blue" | "green" | "neutral";
};

function normalize(value: string) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function uniq<T>(values: T[]) {
  return Array.from(new Set(values));
}

export function friendlySectionLabel(value: string) {
  const normalized = normalize(value);
  if (!normalized) return "Matched passage";
  if (/conclusions?_of_law|authority_discussion|analysis_reasoning/.test(normalized)) return "Conclusions of Law";
  if (/findings?_of_fact|fact_findings/.test(normalized)) return "Findings of Fact";
  if (/procedural|history|background/.test(normalized)) return "Procedural History";
  if (/order|disposition|holding/.test(normalized)) return "Order";
  return String(value || "")
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function humanizeRetrievalReason(reason: string): string | null {
  const normalized = normalize(reason);
  if (!normalized) return null;
  if (normalized === "exact_phrase_match") return "Your wording closely matches this passage.";
  if (normalized.startsWith("sentence_phrase_overlap_boost")) return "This passage uses wording that is very close to your search.";
  if (normalized.startsWith("market_condition_reasoning_boost")) return "This passage speaks directly to market-condition rent reasoning.";
  if (normalized === "citation_exact_or_near") return "The citation lines up closely with what you searched for.";
  if (normalized === "index_code_overlap") return "It matches the index code filter you selected.";
  if (normalized === "index_code_rules_compat_overlap") return "It lines up with rules tied to your selected index code.";
  if (normalized === "index_code_ordinance_compat_overlap") return "It lines up with ordinance sections tied to your selected index code.";
  if (normalized === "index_code_phrase_compat_overlap") return "Its wording lines up with the issue described by your selected index code.";
  if (normalized === "rules_overlap") return "It matches the rules section you selected.";
  if (normalized === "ordinance_overlap") return "It matches the ordinance section you selected.";
  if (normalized === "party_name_exactish") return "It appears to match the party name you entered.";
  if (normalized === "judge_name_filter_match" || normalized === "judge_name_query_match" || normalized === "judge_only_author_match_boost") {
    return "It matches the judge context in your search.";
  }
  if (normalized === "trusted_tier_boost") return "It comes from the trusted portion of the decision corpus.";
  if (normalized === "reasoning_section_boost" || normalized === "conclusion_sentence_query_boost" || normalized === "conclusion_authority_query_boost") {
    return "The strongest passage is in the decision's legal reasoning section.";
  }
  if (normalized === "conclusion_issue_overlap_boost") return "The legal reasoning section directly discusses the issue you searched for.";
  if (normalized === "legal_section_boost") return "This passage appears in a section focused on legal authority or rules.";
  if (normalized === "issue_section_boost" || normalized === "issue_preferred_chunk_type_boost") {
    return "This passage deals directly with the issue you searched for.";
  }
  if (normalized.startsWith("issue_term_overlap")) return "Several of your search terms appear in this passage.";
  if (normalized === "procedural_section_boost") return "This passage appears in the part of the decision that explains procedural history or the ruling path.";
  return null;
}

export function plainEnglishMatchReasons(row: SearchResultRow, limit = 3) {
  const reasons = row.retrievalReason.map(humanizeRetrievalReason).filter((value): value is string => Boolean(value));
  const strongestSection = row.matchedPassage?.sectionLabel || row.sectionLabel || row.chunkType || "";
  if (/conclusions? of law|authority discussion|analysis reasoning/i.test(strongestSection)) {
    reasons.unshift("This match comes from the part of the decision where the judge explains the legal reasoning.");
  }
  const uniqueReasons = uniq(reasons).slice(0, limit);
  return uniqueReasons.length > 0 ? uniqueReasons : ["The wording and context of this passage line up well with your search."];
}

export function matchBadges(row: SearchResultRow, options?: { includeBestMatch?: boolean }) {
  const badges: MatchBadge[] = [];
  if (options?.includeBestMatch) badges.push({ label: "Best match", tone: "gold" });

  const sectionLabel = friendlySectionLabel(row.matchedPassage?.sectionLabel || row.sectionLabel || row.chunkType || "");
  badges.push({ label: sectionLabel, tone: /Conclusions of Law/i.test(sectionLabel) ? "gold" : "blue" });

  const normalizedReasons = row.retrievalReason.map(normalize);
  if (normalizedReasons.some((reason) => reason === "judge_name_filter_match" || reason === "judge_name_query_match" || reason === "judge_only_author_match_boost")) {
    badges.push({ label: "Judge match", tone: "green" });
  }
  if (normalizedReasons.some((reason) => reason === "rules_overlap")) {
    badges.push({ label: "Rules match", tone: "blue" });
  }
  if (normalizedReasons.some((reason) => reason === "ordinance_overlap")) {
    badges.push({ label: "Ordinance match", tone: "blue" });
  }
  if (normalizedReasons.some((reason) => reason.startsWith("sentence_phrase_overlap_boost") || reason === "exact_phrase_match")) {
    badges.push({ label: "Close wording match", tone: "green" });
  }
  if (normalizedReasons.some((reason) => reason.startsWith("market_condition_reasoning_boost"))) {
    badges.push({ label: "Market-condition reasoning", tone: "green" });
  }

  return uniq(badges.map((badge) => `${badge.label}::${badge.tone}`)).map((value) => {
    const [label, tone] = value.split("::");
    return { label, tone: tone as MatchBadge["tone"] };
  });
}

export function decisionMatchSummary(row: SearchResultRow) {
  const section = friendlySectionLabel(row.matchedPassage?.sectionLabel || row.sectionLabel || row.chunkType || "");
  const reasons = plainEnglishMatchReasons(row, 2);
  return `The strongest match came from ${section}. ${reasons[0]}`;
}
