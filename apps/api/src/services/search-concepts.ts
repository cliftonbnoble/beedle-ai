// Pure phrase/concept coverage helpers extracted from search.ts (SEARCH-02c module split, step 3).
//
// These generate concept/surface variants of query tokens, build the phrase FTS query, and compute
// phrase-coverage signals over candidate text. They depend only on the text primitives, the shared
// concept lexicon, and each other -- never on SearchContext / ChunkRow / the DB -- so the relocation is
// behavior-neutral. (Context-coupled phrase guards such as phraseConceptGuardPasses stay in search.ts;
// exactMultiWordPhraseScore stays too because it calls query-classification predicates not yet split.)

import { conceptVariantsForToken, searchIrregularTokenVariants } from "@beedle/shared";
import {
  containsWholeWord,
  escapeRegex,
  ftsQuote,
  meaningfulLexicalTokens,
  normalize,
  normalizeWhitespace,
  STOPWORD_TOKENS,
  tokenize,
  uniq
} from "./search-text";

export function phraseSurfaceVariants(value: string): string[] {
  const normalized = normalize(value || "");
  if (!normalized) return [];
  const variants = new Set<string>([normalized]);
  variants.add(normalized.replace(/['’]/g, ""));
  variants.add(normalized.replace(/['’]/g, " "));
  if (normalized.includes("-")) variants.add(normalized.replace(/-/g, " "));
  if (normalized.includes(" ")) variants.add(normalized.replace(/\s+/g, "-"));
  if (normalized.includes(" ")) variants.add(normalized.replace(/\s+/g, ""));
  return Array.from(variants)
    .map((item) => normalizeWhitespace(item))
    .filter(Boolean);
}

export function tokenSurfaceVariants(token: string): string[] {
  const normalized = normalize(token || "");
  if (!normalized) return [];
  const variants = new Set<string>([normalized, ...(searchIrregularTokenVariants[normalized] || [])]);
  if (normalized.endsWith("ies") && normalized.length > 4) variants.add(`${normalized.slice(0, -3)}y`);
  if (normalized.endsWith("y") && normalized.length > 3) variants.add(`${normalized.slice(0, -1)}ies`);
  if (normalized.endsWith("es") && normalized.length > 4) variants.add(normalized.slice(0, -2));
  if (normalized.endsWith("s") && normalized.length > 3) variants.add(normalized.slice(0, -1));
  if (!normalized.endsWith("s")) variants.add(`${normalized}s`);
  if (!normalized.endsWith("es") && /(s|x|z|ch|sh)$/.test(normalized)) variants.add(`${normalized}es`);
  return Array.from(variants).filter(Boolean);
}

export function keywordSurfaceVariants(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return [];
  const tokens = tokenize(normalized);
  const variants = new Set<string>(phraseSurfaceVariants(normalized));
  const singleToken = tokens.length === 1 ? tokens[0] : null;
  if (singleToken) {
    for (const variant of tokenSurfaceVariants(singleToken)) variants.add(variant);
  }
  return Array.from(variants).filter(Boolean);
}

// Pure function of the token, but previously recomputed per row × per term inside lexicalScore's hot
// loop (surface variants + concept-lexicon expansion each time). Memoized per isolate; callers only
// map/filter the result, never mutate it. Cap mirrors the regex caches in search-text.
const phraseConceptVariantCache = new Map<string, string[]>();
const PHRASE_CONCEPT_VARIANT_CACHE_MAX = 5000;

export function phraseConceptVariantsForToken(token: string): string[] {
  const normalized = normalize(token || "");
  if (!normalized) return [];
  const cached = phraseConceptVariantCache.get(normalized);
  if (cached) return cached;
  const variants = new Set<string>([normalized, ...tokenSurfaceVariants(normalized)]);
  for (const value of conceptVariantsForToken(normalized, "search")) {
    const item = normalizeWhitespace(normalize(value || ""));
    if (item) variants.add(item);
  }

  const result = Array.from(variants).filter(Boolean);
  if (phraseConceptVariantCache.size >= PHRASE_CONCEPT_VARIANT_CACHE_MAX) phraseConceptVariantCache.clear();
  phraseConceptVariantCache.set(normalized, result);
  return result;
}

export function phraseConceptGroups(query: string, precomputed?: { phraseTokens?: string[] }): string[][] {
  const tokens = precomputed?.phraseTokens ?? meaningfulPhraseTokens(query);
  if (tokens.length < 2 || tokens.length > 6) return [];
  return tokens
    .map((token) => phraseConceptVariantsForToken(token))
    .filter((group) => group.length > 0)
    .slice(0, 6);
}

// NS-04 variant for the USER-query channel only: >6 meaningful tokens no longer hard-fail — keep the
// 6 most selective (longest first; length is the selectivity proxy available without corpus DF),
// original order among ties — so long natural-language questions still enter the phrase engine on
// their constraint core. Deliberately NOT folded into phraseConceptGroups: the retrieval-expansion
// channel (expandQueryForRetrieval output routinely exceeds 6 tokens for curated keyword families)
// is tuned — and golden-pinned — around the >6 no-op, so relaxing it there reshuffles keyword-family
// rankings (measured: water_heater/noise_nuisance/infestation golden order changed).
export function selectivePhraseConceptGroups(query: string, precomputed?: { phraseTokens?: string[] }): string[][] {
  const tokens = precomputed?.phraseTokens ?? meaningfulPhraseTokens(query);
  if (tokens.length < 2) return [];
  const selected =
    tokens.length > 6
      ? tokens
          .map((token, position) => ({ token, position }))
          .sort((a, b) => b.token.length - a.token.length || a.position - b.position)
          .slice(0, 6)
          .sort((a, b) => a.position - b.position)
          .map((item) => item.token)
      : tokens;
  return selected
    .map((token) => phraseConceptVariantsForToken(token))
    .filter((group) => group.length > 0)
    .slice(0, 6);
}

export function phrasePriorityLexicalTerms(query: string): string[] {
  const full = normalizeWhitespace(normalize(String(query || "").slice(0, 260)));
  if (!full) return [];
  const tokens = meaningfulLexicalTokens(full);
  const normalizedQueryContext = { normalizedQuery: full };
  if (tokens.length < 2) return [full, ...keywordSurfaceVariants(full, normalizedQueryContext)].filter(Boolean).slice(0, 8);

  const conceptVariants = tokens.flatMap((token) => phraseConceptVariantsForToken(token));
  return uniq([
    full,
    ...phraseSurfaceVariants(full),
    ...conceptVariants,
    ...tokens
  ].filter(Boolean)).slice(0, 14);
}

export function meaningfulPhraseTokens(query: string): string[] {
  return uniq(tokenize(query))
    .filter((token) => token.length >= 3 && !STOPWORD_TOKENS.has(token))
    .slice(0, 8);
}

export function sentencePhraseOverlapScore(
  query: string,
  text: string,
  precomputed?: { queryTokens?: string[]; normalizedText?: string }
): number {
  const queryTokens = precomputed?.queryTokens ?? tokenize(query).filter((token) => token.length > 2 && !STOPWORD_TOKENS.has(token));
  if (queryTokens.length < 5) return 0;

  const textTokenSet = new Set(
    tokenize(precomputed?.normalizedText ?? text).filter((token) => token.length > 2 && !STOPWORD_TOKENS.has(token))
  );
  if (!textTokenSet.size) return 0;

  let longestRun = 0;
  let currentRun = 0;
  for (const token of queryTokens) {
    if (textTokenSet.has(token)) {
      currentRun += 1;
      longestRun = Math.max(longestRun, currentRun);
    } else {
      currentRun = 0;
    }
  }

  if (longestRun >= 8) return 0.12;
  if (longestRun >= 6) return 0.08;
  if (longestRun >= 5) return 0.05;
  return 0;
}

export function wholePhraseIndexInNormalizedText(normalizedText: string, normalizedTerm: string): number {
  if (!normalizedText || !normalizedTerm) return -1;
  const match = new RegExp(`(^|[^a-z0-9])(${escapeRegex(normalizedTerm)})(?=$|[^a-z0-9])`, "i").exec(normalizedText);
  if (!match) return -1;
  return (match.index ?? 0) + (match[1]?.length || 0);
}

export function phraseSearchFtsQuery(query: string, precomputed?: { normalizedQuery?: string; normalizedGroups?: string[][]; phraseTokens?: string[] }): string {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalizeWhitespace(normalize(query || ""));
  const groups = precomputed?.normalizedGroups ?? phraseConceptGroups(normalizedQuery);
  if (groups.length < 2) return "";

  const phraseTokens = precomputed?.phraseTokens ?? meaningfulPhraseTokens(normalizedQuery);
  const exactPhrase = ftsQuote(phraseTokens.join(" "));
  const conceptExpression = groups
    .map((group) => {
      const variants = uniq(group.map(ftsQuote).filter(Boolean)).slice(0, 7);
      if (variants.length === 0) return "";
      return variants.length === 1 ? variants[0] : `(${variants.join(" OR ")})`;
    })
    .filter(Boolean)
    .join(" AND ");

  return [exactPhrase, conceptExpression ? `(${conceptExpression})` : ""].filter(Boolean).join(" OR ");
}

// OR-of-prefix-terms FTS expression over an explicit vocabulary: matches a chunk if ANY term appears
// as a word prefix — the recall shape of the substring fallback scan, answered by the FTS index.
// Prefix syntax ("habitab"*) keeps truncated-word queries covered, since the scan this stands in for
// matches substrings, not whole tokens.
export function prefixedFtsTermsQuery(terms: string[]): string {
  return uniq(terms.map(ftsQuote).filter(Boolean))
    .slice(0, 24)
    .map((variant) => `${variant}*`)
    .join(" OR ");
}

// Same expression built from the query's concept variants. An empty FTS result for this proves the
// unindexed scan cannot match either — the basis for skipping it (NS-29).
export function anyTokenFtsQuery(query: string, precomputed?: { normalizedGroups?: string[][]; phraseTokens?: string[] }): string {
  const groups = precomputed?.normalizedGroups ?? phraseConceptGroups(query, { phraseTokens: precomputed?.phraseTokens });
  const terms = groups.length ? groups.flat() : precomputed?.phraseTokens ?? meaningfulPhraseTokens(query);
  return prefixedFtsTermsQuery(terms);
}

// Relaxed AND (NS-04/NS-07): when the full AND-across-all-concept-groups FTS query matches nothing
// for a long natural-language query, retry requiring only the `keepGroups` most selective groups
// (longest source token first — the group's first variant is its normalized token). Returns "" when
// no relaxation is possible (already at or below keepGroups).
export function relaxedPhraseFtsQuery(
  query: string,
  precomputed: { normalizedGroups?: string[][]; phraseTokens?: string[] } | undefined,
  keepGroups: number
): string {
  const groups = precomputed?.normalizedGroups ?? phraseConceptGroups(query, { phraseTokens: precomputed?.phraseTokens });
  if (groups.length <= keepGroups) return "";
  const selected = groups
    .map((group, position) => ({ group, position }))
    .sort((a, b) => (b.group[0]?.length || 0) - (a.group[0]?.length || 0) || a.position - b.position)
    .slice(0, keepGroups)
    .sort((a, b) => a.position - b.position)
    .map(({ group }) => group);
  return selected
    .map((group) => {
      const variants = uniq(group.map(ftsQuote).filter(Boolean)).slice(0, 7);
      if (variants.length === 0) return "";
      return variants.length === 1 ? variants[0] : `(${variants.join(" OR ")})`;
    })
    .filter(Boolean)
    .join(" AND ");
}

export function phraseConceptCoverage(
  query: string,
  text: string,
  precomputed?: { normalizedQuery?: string; normalizedGroups?: string[][]; normalizedText?: string }
): {
  totalCount: number;
  matchedCount: number;
  coverageRatio: number;
  exactPhrase: boolean;
  proximityBoost: number;
} {
  const groups =
    precomputed?.normalizedGroups ??
    phraseConceptGroups(query).map((group) => group.map((variant) => normalizeWhitespace(normalize(variant))).filter(Boolean));
  const normalizedText = precomputed?.normalizedText ?? normalize(text || "");
  if (groups.length < 2 || !normalizedText) {
    return { totalCount: groups.length, matchedCount: 0, coverageRatio: 0, exactPhrase: false, proximityBoost: 0 };
  }

  const normalizedQuery = precomputed?.normalizedQuery ?? normalizeWhitespace(normalize(query || ""));
  const exactPhrase = Boolean(normalizedQuery && containsWholeWord(normalizedText, normalizedQuery));
  const matchedPositions = groups
    .map((group) => {
      let bestIndex = -1;
      for (const normalizedVariant of group) {
        if (!normalizedVariant) continue;
        const index = wholePhraseIndexInNormalizedText(normalizedText, normalizedVariant);
        if (index >= 0 && (bestIndex < 0 || index < bestIndex)) bestIndex = index;
      }
      return bestIndex;
    })
    .filter((index) => index >= 0)
    .sort((a, b) => a - b);
  const matchedCount = matchedPositions.length;
  const coverageRatio = matchedCount > 0 ? matchedCount / groups.length : 0;

  let proximityBoost = 0;
  if (matchedPositions.length >= 2) {
    const first = matchedPositions[0] ?? 0;
    const last = matchedPositions[matchedPositions.length - 1] ?? first;
    const span = last - first;
    if (span <= 120) proximityBoost = 0.18;
    else if (span <= 240) proximityBoost = 0.12;
    else if (span <= 420) proximityBoost = 0.07;
    else if (span <= 700) proximityBoost = 0.035;
  }

  return {
    totalCount: groups.length,
    matchedCount,
    coverageRatio: Number(coverageRatio.toFixed(4)),
    exactPhrase,
    proximityBoost
  };
}
