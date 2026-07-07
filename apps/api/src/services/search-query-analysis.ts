// Pure query-analysis layer extracted from search.ts (SEARCH-02c module split, step 5b, layer 1).
//
// Everything needed to turn a query + filters into recall config, scope, expansion terms, index-code /
// reference filters, citation + section-label helpers and query-intent inference -- but NO database I/O
// and NO mutable runtime state. Depends only on the text / concept / classification / lexical-SQL leaf
// modules, the shared types, and each other. The DB-fetch layer (search-fts, step 5c) builds on this.
import { canonicalizeJudgeName, normalizeJudgeLookupKey, queryReferencesJudge } from "./judges";
import { normalizeFilterValue } from "./legal-references";
import {
  keywordSurfaceVariants,
  meaningfulPhraseTokens,
  phraseConceptGroups,
  phrasePriorityLexicalTerms,
  phraseSurfaceVariants,
  selectivePhraseConceptGroups,
  tokenSurfaceVariants
} from "./search-concepts";
import {
  hasAccommodationContext,
  hasCameraPrivacyContext,
  hasCoLivingContext,
  hasCommonAreasContext,
  hasExplicitOrdinance379Mention,
  hasGarageSpaceContext,
  hasHabitabilityServiceRestorationSignals,
  hasHomeownersExemptionContext,
  hasIntercomContext,
  hasOwnerMoveInPhrase,
  hasPackageSecurityContext,
  hasPorchContext,
  hasSection8Context,
  hasStairsContext,
  hasUnlawfulDetainerContext,
  hasWindowsContext,
  hasWrongfulEvictionContext,
  hasWrongfulEvictionLockoutContext,
  hasWrongfulEvictionPhrase,
  isAccommodationQuery,
  isAdjudicatedQuery,
  isAntInfestationQuery,
  isBuyoutPressureQuery,
  isBuyoutQuery,
  isCameraPrivacyQuery,
  isCaregiverQuery,
  isCoLivingQuery,
  isCollegeQuery,
  isCommonAreasQuery,
  isCoolingIssueQuery,
  isDivorceQuery,
  isDogQuery,
  isEvictionProtectionQuery,
  isGarageSpaceQuery,
  isHomeownersExemptionQuery,
  isInfestationAliasQuery,
  isIntercomQuery,
  isLeakWindowQuery,
  isLockBoxQuery,
  isMootQuery,
  isNuisanceQuery,
  isOwnerMoveInIssueSearch,
  isPackageSecurityQuery,
  isPhraseEvidenceQuery,
  isPoopQuery,
  isPorchQuery,
  isRemoteWorkQuery,
  isRentReductionQuery,
  isRoomHeatQuery,
  isSection8Query,
  isSection8UnlawfulDetainerQuery,
  isSelfEmployedQuery,
  isSocialMediaQuery,
  isStairsQuery,
  isUnlawfulDetainerQuery,
  isVectorFirstIssueSearch,
  isWindowsQuery,
  isWrongfulEvictionIssueSearch,
  requiresLockoutSpecificity,
  requiresOwnerMoveInFollowThroughSpecificity,
  requiresStrongIssueEvidence
} from "./search-query-classification";
import {
  STOPWORD_TOKENS,
  containsWholeWord,
  meaningfulLexicalTokens,
  normalize,
  normalizeWhitespace,
  tokenize,
  uniq
} from "./search-text";
import { SearchRequest, canonicalIndexCodeOptions } from "@beedle/shared";
import type {
  ChunkRow,
  CuratedKeywordFamily,
  DocumentReferenceSectionFacet,
  IndexCodeFilterContext,
  IndexCodeFilterContextOptions,
  QueryDerivedContext,
  QueryIntent,
  SearchContext,
  SearchScopeOptions
} from "./search-types";

// D1 rejects a prepared statement with more than ~100 bound parameters. The lexical recall queries bind
// several parameters per query term, so term expansions must be capped to keep each statement under it.
const D1_MAX_BOUND_PARAMS = 100;

// Cap a lexical query's term expansion so the whole statement stays under D1's bound-parameter limit. A
// statement binds `perTerm` parameters per term plus `fixedParams` non-term parameters (scope ids bound
// once or twice, filter binds, the LIMIT). Trimming to the largest term count that still fits is a no-op
// for any query already under the limit — it only trims queries that would otherwise overflow — so it is
// safe everywhere. Highest-priority terms come first, so slicing keeps the strongest signals.
export function boundLexicalTermsForD1(terms: string[], perTerm: number, fixedParams: number): string[] {
  const maxTerms = Math.max(1, Math.floor((D1_MAX_BOUND_PARAMS - fixedParams) / perTerm));
  return terms.length > maxTerms ? terms.slice(0, maxTerms) : terms;
}

const canonicalIndexCodeByNormalized = new Map(
  canonicalIndexCodeOptions.map((option) => [normalizeFilterValue("index_code", option.code), option] as const)
);

function isDhsFamilyIndexCodeOption(option: (typeof canonicalIndexCodeOptions)[number] | undefined) {
  if (!option) return false;
  const description = normalizeWhitespace(option.description || "");
  return /^DHS\s*--/i.test(description);
}

const MANUAL_INDEX_CODE_ALIAS_MAP: Record<string, { legacyCodes?: string[]; searchPhrases?: string[] }> = {
  g27: {
    legacyCodes: ["13"],
    searchPhrases: ["substantial decrease in housing services", "code violation substantial decrease in housing services"]
  },
  g28: {
    legacyCodes: ["13"],
    searchPhrases: [
      "not substantial decrease in housing services",
      "decrease in service not substantial",
      "not a substantial decrease in housing services",
      "does not constitute a substantial decrease in housing services",
      "did not constitute a substantial decrease in housing services"
    ]
  },
  g93: {
    searchPhrases: [
      "uniform hotel visitor policy",
      "visitor policy for residential hotel",
      "uniform visitor policy",
      "rent reduction for noncompliance with uniform policy",
      "supplemental visitor policy",
      "chapter 41d"
    ]
  }
};

function requestedJudgeFilters(filters: SearchRequest["filters"]): string[] {
  const raw = uniq([...(filters.judgeNames || []), filters.judgeName || ""].filter(Boolean));
  return raw
    .map((value) => canonicalizeJudgeName(value))
    .filter((value): value is string => Boolean(value));
}

export function requestedIndexCodeFilters(filters: SearchRequest["filters"]): string[] {
  return uniq([...(filters.indexCodes || []), filters.indexCode || ""].filter(Boolean).map((value) => String(value).trim())).filter(Boolean);
}

function extractCatalogReferenceCitations(raw: string): string[] {
  const text = String(raw || "");
  const matches = new Set<string>();

  for (const match of text.matchAll(/\b\d+\.\d+[a-z]?(?:\([a-z0-9]+\))*[a-z]?\b/gi)) {
    matches.add(match[0]);
  }

  for (const match of text.matchAll(/§\s*([0-9]+(?:\.[0-9]+)?(?:\([a-z0-9]+\))*)/gi)) {
    if (match[1]) matches.add(match[1]);
  }

  return Array.from(matches);
}

function extractBaseCitation(value: string): string | null {
  const match = String(value || "").match(/^(\d+\.\d+[a-z]?)/i);
  return match?.[1] || null;
}

function buildCitationVariants(values: string[]): string[] {
  const variants = new Set<string>();
  for (const value of values) {
    const trimmed = String(value || "").trim();
    if (!trimmed) continue;
    variants.add(trimmed);
    const base = extractBaseCitation(trimmed);
    if (base) variants.add(base);
  }
  return Array.from(variants);
}

function buildIndexCodeDescriptionPhrases(description: string): string[] {
  const text = String(description || "").replace(/\s+/g, " ").trim();
  if (!text) return [];
  const phrases = new Set<string>();
  const addPhrase = (value: string) => {
    const phrase = String(value || "").replace(/\s+/g, " ").trim();
    if (!phrase || /\[reserved\]/i.test(phrase)) return;
    if (/^(code violation|dhs|other)$/i.test(phrase)) return;
    const wordCount = phrase.split(/\s+/).length;
    if (wordCount <= 0) return;
    if (wordCount === 1 && phrase.length < 20) return;
    if (wordCount === 2 && phrase.length < 12) return;
    phrases.add(phrase);
  };

  addPhrase(text);

  const doubleDashParts = text
    .split("--")
    .map((part) => part.trim())
    .filter(Boolean);
  for (const part of doubleDashParts) {
    addPhrase(part);
    for (const subPart of part.split(" - ").map((item) => item.trim()).filter(Boolean)) {
      addPhrase(subPart);
    }
  }

  return Array.from(phrases).slice(0, 4);
}

export function buildIndexCodeFilterContext(
  filters: SearchRequest["filters"],
  options: IndexCodeFilterContextOptions = {}
): IndexCodeFilterContext {
  const includeGenericDhsFamilyAlias = options.includeGenericDhsFamilyAlias !== false;
  const requestedCodes = requestedIndexCodeFilters(filters);
  const normalizedCodes = uniq(requestedCodes.map((value) => normalizeFilterValue("index_code", value))).filter(Boolean);
  const legacyCodeAliases = new Set<string>();
  const relatedRulesSections = new Set<string>();
  const relatedOrdinanceSections = new Set<string>();
  const searchPhrases = new Set<string>();

  for (const normalizedCode of normalizedCodes) {
    const option = canonicalIndexCodeByNormalized.get(normalizedCode);
    const manualAliases = MANUAL_INDEX_CODE_ALIAS_MAP[normalizedCode];
    for (const legacyCode of manualAliases?.legacyCodes || []) legacyCodeAliases.add(String(legacyCode).trim());
    if (includeGenericDhsFamilyAlias && isDhsFamilyIndexCodeOption(option)) legacyCodeAliases.add("13");
    for (const phrase of manualAliases?.searchPhrases || []) searchPhrases.add(phrase);
    if (!option) continue;

    for (const ruleCitation of buildCitationVariants(extractCatalogReferenceCitations(option.rules || ""))) {
      relatedRulesSections.add(ruleCitation);
    }

    for (const ordinanceCitation of buildCitationVariants(extractCatalogReferenceCitations(option.ordinance || ""))) {
      relatedOrdinanceSections.add(ordinanceCitation);
    }

    for (const phrase of buildIndexCodeDescriptionPhrases(option.description || "")) {
      searchPhrases.add(phrase);
    }
  }

  return {
    requestedCodes,
    normalizedCodes,
    legacyCodeAliases: Array.from(legacyCodeAliases).filter(Boolean),
    relatedRulesSections: Array.from(relatedRulesSections),
    relatedOrdinanceSections: Array.from(relatedOrdinanceSections),
    searchPhrases: Array.from(searchPhrases).filter(Boolean)
  };
}

export function directIndexCodeMatchValuesForRequestedCode(code: string, options: IndexCodeFilterContextOptions = {}): string[] {
  const includeGenericDhsFamilyAlias = options.includeGenericDhsFamilyAlias !== false;
  const normalizedCode = normalizeFilterValue("index_code", code);
  const option = canonicalIndexCodeByNormalized.get(normalizedCode);
  const manualAliases = MANUAL_INDEX_CODE_ALIAS_MAP[normalizedCode];
  const values = new Set<string>([code]);
  for (const legacyCode of manualAliases?.legacyCodes || []) values.add(String(legacyCode).trim());
  if (includeGenericDhsFamilyAlias && isDhsFamilyIndexCodeOption(option)) values.add("13");
  return Array.from(values).filter(Boolean);
}

export function bindIndexCodeMatchValues(params: Array<string | number>, values: string[]) {
  for (const value of values) {
    params.push(normalizeFilterValue("index_code", value), value);
  }
  for (const value of values) {
    params.push(normalizeFilterValue("index_code", value), value);
  }
}

export function buildDirectIndexCodeCompatibilityClause(values: string[]): string {
  const facetClauses = values.map(() => "(dic.normalized_code = ? OR lower(dic.code) = lower(?))").join(" OR ");
  const referenceClauses = values.map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))").join(" OR ");
  return `(
    EXISTS (
      SELECT 1 FROM document_index_codes dic
      WHERE dic.document_id = d.id
        AND (${facetClauses})
    )
    OR EXISTS (
      SELECT 1 FROM document_reference_links l
      WHERE l.document_id = d.id
        AND l.reference_type = 'index_code'
        AND l.is_valid = 1
        AND (${referenceClauses})
    )
  )`;
}

export function bindReferenceSectionMatchValues(
  params: Array<string | number>,
  referenceType: DocumentReferenceSectionFacet,
  values: string[],
  options: { includePrefixMatch?: boolean } = {}
) {
  for (const value of values) {
    const normalizedValue = normalizeFilterValue(referenceType, value);
    params.push(normalizedValue, value);
    if (options.includePrefixMatch) params.push(`${normalizedValue}%`, `${value}%`);
  }
  for (const value of values) {
    const normalizedValue = normalizeFilterValue(referenceType, value);
    params.push(normalizedValue, value);
    if (options.includePrefixMatch) params.push(`${normalizedValue}%`, `${value}%`);
  }
}

export function buildReferenceSectionCompatibilityClause(
  referenceType: DocumentReferenceSectionFacet,
  values: string[],
  options: { includePrefixMatch?: boolean } = {}
): string {
  const isRules = referenceType === "rules_section";
  const table = isRules ? "document_rules_sections" : "document_ordinance_sections";
  const alias = isRules ? "drs" : "dos";
  const facetClauses = values
    .map(() =>
      options.includePrefixMatch
        ? `(${alias}.normalized_section = ? OR lower(${alias}.section) = lower(?) OR ${alias}.normalized_section LIKE ? OR lower(${alias}.section) LIKE lower(?))`
        : `(${alias}.normalized_section = ? OR lower(${alias}.section) = lower(?))`
    )
    .join(" OR ");
  const referenceClauses = values
    .map(() =>
      options.includePrefixMatch
        ? "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?) OR l.normalized_value LIKE ? OR lower(coalesce(l.canonical_value, '')) LIKE lower(?))"
        : "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))"
    )
    .join(" OR ");
  return `(
    EXISTS (
      SELECT 1 FROM ${table} ${alias}
      WHERE ${alias}.document_id = d.id
        AND (${facetClauses})
    )
    OR EXISTS (
      SELECT 1 FROM document_reference_links l
      WHERE l.document_id = d.id
        AND l.reference_type = '${referenceType}'
        AND l.is_valid = 1
        AND (${referenceClauses})
    )
  )`;
}

function buildExactIndexCodeIntersectionClauses(
  requestedCodes: string[],
  params: Array<string | number>,
  options: IndexCodeFilterContextOptions = {}
): string[] {
  return requestedCodes.map((code) => {
    const directValues = directIndexCodeMatchValuesForRequestedCode(code, options);
    bindIndexCodeMatchValues(params, directValues);
    return buildDirectIndexCodeCompatibilityClause(directValues);
  });
}

const CURATED_KEYWORD_FAMILIES: CuratedKeywordFamily[] = [
  {
    triggers: ["infestation", "infestations", "pest", "pests"],
    expansions: [
      "infestation",
      "rodent",
      "mouse",
      "cockroach",
      "bed bug",
      "flea",
      "pest",
      "rat",
      "ant"
    ]
  },
  {
    triggers: ["rodent", "rodents", "rat", "rats", "mouse", "mice"],
    expansions: ["rodent", "mouse", "rat", "infestation", "pest"]
  },
  {
    triggers: ["cockroach", "cockroaches", "roach", "roaches"],
    expansions: ["cockroach", "infestation", "pest"]
  },
  {
    triggers: ["ant", "ants"],
    expansions: ["ant", "infestation", "pest"]
  },
  {
    triggers: ["flea", "fleas"],
    expansions: ["flea", "infestation", "pest"]
  },
  {
    triggers: ["bed bug", "bed bugs"],
    expansions: ["bed bug", "infestation", "pest"]
  },
  {
    triggers: ["mold", "mildew"],
    expansions: ["mold", "mildew"]
  },
  {
    triggers: ["heat", "heater", "space heater", "excessive heat"],
    expansions: ["heat", "heater", "heating", "boiler", "radiator", "space heater", "excessive heat"]
  },
  {
    triggers: ["internet", "cable"],
    expansions: ["internet", "cable", "wifi", "wi fi", "wi-fi"]
  },
  {
    triggers: ["common area", "common areas"],
    expansions: ["common area", "common areas"]
  },
  {
    triggers: ["common area", "common areas", "janitorial service"],
    expansions: ["common area", "common areas", "janitorial service", "unclean common areas", "clean common areas"]
  },
  {
    triggers: ["cleaning service", "cleaning services"],
    expansions: ["cleaning service", "cleaning services"]
  },
  {
    triggers: ["stairs", "stair", "handrail"],
    expansions: ["stairs", "stair", "loose stairs", "handrail", "back stairs", "stairwell"]
  },
  {
    triggers: ["porch", "landing"],
    expansions: ["porch", "front porch", "back porch", "landing", "storage room", "porch door"]
  },
  {
    triggers: ["window", "windows"],
    expansions: ["window", "windows", "inoperable windows", "broken windows", "window latch", "window sash", "operable windows"]
  },
  {
    triggers: ["cleaning supply", "cleaning supplies"],
    expansions: ["cleaning supply", "cleaning supplies"]
  },
  {
    triggers: ["co living", "co-living"],
    expansions: ["co living", "co-living", "coliving", "separate tenancy", "separate rental agreements", "individual room", "separately rented", "common areas", "shared kitchen"]
  },
  {
    triggers: ["coin operated", "coin-operated"],
    expansions: ["coin operated", "coin-operated"]
  },
  {
    triggers: ["garage space", "parking space", "garage parking"],
    expansions: ["garage space", "parking space", "garage parking", "carport parking", "tandem space"]
  },
  {
    triggers: ["self employed", "self-employed"],
    expansions: ["self employed", "self-employed"]
  },
  {
    triggers: ["on site resident manager", "on-site resident manager", "onsite resident manager"],
    expansions: ["on site resident manager", "on-site resident manager", "onsite resident manager"]
  },
  {
    triggers: ["homeowner's exemption", "homeowners exemption", "homeowner s exemption"],
    expansions: ["homeowner's exemption", "homeowners exemption", "homeowner s exemption"]
  },
  {
    triggers: ["director's hearing", "directors hearing", "director s hearing"],
    expansions: ["director's hearing", "directors hearing", "director s hearing"]
  },
  {
    triggers: ["lock box", "lockbox"],
    expansions: ["lock box", "lockbox"]
  },
  {
    triggers: ["service animal", "reasonable accommodation"],
    expansions: ["service animal", "reasonable accommodation", "emotional support animal", "support animal"]
  },
  {
    triggers: ["notice of violation", "written notice", "oral notice", "insufficient notice"],
    expansions: ["notice of violation", "written notice", "oral notice", "insufficient notice"]
  },
  {
    triggers: ["section 8", "hud"],
    expansions: ["section 8", "hud"]
  },
  {
    triggers: ["poop"],
    expansions: ["poop", "feces", "dog waste", "animal waste", "human feces", "sewage"]
  }
];

// Evaluated 3-4 times per request (family flag, lexical expansions, whole-word expansions) over 22
// families x surface-variant generation — a pure function of the normalized query, so memoize per
// isolate with the same capped-Map pattern as the regex caches.
const matchedCuratedKeywordFamiliesCache = new Map<string, CuratedKeywordFamily[]>();
const MATCHED_FAMILIES_CACHE_MAX = 500;

function matchedCuratedKeywordFamilies(query: string, precomputed?: { normalizedQuery?: string }): CuratedKeywordFamily[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return [];
  const cached = matchedCuratedKeywordFamiliesCache.get(normalized);
  if (cached) return cached;
  const matches = CURATED_KEYWORD_FAMILIES.filter((family) =>
    family.triggers.some((trigger) => phraseSurfaceVariants(trigger).some((variant) => containsWholeWord(normalized, variant)))
  );
  const result = isAntInfestationQuery(query, { normalizedQuery: normalized })
    ? matches.filter((family) => family.triggers.some((trigger) => /\b(?:ant|ants)\b/.test(normalize(trigger))))
    : matches;
  if (matchedCuratedKeywordFamiliesCache.size >= MATCHED_FAMILIES_CACHE_MAX) matchedCuratedKeywordFamiliesCache.clear();
  matchedCuratedKeywordFamiliesCache.set(normalized, result);
  return result;
}

function curatedKeywordExpansionTerms(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const expansions = new Set<string>();
  for (const family of matchedCuratedKeywordFamilies(query, precomputed)) {
    for (const expansion of family.expansions) {
      for (const variant of phraseSurfaceVariants(expansion)) expansions.add(variant);
      const normalizedExpansion = normalize(expansion || "");
      const tokens = tokenize(normalizedExpansion);
      if (tokens.length === 1 && tokens[0]) {
        for (const variant of tokenSurfaceVariants(tokens[0])) expansions.add(variant);
      }
    }
  }
  return Array.from(expansions).filter(Boolean);
}

function curatedKeywordLexicalExpansionTerms(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const expansions = new Set<string>();
  for (const family of matchedCuratedKeywordFamilies(query, precomputed)) {
    for (const expansion of family.expansions) {
      const normalizedExpansion = normalize(expansion || "");
      if (!normalizedExpansion) continue;
      expansions.add(normalizedExpansion);
      const tokens = tokenize(normalizedExpansion);
      if (tokens.length === 1 && tokens[0]) {
        for (const variant of tokenSurfaceVariants(tokens[0])) expansions.add(variant);
      } else if (normalizedExpansion.includes("-")) {
        expansions.add(normalizedExpansion.replace(/-/g, " "));
      } else if (normalizedExpansion.includes(" ")) {
        expansions.add(normalizedExpansion.replace(/\s+/g, "-"));
      }
    }
  }
  return Array.from(expansions).filter(Boolean);
}

function curatedKeywordWholeWordExpansionTerms(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const expansions = new Set<string>();
  for (const family of matchedCuratedKeywordFamilies(query, precomputed)) {
    for (const expansion of family.expansions) {
      const normalizedExpansion = normalizeWhitespace(normalize(expansion || ""));
      if (normalizedExpansion) expansions.add(normalizedExpansion);
    }
  }
  return Array.from(expansions).filter(Boolean);
}

export function keywordBoundaryGuardTerms(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedQueryContext = { normalizedQuery: normalized };
  const tokens = tokenize(normalized);
  if (tokens.length === 0 || tokens.length > 4) return [];
  const curatedExpansions = curatedKeywordExpansionTerms(query, normalizedQueryContext);
  if (curatedExpansions.length > 0) {
    return uniq([...keywordSurfaceVariants(query, normalizedQueryContext), ...curatedExpansions]).slice(0, 32);
  }
  if (tokens.length === 1) {
    return keywordSurfaceVariants(query, normalizedQueryContext).slice(0, 12);
  }
  if (/[-'’]/.test(String(query || "")) || /(self employed|co living|coin operated|garage space|parking space|garage parking|on ?site resident manager|homeowner.?s exemption|director.?s hearing|lock box)/.test(normalized)) {
    return keywordSurfaceVariants(query, normalizedQueryContext).slice(0, 12);
  }
  return [];
}

function isMarketConditionReasoningQuery(
  context: SearchContext,
  precomputed?: { normalizedQuery?: string; sentenceStyleReasoningQuery?: boolean }
): boolean {
  const normalized = precomputed?.normalizedQuery ?? normalize(context.query || "");
  if (!normalized) return false;
  const sentenceStyleReasoningQuery = precomputed?.sentenceStyleReasoningQuery ?? isSentenceStyleReasoningQuery(context);
  if (!sentenceStyleReasoningQuery) return false;
  return (
    normalized.includes("market conditions") ||
    normalized.includes("new agreement") ||
    normalized.includes("new base rent") ||
    normalized.includes("anniversary date")
  );
}

export function cachedNormalizedChunkText(row: ChunkRow, context: SearchContext): string {
  const cached = context.normalizedRowChunkTextCache?.get(row.chunkId);
  if (cached !== undefined) return cached;
  const normalizedText = normalize(row.chunkText || "");
  if (!context.normalizedRowChunkTextCache) context.normalizedRowChunkTextCache = new Map();
  context.normalizedRowChunkTextCache.set(row.chunkId, normalizedText);
  return normalizedText;
}

export function isKeywordFamilyRecallQuery(query: string, precomputed?: { normalizedQuery?: string }): boolean {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return false;
  return (
    isLiteralKeywordQuery(query, { normalizedQuery: normalized }) ||
    isInfestationAliasQuery(query, { normalizedQuery: normalized }) ||
    matchedCuratedKeywordFamilies(query, { normalizedQuery: normalized }).length > 0
  );
}

export function literalKeywordTokens(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  const rawTokens = tokenize(normalized);
  const lexicalTokens = meaningfulLexicalTokens(normalized);
  if (rawTokens.length !== 1 || lexicalTokens.length !== 1) return [];
  const normalizedTextContext = { normalizedText: normalized };
  if (isInfestationAliasQuery(query, { normalizedQuery: normalized })) return [];
  if (containsWholeWord(normalized, "omi", normalizedTextContext) || containsWholeWord(normalized, "awe", normalizedTextContext)) return [];
  const token = lexicalTokens[0];
  if (!token || token.length < 3) return [];
  return [token];
}

export function isLiteralKeywordQuery(query: string, precomputed?: { normalizedQuery?: string }): boolean {
  return literalKeywordTokens(query, precomputed).length > 0;
}

export function keywordCandidateTerms(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return [];
  const phraseTerms = phrasePriorityLexicalTerms(normalized);

  const curated = curatedKeywordWholeWordExpansionTerms(query, { normalizedQuery: normalized });
  if (curated.length > 0) {
    return uniq([...phraseTerms, ...curated]).filter(Boolean).slice(0, 12);
  }

  const literal = literalKeywordTokens(query, { normalizedQuery: normalized });
  if (literal.length > 0) return literal;

  return phraseTerms.length > 0 ? phraseTerms.slice(0, 10) : meaningfulLexicalTokens(normalized).slice(0, 4);
}

export function lexicalTerms(query: string): string[] {
  const full = String(query || "").slice(0, 260).trim();
  const normalizedFull = normalize(full);
  const normalizedQueryContext = { normalizedQuery: normalizedFull };
  const tokens = meaningfulLexicalTokens(full);
  const surfaceVariants = keywordSurfaceVariants(full, normalizedQueryContext);
  const curatedExpansions = curatedKeywordLexicalExpansionTerms(full, normalizedQueryContext);
  const curatedKeywordFamilyQuery = matchedCuratedKeywordFamilies(full, normalizedQueryContext).length > 0;
  const prioritizedCuratedExpansions = curatedKeywordFamilyQuery
    ? uniq([
        ...curatedExpansions.filter((term) => tokenize(term).length === 1),
        ...curatedExpansions.filter((term) => tokenize(term).length > 1)
      ])
    : curatedExpansions;
  const inferredIssueTerms = inferIssueTerms(full);
  const issueTerms = inferredIssueTerms
    .flatMap((term) => meaningfulLexicalTokens(term))
    .slice(0, 6);
  const judgeTerms = queryReferencesJudge(full).flatMap((judge) => [judge, normalizeJudgeLookupKey(judge)]);
  const fullTokens = tokenize(full);
  const fullIsStopwordOnly = fullTokens.length > 0 && tokens.length === 0 && fullTokens.every((token) => STOPWORD_TOKENS.has(token));
  if (isOwnerMoveInIssueSearch(full)) {
    const ownerMoveTerms = uniq([
      fullIsStopwordOnly ? "" : full,
      "owner move-in",
      "owner move in",
      "relative move-in",
      "relative move in",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "owner occupancy" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupy the unit" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupied the unit" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupancy" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupy" : "",
      /\boccup(?:y|ied|ancy)\b/.test(normalizedFull) ? "occupied" : "",
      /\boccup(?:y|ied|ancy)|resid(?:e|ed|ency)\b/.test(normalizedFull) ? "reside" : "",
      /\boccup(?:y|ied|ancy)|resid(?:e|ed|ency)\b/.test(normalizedFull) ? "resided" : "",
      /\bnotice\b/.test(normalizedFull) ? "notice" : "",
      containsWholeWord(normalizedFull, "omi", { normalizedText: normalizedFull }) ? "omi" : ""
    ].filter(Boolean));
    return ownerMoveTerms.slice(0, fullTokens.length <= 3 ? 4 : 6);
  }
  if (hasWrongfulEvictionPhrase(full)) {
    const wrongfulEvictionTerms = uniq([
      fullIsStopwordOnly ? "" : full,
      "wrongful eviction",
      "report of alleged wrongful eviction",
      "unlawful eviction",
      /\block(?:ed)? out|lockout\b/.test(normalizedFull) ? "lockout" : "",
      /\block(?:ed)? out|lockout\b/.test(normalizedFull) ? "locked out" : "",
      /\bself[-\s]?help\b/.test(normalizedFull) ? "self-help eviction" : "",
      /\bself[-\s]?help\b/.test(normalizedFull) ? "self help eviction" : "",
      /\b(?:repair|repairs\b)/.test(normalizedFull) ? "repair" : "",
      /\b(?:repair|repairs\b)/.test(normalizedFull) ? "repairs" : "",
      /\bcomplain(?:ed|ing)?\b/.test(normalizedFull) ? "complaining" : "",
      containsWholeWord(normalizedFull, "awe", { normalizedText: normalizedFull }) ? "awe" : ""
    ].filter(Boolean));
    return wrongfulEvictionTerms.slice(0, fullTokens.length <= 3 ? 4 : 7);
  }
  const broadIssueQuery = fullTokens.length <= 12 && inferredIssueTerms.length > 0;
  const shortBroadIssueQuery = broadIssueQuery && fullTokens.length <= 3;
  if (broadIssueQuery && !shortBroadIssueQuery) {
    const normalizedInferredIssueTerms = inferredIssueTerms.map((term) => ({
      term,
      normalizedTerm: normalize(term)
    }));
    const presentIssueTerms = normalizedInferredIssueTerms
      .filter(({ normalizedTerm }) => normalizedFull.includes(normalizedTerm))
      .flatMap(({ term }) => meaningfulLexicalTokens(term))
      .slice(0, 3);
    const remainingTokens = tokens.filter((token) => !presentIssueTerms.includes(token));
    return uniq(
      [fullIsStopwordOnly ? "" : full, ...surfaceVariants, ...prioritizedCuratedExpansions, ...presentIssueTerms, ...remainingTokens, ...judgeTerms].filter(Boolean)
    ).slice(0, curatedKeywordFamilyQuery ? 10 : 10);
  }
  const terms = uniq([fullIsStopwordOnly ? "" : full, ...surfaceVariants, ...prioritizedCuratedExpansions, ...issueTerms, ...tokens, ...judgeTerms].filter(Boolean));
  const limit = curatedKeywordFamilyQuery ? 10 : shortBroadIssueQuery ? 6 : broadIssueQuery ? 8 : 8;
  return terms.slice(0, limit);
}

export function wholeWordLexicalTerms(query: string): string[] {
  const full = String(query || "").slice(0, 260).trim();
  const normalizedQueryContext = { normalizedQuery: normalize(full) };
  const surfaceVariants = keywordSurfaceVariants(full, normalizedQueryContext);
  const curatedExpansions = curatedKeywordWholeWordExpansionTerms(full, normalizedQueryContext);
  return uniq([full, ...surfaceVariants, ...curatedExpansions].filter(Boolean)).slice(0, 5);
}

export function inferIssueTerms(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const q = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!q) return [];
  const out: string[] = [];
  const add = (...values: string[]) => out.push(...values);
  const hasOmiAcronym = containsWholeWord(q, "omi");
  const hasAweAcronym = containsWholeWord(q, "awe");

  if (/\b(?:heat|heating|heater|boiler|radiator|hot water\b)/.test(q)) {
    add("heat", "heating", "heater", "boiler", "radiator", "hot water");
  }
  if (/\b(?:cool|cooling|ventilation|air flow|air circulation|overheating|temperature control\b)/.test(q)) {
    add("cooling", "ventilation", "air flow", "air circulation", "overheating", "temperature control");
  }
  if (/\b(?:repair|maintenance|habitability|condition|defect\b)/.test(q)) {
    add("repair", "maintenance", "habitability", "condition", "defect");
  }
  if (/\b(?:mold|leak|water intrusion|plumbing|sewage\b)/.test(q)) {
    add("mold", "leak", "water intrusion", "plumbing", "sewage");
  }
  if (isInfestationAliasQuery(q)) {
    if (isAntInfestationQuery(q)) {
      add("ant infestation", "ants", "ant", "pest", "pests");
    } else {
      add(
        "infestation",
        "infestations",
        "rodent infestation",
        "cockroach infestation",
        "bed bug infestation",
        "rodent",
        "rodents",
        "cockroach",
        "cockroaches",
        "bed bug",
        "bed bugs",
        "mouse",
        "mice",
        "rat",
        "rats",
        "pest",
        "pests"
      );
    }
  }
  if (hasOwnerMoveInPhrase(q) || hasOmiAcronym || /\bowner occupancy\b/.test(q)) {
    add("owner move-in", "relative move-in", "recover possession", "owner occupancy", "occupy the unit", "occupied the unit", "principal place of residence", "tenant in occupancy", "actually resides in a rental unit");
    if (requiresOwnerMoveInFollowThroughSpecificity(q)) {
      add("never moved in", "did not move in", "never occupied", "did not occupy", "never resided", "did not reside");
    }
    if (hasOmiAcronym) add("omi");
  }
  if (hasWrongfulEvictionPhrase(q) || hasAweAcronym) {
    add("wrongful eviction", "report of alleged wrongful eviction", "unlawful eviction", "lockout", "locked out", "eviction");
    if (hasAweAcronym) add("awe");
  }
  if (/\b(?:harassment|retaliation\b)/.test(q)) {
    add("harassment", "harass", "harassed", "harassing", "retaliation", "tenant harassment", "landlord conduct");
  }
  if (isAccommodationQuery(q)) {
    add(
      "reasonable accommodation",
      "accommodation request",
      "service animal",
      "support animal",
      "emotional support animal",
      "assistance animal",
      "disability accommodation"
    );
  }
  if (isHomeownersExemptionQuery(q)) {
    add(
      "homeowner's exemption",
      "homeowners exemption",
      "homeowner s exemption",
      "property tax exemption",
      "principal place of residence",
      "principal residence"
    );
  }
  if (isCameraPrivacyQuery(q)) {
    add("camera privacy", "security camera", "security cameras", "surveillance", "invasion of privacy", "video monitoring");
  }
  if (isPackageSecurityQuery(q)) {
    add("package security", "package theft", "stolen packages", "mail theft", "mailroom security", "delivery security");
  }
  if (isDogQuery(q)) {
    add("dog", "dogs", "dog-free building", "pet policy", "no pets", "service animal", "dog park", "housing service");
  }
  if (isIntercomQuery(q)) {
    add("intercom", "broken intercom", "door buzzer", "entry system", "security gate", "buzz in", "housing service");
  }
  if (isGarageSpaceQuery(q)) {
    add("garage space", "parking space", "garage parking", "carport parking", "tandem space", "parking", "housing service");
  }
  if (isCommonAreasQuery(q)) {
    add("common areas", "common area", "janitorial service", "unclean common areas", "clean common areas", "hallways", "housing service");
  }
  if (isStairsQuery(q)) {
    add("stairs", "loose stairs", "handrail", "back stairs", "stairwell", "fall hazard", "housing service");
  }
  if (isPorchQuery(q)) {
    add("porch", "front porch", "back porch", "landing", "storage room", "porch door", "housing service");
  }
  if (isWindowsQuery(q)) {
    add("windows", "window", "inoperable windows", "broken windows", "window latch", "window sash", "operable windows", "housing service");
  }
  if (isCollegeQuery(q)) {
    add("college", "school", "student housing", "school breaks", "temporary absence", "attend college", "permanent residence", "return");
  }
  if (isSelfEmployedQuery(q)) {
    add("self employed", "self-employed", "1099", "schedule c", "tax return", "principal residence", "subject unit address");
  }
  if (isAdjudicatedQuery(q)) {
    add("adjudicated", "adjudicate", "already decided", "previously decided", "precluded", "preclusion", "state court");
  }
  if (isSocialMediaQuery(q)) {
    add("social media", "facebook", "instagram", "nextdoor", "facebook marketplace", "posted online", "roommate search", "principal residence");
  }
  if (isCaregiverQuery(q)) {
    add("caregiver", "caregiving", "primary caregiver", "care for", "care for mother", "care for father", "principal residence", "return");
  }
  if (isPoopQuery(q)) {
    add("poop", "feces", "faeces", "dog waste", "animal waste", "human feces", "sewage", "contamination");
  }
  if (isMootQuery(q)) {
    add("moot", "rendered moot", "null and void", "rescinded", "administratively dismissed", "withdrawn");
  }
  if (isRemoteWorkQuery(q)) {
    add("remote work", "work from home", "working from home", "unable to work from home", "construction noise", "utility outage", "peaceful enjoyment");
  }
  if (isDivorceQuery(q)) {
    add("divorce", "divorced", "separated", "separation", "spouse moved out", "marital issues", "live separately", "residence");
  }
  if (isCoLivingQuery(q)) {
    add("co-living", "coliving", "separate tenancy", "separate rental agreements", "individual room", "separately rented", "shared kitchen", "common areas");
  }
  if (isSection8Query(q)) {
    add("section 8", "hud", "voucher", "housing choice voucher", "subsidized tenancy", "subsidized tenant");
  }
  if (isUnlawfulDetainerQuery(q)) {
    add("unlawful detainer", "notice to quit", "three day notice", "detainer action", "eviction lawsuit", "eviction action", "eviction");
  }
  if (isSection8UnlawfulDetainerQuery(q)) {
    add("section 8 eviction", "section 8 eviction action", "voucher eviction", "housing choice voucher eviction");
  }

  return uniq(out);
}

function primaryIssueSignals(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return [];
  const signals = new Set<string>();

  if (/\bmold\b/.test(normalized)) signals.add("mold");
  if (/\b(?:heat|heating|heater|boiler|radiator\b)/.test(normalized)) signals.add("heat");
  if (/\bhot water\b/.test(normalized)) signals.add("hot water");
  if (/\brodent\b/.test(normalized)) signals.add("rodent");
  if (/\bcockroach\b/.test(normalized)) signals.add("cockroach");
  if (/\b(?:bed bug|bed bugs\b)/.test(normalized)) signals.add("bed bug");
  if (isInfestationAliasQuery(normalized)) signals.add("infestation");
  if (hasOwnerMoveInPhrase(normalized, { normalizedText: normalized }) || containsWholeWord(normalized, "omi", { normalizedText: normalized }) || /\bowner occupancy\b/.test(normalized)) signals.add("owner move in");
  if (hasWrongfulEvictionPhrase(normalized, { normalizedText: normalized }) || containsWholeWord(normalized, "awe", { normalizedText: normalized })) signals.add("wrongful eviction");
  if (/\block(?:ed)? out|lockout|changed locks?|denied access|self[-\s]?help eviction|shut off utilities\b/.test(normalized)) {
    signals.add("lockout");
  }
  if (/\b(?:harassment|retaliation\b)/.test(normalized)) signals.add("harassment");
  if (/\bbuyout\b/.test(normalized)) signals.add("buyout");
  if (isBuyoutPressureQuery(normalized)) {
    signals.add("harassment");
    signals.add("coercion");
    signals.add("vacate");
    signals.add("intimidation");
  }
  if (/\bcapital improvement\b/.test(normalized)) signals.add("capital improvement");
  if (isAccommodationQuery(normalized)) {
    signals.add("reasonable accommodation");
    signals.add("service animal");
  }
  if (isHomeownersExemptionQuery(normalized)) {
    signals.add("homeowner's exemption");
    signals.add("principal residence");
  }
  if (isCameraPrivacyQuery(normalized)) {
    signals.add("camera privacy");
    signals.add("privacy");
    signals.add("camera");
  }
  if (isPackageSecurityQuery(normalized)) {
    signals.add("package security");
    signals.add("package theft");
    signals.add("mail theft");
  }
  if (isDogQuery(normalized)) {
    signals.add("dog");
    signals.add("pet policy");
    signals.add("dog-free building");
  }
  if (isCollegeQuery(normalized)) {
    signals.add("college");
    signals.add("student housing");
    signals.add("temporary absence");
  }
  if (isSelfEmployedQuery(normalized)) {
    signals.add("self employed");
    signals.add("1099");
    signals.add("tax return");
  }
  if (isAdjudicatedQuery(normalized)) {
    signals.add("adjudicated");
    signals.add("already decided");
    signals.add("precluded");
  }
  if (isSocialMediaQuery(normalized)) {
    signals.add("social media");
    signals.add("facebook");
    signals.add("instagram");
  }
  if (isCaregiverQuery(normalized)) {
    signals.add("caregiver");
    signals.add("caregiving");
    signals.add("primary caregiver");
  }
  if (isPoopQuery(normalized)) {
    signals.add("poop");
    signals.add("feces");
    signals.add("animal waste");
  }
  if (isMootQuery(normalized)) {
    signals.add("moot");
    signals.add("null and void");
    signals.add("rescinded");
  }
  if (isRemoteWorkQuery(normalized)) {
    signals.add("remote work");
    signals.add("work from home");
    signals.add("construction noise");
  }
  if (isDivorceQuery(normalized)) {
    signals.add("divorce");
    signals.add("separation");
    signals.add("spouse");
  }
  if (isIntercomQuery(normalized)) {
    signals.add("intercom");
    signals.add("door buzzer");
    signals.add("entry system");
  }
  if (isGarageSpaceQuery(normalized)) {
    signals.add("garage space");
    signals.add("parking space");
    signals.add("garage parking");
  }
  if (isCommonAreasQuery(normalized)) {
    signals.add("common areas");
    signals.add("common area");
    signals.add("janitorial service");
  }
  if (isStairsQuery(normalized)) {
    signals.add("stairs");
    signals.add("handrail");
    signals.add("stairwell");
  }
  if (isPorchQuery(normalized)) {
    signals.add("porch");
    signals.add("landing");
    signals.add("storage room");
  }
  if (isWindowsQuery(normalized)) {
    signals.add("windows");
    signals.add("window");
    signals.add("window latch");
    signals.add("window sash");
  }
  if (isCoLivingQuery(normalized)) {
    signals.add("co-living");
    signals.add("common areas");
    signals.add("individual room");
  }
  if (isSection8Query(normalized)) {
    signals.add("section 8");
  }
  if (isUnlawfulDetainerQuery(normalized)) {
    signals.add("unlawful detainer");
  }

  return Array.from(signals);
}

export function sentenceIssueAnchorTerms(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return [];

  const anchors = new Set<string>();
  if (/\bmold\b/.test(normalized) && /\b(?:repair|repairs\b)/.test(normalized)) {
    anchors.add("repair");
    anchors.add("repairs");
    anchors.add("failed to repair");
    anchors.add("reported");
  }
  if (hasOwnerMoveInPhrase(normalized, { normalizedText: normalized }) || containsWholeWord(normalized, "omi", { normalizedText: normalized })) {
    if (/\boccup(?:y|ied|ancy)|resid(?:e|ed|ency)\b/.test(normalized)) {
      anchors.add("owner occupancy");
      anchors.add("occupancy");
      anchors.add("occupy");
      anchors.add("occupied");
      anchors.add("reside");
      anchors.add("resided");
      anchors.add("principal place of residence");
      anchors.add("tenant in occupancy");
    }
    if (/\bnotice\b/.test(normalized)) anchors.add("notice");
    if (requiresOwnerMoveInFollowThroughSpecificity(normalized)) {
      anchors.add("never moved in");
      anchors.add("did not move in");
      anchors.add("never occupied");
      anchors.add("did not occupy");
      anchors.add("never resided");
      anchors.add("did not reside");
    }
  }
  if (hasWrongfulEvictionPhrase(normalized, { normalizedText: normalized }) || containsWholeWord(normalized, "awe", { normalizedText: normalized })) {
    if (/\block(?:ed)? out|lockout\b/.test(normalized)) {
      anchors.add("lockout");
      anchors.add("locked out");
    }
    if (/\bchanged locks?\b/.test(normalized)) anchors.add("changed locks");
    if (/\bdenied access\b/.test(normalized)) anchors.add("denied access");
    if (/\b(?:shut off utilities|utility shutoff|utilities shut off\b)/.test(normalized)) {
      anchors.add("shut off utilities");
      anchors.add("utility shutoff");
    }
    if (/\bself[-\s]?help\b/.test(normalized)) {
      anchors.add("self-help eviction");
      anchors.add("self help eviction");
    }
    if (/\b(?:repair|repairs\b)/.test(normalized)) {
      anchors.add("repair");
      anchors.add("repairs");
    }
    if (/\bcomplain(?:ed|ing)?\b/.test(normalized)) {
      anchors.add("complain");
      anchors.add("complaining");
    }
    if (/\bnotice\b/.test(normalized)) anchors.add("notice");
  }
  if (/\b(?:harassment|retaliation\b)/.test(normalized)) {
    anchors.add("harassment");
    anchors.add("retaliation");
    if (/\b(?:notice|notices\b)/.test(normalized)) anchors.add("notice");
    if (/\bentr(?:y|ies)\b/.test(normalized)) {
      anchors.add("entry");
      anchors.add("entries");
    }
  }
  if (isBuyoutPressureQuery(normalized)) {
    anchors.add("harassment");
    anchors.add("harass");
    anchors.add("harassing");
    anchors.add("pressure");
    anchors.add("pressured");
    anchors.add("pressuring");
    anchors.add("coerce");
    anchors.add("coerced");
    anchors.add("coercion");
    anchors.add("coercive");
    anchors.add("threat");
    anchors.add("threaten");
    anchors.add("threatened");
    anchors.add("intimidation");
    anchors.add("fraud");
    anchors.add("vacate");
    anchors.add("payment to vacate");
    anchors.add("payments to vacate");
    anchors.add("offer to vacate");
    anchors.add("offers to vacate");
    anchors.add("offer of payment to vacate");
    anchors.add("offers of payment to vacate");
    anchors.add("threats or intimidation");
  }
  if (
    /\b(?:mold|hot water|heat|heating|heater|boiler|radiator|rodent|cockroach|bed bug|ventilation|leak|water intrusion|plumbing|sewage\b)/.test(
      normalized
    )
  ) {
    if (/\breport(?:ed|ing)?|complain(?:ed|ing)?|notified|notice\b/.test(normalized)) {
      anchors.add("reported");
      anchors.add("complained");
      anchors.add("notified");
      anchors.add("notice");
    }
    if (/\b(?:repair|repairs|restore|restored|service|services\b)/.test(normalized)) {
      anchors.add("repair");
      anchors.add("repairs");
      anchors.add("failed to repair");
      anchors.add("restore service");
      anchors.add("failed to restore service");
      anchors.add("service restoration");
    }
  }
  if (isAccommodationQuery(normalized)) {
    anchors.add("reasonable accommodation");
    anchors.add("service animal");
    anchors.add("support animal");
    anchors.add("emotional support animal");
    if (/\brequest(?:ed|ing)?|ask(?:ed|ing)? for\b/.test(normalized)) {
      anchors.add("requested");
      anchors.add("request");
    }
    if (/\bdoctor|medical|disab(?:ility|led)\b/.test(normalized) || /\bservice animal\b/.test(normalized)) {
      anchors.add("medical provider");
      anchors.add("doctor");
      anchors.add("disability");
    }
  }
  if (isCameraPrivacyQuery(normalized)) {
    anchors.add("camera");
    anchors.add("cameras");
    anchors.add("security camera");
    anchors.add("surveillance");
    anchors.add("privacy");
    anchors.add("invasion of privacy");
    anchors.add("video monitoring");
  }
  if (isPackageSecurityQuery(normalized)) {
    anchors.add("package security");
    anchors.add("package theft");
    anchors.add("stolen packages");
    anchors.add("mail theft");
    anchors.add("mailroom security");
    anchors.add("housing service");
  }
  if (isDogQuery(normalized)) {
    anchors.add("dog");
    anchors.add("dogs");
    anchors.add("dog-free building");
    anchors.add("no pets");
    anchors.add("pet policy");
    anchors.add("dog park");
    anchors.add("service animal");
    anchors.add("housing service");
  }
  if (isCollegeQuery(normalized)) {
    anchors.add("college");
    anchors.add("attend college");
    anchors.add("student housing");
    anchors.add("school breaks");
    anchors.add("temporary absence");
    anchors.add("return to live in the unit");
    anchors.add("permanent residence");
  }
  if (isSelfEmployedQuery(normalized)) {
    anchors.add("self employed");
    anchors.add("self-employed");
    anchors.add("1099");
    anchors.add("tax return");
    anchors.add("tax returns");
    anchors.add("subject unit as his address");
    anchors.add("files his tax returns");
    anchors.add("principal residence");
  }
  if (isAdjudicatedQuery(normalized)) {
    anchors.add("adjudicated");
    anchors.add("adjudicate");
    anchors.add("already decided");
    anchors.add("previously decided");
    anchors.add("precluded");
    anchors.add("preclusion");
    anchors.add("state court");
  }
  if (isSocialMediaQuery(normalized)) {
    anchors.add("social media");
    anchors.add("facebook");
    anchors.add("instagram");
    anchors.add("nextdoor");
    anchors.add("facebook marketplace");
    anchors.add("posted online");
    anchors.add("principal residence");
    anchors.add("roommate search");
  }
  if (isCaregiverQuery(normalized)) {
    anchors.add("caregiver");
    anchors.add("caregiving");
    anchors.add("primary caregiver");
    anchors.add("care for");
    anchors.add("return to live in the unit");
    anchors.add("principal residence");
    anchors.add("family assistance");
  }
  if (isPoopQuery(normalized)) {
    anchors.add("poop");
    anchors.add("feces");
    anchors.add("faeces");
    anchors.add("dog waste");
    anchors.add("animal waste");
    anchors.add("human feces");
    anchors.add("sewage");
    anchors.add("contamination");
  }
  if (isMootQuery(normalized)) {
    anchors.add("moot");
    anchors.add("rendered moot");
    anchors.add("null and void");
    anchors.add("rescinded");
    anchors.add("administratively dismissed");
    anchors.add("withdrawn");
  }
  if (isRemoteWorkQuery(normalized)) {
    anchors.add("remote work");
    anchors.add("work from home");
    anchors.add("working from home");
    anchors.add("unable to work from home");
    anchors.add("construction noise");
    anchors.add("power was turned off");
    anchors.add("telephone conversations");
    anchors.add("quiet enjoyment");
  }
  if (isDivorceQuery(normalized)) {
    anchors.add("divorce");
    anchors.add("divorced");
    anchors.add("separated");
    anchors.add("separation");
    anchors.add("spouse moved out");
    anchors.add("marital issues");
    anchors.add("live separately");
    anchors.add("residence");
  }
  if (isIntercomQuery(normalized)) {
    anchors.add("intercom");
    anchors.add("broken intercom");
    anchors.add("door buzzer");
    anchors.add("entry system");
    anchors.add("security gate");
    anchors.add("housing service");
  }
  if (isGarageSpaceQuery(normalized)) {
    anchors.add("garage space");
    anchors.add("parking space");
    anchors.add("garage parking");
    anchors.add("carport parking");
    anchors.add("tandem space");
    anchors.add("housing service");
  }
  if (isCommonAreasQuery(normalized)) {
    anchors.add("common areas");
    anchors.add("common area");
    anchors.add("janitorial service");
    anchors.add("unclean common areas");
    anchors.add("clean common areas");
    anchors.add("housing service");
  }
  if (isStairsQuery(normalized)) {
    anchors.add("stairs");
    anchors.add("loose stairs");
    anchors.add("handrail");
    anchors.add("back stairs");
    anchors.add("stairwell");
    anchors.add("housing service");
  }
  if (isPorchQuery(normalized)) {
    anchors.add("porch");
    anchors.add("front porch");
    anchors.add("back porch");
    anchors.add("landing");
    anchors.add("storage room");
    anchors.add("porch door");
    anchors.add("housing service");
  }
  if (isWindowsQuery(normalized)) {
    anchors.add("windows");
    anchors.add("window");
    anchors.add("inoperable windows");
    anchors.add("broken windows");
    anchors.add("window latch");
    anchors.add("window sash");
    anchors.add("operable windows");
    anchors.add("housing service");
  }
  if (isCoLivingQuery(normalized)) {
    anchors.add("separate tenancy");
    anchors.add("separate rental agreement");
    anchors.add("separate rental agreements");
    anchors.add("individual room");
    anchors.add("separately rented");
    anchors.add("common areas");
    anchors.add("shared kitchen");
  }
  if (isSection8Query(normalized)) {
    anchors.add("section 8");
    anchors.add("hud");
    anchors.add("voucher");
    anchors.add("housing choice voucher");
  }
  if (isUnlawfulDetainerQuery(normalized)) {
    anchors.add("unlawful detainer");
    anchors.add("notice to quit");
    anchors.add("three day notice");
    anchors.add("detainer");
    anchors.add("eviction action");
    anchors.add("eviction");
  }
  if (isSection8UnlawfulDetainerQuery(normalized)) {
    anchors.add("section 8 eviction");
    anchors.add("section 8 eviction action");
    anchors.add("voucher eviction");
  }

  return Array.from(anchors);
}

export function sentenceSecondaryFactTokens(query: string, precomputed?: { issueTerms?: string[]; normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return [];
  const normalizedQueryContext = { normalizedQuery: normalized };
  if (isBuyoutPressureQuery(query, normalizedQueryContext)) {
    return ["payment to vacate", "payments to vacate", "vacate", "intimidation", "coercion"];
  }

  const issueTokenSet = new Set(
    (precomputed?.issueTerms ?? inferIssueTerms(query, normalizedQueryContext))
      .flatMap((term) => meaningfulPhraseTokens(term))
      .map((token) => normalize(token))
  );

  const dropTokens = new Set([
    "tenant",
    "landlord",
    "unit",
    "eviction",
    "owner",
    "move",
    "wrongful",
    "mold",
    "repair",
    "repairs",
    "notice",
    "service",
    "services"
  ]);

  return meaningfulPhraseTokens(query)
    .filter((token) => {
      const normalizedToken = normalize(token);
      return normalizedToken && !issueTokenSet.has(normalizedToken) && !dropTokens.has(normalizedToken);
    })
    .slice(0, 6);
}

export function textContainsIssueSignal(text: string, signal: string, precomputed?: { normalizedText?: string; normalizedSignal?: string }): boolean {
  const normalizedText = precomputed?.normalizedText ?? normalize(text);
  const normalizedSignal = precomputed?.normalizedSignal ?? normalize(signal);
  if (!normalizedText || !normalizedSignal) return false;
  const normalizedTextContext = { normalizedText };
  if (normalizedSignal === "reasonable accommodation") {
    return hasAccommodationContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "service animal") {
    return /service animal|support animal|emotional support animal|assistance animal/.test(normalizedText);
  }
  if (normalizedSignal === "camera privacy") {
    return hasCameraPrivacyContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "package security") {
    return hasPackageSecurityContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "package theft") {
    return hasPackageSecurityContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "mail theft") {
    return hasPackageSecurityContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "intercom") {
    return hasIntercomContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "door buzzer") {
    return hasIntercomContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "entry system") {
    return hasIntercomContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "garage space") {
    return hasGarageSpaceContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "parking space") {
    return hasGarageSpaceContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "garage parking") {
    return hasGarageSpaceContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "common areas") {
    return hasCommonAreasContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "common area") {
    return hasCommonAreasContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "janitorial service") {
    return hasCommonAreasContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "stairs") {
    return hasStairsContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "handrail") {
    return hasStairsContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "stairwell") {
    return hasStairsContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "porch") {
    return hasPorchContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "landing") {
    return hasPorchContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "storage room") {
    return hasPorchContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "windows") {
    return hasWindowsContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "window") {
    return hasWindowsContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "window latch") {
    return hasWindowsContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "window sash") {
    return hasWindowsContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "co-living") {
    return hasCoLivingContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "homeowner's exemption") {
    return hasHomeownersExemptionContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "section 8") {
    return hasSection8Context(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "unlawful detainer") {
    return hasUnlawfulDetainerContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "owner move in") {
    return (
      hasOwnerMoveInPhrase(normalizedText, { normalizedText }) ||
      normalizedText.includes("owner occupancy") ||
      normalizedText.includes("occupy the unit") ||
      normalizedText.includes("occupied the unit")
    );
  }
  if (normalizedSignal === "wrongful eviction") {
    return hasWrongfulEvictionContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "lockout") {
    return hasWrongfulEvictionLockoutContext(normalizedText, normalizedTextContext);
  }
  if (normalizedSignal === "infestation") {
    return /\b(?:infestation|infestations|rodent|rodents|cockroach|cockroaches|roach|roaches|bed bug|bed bugs|mouse|mice|rat|rats|pest|pests\b)/.test(
      normalizedText
    );
  }
  return containsWholeWord(normalizedText, normalizedSignal) || normalizedText.includes(normalizedSignal);
}

function inferProceduralTerms(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const q = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!q) return [];
  const out: string[] = [];
  const add = (...values: string[]) => out.push(...values);

  if (/\b(?:notice|service|served|mail|mailing|posting\b)/.test(q)) {
    add("notice", "service", "served", "mail", "mailing", "posting", "repair request", "work order", "written notice");
  }
  if (/\b(?:hearing|continuance|appearance|filing|deadline|extension\b)/.test(q)) {
    add("hearing", "continuance", "appearance", "filing", "deadline", "extension");
  }
  if (/\b(?:harassment|retaliation\b)/.test(q)) {
    add("tenant petition", "petition", "claim", "retaliation", "harassment", "section 37.10b");
  }

  return uniq(out);
}

function isJudgeDrivenQuery(
  query: string,
  precomputed?: { referencedJudges?: string[]; issueTerms?: string[]; proceduralTerms?: string[] }
): boolean {
  const referencedJudges = precomputed?.referencedJudges ?? queryReferencesJudge(query);
  const issueTerms = precomputed?.issueTerms ?? inferIssueTerms(query);
  const proceduralTerms = precomputed?.proceduralTerms ?? inferProceduralTerms(query);
  return referencedJudges.length > 0 && issueTerms.length === 0 && proceduralTerms.length === 0;
}

// True for search sub-query errors that should DEGRADE the affected recall/scope stage to empty rather
// than fail the whole request (every caller returns [] / skips on a true result — it does not actually
// retry). Covers transient D1 errors (1031 / fetch failed) and SQLite/D1 hard resource limits — most
// importantly "too many SQL variables", which a sufficiently broad recall query (e.g. a large curated
// keyword family combined with a structured filter) can hit. Degrading that one stage still lets the
// other recall paths answer the query instead of returning an HTTP 400. (SEARCH-05)
export function isRetryableSearchError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error || "");
  return (
    message.includes("error code: 1031") ||
    message.includes("fetch failed") ||
    /too many SQL variables/i.test(message)
  );
}

export function isMissingDocumentFacetTableError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error || "");
  return /no such table:\s*document_(?:index_codes|rules_sections|ordinance_sections)/i.test(message);
}

export function normalizeChunkTypeLabel(value: string): string {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function inferQueryIntent(context: SearchContext, precomputed?: { normalizedQuery?: string }): QueryIntent {
  if (context.queryType === "citation_lookup" || context.queryType === "rules_ordinance" || context.queryType === "index_code") {
    return "citation";
  }
  const q = precomputed?.normalizedQuery ?? normalize(context.query || "");
  if (!q) return "unknown";
  if (/ordinance|rule|rules|authority|section|citation/.test(q)) return "authority";
  if (/findings?|credibility|evidence|fact/.test(q)) return "findings";
  if (/procedural|history|hearing|notice|continuance|appearance/.test(q)) return "procedural";
  if (/issue|holding|disposition|order|decision/.test(q)) return "disposition";
  if (/analysis|reasoning|legal standard|application|conclusions?/.test(q)) return "analysis";
  if (/compare|comparison|across decisions|prior decisions/.test(q)) return "comparative";
  return "unknown";
}

export function isConclusionsLikeSectionLabel(sectionLabel: string): boolean {
  const raw = String(sectionLabel || "");
  const normalized = normalizeChunkTypeLabel(raw);
  return (
    /conclusions? of law/i.test(raw) ||
    normalized === "conclusions_of_law" ||
    normalized === "authority_discussion" ||
    normalized === "analysis_reasoning"
  );
}

export function isFindingsLikeSectionLabel(sectionLabel: string): boolean {
  const raw = String(sectionLabel || "");
  const normalized = normalizeChunkTypeLabel(raw);
  return (
    /findings? of fact/i.test(raw) ||
    normalized === "findings_of_fact" ||
    normalized === "findings" ||
    normalized === "factual_findings" ||
    normalized === "credibility_findings"
  );
}

export function isSupportingFactSectionLabel(sectionLabel: string): boolean {
  const raw = String(sectionLabel || "");
  if (isFindingsLikeSectionLabel(raw)) return true;
  return /summary\s+of\s+the\s+evidence|factual\s+background|background|history|evidence|testimony/i.test(raw);
}

function isSentenceStyleReasoningQuery(context: SearchContext): boolean {
  const raw = String(context.query || "").trim();
  const normalized = normalize(raw);
  if (!normalized) return false;
  if (
    context.queryType === "citation_lookup" ||
    context.queryType === "party_name" ||
    context.queryType === "rules_ordinance" ||
    context.queryType === "index_code"
  ) {
    return false;
  }

  const tokens = tokenize(raw).filter((token) => token.length > 2);
  if (tokens.length >= 9) return true;
  if (tokens.length >= 7) return true;
  if (raw.length >= 70) return true;
  if (/[,:;]/.test(raw) && tokens.length >= 6) return true;

  return /\b(?:burden of proving|failed to prove|did not establish|based on all of the evidence|credible testimony|substantial decrease(?: in housing services)?|good faith efforts|not justified|is justified|not entitled|entitled to|rent reduction|corresponding rent reduction|code violation)\b/.test(
    normalized
  );
}

function isStructuralIntent(context: SearchContext, precomputed?: { normalizedQuery?: string }): boolean {
  const q = precomputed?.normalizedQuery ?? normalize(context.query || "");
  if (!q) return false;
  if (context.queryType === "party_name") return true;
  return /appearance|appearances|caption|questions presented|parties/.test(q);
}

export function buildSearchScope(
  parsed: SearchRequest,
  corpusMode: SearchRequest["corpusMode"],
  options: SearchScopeOptions = {}
) {
  const hasActiveRetrievalChunkClause =
    "EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1)";
  const hasBasicChunkedDecisionClause =
    "EXISTS (SELECT 1 FROM document_chunks c_basic WHERE c_basic.document_id = d.id) AND d.source_r2_key IS NOT NULL AND COALESCE(d.title, '') != ''";
  const clauses: string[] = [
    corpusMode === "trusted_plus_provisional"
      ? `(d.file_type != 'decision_docx' OR ${hasBasicChunkedDecisionClause} OR ${hasActiveRetrievalChunkClause})`
      : `(d.file_type != 'decision_docx' OR (d.approved_at IS NOT NULL AND ${hasBasicChunkedDecisionClause}) OR ${hasActiveRetrievalChunkClause})`,
    "d.rejected_at IS NULL"
  ];
  const params: Array<string | number> = [];

  if (parsed.filters.jurisdiction) {
    clauses.push("d.jurisdiction = ?");
    params.push(parsed.filters.jurisdiction);
  }

  if (parsed.filters.documentId) {
    clauses.push("d.id = ?");
    params.push(parsed.filters.documentId);
  }

  if (parsed.filters.fileType) {
    clauses.push("d.file_type = ?");
    params.push(parsed.filters.fileType);
  } else {
    // Product search is decision-first by default; other file types remain available via explicit filter.
    clauses.push("d.file_type = 'decision_docx'");
  }

  const indexCodeFilterContext = buildIndexCodeFilterContext(parsed.filters, { includeGenericDhsFamilyAlias: false });
  const useSoftIndexCodeScope = Boolean(options.useSoftIndexCodeScope);
  if (indexCodeFilterContext.requestedCodes.length > 0 && !useSoftIndexCodeScope) {
    const compatibilityClauses: string[] = [];

    if (indexCodeFilterContext.requestedCodes.length > 1) {
      compatibilityClauses.push(
        `(${buildExactIndexCodeIntersectionClauses(indexCodeFilterContext.requestedCodes, params, {
          includeGenericDhsFamilyAlias: false
        }).join(" AND ")})`
      );
    } else {
      const directIndexCodeValues = uniq([...indexCodeFilterContext.requestedCodes, ...indexCodeFilterContext.legacyCodeAliases]).filter(Boolean);

      if (directIndexCodeValues.length > 0) {
        compatibilityClauses.push(buildDirectIndexCodeCompatibilityClause(directIndexCodeValues));
        bindIndexCodeMatchValues(params, directIndexCodeValues);
      }

      if (indexCodeFilterContext.relatedRulesSections.length > 0) {
        compatibilityClauses.push(buildReferenceSectionCompatibilityClause("rules_section", indexCodeFilterContext.relatedRulesSections));
        bindReferenceSectionMatchValues(params, "rules_section", indexCodeFilterContext.relatedRulesSections);
      }

      if (indexCodeFilterContext.relatedOrdinanceSections.length > 0) {
        compatibilityClauses.push(buildReferenceSectionCompatibilityClause("ordinance_section", indexCodeFilterContext.relatedOrdinanceSections));
        bindReferenceSectionMatchValues(params, "ordinance_section", indexCodeFilterContext.relatedOrdinanceSections);
      }
    }

    // Phrase-based compatibility is intentionally applied later during reranking/filtering.
    // Keeping it out of the SQL scope avoids expensive chunk-text EXISTS scans that can timeout
    // on broad local searches while still allowing phrase evidence to drive ranking.
    if (compatibilityClauses.length > 0) {
      clauses.push(`(${compatibilityClauses.join(" OR ")})`);
    }
  }

  if (parsed.filters.rulesSection) {
    clauses.push(buildReferenceSectionCompatibilityClause("rules_section", [parsed.filters.rulesSection]));
    bindReferenceSectionMatchValues(params, "rules_section", [parsed.filters.rulesSection]);
  }

  if (parsed.filters.ordinanceSection) {
    clauses.push(buildReferenceSectionCompatibilityClause("ordinance_section", [parsed.filters.ordinanceSection]));
    bindReferenceSectionMatchValues(params, "ordinance_section", [parsed.filters.ordinanceSection]);
  }

  if (parsed.filters.partyName) {
    clauses.push("instr(lower(d.title), lower(?)) > 0");
    params.push(parsed.filters.partyName);
  }

  const judgeFilters = requestedJudgeFilters(parsed.filters);
  if (judgeFilters.length > 0) {
    clauses.push(`(${judgeFilters.map(() => "lower(coalesce(d.author_name, '')) = lower(?)").join(" OR ")})`);
    params.push(...judgeFilters);
  }

  if (parsed.filters.fromDate) {
    clauses.push("(d.decision_date IS NOT NULL AND d.decision_date >= ?)");
    params.push(parsed.filters.fromDate);
  }

  if (parsed.filters.toDate) {
    clauses.push("(d.decision_date IS NOT NULL AND d.decision_date <= ?)");
    params.push(parsed.filters.toDate);
  }

  if (parsed.filters.approvedOnly) {
    clauses.push(`(d.file_type != 'decision_docx' OR d.approved_at IS NOT NULL OR ${hasActiveRetrievalChunkClause})`);
  }

  return { where: `WHERE ${clauses.join(" AND ")}`, params };
}

export function activeStructuredFilterKinds(
  filters: SearchRequest["filters"],
  precomputed?: { requestedJudgeFilters?: string[]; requestedIndexCodeFilters?: string[] }
): string[] {
  const kinds: string[] = [];
  if ((precomputed?.requestedJudgeFilters ?? requestedJudgeFilters(filters)).length > 0) kinds.push("judge");
  if ((precomputed?.requestedIndexCodeFilters ?? requestedIndexCodeFilters(filters)).length > 0) kinds.push("index_code");
  if (filters.rulesSection) kinds.push("rules_section");
  if (filters.ordinanceSection) kinds.push("ordinance_section");
  if (filters.partyName) kinds.push("party_name");
  if (filters.fromDate || filters.toDate) kinds.push("date_range");
  return kinds;
}

function isShortBroadIssueSearch(parsed: SearchRequest, precomputed?: { issueTerms?: string[] }): boolean {
  if (isKeywordFamilyRecallQuery(parsed.query || "")) return false;
  const tokens = tokenize(parsed.query || "");
  if (tokens.length === 0 || tokens.length > 3) return false;
  return (precomputed?.issueTerms ?? inferIssueTerms(parsed.query || "")).length > 0;
}

function isIssueGuidedSearch(parsed: SearchRequest, precomputed?: { issueTerms?: string[] }): boolean {
  if (isKeywordFamilyRecallQuery(parsed.query || "")) return false;
  const tokens = tokenize(parsed.query || "");
  if (tokens.length === 0 || tokens.length > 16) return false;
  return (precomputed?.issueTerms ?? inferIssueTerms(parsed.query || "")).length > 0;
}

export function issueQueryIndexCodeHints(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return [];
  const hints = new Set<string>();

  if (/\b(?:heat|heating|heater|boiler|radiator\b)/.test(normalized)) {
    hints.add("G49");
    hints.add("G50");
  }
  if (/\bhot water\b/.test(normalized)) {
    hints.add("G52");
    hints.add("G53");
  }
  if (/\b(?:mold|leak|water intrusion|plumbing|sewage\b)/.test(normalized)) {
    hints.add("G64");
  }
  if (/\bcockroach\b/.test(normalized)) {
    hints.add("G44");
    hints.add("G54");
  }
  if (/\brodent\b/.test(normalized)) {
    hints.add("G76");
    hints.add("G54");
  }
  if (/\b(?:bed bug|bed bugs\b)/.test(normalized)) {
    hints.add("G40.1");
    hints.add("G54");
  }
  if (isInfestationAliasQuery(query, { normalizedQuery: normalized })) {
    hints.add("G54");
    hints.add("G44");
    hints.add("G76");
    hints.add("G40.1");
  }
  if (/\b(?:rent reduction|decrease in services|housing services\b)/.test(normalized)) {
    hints.add("G27");
    hints.add("G28");
  }

  return Array.from(hints);
}

export function issueQueryPhraseHints(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return [];
  const normalizedQueryContext = { normalizedQuery: normalized };
  const hints = new Set<string>(inferIssueTerms(query, normalizedQueryContext));
  const normalizedText = { normalizedText: normalized };
  const hasOmiAcronym = containsWholeWord(normalized, "omi", normalizedText);
  const hasAweAcronym = containsWholeWord(normalized, "awe", normalizedText);

  if (hasOwnerMoveInPhrase(normalized, normalizedText) || hasOmiAcronym) {
    hints.add("owner move-in");
    hints.add("relative move-in");
    hints.add("owner occupancy");
    hints.add("recover possession");
    if (requiresOwnerMoveInFollowThroughSpecificity(query, normalizedQueryContext)) {
      hints.add("never moved in");
      hints.add("did not move in");
      hints.add("never occupied");
      hints.add("did not occupy");
      hints.add("never resided");
      hints.add("did not reside");
    }
    if (hasExplicitOrdinance379Mention(query, normalizedQueryContext)) hints.add("section 37.9");
    if (hasOmiAcronym) hints.add("omi");
  }
  if (/\b(?:harassment|retaliation\b)/.test(normalized)) {
    hints.add("harassment");
    hints.add("tenant harassment");
    hints.add("retaliation");
    hints.add("section 37.10b");
  }
  if (/\bbuyout\b/.test(normalized)) {
    hints.add("buyout");
    hints.add("buyout agreement");
    hints.add("buyout negotiations");
    hints.add("disclosure");
    hints.add("rescission");
    if (isBuyoutPressureQuery(query, normalizedQueryContext)) {
      hints.add("pressure");
      hints.add("pressured");
      hints.add("pressuring");
      hints.add("harassment");
      hints.add("harassing");
      hints.add("coerce");
      hints.add("coerced");
      hints.add("coercion");
      hints.add("coercive");
      hints.add("buyout coercion");
      hints.add("coercive buyout");
      hints.add("payment to vacate");
      hints.add("payments to vacate");
      hints.add("offer to vacate");
      hints.add("offers to vacate");
      hints.add("offer of payment to vacate");
      hints.add("offers of payment to vacate");
      hints.add("threats or intimidation");
      hints.add("fraud intimidation or coercion");
      hints.add("threat");
      hints.add("threaten");
      hints.add("threatened");
    }
  }
  if (isInfestationAliasQuery(query, normalizedQueryContext)) {
    hints.add("infestation");
    hints.add("infestations");
    hints.add("rodent infestation");
    hints.add("cockroach infestation");
    hints.add("bed bug infestation");
    hints.add("rodent");
    hints.add("cockroach");
    hints.add("bed bug");
    hints.add("mice");
    hints.add("rats");
    hints.add("pest control");
  }
  if (hasWrongfulEvictionPhrase(normalized, normalizedText) || hasAweAcronym) {
    hints.add("wrongful eviction");
    hints.add("report of alleged wrongful eviction");
    hints.add("unlawful eviction");
    hints.add("lockout");
    hints.add("locked out");
    hints.add("changed locks");
    hints.add("denied access");
    hints.add("shut off utilities");
    hints.add("self-help eviction");
    if (hasAweAcronym) hints.add("awe");
  }
  if (isAccommodationQuery(query, normalizedQueryContext)) {
    hints.add("reasonable accommodation");
    hints.add("service animal");
    hints.add("support animal");
    hints.add("emotional support animal");
    hints.add("accommodation request");
  }
  if (isSection8Query(query, normalizedQueryContext)) {
    hints.add("section 8");
    hints.add("hud");
    hints.add("housing choice voucher");
    hints.add("voucher");
  }
  if (isUnlawfulDetainerQuery(query, normalizedQueryContext)) {
    hints.add("unlawful detainer");
    hints.add("notice to quit");
    hints.add("three day notice");
    hints.add("detainer action");
    hints.add("eviction action");
    hints.add("eviction");
  }
  if (isSection8UnlawfulDetainerQuery(query, normalizedQueryContext)) {
    hints.add("section 8 eviction");
    hints.add("section 8 eviction action");
    hints.add("voucher eviction");
    hints.add("housing choice voucher eviction");
  }
  if (/\bcapital improvement\b/.test(normalized)) {
    hints.add("capital improvement");
    hints.add("passthrough");
  }

  const hintedCodes = issueQueryIndexCodeHints(query, normalizedQueryContext);
  if (hintedCodes.length > 0) {
    const syntheticFilters = { approvedOnly: false, indexCodes: hintedCodes } as SearchRequest["filters"];
    const issueContext = buildIndexCodeFilterContext(syntheticFilters, { includeGenericDhsFamilyAlias: false });
    for (const phrase of issueContext.searchPhrases) hints.add(phrase);
    for (const rulesCitation of issueContext.relatedRulesSections) hints.add(rulesCitation);
    for (const ordinanceCitation of issueContext.relatedOrdinanceSections) hints.add(ordinanceCitation);
  }

  return Array.from(hints).filter(Boolean).slice(0, 8);
}

export function issueQueryReferenceHints(query: string, precomputed?: { normalizedQuery?: string }): { rulesSections: string[]; ordinanceSections: string[] } {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return { rulesSections: [], ordinanceSections: [] };
  const normalizedQueryContext = { normalizedQuery: normalized };
  const normalizedText = { normalizedText: normalized };

  const rulesSections = new Set<string>();
  const ordinanceSections = new Set<string>();

  if (hasExplicitOrdinance379Mention(query, normalizedQueryContext)) {
    ordinanceSections.add("37.9");
  }
  if (hasWrongfulEvictionPhrase(normalized, normalizedText) || containsWholeWord(normalized, "awe", normalizedText)) {
    ordinanceSections.add("37.9");
  }
  if (/\b(?:harassment|retaliation\b)/.test(normalized)) {
    ordinanceSections.add("37.10B");
    ordinanceSections.add("37.10b");
  }
  if (/\bcapital improvement\b/.test(normalized)) {
    rulesSections.add("VII-7.12");
  }

  return {
    rulesSections: Array.from(rulesSections),
    ordinanceSections: Array.from(ordinanceSections)
  };
}

export function lockoutScopePhraseHints(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedQueryContext = { normalizedQuery: normalized };
  if (!requiresLockoutSpecificity(query, normalizedQueryContext)) return [];
  const hints = new Set<string>([
    "lockout",
    "locked out",
    "changed locks",
    "denied access",
    "self-help eviction",
    "shut off utilities",
    "utility shutoff"
  ]);
  if (normalized.includes("utility")) {
    hints.add("utility shutoff");
    hints.add("shut off utilities");
  }
  if (normalized.includes("repair")) {
    hints.add("complained about repairs");
    hints.add("requested repairs");
  }
  return Array.from(hints);
}

export function requiresHabitabilitySpecificity(query: string, precomputed?: { normalizedQuery?: string; primarySignals?: string[] }): boolean {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!normalized) return false;
  const conditionSignals = precomputed?.primarySignals ?? requiredHabitabilityPrimarySignals(query, { normalizedQuery: normalized });
  if (conditionSignals.length === 0) return false;
  const hasReportingSignals = /\breport(?:ed|ing)?|complain(?:ed|ing)?|notified|notice\b/.test(normalized);
  const hasRepairSignals = /\b(?:repair|repairs|restore|restored|service|services\b)/.test(normalized);
  return hasReportingSignals || hasRepairSignals;
}

const HABITABILITY_REPORTING_HINT_TERMS = [
  "reported",
  "complained",
  "notified",
  "notice",
  "repair request",
  "work order"
] as const;

const HABITABILITY_REPAIR_HINT_TERMS = [
  "failed to repair",
  "did not repair",
  "refused to repair",
  "not repaired",
  "failed to restore",
  "restore service",
  "service restoration"
] as const;

const NORMALIZED_HABITABILITY_REPORTING_HINT_TERMS = HABITABILITY_REPORTING_HINT_TERMS.map((term) => ({
  term,
  normalizedTerm: normalize(term)
}));

const NORMALIZED_HABITABILITY_REPAIR_HINT_TERMS = HABITABILITY_REPAIR_HINT_TERMS.map((term) => ({
  term,
  normalizedTerm: normalize(term)
}));

const HABITABILITY_REPORTING_HINT_PATTERN = /report|complain|notified|notice|repair request|work order/;

const HABITABILITY_REPAIR_HINT_PATTERN = /repair|restore|service/;

export function habitabilityScopePhraseHints(
  query: string,
  precomputed?: { normalizedQuery?: string; primarySignals?: string[] }
): { conditionSignals: string[]; reportingHints: string[]; repairHints: string[] } {
  const normalized = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedQueryContext = { normalizedQuery: normalized, primarySignals: precomputed?.primarySignals };
  const conditionSignals = precomputed?.primarySignals ?? requiredHabitabilityPrimarySignals(query, normalizedQueryContext);
  if (!requiresHabitabilitySpecificity(query, normalizedQueryContext)) {
    return { conditionSignals, reportingHints: [], repairHints: [] };
  }
  const reportingHints = NORMALIZED_HABITABILITY_REPORTING_HINT_TERMS.filter(
    ({ normalizedTerm }) => normalized.includes(normalizedTerm) || HABITABILITY_REPORTING_HINT_PATTERN.test(normalized)
  ).map(({ term }) => term);
  const repairHints = NORMALIZED_HABITABILITY_REPAIR_HINT_TERMS.filter(
    ({ normalizedTerm }) => normalized.includes(normalizedTerm) || HABITABILITY_REPAIR_HINT_PATTERN.test(normalized)
  ).map(({ term }) => term);
  return {
    conditionSignals,
    reportingHints: uniq(reportingHints),
    repairHints: uniq(repairHints)
  };
}

export function buildAdaptiveRecallConfig(parsed: SearchRequest, pageWindow: number, precomputed?: { activeStructuredFilterKinds?: string[] }) {
  const activeKinds = precomputed?.activeStructuredFilterKinds ?? activeStructuredFilterKinds(parsed.filters);
  const hasStructuredFilters = activeKinds.length > 0;
  const hasCombinedStructuredFilters = activeKinds.length >= 2;
  const recallIssueTerms = inferIssueTerms(parsed.query || "");
  const recallIssueTermContext = { issueTerms: recallIssueTerms };
  const issueGuidedSearch = isIssueGuidedSearch(parsed, recallIssueTermContext);
  const shortBroadIssueSearch = isShortBroadIssueSearch(parsed, recallIssueTermContext);
  const sentenceIssueSearch = issueGuidedSearch && !shortBroadIssueSearch;
  const shortBroadUnfilteredIssueSearch = shortBroadIssueSearch && !hasStructuredFilters;
  const shortBroadFilteredIssueSearch = shortBroadIssueSearch && hasStructuredFilters;
  const sentenceIssueUnfilteredSearch = sentenceIssueSearch && !hasStructuredFilters;
  const sentenceIssueFilteredSearch = sentenceIssueSearch && hasStructuredFilters;

  const lexicalSearchLimit = Math.min(
    shortBroadFilteredIssueSearch
      ? 48
      : shortBroadUnfilteredIssueSearch
        ? 80
        : sentenceIssueFilteredSearch
          ? 72
          : sentenceIssueUnfilteredSearch
            ? 96
        : 2400,
    shortBroadFilteredIssueSearch
      ? pageWindow * 2
      : shortBroadUnfilteredIssueSearch
        ? pageWindow * 4
        : sentenceIssueFilteredSearch
          ? pageWindow * 3
          : sentenceIssueUnfilteredSearch
            ? pageWindow * 4
        : hasCombinedStructuredFilters
          ? pageWindow * 20
          : hasStructuredFilters
            ? pageWindow * 15
            : pageWindow * 12
  );
  // NS-21: semantic recall must not shrink with the page window — a default request derived
  // vectorSearchLimit ~10-15 (Vectorize topK ~20-25 over 667k chunks). Floor at 50 so downstream
  // topK (limit*2, ceiling 100) reaches a meaningful slice of the index regardless of page size.
  const vectorSearchLimit = Math.min(
    shortBroadIssueSearch ? 30 : 120,
    shortBroadIssueSearch
      ? Math.max(pageWindow, 10)
      : Math.max(hasCombinedStructuredFilters ? pageWindow * 2 : pageWindow, 50)
  );
  const decisionScopeDocumentLimit = parsed.filters.documentId
    ? 1
    : Math.min(
        shortBroadFilteredIssueSearch
          ? 10
          : shortBroadUnfilteredIssueSearch
            ? 12
            : sentenceIssueFilteredSearch
              ? 12
              : sentenceIssueUnfilteredSearch
                ? 14
            : 160,
        shortBroadFilteredIssueSearch
          ? Math.max(pageWindow, 8)
          : shortBroadUnfilteredIssueSearch
            ? Math.max(pageWindow, 10)
            : sentenceIssueFilteredSearch
              ? Math.max(pageWindow, 10)
              : sentenceIssueUnfilteredSearch
                ? Math.max(pageWindow, 12)
            : hasCombinedStructuredFilters
              ? Math.max(pageWindow * 8, 24)
              : hasStructuredFilters
                ? Math.max(pageWindow * 5, 16)
              : Math.max(pageWindow * 4, 12)
      );
  const fallbackDocumentLimit = parsed.filters.documentId
    ? 0
    : Math.min(
        issueGuidedSearch
          ? 0
          : 140,
        hasCombinedStructuredFilters
            ? Math.max(pageWindow * 10, 40)
            : hasStructuredFilters
              ? Math.max(pageWindow * 6, 24)
              : 0
      );
  const lexicalScopeDocumentLimit = parsed.filters.documentId
    ? 1
    : Math.min(
        shortBroadFilteredIssueSearch
          ? 12
          : shortBroadUnfilteredIssueSearch
            ? 0
            : sentenceIssueFilteredSearch
              ? 16
              : sentenceIssueUnfilteredSearch
                ? 18
            : 96,
        shortBroadFilteredIssueSearch
          ? Math.max(pageWindow, 8)
          : sentenceIssueSearch
            ? Math.max(decisionScopeDocumentLimit, pageWindow * 2)
          : Math.max(fallbackDocumentLimit, decisionScopeDocumentLimit, pageWindow * 10)
      );

  return {
    activeKinds,
    hasStructuredFilters,
    hasCombinedStructuredFilters,
    issueGuidedSearch,
    shortBroadIssueSearch,
    lexicalScopeDocumentLimit,
    lexicalSearchLimit,
    vectorSearchLimit,
    decisionScopeDocumentLimit,
    fallbackDocumentLimit
  };
}

// Section labels the decision-layer authority/supporting-fact fallbacks actually keep
// (see isConclusionsLikeSectionLabel / isSupportingFactSectionLabel). Used as a SQL
// prefilter superset so those fallbacks stop pulling every chunk for a document when only
// conclusions/findings/evidence-style sections are ever used (~50-85% of a document's
// chunks are discarded by the JS classifiers today). The keyword set is a strict superset
// of those classifiers, so the JS filtering that runs afterward returns identical rows.
// Values are hardcoded (no user input) and safe to inline.
const DECISION_LAYER_SECTION_LABEL_KEYWORDS = [
  "conclusion",
  "authority",
  "analysis",
  "reasoning",
  "discussion",
  "finding",
  "fact",
  "evidence",
  "background",
  "history",
  "testimony",
  "summary"
];

// Static keyword list x two fixed column names — build each clause once per isolate.
const decisionLayerSectionLabelClauseCache = new Map<string, string>();

export function decisionLayerSectionLabelClause(column: string): string {
  const cached = decisionLayerSectionLabelClauseCache.get(column);
  if (cached) return cached;
  const ors = DECISION_LAYER_SECTION_LABEL_KEYWORDS.map((keyword) => `lower(${column}) LIKE '%${keyword}%'`).join(" OR ");
  const clause = ` AND (${ors})`;
  decisionLayerSectionLabelClauseCache.set(column, clause);
  return clause;
}

export function buildQueryDerivedContext(context: SearchContext): QueryDerivedContext {
  const normalizedQuery = normalize(context.query || "");
  const normalizedQueryContext = { normalizedQuery };
  const normalizedRetrievalQuery = normalizeWhitespace(normalize(context.retrievalQuery || ""));
  const normalizedRetrievalQueryContext = { normalizedQuery: normalizedRetrievalQuery };
  const issueTerms = inferIssueTerms(context.query, normalizedQueryContext);
  const proceduralTerms = inferProceduralTerms(context.query, normalizedQueryContext);
  const retrievalPrimarySignals = primaryIssueSignals(context.retrievalQuery, normalizedRetrievalQueryContext);
  const sentenceIssueAnchors = sentenceIssueAnchorTerms(context.query, normalizedQueryContext);
  const sentenceSecondaryTokens = sentenceSecondaryFactTokens(context.query, { issueTerms, normalizedQuery });
  const indexCodeFilterContext = buildIndexCodeFilterContext(context.filters);
  const explicitIndexCodeFilters = uniq([
    ...indexCodeFilterContext.normalizedCodes,
    ...indexCodeFilterContext.legacyCodeAliases.map((item) => normalizeFilterValue("index_code", item))
  ]).map(normalize);
  const explicitJudgeFilters = requestedJudgeFilters(context.filters);
  const referencedJudges = queryReferencesJudge(`${context.query} ${context.retrievalQuery}`);
  const queryTokens = tokenize(context.query);
  const phraseTokens = meaningfulPhraseTokens(context.query);
  const normalizedSentenceFactualTokens = uniq([...sentenceIssueAnchors, ...sentenceSecondaryTokens])
    .map((token) => normalize(token))
    .filter(Boolean)
    .slice(0, 8);
  // NS-04: the USER-query channel tolerates >6 meaningful tokens (selective longest-6), so long
  // natural-language questions keep phrase understanding. The retrieval-expansion channel below stays
  // on the hard-cliff phraseConceptGroups — family expansions routinely exceed 6 tokens and their
  // ranking is golden-pinned around the >6 no-op.
  const normalizedPhraseConceptGroups = selectivePhraseConceptGroups(context.query, { phraseTokens }).map((group) =>
    group.map((variant) => normalizeWhitespace(normalize(variant))).filter(Boolean)
  );
  const normalizedRetrievalPhraseConceptGroups = phraseConceptGroups(context.retrievalQuery).map((group) =>
    group.map((variant) => normalizeWhitespace(normalize(variant))).filter(Boolean)
  );
  const primarySignals = primaryIssueSignals(context.query, normalizedQueryContext);
  const literalKeywordTokensForQuery = literalKeywordTokens(context.query, normalizedQueryContext);
  const sentenceStyleReasoningQuery = isSentenceStyleReasoningQuery(context);
  return {
    normalizedQuery,
    normalizedRetrievalQuery,
    queryIntent: inferQueryIntent(context, normalizedQueryContext),
    issueTerms,
    normalizedIssueTerms: issueTerms.map((term) => normalize(term)).filter(Boolean),
    proceduralTerms,
    normalizedProceduralTerms: proceduralTerms.map((term) => normalize(term)).filter(Boolean),
    longQueryTokens: queryTokens.filter((token) => token.length > 3),
    retrievalLexicalTokens: meaningfulLexicalTokens(context.retrievalQuery),
    normalizedRetrievalPhraseConceptGroups,
    primarySignals,
    normalizedPrimarySignals: primarySignals.map((signal) => normalize(signal)),
    sentenceIssueAnchors,
    normalizedSentenceIssueAnchors: sentenceIssueAnchors.map((term) => normalize(term)),
    sentenceSecondaryTokens,
    normalizedSentenceSecondaryTokens: sentenceSecondaryTokens.map((term) => normalize(term)),
    normalizedSentenceFactualTokens,
    phraseTokens,
    sentencePhraseOverlapTokens: queryTokens.filter((token) => token.length > 2 && !STOPWORD_TOKENS.has(token)),
    normalizedPhraseConceptGroups,
    structuralIntent: isStructuralIntent(context, normalizedQueryContext),
    sentenceStyleReasoningQuery,
    marketConditionReasoningQuery: isMarketConditionReasoningQuery(context, {
      normalizedQuery,
      sentenceStyleReasoningQuery
    }),
    phraseEvidenceQuery: isPhraseEvidenceQuery(context.query, { normalizedGroups: normalizedPhraseConceptGroups }),
    antInfestationQuery: isAntInfestationQuery(context.query, normalizedQueryContext),
    retrievalInfestationAliasQuery: isInfestationAliasQuery(context.retrievalQuery, normalizedRetrievalQueryContext),
    retrievalOwnerMoveInIssueQuery: isOwnerMoveInIssueSearch(context.retrievalQuery, normalizedRetrievalQueryContext),
    retrievalWrongfulEvictionIssueQuery: isWrongfulEvictionIssueSearch(context.retrievalQuery, normalizedRetrievalQueryContext),
    retrievalLockoutSpecificityRequired: requiresLockoutSpecificity(context.retrievalQuery, normalizedRetrievalQueryContext),
    retrievalHabitabilitySpecificityRequired: requiresHabitabilitySpecificity(context.retrievalQuery, {
      normalizedQuery: normalizedRetrievalQuery,
      primarySignals: retrievalPrimarySignals
    }),
    vectorFirstIssueQuery: isVectorFirstIssueSearch(context.retrievalQuery, normalizedRetrievalQueryContext),
    keywordFamilyRecallQuery: isKeywordFamilyRecallQuery(context.query, normalizedQueryContext),
    curatedKeywordFamilyQuery: matchedCuratedKeywordFamilies(context.query, normalizedQueryContext).length > 0,
    literalKeywordQuery: literalKeywordTokensForQuery.length > 0,
    literalKeywordTokens: literalKeywordTokensForQuery,
    keywordBoundaryGuardTerms: keywordBoundaryGuardTerms(context.query, normalizedQueryContext),
    leakWindowQuery: isLeakWindowQuery(context.query, normalizedQueryContext),
    section8UdQuery: isSection8UnlawfulDetainerQuery(context.query, normalizedQueryContext),
    ownerMoveInQuery: hasOwnerMoveInPhrase(normalizedQuery, { normalizedText: normalizedQuery }),
    ownerMoveInFollowThroughRequired: requiresOwnerMoveInFollowThroughSpecificity(context.query, normalizedQueryContext),
    habitabilityServiceQuery: hasHabitabilityServiceRestorationSignals(context.query, normalizedQueryContext),
    requiredHabitabilitySignals: primarySignals.filter((signal) =>
      ["mold", "heat", "hot water", "rodent", "cockroach", "bed bug"].includes(signal)
    ),
    lockoutSpecificityRequired: requiresLockoutSpecificity(context.query, normalizedQueryContext),
    lockBoxQuery: isLockBoxQuery(context.query, normalizedQueryContext),
    harassmentRetaliationQuery: /\b(?:harassment|retaliation\b)/.test(normalizedQuery),
    wrongfulEvictionQuery: hasWrongfulEvictionPhrase(normalizedQuery, { normalizedText: normalizedQuery }),
    wrongfulEvictionIssueQuery: isWrongfulEvictionIssueSearch(context.query, normalizedQueryContext),
    coolingIssueQuery: isCoolingIssueQuery(context.query, normalizedQueryContext),
    conditionIssueQuery: issueTerms.length > 0,
    noticeProceduralQuery: proceduralTerms.length > 0,
    strongIssueEvidenceRequired: requiresStrongIssueEvidence(context.query, normalizedQueryContext),
    accommodationQuery: isAccommodationQuery(context.query, normalizedQueryContext),
    homeownersExemptionQuery: isHomeownersExemptionQuery(context.query, normalizedQueryContext),
    selfEmployedQuery: isSelfEmployedQuery(context.query, normalizedQueryContext),
    adjudicatedQuery: isAdjudicatedQuery(context.query, normalizedQueryContext),
    socialMediaQuery: isSocialMediaQuery(context.query, normalizedQueryContext),
    caregiverQuery: isCaregiverQuery(context.query, normalizedQueryContext),
    mootQuery: isMootQuery(context.query, normalizedQueryContext),
    divorceQuery: isDivorceQuery(context.query, normalizedQueryContext),
    remoteWorkQuery: isRemoteWorkQuery(context.query, normalizedQueryContext),
    collegeQuery: isCollegeQuery(context.query, normalizedQueryContext),
    coLivingQuery: isCoLivingQuery(context.query, normalizedQueryContext),
    buyoutQuery: isBuyoutQuery(context.query, normalizedQueryContext),
    buyoutPressureQuery: isBuyoutPressureQuery(context.query, normalizedQueryContext),
    rentReductionQuery: isRentReductionQuery(context.query, normalizedQueryContext),
    nuisanceQuery: isNuisanceQuery(context.query, normalizedQueryContext),
    evictionProtectionQuery: isEvictionProtectionQuery(context.query, normalizedQueryContext),
    packageSecurityQuery: isPackageSecurityQuery(context.query, normalizedQueryContext),
    cameraPrivacyQuery: isCameraPrivacyQuery(context.query, normalizedQueryContext),
    poopQuery: isPoopQuery(context.query, normalizedQueryContext),
    dogQuery: isDogQuery(context.query, normalizedQueryContext),
    intercomQuery: isIntercomQuery(context.query, normalizedQueryContext),
    garageSpaceQuery: isGarageSpaceQuery(context.query, normalizedQueryContext),
    commonAreasQuery: isCommonAreasQuery(context.query, normalizedQueryContext),
    stairsQuery: isStairsQuery(context.query, normalizedQueryContext),
    porchQuery: isPorchQuery(context.query, normalizedQueryContext),
    windowsQuery: isWindowsQuery(context.query, normalizedQueryContext),
    section8Query: isSection8Query(context.query, normalizedQueryContext),
    unlawfulDetainerQuery: isUnlawfulDetainerQuery(context.query, normalizedQueryContext),
    roomHeatQuery: isRoomHeatQuery(context.query, normalizedQueryContext),
    judgeDrivenQuery: isJudgeDrivenQuery(context.query, { referencedJudges, issueTerms, proceduralTerms }),
    referencedJudges,
    queryMentionsMold: containsWholeWord(context.query, "mold", { normalizedText: normalizedQuery }),
    queryMentionsMildew: containsWholeWord(context.query, "mildew", { normalizedText: normalizedQuery }),
    indexCodeFilterContext,
    explicitIndexCodeFilters,
    normalizedIndexCodeRelatedRulesSections: indexCodeFilterContext.relatedRulesSections.map((item) => normalize(item)).filter(Boolean),
    normalizedIndexCodeRelatedOrdinanceSections: indexCodeFilterContext.relatedOrdinanceSections.map((item) => normalize(item)).filter(Boolean),
    normalizedIndexCodeSearchPhrases: indexCodeFilterContext.searchPhrases.map((item) => normalize(item)).filter(Boolean),
    normalizedRulesSectionFilter: normalize(context.filters.rulesSection || ""),
    normalizedOrdinanceSectionFilter: normalize(context.filters.ordinanceSection || ""),
    normalizedPartyNameFilter: normalize(context.filters.partyName || ""),
    activeStructuredFilterKinds: activeStructuredFilterKinds(context.filters, {
      requestedJudgeFilters: explicitJudgeFilters,
      requestedIndexCodeFilters: indexCodeFilterContext.requestedCodes
    }),
    explicitJudgeFilters,
    explicitJudgeLookupKeys: explicitJudgeFilters.map((judge) => normalizeJudgeLookupKey(judge)),
    referencedJudgeLookupKeys: referencedJudges.map((judge) => normalizeJudgeLookupKey(judge))
  };
}

export function getQueryDerivedContext(context: SearchContext): QueryDerivedContext {
  if (!context.derived) {
    context.derived = buildQueryDerivedContext(context);
  }
  return context.derived;
}

export function requiredHabitabilityPrimarySignals(query: string, precomputed?: { normalizedQuery?: string }): string[] {
  return primaryIssueSignals(query, precomputed).filter((signal) =>
    ["mold", "heat", "hot water", "rodent", "cockroach", "bed bug"].includes(signal)
  );
}
