// Scoring / ranking / snippet / display layer extracted from search.ts (SEARCH-02c module split, step 6).
//
// Everything below orchestration: per-row scoring (scoreRow), candidate assembly + ordering, decision
// layering, snippet + passage building, evidence summaries, and the ranking helper predicates. Depends
// on the query-analysis + DB (search-fts) layers and the leaf modules. runSearchInternal / search /
// searchDebug stay in search.ts as orchestration and import these back.
import {
  canonicalizeJudgeName,
  inferJudgeFromTextFragments,
  judgeSearchTerms,
  normalizeJudgeLookupKey,
  queryReferencesJudge,
  sanitizeDisplayJudgeName
} from "./judges";
import { normalizeFilterValue } from "./legal-references";
import {
  meaningfulPhraseTokens,
  phraseConceptCoverage,
  phraseConceptGroups,
  phraseConceptVariantsForToken,
  sentencePhraseOverlapScore,
  wholePhraseIndexInNormalizedText
} from "./search-concepts";
import {
  buildAdaptiveRecallConfig,
  buildIndexCodeFilterContext,
  cachedNormalizedChunkText,
  directIndexCodeMatchValuesForRequestedCode,
  getQueryDerivedContext,
  inferIssueTerms,
  isConclusionsLikeSectionLabel,
  isFindingsLikeSectionLabel,
  isRetryableSearchError,
  isSupportingFactSectionLabel,
  keywordCandidateTerms,
  normalizeChunkTypeLabel,
  requestedIndexCodeFilters,
  requiredHabitabilityPrimarySignals,
  sentenceIssueAnchorTerms,
  sentenceSecondaryFactTokens,
  textContainsIssueSignal
} from "./search-query-analysis";
import {
  hasAccommodationContext,
  hasAdjudicatedContext,
  hasBathroomLocationContext,
  hasBathroomWindowContext,
  hasBuyoutContext,
  hasBuyoutPressureContext,
  hasCameraPrivacyContext,
  hasCaregiverContext,
  hasCoLivingContext,
  hasCollegeContext,
  hasCommonAreasContext,
  hasConcretePhraseFactSignal,
  hasCoolingProxyDrift,
  hasDivorceContext,
  hasDogContext,
  hasDogParkContext,
  hasDogPolicyContext,
  hasEmploymentAccommodationDrift,
  hasGarageSpaceContext,
  hasHarassmentContext,
  hasHomeownersExemptionContext,
  hasIntercomContext,
  hasLeakWindowContext,
  hasLockBoxContext,
  hasMoldCollision,
  hasMootContext,
  hasNuisanceContext,
  hasOwnerMoveInContext,
  hasOwnerMoveInFollowThroughContext,
  hasOwnerMoveInOccupancyStandardContext,
  hasOwnerMoveInPhrase,
  hasPackageDeliverySecurityContext,
  hasPackageSecurityContext,
  hasPetPolicyDrift,
  hasPoopContext,
  hasPorchContext,
  hasRemoteWorkContext,
  hasRentReductionContext,
  hasRepairNoticeContext,
  hasSection827RentIncreaseDrift,
  hasSection8Context,
  hasSection8RehabDrift,
  hasSelfEmployedContext,
  hasSocialMediaContext,
  hasStairsContext,
  hasStrongPoopDecisionContext,
  hasUnlawfulDetainerContext,
  hasWeakRodentPoopContext,
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
  isCapitalImprovementBoilerplate,
  isCaregiverQuery,
  isCoLivingQuery,
  isCollegeQuery,
  isCommonAreasQuery,
  isCoolingIssueQuery,
  isDivorceQuery,
  isDogQuery,
  isEvictionProtectionQuery,
  isGarageSpaceQuery,
  isGenericHousingServiceStandard,
  isHomeownersExemptionQuery,
  isHousingServicesDefinitionBoilerplate,
  isInfestationAliasQuery,
  isIntercomQuery,
  isLeakWindowQuery,
  isMootQuery,
  isNuisanceQuery,
  isOwnerMoveInLegalStandardBoilerplate,
  isPackageSecurityQuery,
  isPoopQuery,
  isPorchQuery,
  isRemoteWorkQuery,
  isRentReductionQuery,
  isSection8UnlawfulDetainerQuery,
  isSelfEmployedQuery,
  isShortAlphabeticQuery,
  isSocialMediaQuery,
  isStairsQuery,
  isWindowsQuery,
  requiresOwnerMoveInFollowThroughSpecificity
} from "./search-query-classification";
import {
  containsWholeWord,
  meaningfulLexicalTokens,
  normalize,
  normalizeWhitespace,
  tokenize,
  uniq,
  wholeWordCountRegex,
  wholeWordRegex
} from "./search-text";
import { SearchRequest } from "@beedle/shared";
import type { Env } from "../lib/types";
import type { ChunkRow, QueryIntent, RankingDiagnostics, RowMetadata, SearchContext, SearchResultPassage, SupportingFactDebug } from "./search-types";

// Upper bound on the keyword-family recall universe. fetchKeywordCandidateDocumentIds re-ranks this
// pre-ranked pool 12 docs at a time — one small lexical query per batch — so an unbounded pool fires
// hundreds of queries per request. The top of the (scope-ranked) pool holds the answers, so cap it to
// keep the bypass path fast without changing top-N quality.
export const KEYWORD_RECALL_UNIVERSE_MAX = 200;

export function inferDocumentJudgeNames(rows: ChunkRow[]): Map<string, string> {
  const fragmentsByDocument = new Map<string, string[]>();
  for (const row of rows) {
    if (sanitizeDisplayJudgeName(row.authorName)) continue;
    const current = fragmentsByDocument.get(row.documentId) || [];
    current.push(row.chunkText || "");
    fragmentsByDocument.set(row.documentId, current);
  }

  const inferred = new Map<string, string>();
  for (const [documentId, fragments] of fragmentsByDocument.entries()) {
    const judgeName = inferJudgeFromTextFragments(fragments);
    const sanitized = sanitizeDisplayJudgeName(judgeName);
    if (sanitized) inferred.set(documentId, sanitized);
  }
  return inferred;
}

const GENERIC_DECISION_QUERY_TERMS = new Set(["decision", "decisions", "document", "documents", "case", "cases", "search"]);

function isGenericDecisionQuery(query: string): boolean {
  return GENERIC_DECISION_QUERY_TERMS.has(normalize(query));
}

function shouldUseSoftIndexCodeScope(query: string, filters: SearchRequest["filters"]): boolean {
  const requestedCodes = requestedIndexCodeFilters(filters);
  const trimmedQuery = String(query || "").trim();
  if (!requestedCodes.length || !trimmedQuery) return false;
  if (isGenericDecisionQuery(trimmedQuery)) return false;
  // Index-code filters should now apply at the document level by default.
  // Broadening happens later through recovery paths if the filtered universe underperforms.
  return false;
}

const VECTOR_SKIP_BROAD_ISSUE_TERMS = [
  "rent reduction",
  "heat",
  "hot water",
  "mold",
  "owner move in",
  "owner move-in",
  "relative move in",
  "relative move-in",
  "harassment",
  "buyout",
  "habitability",
  "capital improvement",
  "cockroach",
  "rodent",
  "bed bug",
  "bed bugs",
  "infestation",
  "noise",
  "leak",
  "leaks",
  "water leak",
  "decrease in services"
] as const;

const VECTOR_FIRST_ISSUE_TERMS = [
  "harassment",
  "buyout",
  "capital improvement"
] as const;

const NORMALIZED_VECTOR_SKIP_BROAD_ISSUE_TERMS = VECTOR_SKIP_BROAD_ISSUE_TERMS.map((term) => normalize(term));

const NORMALIZED_VECTOR_FIRST_ISSUE_TERMS = VECTOR_FIRST_ISSUE_TERMS.map((term) => normalize(term));

export function shouldSkipVectorSearch(
  query: string,
  filters: SearchRequest["filters"],
  queryType: SearchContext["queryType"]
): boolean {
  if (queryType === "citation_lookup" || queryType === "party_name" || queryType === "rules_ordinance") {
    return false;
  }

  const tokenCount = tokenize(query).length;
  if (tokenCount === 0) return true;

  const normalizedQuery = normalize(query);

  const normalizedQueryContext = { normalizedQuery };
  if (
    requiresOwnerMoveInFollowThroughSpecificity(query, normalizedQueryContext) ||
    isBuyoutPressureQuery(query, normalizedQueryContext) ||
    isSection8UnlawfulDetainerQuery(query, normalizedQueryContext) ||
    isCameraPrivacyQuery(query, normalizedQueryContext) ||
    isPackageSecurityQuery(query, normalizedQueryContext) ||
    isDogQuery(query, normalizedQueryContext) ||
    isIntercomQuery(query, normalizedQueryContext) ||
    isGarageSpaceQuery(query, normalizedQueryContext) ||
    isCommonAreasQuery(query, normalizedQueryContext) ||
    isStairsQuery(query, normalizedQueryContext) ||
    isCoLivingQuery(query, normalizedQueryContext) ||
    isHomeownersExemptionQuery(query, normalizedQueryContext) ||
    isCollegeQuery(query, normalizedQueryContext) ||
    isDivorceQuery(query, normalizedQueryContext)
  ) return false;
  // NS-10: the vector-first check must precede the <=2-token skip — otherwise bare "harassment"/
  // "buyout" (the topics DESIGNED to lean on semantic recall) skipped the vector channel entirely
  // and, with lexical also skipped for the vector-first class, got neither.
  if (tokenCount <= 3 && NORMALIZED_VECTOR_FIRST_ISSUE_TERMS.some((term) => normalizedQuery.includes(term))) return false;
  if (tokenCount <= 2) return true;
  if (inferIssueTerms(query, normalizedQueryContext).length > 0 && tokenCount <= 12) return true;
  if (tokenCount <= 3 && NORMALIZED_VECTOR_SKIP_BROAD_ISSUE_TERMS.some((term) => normalizedQuery.includes(term))) return true;
  if (tokenCount <= 3 && shouldUseSoftIndexCodeScope(query, filters)) return true;
  return false;
}

export function enhanceQueryWithIndexCodeContext(query: string, filters: SearchRequest["filters"]): string {
  const trimmedQuery = String(query || "").trim();
  const indexContext = buildIndexCodeFilterContext(filters);
  if (!trimmedQuery || !indexContext.requestedCodes.length) return trimmedQuery;

  const contextualPhrases = uniq(indexContext.searchPhrases).filter(Boolean);
  const referenceHints = uniq([...indexContext.relatedRulesSections, ...indexContext.relatedOrdinanceSections]).filter(Boolean);

  if (isGenericDecisionQuery(trimmedQuery)) {
    const extras = uniq([...indexContext.requestedCodes, ...referenceHints, ...contextualPhrases]).slice(0, 8);
    return uniq([trimmedQuery, ...extras].filter(Boolean)).join(" ");
  }

  const normalizedQuery = normalize(trimmedQuery);
  const queryTokenCount = tokenize(trimmedQuery).length;
  const alreadyContainsContext = contextualPhrases
    .map((item) => normalize(item))
    .some((phrase) => phrase && normalizedQuery.includes(phrase));

  const allowShortFilteredExpansion =
    indexContext.requestedCodes.length >= 1 && indexContext.requestedCodes.length <= 3 && queryTokenCount <= 4 && !alreadyContainsContext;

  if (!allowShortFilteredExpansion) {
    return trimmedQuery;
  }

  const phraseExtras = contextualPhrases
    .filter((item) => {
      const normalizedPhrase = normalize(item);
      return normalizedPhrase && !normalizedQuery.includes(normalizedPhrase);
    })
    .slice(0, indexContext.requestedCodes.length === 1 ? 2 : 4);
  const referenceExtras = referenceHints
    .filter((item) => {
      const normalizedReference = normalize(item);
      return normalizedReference && !normalizedQuery.includes(normalizedReference);
    })
    .slice(0, 1);
  const extras = uniq([...phraseExtras, ...referenceExtras]).slice(0, 3);

  if (!extras.length) return trimmedQuery;

  return uniq([trimmedQuery, ...extras]).join(" ");
}

export function chunkTypeMatchesFilter(sectionLabel: string, chunkTypeFilter?: string): boolean {
  const normalizedFilter = normalizeChunkTypeLabel(chunkTypeFilter || "");
  if (!normalizedFilter) return true;
  return normalizeChunkTypeLabel(sectionLabel || "") === normalizedFilter;
}

export function exactMultiWordPhraseScore(
  query: string,
  text: string,
  precomputed?: { normalizedGroups?: string[][]; normalizedQuery?: string; normalizedText?: string; phraseTokens?: string[] }
): number {
  const tokens = precomputed?.phraseTokens ?? meaningfulPhraseTokens(query);
  if (tokens.length < 2) return 0;
  const normalizedCoverageText = precomputed?.normalizedText ?? normalize(text);
  const normalizedText = normalizedCoverageText.replace(/[^a-z0-9]+/g, " ");
  const normalizedPhrase = tokens.join(" ");
  if (!normalizedText || !normalizedPhrase) return 0;
  if (wholePhraseIndexInNormalizedText(normalizedText, normalizedPhrase) >= 0) return 0.68;
  const coverage = phraseConceptCoverage(query, text, {
    normalizedGroups: precomputed?.normalizedGroups,
    normalizedQuery: precomputed?.normalizedQuery,
    normalizedText: normalizedCoverageText
  });
  if (coverage.totalCount < 2) return 0;
  if (
    isLeakWindowQuery(query, precomputed?.normalizedQuery ? { normalizedQuery: precomputed.normalizedQuery } : undefined) &&
    !hasLeakWindowContext(text, { normalizedText: normalizedCoverageText })
  ) {
    return coverage.matchedCount >= 2 ? 0.02 : 0;
  }
  if (coverage.matchedCount >= coverage.totalCount) return 0.2 + coverage.proximityBoost;
  if (coverage.matchedCount >= 2) return 0.08 + coverage.proximityBoost;
  return 0;
}

function marketConditionReasoningScore(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): number {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query);
  const normalizedText = precomputed?.normalizedText ?? normalize(text);
  let hits = 0;

  const signals = ["market conditions", "new agreement", "new base rent", "anniversary date"];
  for (const signal of signals) {
    if (normalizedQuery.includes(signal) && normalizedText.includes(signal)) {
      hits += 1;
    }
  }

  if (hits >= 3) return 0.12;
  if (hits === 2) return 0.08;
  if (hits === 1) return 0.04;
  return 0;
}

function metadataTerms(row: ChunkRow): string[] {
  return [
    row.title,
    row.citation,
    row.authorName || "",
    row.sectionLabel,
    ...judgeSearchTerms(row.authorName),
    ...parseJsonList(row.indexCodesJson),
    ...parseJsonList(row.rulesSectionsJson),
    ...parseJsonList(row.ordinanceSectionsJson)
  ]
    .map((value) => String(value || "").trim())
    .filter(Boolean);
}

function combinedSearchableText(row: ChunkRow): string {
  return [row.chunkText, ...metadataTerms(row)].join(" ");
}

export function cachedCombinedSearchableText(row: ChunkRow, context: SearchContext): string {
  const cached = context.rowSearchableTextCache?.get(row.chunkId);
  if (cached !== undefined) return cached;
  const text = combinedSearchableText(row);
  if (!context.rowSearchableTextCache) context.rowSearchableTextCache = new Map();
  context.rowSearchableTextCache.set(row.chunkId, text);
  return text;
}

export function cachedNormalizedSearchableText(row: ChunkRow, context: SearchContext): string {
  const cached = context.normalizedRowSearchableTextCache?.get(row.chunkId);
  if (cached !== undefined) return cached;
  const normalizedText = normalize(cachedCombinedSearchableText(row, context));
  if (!context.normalizedRowSearchableTextCache) context.normalizedRowSearchableTextCache = new Map();
  context.normalizedRowSearchableTextCache.set(row.chunkId, normalizedText);
  return normalizedText;
}

function buildRowMetadata(row: ChunkRow): RowMetadata {
  return {
    normalizedIndexCodes: parseJsonList(row.indexCodesJson).map(normalize),
    normalizedRulesSections: parseJsonList(row.rulesSectionsJson).map(normalize),
    normalizedOrdinanceSections: parseJsonList(row.ordinanceSectionsJson).map(normalize),
    normalizedTitle: normalize(row.title),
    normalizedCitation: normalize(row.citation)
  };
}

function cachedRowMetadata(row: ChunkRow, context: SearchContext): RowMetadata {
  const cached = context.rowMetadataCache?.get(row.chunkId);
  if (cached) return cached;
  const metadata = buildRowMetadata(row);
  if (!context.rowMetadataCache) context.rowMetadataCache = new Map();
  context.rowMetadataCache.set(row.chunkId, metadata);
  return metadata;
}

export function keywordExecutionTerms(query: string, precomputed?: { normalizedQuery?: string; normalizedGroups?: string[][] }): string[] {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedGroups = precomputed?.normalizedGroups ?? phraseConceptGroups(normalizedQuery);
  if (normalizedGroups.length >= 2) {
    const normalized = normalizeWhitespace(normalizedQuery);
    const tokens = meaningfulLexicalTokens(normalizedQuery).slice(0, 4);
    return uniq([normalized, ...tokens].filter(Boolean)).slice(0, 5);
  }
  return keywordCandidateTerms(query, { normalizedQuery }).slice(0, 5);
}

function rowHasLiteralKeywordMatch(
  row: ChunkRow,
  context: SearchContext,
  precomputed: { literalTokens: string[] }
): boolean {
  const tokens = precomputed.literalTokens;
  if (!tokens.length) return false;
  const text = cachedNormalizedSearchableText(row, context);
  return tokens.every((token) => wholeWordRegex(token).test(text));
}

export function rowMatchesQueryGuard(row: ChunkRow, query: string, context: SearchContext): boolean {
  const searchableText = cachedCombinedSearchableText(row, context);
  const normalizedText = cachedNormalizedSearchableText(row, context);
  const queryDerived = getQueryDerivedContext(context);
  if (queryDerived.antInfestationQuery) {
    return (
      containsWholeWord(searchableText, "ant", { normalizedText }) ||
      containsWholeWord(searchableText, "ants", { normalizedText }) ||
      containsWholeWord(searchableText, "ant infestation", { normalizedText })
    );
  }
  if (queryDerived.homeownersExemptionQuery) {
    return hasHomeownersExemptionContext(searchableText, { normalizedText });
  }
  const boundaryGuardTerms = queryDerived.keywordBoundaryGuardTerms;
  if (boundaryGuardTerms.length > 0) {
    return boundaryGuardTerms.some((term) => containsWholeWord(searchableText, term, { normalizedText }));
  }
  if (queryDerived.literalKeywordQuery) {
    return rowHasLiteralKeywordMatch(row, context, { literalTokens: queryDerived.literalKeywordTokens });
  }
  // NS-17 (increment A): multi-concept phrase queries used to HARD-ELIMINATE any chunk matching
  // fewer than 2 concept groups — per chunk, before document aggregation — so a document covering
  // "quiet enjoyment" in one chunk and "construction noise" in another was dropped wholesale. The
  // same condition already carries the phrase_concept_undercoverage_penalty (-0.28) in scoreRow, so
  // under-coverage rows now survive DEMOTED instead of vanishing, and document-level aggregation can
  // lift documents whose chunks jointly cover the concepts.
  if (!isShortAlphabeticQuery(query, { normalizedQuery: queryDerived.normalizedQuery })) return true;
  const trimmed = queryDerived.normalizedQuery;
  if (!trimmed) return true;
  const regex = wholeWordRegex(trimmed);
  return regex.test(normalizedText);
}

export function expandQueryForRetrieval(query: string): string {
  const q = normalize(query);
  if (!q) return query;

  const additions: string[] = [];
  const add = (...values: string[]) => additions.push(...values);
  const hasOmiAcronym = containsWholeWord(q, "omi");
  const hasAweAcronym = containsWholeWord(q, "awe");

  if (/\b(?:heat|heating|heater|boiler|radiator\b)/.test(q)) {
    add("heat", "heating", "heater", "boiler", "radiator", "hot water");
  }
  if (/\b(?:cool|cooling|ventilation|air\b)/.test(q)) {
    add("cooling", "ventilation", "air flow", "air circulation", "overheating", "temperature control");
  }
  if (/\b(?:notice|service|served|mail\b)/.test(q)) {
    add("notice", "service", "served", "mailing", "posting", "repair request", "work order", "written notice");
  }
  if (/\b(?:repair|maintenance|condition|habitability\b)/.test(q)) {
    add("repair", "maintenance", "habitability", "condition", "defect");
  }
  if (/\bbuyout\b/.test(q)) {
    add("buyout", "buyout agreement", "buyout negotiations", "disclosure", "rescission", "settlement");
    if (/\b(?:pressure|pressured|pressuring|harass|harassing|harassment|coerce|coerced|coercion|coercive|threat|threaten|threatened)\b/.test(q)) {
      add(
        "pressure",
        "pressured",
        "pressuring",
        "harass",
        "harassing",
        "harassment",
        "coerce",
        "coerced",
        "coercion",
        "coercive",
        "buyout coercion",
        "coercive buyout",
        "payment to vacate",
        "payments to vacate",
        "offer to vacate",
        "offers to vacate",
        "offer of payment to vacate",
        "offers of payment to vacate",
        "threats or intimidation",
        "fraud intimidation or coercion",
        "threat",
        "threaten",
        "threatened"
      );
    }
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
        "roach",
        "roaches",
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
    add(
      "owner move-in",
      "owner move in",
      "relative move-in",
      "relative move in",
      "recover possession",
      "owner occupancy",
      "recover possession for owner",
      "occupy the unit",
      "occupied the unit",
      "principal place of residence",
      "tenant in occupancy",
      "actually resides in a rental unit"
    );
    if (requiresOwnerMoveInFollowThroughSpecificity(q)) {
      add("never moved in", "did not move in", "never occupied", "did not occupy", "never resided", "did not reside");
    }
    if (hasOmiAcronym) add("omi");
  }
  if (/\bnuisance\b/.test(q)) {
    add("nuisance", "substantial nuisance", "tenant conduct", "disturbance");
  }
  if (hasWrongfulEvictionPhrase(q) || hasAweAcronym) {
    add(
      "wrongful eviction",
      "report of alleged wrongful eviction",
      "eviction",
      "unlawful eviction",
      "wrongfully evicted",
      "lockout",
      "locked out",
      "self-help eviction"
    );
    if (hasAweAcronym) add("awe");
  }
  if (/\brent reduction\b/.test(q)) {
    add("rent reduction", "decrease in services", "reduction in housing services", "corresponding rent reduction");
  }
  if (/\b(?:harassment|retaliation\b)/.test(q)) {
    add("harassment", "retaliation", "tenant harassment", "landlord conduct", "37.10b", "wrongful endeavor");
  }
  if (isSection8UnlawfulDetainerQuery(q)) {
    add(
      "section 8 eviction",
      "section 8 eviction action",
      "section 8 notice to quit",
      "voucher eviction",
      "housing choice voucher eviction",
      "housing choice voucher program",
      "unlawful detainer complaint",
      "housing choice voucher program unlawful detainer complaint",
      "eviction action",
      "eviction"
    );
  }
  if (isCameraPrivacyQuery(q)) {
    add("camera privacy", "security camera privacy", "surveillance camera privacy", "security cameras", "surveillance", "invasion of privacy");
  }
  if (isPackageSecurityQuery(q)) {
    add("package security", "package theft", "stolen packages", "package safety", "mail theft", "mailroom security");
  }
  if (isDogQuery(q)) {
    add("dog", "dogs", "dog-free building", "no pets", "pet policy", "dog park", "service animal");
  }
  if (isIntercomQuery(q)) {
    add("intercom", "broken intercom", "door buzzer", "entry system", "security gate", "buzz in");
  }
  if (isGarageSpaceQuery(q)) {
    add("garage space", "parking space", "garage parking", "carport parking", "tandem space", "parking housing service");
  }
  if (isCommonAreasQuery(q)) {
    add("common areas", "common area", "janitorial service", "unclean common areas", "clean common areas", "common-area cleanliness");
  }
  if (isStairsQuery(q)) {
    add("stairs", "loose stairs", "handrail", "back stairs", "stairwell", "fall hazard");
  }
  if (isPorchQuery(q)) {
    add("porch", "front porch", "back porch", "landing", "storage room", "porch door");
  }
  if (isWindowsQuery(q)) {
    add("windows", "window", "inoperable windows", "broken windows", "window latch", "window sash", "operable windows");
  }
  if (isCollegeQuery(q)) {
    add("college", "attend college", "student housing", "school breaks", "temporary absence", "return to live in the unit");
  }
  if (isSelfEmployedQuery(q)) {
    add("self employed", "self-employed", "1099", "schedule c", "tax returns", "principal residence");
  }
  if (isAdjudicatedQuery(q)) {
    add("adjudicated", "adjudicate", "already decided", "previously decided", "precluded", "state court");
  }
  if (isSocialMediaQuery(q)) {
    add("social media", "facebook", "instagram", "nextdoor", "facebook marketplace", "posted on social media");
  }
  if (isCaregiverQuery(q)) {
    add("caregiver", "caregiving", "primary caregiver", "care for", "return to live in the unit", "principal residence");
  }
  if (isPoopQuery(q)) {
    add("poop", "feces", "faeces", "dog waste", "animal waste", "human feces", "sewage");
  }
  if (isMootQuery(q)) {
    add("moot", "rendered moot", "null and void", "rescinded", "administratively dismissed", "withdrawn");
  }
  if (isRemoteWorkQuery(q)) {
    add("remote work", "work from home", "working from home", "unable to work from home", "construction noise", "utility outage");
  }
  if (isDivorceQuery(q)) {
    add("divorce", "divorced", "separated", "separation", "spouse moved out", "marital issues", "live separately");
  }
  if (isCoLivingQuery(q)) {
    add("co-living", "coliving", "separate tenancy", "separate rental agreements", "individual room", "separately rented", "common areas");
  }

  const expanded = uniq([query.trim(), ...additions].filter(Boolean)).join(" ");
  return expanded || query;
}

export function chooseVectorQuery(originalQuery: string): string {
  if (isCameraPrivacyQuery(originalQuery)) {
    return "camera privacy security camera surveillance invasion of privacy video monitoring";
  }
  if (isPackageSecurityQuery(originalQuery)) {
    return "package security package theft stolen packages mail theft mailroom security delivery security secure package delivery";
  }
  if (isDogQuery(originalQuery)) {
    return "dog dogs dog-free building no pets pet policy service animal emotional support animal dog park housing service";
  }
  if (isIntercomQuery(originalQuery)) {
    return "intercom broken intercom door buzzer entry system security gate housing service";
  }
  if (isGarageSpaceQuery(originalQuery)) {
    return "garage space parking space garage parking carport parking tandem space housing service";
  }
  if (isCommonAreasQuery(originalQuery)) {
    return "common areas common area janitorial service clean common areas housing service";
  }
  if (isStairsQuery(originalQuery)) {
    return "stairs loose stairs handrail back stairs stairwell fall hazard housing service";
  }
  if (isPorchQuery(originalQuery)) {
    return "porch front porch back porch landing porch door storage room housing service hazard leak";
  }
  if (isWindowsQuery(originalQuery)) {
    return "windows inoperable windows broken windows window latch window sash operable windows housing service draft leak";
  }
  if (isCollegeQuery(originalQuery)) {
    return "college attend college student housing school breaks temporary absence return to live in the unit permanent residence";
  }
  if (isSelfEmployedQuery(originalQuery)) {
    return "self employed self-employed 1099 schedule c tax returns principal residence address";
  }
  if (isAdjudicatedQuery(originalQuery)) {
    return "adjudicated adjudicate already decided previously decided precluded preclusion state court";
  }
  if (isSocialMediaQuery(originalQuery)) {
    return "social media facebook instagram nextdoor facebook marketplace posted online residency occupancy roommate search";
  }
  if (isCaregiverQuery(originalQuery)) {
    return "caregiver caregiving primary caregiver care for principal residence return to live in the unit family assistance";
  }
  if (isPoopQuery(originalQuery)) {
    return "poop feces faeces dog waste animal waste human feces sewage sanitation contamination";
  }
  if (isMootQuery(originalQuery)) {
    return "moot rendered moot null and void rescinded administratively dismissed withdrawn";
  }
  if (isRemoteWorkQuery(originalQuery)) {
    return "remote work work from home working from home unable to work from home construction noise utility outage peaceful enjoyment";
  }
  if (isDivorceQuery(originalQuery)) {
    return "divorce divorced separated separation spouse moved out marital issues live separately residence occupancy";
  }
  if (isCoLivingQuery(originalQuery)) {
    return "co-living separate tenancy individual room separate rental agreements common areas shared kitchen";
  }
  return String(originalQuery || "").trim();
}

function sentenceFactualTokenMetrics(
  query: string,
  text: string,
  precomputedFactualTokens?: string[],
  precomputed?: { normalizedText?: string }
): {
  matchedCount: number;
  totalCount: number;
  coverageRatio: number;
  proximityBoost: number;
} {
  const normalizedText = precomputed?.normalizedText ?? normalize(text || "");
  if (!normalizedText) {
    return { matchedCount: 0, totalCount: 0, coverageRatio: 0, proximityBoost: 0 };
  }

  const factualTokens =
    precomputedFactualTokens ??
    uniq([...sentenceIssueAnchorTerms(query), ...sentenceSecondaryFactTokens(query)])
      .map((token) => normalize(token))
      .filter(Boolean)
      .slice(0, 8);

  if (factualTokens.length === 0) {
    return { matchedCount: 0, totalCount: 0, coverageRatio: 0, proximityBoost: 0 };
  }

  const matchedPositions = factualTokens
    .map((token) => ({ token, index: normalizedText.indexOf(token) }))
    .filter((item) => item.index >= 0)
    .sort((a, b) => a.index - b.index);

  const matchedCount = matchedPositions.length;
  const coverageRatio = matchedCount > 0 ? matchedCount / factualTokens.length : 0;

  let proximityBoost = 0;
  if (matchedPositions.length >= 2) {
    const firstMatch = matchedPositions[0];
    const lastMatch = matchedPositions[matchedPositions.length - 1];
    if (firstMatch && lastMatch) {
      const span = lastMatch.index - firstMatch.index;
      if (span <= 180) proximityBoost = 0.12;
      else if (span <= 320) proximityBoost = 0.08;
      else if (span <= 520) proximityBoost = 0.04;
    }
  }

  return {
    matchedCount,
    totalCount: factualTokens.length,
    coverageRatio: Number(coverageRatio.toFixed(4)),
    proximityBoost
  };
}

function issueSignalHitCount(text: string, signals: string[], precomputed?: { normalizedText?: string; normalizedSignals?: string[] }): number {
  return signals.filter((signal, index) =>
    textContainsIssueSignal(text, signal, {
      normalizedText: precomputed?.normalizedText,
      normalizedSignal: precomputed?.normalizedSignals?.[index]
    })
  ).length;
}

function rowMatchesReferencedJudge(row: ChunkRow, query: string, explicitJudgeFilters?: string[]): boolean {
  const rowJudge = canonicalizeJudgeName(row.authorName);
  if (!rowJudge) return false;
  const candidates = explicitJudgeFilters && explicitJudgeFilters.length > 0 ? explicitJudgeFilters : queryReferencesJudge(query);
  return candidates.some((judge) => normalizeJudgeLookupKey(judge) === normalizeJudgeLookupKey(rowJudge));
}

function hasWrongContextForQuery(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): boolean {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedQueryContext = { normalizedQuery };
  const normalizedText = precomputed?.normalizedText ?? normalize(text);
  if (!normalizedText) return false;
  if (isPackageSecurityQuery(query, normalizedQueryContext)) {
    const packageSpecificSignal =
      /\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom security\b|\bdelivery\b|\bpackage\b|\bpackages\b|\bmailroom\b|\bmail\b/.test(
        normalizedText
      );
    const packageCollateralDrift =
      /\bsecurity deposit\b|\bsecurity deposits\b|\bsocial security\b|\bsocial security number\b|\bdriver'?s license number\b/.test(
        normalizedText
      ) ||
      (
        /\bplanning code section 207\b|\baccessory dwelling unit\b|\badu\b/.test(normalizedText) &&
        !hasPackageDeliverySecurityContext(normalizedText, { normalizedText }) &&
        !/\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bdelivered\b|\bsign for packages\b|\bapprehend\b/.test(
          normalizedText
        )
      ) ||
      (/loss of any tenant housing services|housing services reasonably expected|planning code section 207/.test(normalizedText) &&
        !packageSpecificSignal);
    if (packageCollateralDrift) return true;
  }
  if (isSection8UnlawfulDetainerQuery(query, normalizedQueryContext)) {
    return hasSection827RentIncreaseDrift(normalizedText, { normalizedText });
  }
  if (isDogQuery(query, normalizedQueryContext)) {
    return /\bdogs?\b/.test(normalizedText) && !hasDogContext(normalizedText, { normalizedText });
  }
  if (isCollegeQuery(query, normalizedQueryContext)) {
    return (
      /\bcommunity college district\b|\bschool district\b|\bgeneral obligation bonds?\b|\bbond passthrough\b|\bpassthrough\b/.test(normalizedText) &&
      !hasCollegeContext(normalizedText, { normalizedText })
    );
  }
  if (isSelfEmployedQuery(query, normalizedQueryContext)) {
    return /\b1099\b|\btax return\b|\btax returns\b|\bbusiness\b/.test(normalizedText) && !hasSelfEmployedContext(normalizedText, { normalizedText });
  }
  if (isAdjudicatedQuery(query, normalizedQueryContext)) {
    return /\bdecid(?:ed|e)\b|\bcourt\b/.test(normalizedText) && !hasAdjudicatedContext(normalizedText, { normalizedText });
  }
  if (isSocialMediaQuery(query, normalizedQueryContext)) {
    const socialSecurityDrift =
      /\bsocial security\b|\bsocial security number\b|\bsupplemental security income\b|\bssi\b/.test(normalizedText);
    if (socialSecurityDrift) return true;
    return /\bfacebook\b|\binstagram\b|\bonline\b|\bposted\b/.test(normalizedText) && !hasSocialMediaContext(normalizedText, { normalizedText });
  }
  if (isCaregiverQuery(query, normalizedQueryContext)) {
    return /\bcaregiver\b|\bcaretaker\b|\bcare\b/.test(normalizedText) && !hasCaregiverContext(normalizedText, { normalizedText });
  }
  if (isPoopQuery(query, normalizedQueryContext)) {
    return /\bfeces\b|\bpoop\b|\bwaste\b/.test(normalizedText) && !hasPoopContext(normalizedText, { normalizedText });
  }
  if (isMootQuery(query, normalizedQueryContext)) {
    return /\bnull and void\b|\brescinded\b|\bdismissed\b/.test(normalizedText) && !hasMootContext(normalizedText, { normalizedText });
  }
  if (isRemoteWorkQuery(query, normalizedQueryContext)) {
    return /\bremote\b|\bwork\b/.test(normalizedText) && !hasRemoteWorkContext(normalizedText, { normalizedText });
  }
  if (isDivorceQuery(query, normalizedQueryContext)) {
    return /\bspouse\b|\bhusband\b|\bwife\b/.test(normalizedText) && !hasDivorceContext(normalizedText, { normalizedText });
  }
  if (isAccommodationQuery(query, normalizedQueryContext)) {
    return (
      hasPetPolicyDrift(normalizedText, { normalizedText }) ||
      (/reasonable costs?|reasonable time|reasonable period/.test(normalizedText) && !hasAccommodationContext(normalizedText, { normalizedText }))
    );
  }
  if (isCoolingIssueQuery(query, normalizedQueryContext)) {
    return hasCoolingProxyDrift(normalizedText, { normalizedText });
  }
  if (isBuyoutQuery(query, normalizedQueryContext)) {
    return isCapitalImprovementBoilerplate(normalizedText, { normalizedText }) || /capital improvement|passthrough/.test(normalizedText);
  }
  if (
    isEvictionProtectionQuery(query, normalizedQueryContext) &&
    !hasOwnerMoveInContext(normalizedText, { normalizedText }) &&
    !hasWrongfulEvictionContext(normalizedText, { normalizedText }) &&
    !hasHarassmentContext(normalizedText, { normalizedText })
  ) {
    return /condominium|tenants-in-common|homeowners association|capital improvement|passthrough/.test(normalizedText);
  }
  if (isRentReductionQuery(query, normalizedQueryContext)) {
    return /capital improvement|certified for|petitioned cost/.test(normalizedText) && !hasRentReductionContext(normalizedText, { normalizedText });
  }
  if (isNuisanceQuery(query, normalizedQueryContext)) {
    return /notice to abate plumbing nuisance|abatement remediation/.test(normalizedText) && !/tenant conduct|noise|waste|disturbance/.test(normalizedText);
  }
  return false;
}

function hasStrongIssueEvidence(
  query: string,
  row: ChunkRow,
  issueTermHits: number,
  proceduralTermHits: number,
  context: SearchContext
): boolean {
  const searchableText = cachedCombinedSearchableText(row, context);
  const normalizedText = cachedNormalizedSearchableText(row, context);
  const queryDerived = getQueryDerivedContext(context);
  const normalizedQuery = queryDerived.normalizedQuery;
  const normalizedQueryContext = { normalizedText: normalizedQuery };
  if (issueTermHits >= 2 || proceduralTermHits >= 2) return true;
  if (containsWholeWord(searchableText, query, { normalizedText })) return true;
  if (queryDerived.section8UdQuery) {
    return hasSection8Context(searchableText, { normalizedText }) && hasUnlawfulDetainerContext(searchableText, { normalizedText });
  }
  if (queryDerived.cameraPrivacyQuery) return hasCameraPrivacyContext(searchableText, { normalizedText });
  if (queryDerived.packageSecurityQuery) {
    return (
      hasPackageDeliverySecurityContext(searchableText, { normalizedText }) ||
      ((/\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bpackages\b/.test(normalizedText) &&
        /\btheft\b|\bstolen\b|\bthief\b|\bapprehend\b|\bsign for packages\b|\bsecure\b|\bdelivery person\b|\bextra keys?\b|\bentry\b|\baccess\b/.test(
          normalizedText
        )))
    );
  }
  if (queryDerived.dogQuery) return hasDogContext(searchableText, { normalizedText });
  if (queryDerived.collegeQuery) return hasCollegeContext(searchableText, { normalizedText });
  if (queryDerived.selfEmployedQuery) return hasSelfEmployedContext(searchableText, { normalizedText });
  if (queryDerived.adjudicatedQuery) return hasAdjudicatedContext(searchableText, { normalizedText });
  if (queryDerived.socialMediaQuery) return hasSocialMediaContext(searchableText, { normalizedText });
  if (queryDerived.caregiverQuery) return hasCaregiverContext(searchableText, { normalizedText });
  if (queryDerived.poopQuery) return hasPoopContext(searchableText, { normalizedText });
  if (queryDerived.mootQuery) return hasMootContext(searchableText, { normalizedText });
  if (queryDerived.remoteWorkQuery) return hasRemoteWorkContext(searchableText, { normalizedText });
  if (queryDerived.divorceQuery) return hasDivorceContext(searchableText, { normalizedText });
  if (queryDerived.intercomQuery) return hasIntercomContext(searchableText, { normalizedText });
  if (queryDerived.garageSpaceQuery) return hasGarageSpaceContext(searchableText, { normalizedText });
  if (queryDerived.commonAreasQuery) return hasCommonAreasContext(searchableText, { normalizedText });
  if (queryDerived.stairsQuery) return hasStairsContext(searchableText, { normalizedText });
  if (queryDerived.coLivingQuery) return hasCoLivingContext(searchableText, { normalizedText });
  if (queryDerived.homeownersExemptionQuery) return hasHomeownersExemptionContext(searchableText, { normalizedText });
  if (queryDerived.section8Query) return hasSection8Context(searchableText, { normalizedText });
  if (queryDerived.unlawfulDetainerQuery) return hasUnlawfulDetainerContext(searchableText, { normalizedText });
  if (queryDerived.accommodationQuery) return hasAccommodationContext(searchableText, { normalizedText });
  if (queryDerived.buyoutPressureQuery) return hasBuyoutPressureContext(searchableText, { normalizedText });
  if (queryDerived.buyoutQuery) return hasBuyoutContext(searchableText, { normalizedText });
  if (/\b(?:repair notice|notice\b)/.test(normalizedQuery)) return hasRepairNoticeContext(searchableText, { normalizedText });
  if (queryDerived.rentReductionQuery) return hasRentReductionContext(searchableText, { normalizedText });
  if (queryDerived.nuisanceQuery) return hasNuisanceContext(searchableText, { normalizedText });
  if (
    hasOwnerMoveInPhrase(normalizedQuery, normalizedQueryContext) ||
    containsWholeWord(normalizedQuery, "omi", normalizedQueryContext) ||
    /\bowner occupancy\b/.test(normalizedQuery)
  ) {
    const conclusionsOccupancyProxy =
      isConclusionsLikeSectionLabel(row.sectionLabel || "") && hasOwnerMoveInOccupancyStandardContext(searchableText, { normalizedText });
    if (queryDerived.ownerMoveInFollowThroughRequired) {
      return (
        (hasOwnerMoveInContext(searchableText, { normalizedText }) || conclusionsOccupancyProxy) &&
        (
          hasOwnerMoveInFollowThroughContext(searchableText, { normalizedText }) ||
          containsWholeWord(searchableText, "owner occupancy", { normalizedText }) ||
          conclusionsOccupancyProxy
        )
      );
    }
    return hasOwnerMoveInContext(searchableText, { normalizedText });
  }
  if (
    hasWrongfulEvictionPhrase(normalizedQuery, normalizedQueryContext) ||
    containsWholeWord(normalizedQuery, "awe", normalizedQueryContext)
  ) {
    return hasWrongfulEvictionContext(searchableText, { normalizedText });
  }
  if (/harassment|retaliation/.test(normalizedQuery)) return hasHarassmentContext(searchableText, { normalizedText });
  return issueTermHits > 0 || proceduralTermHits > 0;
}

export function buildSection8UdDocumentSupportSet(rows: ChunkRow[], context: SearchContext): Set<string> {
  const byDocument = new Map<string, { hasSection8: boolean; hasUd: boolean }>();
  for (const row of rows) {
    const searchableText = cachedCombinedSearchableText(row, context);
    const normalizedText = cachedNormalizedSearchableText(row, context);
    const current = byDocument.get(row.documentId) || { hasSection8: false, hasUd: false };
    if (hasSection8Context(searchableText, { normalizedText })) current.hasSection8 = true;
    if (hasUnlawfulDetainerContext(searchableText, { normalizedText })) current.hasUd = true;
    byDocument.set(row.documentId, current);
  }
  const supported = new Set<string>();
  for (const [documentId, state] of byDocument.entries()) {
    if (state.hasSection8 && state.hasUd) supported.add(documentId);
  }
  return supported;
}

function chunkMatchesSection8UdDocumentSupport(
  row: ChunkRow,
  section8UdDocumentSupportIds: Set<string>,
  context: SearchContext
): boolean {
  if (!section8UdDocumentSupportIds.has(row.documentId)) return false;
  const searchableText = cachedCombinedSearchableText(row, context);
  const normalizedText = cachedNormalizedSearchableText(row, context);
  return (
    hasSection8Context(searchableText, { normalizedText }) ||
    hasUnlawfulDetainerContext(searchableText, { normalizedText }) ||
    isConclusionsLikeSectionLabel(row.sectionLabel || "") ||
    normalizeChunkTypeLabel(row.sectionLabel || "") === "authority_discussion"
  );
}

export function chunkQualifiesForSection8UdDocumentSupport(
  row: ChunkRow,
  diagnostics: RankingDiagnostics,
  section8UdDocumentSupportIds: Set<string>,
  context: SearchContext
): boolean {
  if (!chunkMatchesSection8UdDocumentSupport(row, section8UdDocumentSupportIds, context)) return false;
  if (isConclusionsLikeSectionLabel(row.sectionLabel || "")) {
    return diagnostics.sectionBoost >= 0.1 || diagnostics.lexicalScore >= 0.05 || diagnostics.vectorScore >= 0.3;
  }
  return (
    isFindingsLikeSectionLabel(row.sectionLabel || "") ||
    diagnostics.sectionBoost >= 0.12 ||
    diagnostics.lexicalScore >= 0.1 ||
    diagnostics.vectorScore >= 0.45
  );
}

function chunkMatchesIssueTerms(row: ChunkRow, context: SearchContext): boolean {
  const queryDerived = getQueryDerivedContext(context);
  const issueTerms = queryDerived.issueTerms;
  if (!issueTerms.length) return false;
  const text = cachedNormalizedSearchableText(row, context);
  return issueTerms.some((term) => text.includes(term));
}

function chunkMatchesProceduralTerms(row: ChunkRow, context: SearchContext): boolean {
  const queryDerived = getQueryDerivedContext(context);
  const proceduralTerms = queryDerived.proceduralTerms;
  if (!proceduralTerms.length) return false;
  const text = cachedNormalizedSearchableText(row, context);
  return proceduralTerms.some((term) => text.includes(term));
}

export function countBy(values: string[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function parseJsonList(input: string): string[] {
  try {
    const parsed = JSON.parse(input);
    return Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
  } catch {
    return [];
  }
}

function extractReferenceFamilyToken(value: string): string {
  const normalized = normalize(value || "");
  const direct = normalized.match(/\b\d+\.\d+\b/);
  if (direct?.[0]) return direct[0];
  return normalized;
}

export function buildCitationFamilySignature(row: ChunkRow): string {
  const families = uniq([
    ...parseJsonList(row.ordinanceSectionsJson).map((item) => extractReferenceFamilyToken(normalizeFilterValue("ordinance_section", item))),
    ...parseJsonList(row.rulesSectionsJson).map((item) => extractReferenceFamilyToken(normalizeFilterValue("rules_section", item))),
    ...parseJsonList(row.indexCodesJson).map((item) => extractReferenceFamilyToken(normalizeFilterValue("index_code", item)))
  ].filter(Boolean)).sort((a, b) => a.localeCompare(b));
  if (!families.length) return "<none>";
  return families.join("|");
}

function sectionPriorityBoost(sectionLabel: string): { boost: number; why: string | null } {
  if (isConclusionsLikeSectionLabel(sectionLabel)) {
    return { boost: 0.24, why: "section_priority_conclusions_of_law" };
  }
  if (isFindingsLikeSectionLabel(sectionLabel)) {
    return { boost: 0.14, why: "section_priority_findings_of_fact" };
  }
  return { boost: 0, why: null };
}

function intentBoostForChunkType(intent: QueryIntent, chunkType: string): number {
  if (intent === "unknown" || intent === "comparative") return 0;
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return 0;

  if (intent === "citation" || intent === "authority") {
    if (/conclusions?_of_law/.test(t)) return 0.22;
    if (/(authority|analysis)/.test(t)) return 0.16;
    if (/findings?_of_fact|findings/.test(t)) return 0.08;
    if (/(caption|issue_statement)/.test(t)) return -0.2;
  }
  if (intent === "findings") {
    if (/findings?_of_fact|findings/.test(t)) return 0.2;
    if (/conclusions?_of_law/.test(t)) return 0.12;
    if (/analysis/.test(t)) return 0.1;
    if (/(caption|issue_statement)/.test(t)) return -0.16;
  }
  if (intent === "procedural") {
    if (/(procedural|background|history|order|conclusions?_of_law)/.test(t)) return 0.2;
    if (/(caption|issue_statement|allowable_rent_increase_s|cap_imp_pass_through|landlord_petitioner|tenant_petitioner)/.test(t)) {
      return -0.18;
    }
  }
  if (intent === "analysis") {
    if (/conclusions?_of_law/.test(t)) return 0.22;
    if (/(analysis|authority)/.test(t)) return 0.16;
    if (/findings?_of_fact|findings/.test(t)) return 0.1;
    if (/(caption|issue_statement)/.test(t)) return -0.18;
  }
  if (intent === "disposition") {
    if (/(holding|disposition|order|decision)/.test(t)) return 0.2;
    if (/(caption|issue_statement)/.test(t)) return -0.14;
  }
  return 0;
}

function isLowSignalStructuralChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function isLowSignalTabularChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /^(base|monthly|total|cost|allowable_rent_increase_s|cap_imp_pass_through)$/.test(t);
}

function isIssuePreferredChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /^(findings_of_fact|order|conclusions_of_law|procedural_history|background)$/.test(t);
}

function isIssueDisfavoredChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /^(minute_order|allowable_rent_increase_s|cap_imp_pass_through|base|monthly|total|cost|introduction|body)$/.test(t);
}

function isLowValueIssueIntentChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  if (isIssueDisfavoredChunkType(t) || isLowSignalTabularChunkType(t) || isLowSignalStructuralChunkType(t)) {
    return true;
  }
  if (/^(landlord_petitioner|tenant_petitioner|total_all_capital_improvements|caption_title)$/.test(t)) {
    return true;
  }
  return /^[a-z]?\d+[a-z]?$/.test(t);
}

function isLowSignalVectorOnlyChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return isLowSignalTabularChunkType(t) || /^(introduction|body|minute_order)$/.test(t);
}

function hasMalformedDocxArtifact(text: string): boolean {
  return /<w:[^>]+>/.test(String(text || ""));
}

function hasSevereExtractionArtifact(text: string): boolean {
  const raw = String(text || "");
  if (!raw) return false;
  return (
    /<w:[^>]+>/.test(raw) ||
    /w:(?:rPr|tabs|spacing|ind|rFonts|sz|lang|tab)\b/.test(raw) ||
    /<\/w:[^>]+>/.test(raw)
  );
}

function parseAnchorOrdinal(anchor: string): number | null {
  const raw = String(anchor || "");
  if (!raw) return null;
  const match = raw.match(/(?:^|[#:_-])p(\d+)\b/i);
  if (!match) return null;
  const parsed = Number.parseInt(match[1] || "", 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function chooseSnippet(text: string, context: SearchContext): string {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  const maxSnippetChars = Math.max(120, Math.min(1200, Number(context.snippetMaxLength || 260)));
  const queryDerived = getQueryDerivedContext(context);

  if (queryDerived.packageSecurityQuery) {
    const packageTargets = uniq([
      "package theft",
      "stolen packages",
      "mail theft",
      "mailroom security",
      "delivery security",
      "secure package delivery",
      "sign for packages",
      "delivery person",
      "packages shipped",
      "package",
      "packages",
      "mailroom",
      ...queryDerived.issueTerms.filter((term) => normalize(term) !== "housing service"),
      ...queryDerived.sentenceIssueAnchors,
      ...queryDerived.sentenceSecondaryTokens
    ]).filter((value): value is string => Boolean(value));

    return chooseSnippetForTargets(normalized, packageTargets, maxSnippetChars);
  }

  if (queryDerived.leakWindowQuery) {
    const leakWindowTargets = uniq([
      context.query,
      "leaky bathroom window",
      "leaky bathroom windows",
      "leaking bathroom window",
      "leaking bathroom windows",
      "bathroom window leak",
      "bathroom windows leak",
      "leaky window",
      "leaky windows",
      "leaking window",
      "leaking windows",
      "window leak",
      "window leaks",
      "window leaking",
      "leakage",
      "water intrusion",
      "window waterproofing",
      "window",
      "windows",
      "bathroom"
    ]).filter((value): value is string => Boolean(value));

    return chooseSnippetForTargets(normalized, leakWindowTargets, maxSnippetChars);
  }

  const targets = uniq([
    context.query,
    context.retrievalQuery,
    context.filters.indexCode,
    ...(context.filters.indexCodes || []),
    context.filters.rulesSection,
    context.filters.ordinanceSection,
    context.filters.partyName,
    ...queryDerived.issueTerms,
    ...queryDerived.proceduralTerms,
    ...queryDerived.normalizedPhraseConceptGroups.flatMap((group) => group.slice(0, 4)),
    ...queryDerived.longQueryTokens
  ])
    .filter((value): value is string => Boolean(value));

  return chooseSnippetForTargets(normalized, targets, maxSnippetChars);
}

function chooseSnippetForTargets(normalizedText: string, rawTargets: string[], maxSnippetChars: number): string {
  const normalized = String(normalizedText || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  const targets = uniq(rawTargets)
    .filter((value): value is string => Boolean(value))
    .map((value) => String(value || "").trim())
    .filter(Boolean)
    .sort((a, b) => b.length - a.length);
  if (targets.length === 0) return normalized.slice(0, maxSnippetChars);

  const lower = normalized.toLowerCase();
  let bestSnippet = "";
  let bestScore = -1;
  const lead = Math.max(60, Math.floor(maxSnippetChars * 0.35));
  const trail = Math.max(60, maxSnippetChars - lead);

  for (const target of targets) {
    const loweredTarget = target.toLowerCase();
    let fromIndex = 0;
    while (fromIndex < lower.length) {
      const idx = lower.indexOf(loweredTarget, fromIndex);
      if (idx < 0) break;
      const start = Math.max(0, idx - lead);
      const end = Math.min(normalized.length, idx + target.length + trail);
      const candidate = normalized.slice(start, end);
      const candidateLower = candidate.toLowerCase();
      const hitCount = targets.filter((item) => candidateLower.includes(item.toLowerCase())).length;
      const score = hitCount * 10 + Math.min(5, target.length / 12);
      if (score > bestScore) {
        bestScore = score;
        bestSnippet = candidate;
      }
      fromIndex = idx + loweredTarget.length;
    }
  }

  if (bestSnippet) return bestSnippet;
  return normalized.slice(0, maxSnippetChars);
}

function chooseSupportingFactSnippet(text: string, context: SearchContext): string {
  const normalized = String(text || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  const maxSnippetChars = Math.max(120, Math.min(1200, Number(context.snippetMaxLength || 260)));
  const queryDerived = getQueryDerivedContext(context);
  const factTargets = new Set<string>([
    ...queryDerived.sentenceIssueAnchors,
    ...queryDerived.sentenceSecondaryTokens,
    ...queryDerived.primarySignals,
    ...queryDerived.issueTerms
  ]);

  if (queryDerived.lockoutSpecificityRequired) {
    [
      "lockout",
      "locked out",
      "changed locks",
      "denied access",
      "self-help eviction",
      "shut off utilities",
      "utility shutoff"
    ].forEach((item) => factTargets.add(item));
  }
  if (queryDerived.habitabilityServiceQuery) {
    [
      "reported",
      "complained",
      "notified",
      "notice",
      "repair request",
      "failed to repair",
      "did not repair",
      "refused to repair",
      "not repaired",
      "restore service",
      "failed to restore",
      "service restoration",
      "restored service"
    ].forEach((item) => factTargets.add(item));
  }
  if (queryDerived.ownerMoveInQuery || containsWholeWord(queryDerived.normalizedQuery, "omi")) {
    [
      "owner occupancy",
      "occupancy",
      "occupy",
      "occupied",
      "reside",
      "resided",
      "never occupied",
      "did not occupy",
      "failed to occupy",
      "never resided",
      "did not reside",
      "never moved in",
      "did not move in"
    ].forEach((item) => factTargets.add(item));
  }

  return chooseSnippetForTargets(normalized, Array.from(factTargets), maxSnippetChars);
}

export function buildLayeredResultSnippet(
  context: SearchContext,
  primaryAuthorityPassage?: SearchResultPassage,
  supportingFactPassage?: SearchResultPassage,
  supportingFactDebug?: SupportingFactDebug,
  fallbackText?: string
): string {
  const fallbackSnippet = fallbackText ? chooseSnippet(fallbackText, context) : "";
  const authoritySnippet = String(primaryAuthorityPassage?.snippet || "").trim();
  const factSnippet = String(supportingFactPassage?.snippet || "").trim();
  const maxSnippetChars = Math.max(120, Math.min(1200, Number(context.snippetMaxLength || 260)));
  const queryDerived = getQueryDerivedContext(context);
  const sentenceAnchors = queryDerived.normalizedSentenceIssueAnchors;
  const sentenceSecondaryTokens = queryDerived.normalizedSentenceSecondaryTokens;
  const normalizedAuthoritySnippet = normalize(authoritySnippet);
  const normalizedFactSnippet = normalize(factSnippet);
  const authoritySnippetContext = { normalizedText: normalizedAuthoritySnippet };
  const factSnippetContext = { normalizedText: normalizedFactSnippet };
  const authorityAnchorHits = sentenceAnchors.filter((term) => normalizedAuthoritySnippet.includes(term)).length;
  const authoritySecondaryHits = sentenceSecondaryTokens.filter((term) => normalizedAuthoritySnippet.includes(term)).length;
  const factAnchorHits = sentenceAnchors.filter((term) => normalizedFactSnippet.includes(term)).length;
  const factSecondaryHits = sentenceSecondaryTokens.filter((term) => normalizedFactSnippet.includes(term)).length;
  const authorityFactualMetrics = authoritySnippet
    ? sentenceFactualTokenMetrics(context.query, authoritySnippet, queryDerived.normalizedSentenceFactualTokens, authoritySnippetContext)
    : { matchedCount: 0, totalCount: 0, coverageRatio: 0, proximityBoost: 0 };
  const factFactualMetrics = factSnippet
    ? sentenceFactualTokenMetrics(context.query, factSnippet, queryDerived.normalizedSentenceFactualTokens, factSnippetContext)
    : { matchedCount: 0, totalCount: 0, coverageRatio: 0, proximityBoost: 0 };
  const phraseConceptContext = { normalizedQuery: queryDerived.normalizedQuery, normalizedGroups: queryDerived.normalizedPhraseConceptGroups };
  const authorityPhraseCoverage = authoritySnippet
    ? phraseConceptCoverage(context.query, authoritySnippet, { ...phraseConceptContext, normalizedText: normalizedAuthoritySnippet })
    : { totalCount: 0, matchedCount: 0, coverageRatio: 0, exactPhrase: false, proximityBoost: 0 };
  const factPhraseCoverage = factSnippet
    ? phraseConceptCoverage(context.query, factSnippet, { ...phraseConceptContext, normalizedText: normalizedFactSnippet })
    : { totalCount: 0, matchedCount: 0, coverageRatio: 0, exactPhrase: false, proximityBoost: 0 };
  const factSupportStrong =
    Boolean(factSnippet) &&
    Boolean(
      (supportingFactDebug?.anchorHits ?? 0) > 0 ||
        (supportingFactDebug?.secondaryHits ?? 0) > 0 ||
        (supportingFactDebug?.coverageRatio ?? 0) >= 0.25 ||
        (supportingFactDebug?.factualAnchorScore ?? 0) >= 0.18
    );
  const authoritySupportScore =
    authorityAnchorHits * 0.32 +
    authoritySecondaryHits * 0.22 +
    authorityFactualMetrics.coverageRatio * 0.55 +
    (authorityFactualMetrics.matchedCount >= 2 ? 0.12 : 0);
  const factSupportScore =
    factAnchorHits * 0.2 +
    factSecondaryHits * 0.16 +
    factFactualMetrics.coverageRatio * 0.42 +
    factFactualMetrics.proximityBoost +
    ((supportingFactDebug?.factualAnchorScore ?? 0) * 0.18) +
    ((supportingFactDebug?.anchorHits ?? 0) * 0.05) +
    ((supportingFactDebug?.secondaryHits ?? 0) * 0.04) +
    (isFindingsLikeSectionLabel(supportingFactPassage?.sectionLabel || "") ? 0.1 : 0) +
    (/procedural|background|history/i.test(supportingFactPassage?.sectionLabel || "") ? 0.05 : 0);
  const authorityConclusionLike = isConclusionsLikeSectionLabel(primaryAuthorityPassage?.sectionLabel || "");
  const factEvidenceLike =
    isFindingsLikeSectionLabel(supportingFactPassage?.sectionLabel || "") ||
    /procedural|background|history/i.test(supportingFactPassage?.sectionLabel || "");
  const authorityHasQueryFamilyContext =
    (queryDerived.accommodationQuery && hasAccommodationContext(authoritySnippet, authoritySnippetContext)) ||
    (queryDerived.buyoutQuery && hasBuyoutContext(authoritySnippet, authoritySnippetContext)) ||
    (queryDerived.section8UdQuery &&
      hasSection8Context(authoritySnippet, authoritySnippetContext) &&
      hasUnlawfulDetainerContext(authoritySnippet, authoritySnippetContext)) ||
    (queryDerived.habitabilityServiceQuery &&
      habitabilityCoverageSignals(authoritySnippet, context.query, {
        ...authoritySnippetContext,
        requiredConditionSignals: queryDerived.requiredHabitabilitySignals
      }).conditionSignalHits > 0) ||
    (sentenceAnchors.length > 0 && authorityAnchorHits > 0);
  const factHasQueryFamilyContext =
    (queryDerived.accommodationQuery && hasAccommodationContext(factSnippet, factSnippetContext)) ||
    (queryDerived.buyoutQuery && hasBuyoutContext(factSnippet, factSnippetContext)) ||
    (queryDerived.section8UdQuery &&
      hasSection8Context(factSnippet, factSnippetContext) &&
      hasUnlawfulDetainerContext(factSnippet, factSnippetContext)) ||
    (queryDerived.habitabilityServiceQuery &&
      habitabilityCoverageSignals(factSnippet, context.query, {
        ...factSnippetContext,
        requiredConditionSignals: queryDerived.requiredHabitabilitySignals
      }).conditionSignalHits > 0) ||
    factAnchorHits > 0 ||
    factSecondaryHits > 0;
  const authorityHasComparableFactualSupport =
    authoritySupportScore >= Math.max(0.3, factSupportScore - 0.1) ||
    authorityAnchorHits > factAnchorHits ||
    authoritySecondaryHits > factSecondaryHits;

  if (!queryDerived.sentenceStyleReasoningQuery) {
    if (queryDerived.literalKeywordQuery) {
      return fallbackSnippet || authoritySnippet || factSnippet;
    }
    if (
      factSnippet &&
      queryDerived.normalizedPhraseConceptGroups.length >= 2 &&
      (
        factPhraseCoverage.exactPhrase ||
        factPhraseCoverage.matchedCount > authorityPhraseCoverage.matchedCount ||
        factPhraseCoverage.proximityBoost > authorityPhraseCoverage.proximityBoost
      )
    ) {
      const separator = "  ";
      const combined = `${factSnippet}${separator}${authoritySnippet}`.replace(/\s+/g, " ").trim();
      if (combined.length <= maxSnippetChars) return combined;
      const factBudget = Math.max(100, Math.floor(maxSnippetChars * 0.68));
      const authorityBudget = Math.max(30, maxSnippetChars - factBudget - separator.length);
      const factPart = factSnippet.slice(0, factBudget).trim();
      const authorityPart = authoritySnippet.slice(0, authorityBudget).trim();
      return `${factPart}${separator}${authorityPart}`.trim();
    }
    return authoritySnippet || factSnippet || fallbackSnippet;
  }

  const shouldLeadWithFactSnippet =
    queryDerived.lockoutSpecificityRequired &&
    Boolean(factSnippet) &&
    hasWrongfulEvictionLockoutContext(factSnippet, factSnippetContext) &&
    Boolean(authoritySnippet) &&
    (
      isHousingServicesDefinitionBoilerplate(authoritySnippet, authoritySnippetContext) ||
      isGenericAweDecisionLayer({
        primaryAuthorityPassage,
        supportingFactPassage,
        supportingFactDebug
      }) ||
      (!hasWrongfulEvictionLockoutContext(authoritySnippet, authoritySnippetContext) &&
        (hasWrongfulEvictionContext(authoritySnippet, authoritySnippetContext) ||
          hasHarassmentContext(authoritySnippet, authoritySnippetContext) ||
          hasRepairNoticeContext(authoritySnippet, authoritySnippetContext)))
    );
  const sentenceFactFirstPreferred =
    factSupportStrong &&
    Boolean(authoritySnippet) &&
    !authorityHasComparableFactualSupport &&
    (
      queryDerived.habitabilityServiceQuery ||
      queryDerived.accommodationQuery ||
      queryDerived.buyoutQuery ||
      queryDerived.section8UdQuery ||
      sentenceAnchors.length > 0 ||
      (factEvidenceLike && factSupportScore >= authoritySupportScore + 0.14) ||
      (authorityConclusionLike && factSupportScore >= authoritySupportScore + 0.08) ||
      (!authorityHasQueryFamilyContext && factHasQueryFamilyContext) ||
      (authorityConclusionLike && factEvidenceLike && factSupportScore >= 0.32 && factHasQueryFamilyContext)
    );
  const forceFactFirstSnippet =
    factSupportStrong &&
    Boolean(factSnippet) &&
    factHasQueryFamilyContext &&
    (
      !authorityHasQueryFamilyContext ||
      (factEvidenceLike && factSupportScore >= authoritySupportScore + 0.22) ||
      ((supportingFactDebug?.factualAnchorScore ?? 0) >= 0.5 && factEvidenceLike && authorityConclusionLike)
    );

  if (shouldLeadWithFactSnippet || sentenceFactFirstPreferred || forceFactFirstSnippet) {
    const separator = "  ";
    const combined = `${factSnippet}${separator}${authoritySnippet}`.replace(/\s+/g, " ").trim();
    if (combined.length <= maxSnippetChars) return combined;
    const factBudget = Math.max(90, Math.floor(maxSnippetChars * 0.62));
    const authorityBudget = Math.max(35, maxSnippetChars - factBudget - separator.length);
    const factPart = factSnippet.slice(0, factBudget).trim();
    const authorityPart = authoritySnippet.slice(0, authorityBudget).trim();
    return `${factPart}${separator}${authorityPart}`.trim();
  }

  if (authoritySnippet && factSnippet && authoritySnippet !== factSnippet) {
    const separator = "  ";
    const combined = `${authoritySnippet}${separator}${factSnippet}`.replace(/\s+/g, " ").trim();
    if (combined.length <= maxSnippetChars) return combined;
    const authorityBudget = Math.max(90, Math.floor(maxSnippetChars * 0.58));
    const factBudget = Math.max(40, maxSnippetChars - authorityBudget - separator.length);
    const authorityPart = authoritySnippet.slice(0, authorityBudget).trim();
    const factPart = factSnippet.slice(0, factBudget).trim();
    return `${authorityPart}${separator}${factPart}`.trim();
  }

  return authoritySnippet || factSnippet || fallbackSnippet;
}

// NS-16: bge-base cosines live in ~0.55-0.95, so the raw score's fusion contribution (×0.23) had a
// differentiation spread of ~0.08 — in practice "+0.15 if retrieved at all", not a ranking signal.
// This FIXED affine calibration spreads that band across [0, 1] before the fusion weight. Fixed (not
// per-result-set min-max) so a chunk's score is independent of what else was retrieved — stable
// across pagination and cap changes. Applied ONLY to the fusion term: every guard threshold in the
// codebase (0.72/0.82/0.84/0.45/...) still reads the raw cosine and keeps its tuned meaning. Zero
// stays zero, so environments with an inert vector channel are byte-identical.
export function calibratedVectorFusionScore(rawCosine: number): number {
  if (rawCosine <= 0) return 0;
  const calibrated = (rawCosine - 0.55) / 0.35;
  return Math.max(0, Math.min(1, calibrated));
}

export function lexicalScore(
  text: string,
  query: string,
  precomputed?: { normalizedGroups?: string[][]; terms?: string[]; normalizedQuery?: string; normalizedText?: string }
): number {
  const terms = precomputed?.terms ?? meaningfulLexicalTokens(query);
  if (terms.length === 0) return 0;
  const lower = precomputed?.normalizedText ?? normalize(text);
  const normalizedQuery = precomputed?.normalizedQuery ?? normalizeWhitespace(normalize(query || ""));
  const exactPhraseHit = terms.length >= 2 && normalizedQuery ? containsWholeWord(lower, normalizedQuery) : false;
  let hits = 0;
  let occurrences = 0;
  for (const term of terms) {
    const variants = phraseConceptVariantsForToken(term);
    const matchedVariants = variants.filter((variant) => containsWholeWord(lower, variant));
    if (matchedVariants.length > 0) {
      hits += 1;
      occurrences += matchedVariants.reduce((sum, variant) => {
        const pattern = wholeWordCountRegex(normalize(variant));
        return sum + (lower.match(pattern)?.length || 0);
      }, 0);
    }
  }
  const coverage = hits / terms.length;
  const density = Math.min(1, occurrences / Math.max(2, terms.length * 2));
  const phraseBoost = exactPhraseHit
    ? 0.18
    : phraseConceptCoverage(query, text, {
        normalizedGroups: precomputed?.normalizedGroups,
        normalizedQuery,
        normalizedText: lower
      }).proximityBoost * 0.45;
  return Number(Math.min(1.2, coverage * 0.75 + density * 0.25 + phraseBoost).toFixed(6));
}

export function phraseFtsSearchLimit(recallConfig: ReturnType<typeof buildAdaptiveRecallConfig>, pageWindow: number): number {
  const adaptivePhraseFloor = recallConfig.shortBroadIssueSearch
    ? Math.max(pageWindow * 6, 120)
    : recallConfig.issueGuidedSearch
      ? Math.max(pageWindow * 8, 160)
      : Math.max(pageWindow * 10, 240);
  return Math.max(recallConfig.lexicalSearchLimit, Math.min(360, adaptivePhraseFloor));
}

export async function hasAnyExactIndexCodeCoverage(env: Env, filters: SearchRequest["filters"]): Promise<boolean> {
  const requestedCodes = requestedIndexCodeFilters(filters);
  if (!requestedCodes.length) return false;

  for (const code of requestedCodes) {
    const directValues = directIndexCodeMatchValuesForRequestedCode(code);
    const codeClause = directValues.map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))").join(" OR ");
    const bindings = directValues.flatMap((value) => [normalizeFilterValue("index_code", value), value]);
    try {
      const rows = await env.DB.prepare(
        `SELECT 1
         FROM document_reference_links l
         JOIN documents d ON d.id = l.document_id
         WHERE d.rejected_at IS NULL
           AND d.file_type = 'decision_docx'
           AND l.reference_type = 'index_code'
           AND l.is_valid = 1
           AND (${codeClause})
         LIMIT 1`
      )
        .bind(...bindings)
        .all<{ "1": number }>();
      if ((rows.results || []).length === 0) return false;
    } catch (error) {
      if (isRetryableSearchError(error)) return false;
      throw error;
    }
  }

  return true;
}

export function scoreRow(row: ChunkRow, vectorScore: number, context: SearchContext): RankingDiagnostics {
  const why: string[] = [];
  const queryDerived = getQueryDerivedContext(context);
  const searchableText = cachedCombinedSearchableText(row, context);
  const loweredSnippet = cachedNormalizedSearchableText(row, context);
  const structuralIntent = queryDerived.structuralIntent;
  const lexical = lexicalScore(searchableText, context.retrievalQuery, {
    normalizedGroups: queryDerived.normalizedRetrievalPhraseConceptGroups,
    terms: queryDerived.retrievalLexicalTokens,
    normalizedQuery: queryDerived.normalizedRetrievalQuery,
    normalizedText: loweredSnippet
  });
  const loweredQuery = queryDerived.normalizedQuery;
  const queryIntent = queryDerived.queryIntent;
  const issueTerms = queryDerived.normalizedIssueTerms;
  const hasIssueTerms = issueTerms.length > 0;
  const issueTermHits = issueTerms.filter((term) => loweredSnippet.includes(term)).length;
  const primarySignals = queryDerived.primarySignals;
  const normalizedPrimarySignals = queryDerived.normalizedPrimarySignals;
  const primarySignalHits = issueSignalHitCount(loweredSnippet, primarySignals, {
    normalizedText: loweredSnippet,
    normalizedSignals: normalizedPrimarySignals
  });
  const sentenceIssueAnchors = queryDerived.normalizedSentenceIssueAnchors;
  const sentenceIssueAnchorHits = sentenceIssueAnchors.filter((term) => loweredSnippet.includes(term)).length;
  const sentenceSecondaryTokens = queryDerived.normalizedSentenceSecondaryTokens;
  const sentenceSecondaryHits = sentenceSecondaryTokens.filter((term) => loweredSnippet.includes(term)).length;
  const sentenceFactualMetrics = sentenceFactualTokenMetrics(context.query, searchableText, queryDerived.normalizedSentenceFactualTokens, {
    normalizedText: loweredSnippet
  });
  const phraseCoverage = phraseConceptCoverage(context.query, searchableText, {
    normalizedQuery: queryDerived.normalizedQuery,
    normalizedGroups: queryDerived.normalizedPhraseConceptGroups,
    normalizedText: loweredSnippet
  });
  const proceduralTerms = queryDerived.normalizedProceduralTerms;
  const hasProceduralTerms = proceduralTerms.length > 0;
  const proceduralTermHits = proceduralTerms.filter((term) => loweredSnippet.includes(term)).length;
  const normalizedChunkType = normalizeChunkTypeLabel(row.sectionLabel || "");
  const sentenceStyleReasoningQuery = queryDerived.sentenceStyleReasoningQuery;
  const marketConditionReasoningQuery = queryDerived.marketConditionReasoningQuery;
  const conclusionsLikeChunk = isConclusionsLikeSectionLabel(row.sectionLabel || "");
  const findingsLikeChunk = isFindingsLikeSectionLabel(row.sectionLabel || "");
  const normalizedTextContext = { normalizedQuery: queryDerived.normalizedQuery, normalizedText: loweredSnippet };
  const accommodationContext = hasAccommodationContext(searchableText, normalizedTextContext);
  const section8Context = hasSection8Context(searchableText, normalizedTextContext);
  const unlawfulDetainerContext = hasUnlawfulDetainerContext(searchableText, normalizedTextContext);
  const section8UdQuery = queryDerived.section8UdQuery;
  const ownerMoveInContext = hasOwnerMoveInContext(searchableText, normalizedTextContext);
  const ownerMoveInFollowThroughContext = hasOwnerMoveInFollowThroughContext(searchableText, normalizedTextContext);
  const ownerMoveInFollowThroughRequired = queryDerived.ownerMoveInFollowThroughRequired;

  let exactPhraseBoost = 0;
  if (loweredSnippet.includes(loweredQuery) && context.queryType === "exact_phrase") {
    exactPhraseBoost = 0.35;
    why.push("exact_phrase_match");
  }

  let citationBoost = 0;
  const rowMetadata = cachedRowMetadata(row, context);
  const normCitation = rowMetadata.normalizedCitation;
  if (loweredQuery === normCitation || loweredQuery.includes(normCitation) || normCitation.includes(loweredQuery)) {
    citationBoost = 0.45;
    why.push("citation_exact_or_near");
  }

  const indexCodes = rowMetadata.normalizedIndexCodes;
  const explicitIndexCodeFilters = queryDerived.explicitIndexCodeFilters;
  const ruleSections = rowMetadata.normalizedRulesSections;
  const ordinanceSections = rowMetadata.normalizedOrdinanceSections;

  let metadataBoost = 0;
  if (
    explicitIndexCodeFilters.length > 0 &&
    explicitIndexCodeFilters.some((filterValue) => indexCodes.some((item) => item.includes(filterValue)))
  ) {
    metadataBoost += 0.22;
    why.push("index_code_overlap");
  }
  if (
    queryDerived.normalizedIndexCodeRelatedRulesSections.length > 0 &&
    queryDerived.normalizedIndexCodeRelatedRulesSections.some((filterValue) => ruleSections.some((item) => item.includes(filterValue)))
  ) {
    metadataBoost += 0.16;
    why.push("index_code_rules_compat_overlap");
  }
  if (
    queryDerived.normalizedIndexCodeRelatedOrdinanceSections.length > 0 &&
    queryDerived.normalizedIndexCodeRelatedOrdinanceSections.some((filterValue) =>
      ordinanceSections.some((item) => item.includes(filterValue))
    )
  ) {
    metadataBoost += 0.16;
    why.push("index_code_ordinance_compat_overlap");
  }
  if (
    queryDerived.normalizedIndexCodeSearchPhrases.length > 0 &&
    queryDerived.normalizedIndexCodeSearchPhrases.some((phrase) => loweredSnippet.includes(phrase))
  ) {
    metadataBoost += 0.14;
    why.push("index_code_phrase_compat_overlap");
  }
  if (queryDerived.normalizedRulesSectionFilter && ruleSections.some((item) => item.includes(queryDerived.normalizedRulesSectionFilter))) {
    metadataBoost += 0.18;
    why.push("rules_overlap");
  }
  if (
    queryDerived.normalizedOrdinanceSectionFilter &&
    ordinanceSections.some((item) => item.includes(queryDerived.normalizedOrdinanceSectionFilter))
  ) {
    metadataBoost += 0.18;
    why.push("ordinance_overlap");
  }

  let sectionBoost = 0;
  const prioritizedSection = sectionPriorityBoost(row.sectionLabel || "");
  if (prioritizedSection.boost > 0) {
    sectionBoost += prioritizedSection.boost;
    if (prioritizedSection.why) why.push(prioritizedSection.why);
  }
  if (!hasIssueTerms && /conclusions? of law|reasoning|analysis/i.test(row.sectionLabel)) {
    sectionBoost += 0.11;
    why.push("reasoning_section_boost");
  }
  if (!hasIssueTerms && findingsLikeChunk) {
    sectionBoost += 0.05;
    why.push("findings_section_context_boost");
  }
  if (/rules?|ordinance/i.test(row.sectionLabel)) {
    sectionBoost += 0.1;
    why.push("legal_section_boost");
  }
  if (conclusionsLikeChunk && sentenceStyleReasoningQuery && lexical > 0.08) {
    sectionBoost += 0.14;
    why.push("conclusion_sentence_query_boost");
  }
  if (
    conclusionsLikeChunk &&
    (queryIntent === "authority" || queryIntent === "analysis" || queryIntent === "disposition") &&
    (lexical > 0.04 || vectorScore > 0.18)
  ) {
    sectionBoost += 0.08;
    why.push("conclusion_authority_query_boost");
  }
  if (conclusionsLikeChunk && issueTermHits > 0 && !hasProceduralTerms) {
    sectionBoost += 0.05;
    why.push("conclusion_issue_overlap_boost");
  }
  if (findingsLikeChunk && issueTermHits > 0 && !hasProceduralTerms) {
    sectionBoost += 0.035;
    why.push("findings_issue_overlap_boost");
  }
  if (conclusionsLikeChunk && (issueTermHits > 0 || primarySignalHits > 0)) {
    sectionBoost += sentenceStyleReasoningQuery
      ? sentenceFactualMetrics.matchedCount > 0 || sentenceIssueAnchorHits > 0 || sentenceSecondaryHits > 0
        ? 0.08
        : 0.02
      : 0.08;
    why.push("conclusion_of_law_priority");
  }
  if (findingsLikeChunk && (issueTermHits > 0 || sentenceIssueAnchorHits > 0 || sentenceSecondaryHits > 0)) {
    sectionBoost += sentenceStyleReasoningQuery ? 0.05 : 0.035;
    why.push("findings_of_fact_secondary_priority");
  }
  if (/^introduction$/i.test(row.sectionLabel)) {
    sectionBoost -= 0.08;
    why.push("intro_section_penalty");
  }
  if (/^body$/i.test(row.sectionLabel)) {
    sectionBoost -= 0.06;
    why.push("generic_body_penalty");
  }

  if (sentenceStyleReasoningQuery && conclusionsLikeChunk) {
    const phraseOverlapBoost = sentencePhraseOverlapScore(context.query, searchableText, {
      queryTokens: queryDerived.sentencePhraseOverlapTokens,
      normalizedText: loweredSnippet
    });
    if (phraseOverlapBoost > 0) {
      exactPhraseBoost += phraseOverlapBoost;
      why.push(`sentence_phrase_overlap_boost:${phraseOverlapBoost.toFixed(2)}`);
    }
  }
  const exactMultiWordBoost = exactMultiWordPhraseScore(context.query, searchableText, {
    normalizedGroups: queryDerived.normalizedPhraseConceptGroups,
    normalizedQuery: queryDerived.normalizedQuery,
    normalizedText: loweredSnippet,
    phraseTokens: queryDerived.phraseTokens
  });
  if (exactMultiWordBoost > 0) {
    exactPhraseBoost += exactMultiWordBoost;
    why.push(`multiword_phrase_match_boost:${exactMultiWordBoost.toFixed(2)}`);
  }
  if (phraseCoverage.totalCount >= 2) {
    if (phraseCoverage.exactPhrase) {
      exactPhraseBoost += 0.12;
      why.push("phrase_exact_concept_boost");
    } else if (phraseCoverage.matchedCount >= phraseCoverage.totalCount) {
      const phraseCoverageBoost = Math.min(0.2, 0.08 + phraseCoverage.proximityBoost);
      exactPhraseBoost += phraseCoverageBoost;
      why.push(`phrase_full_concept_coverage:${phraseCoverage.totalCount}`);
    } else if (phraseCoverage.matchedCount >= 2) {
      const phraseCoverageBoost = Math.min(0.14, 0.04 + phraseCoverage.proximityBoost);
      exactPhraseBoost += phraseCoverageBoost;
      why.push(`phrase_partial_concept_coverage:${phraseCoverage.matchedCount}/${phraseCoverage.totalCount}`);
    }
  }
  if (marketConditionReasoningQuery && conclusionsLikeChunk) {
    const marketBoost = marketConditionReasoningScore(context.query, searchableText, {
      normalizedQuery: queryDerived.normalizedQuery,
      normalizedText: loweredSnippet
    });
    if (marketBoost > 0) {
      exactPhraseBoost += marketBoost;
      why.push(`market_condition_reasoning_boost:${marketBoost.toFixed(2)}`);
    }
  }

  let partyNameBoost = 0;
  if (queryDerived.normalizedPartyNameFilter && rowMetadata.normalizedTitle.includes(queryDerived.normalizedPartyNameFilter)) {
    partyNameBoost = 0.3;
    why.push("party_name_exactish");
  }

  const canonicalRowJudge = canonicalizeJudgeName(row.authorName);
  const explicitJudgeFilters = queryDerived.explicitJudgeFilters;
  const canonicalRowJudgeLookupKey = canonicalRowJudge ? normalizeJudgeLookupKey(canonicalRowJudge) : "";
  const referencedJudges = queryDerived.referencedJudges;
  let judgeNameBoost = 0;
  if (
    explicitJudgeFilters.length > 0 &&
    canonicalRowJudgeLookupKey &&
    queryDerived.explicitJudgeLookupKeys.includes(canonicalRowJudgeLookupKey)
  ) {
    judgeNameBoost += 0.4;
    why.push("judge_name_filter_match");
  }
  if (
    referencedJudges.length > 0 &&
    canonicalRowJudgeLookupKey &&
    queryDerived.referencedJudgeLookupKeys.includes(canonicalRowJudgeLookupKey)
  ) {
    judgeNameBoost += explicitJudgeFilters.length > 0 ? 0.08 : 0.22;
    why.push("judge_name_query_match");
  }
  const rowReferencedJudgeMatch = queryDerived.judgeDrivenQuery
    ? rowMatchesReferencedJudge(row, context.query, explicitJudgeFilters)
    : false;
  if (queryDerived.judgeDrivenQuery && rowReferencedJudgeMatch) {
    judgeNameBoost += 0.28;
    why.push("judge_only_author_match_boost");
  }
  if (queryDerived.judgeDrivenQuery && !rowReferencedJudgeMatch) {
    judgeNameBoost -= 0.35;
    why.push("judge_only_author_mismatch_penalty");
  }

  let trustTierBoost = 0;
  if (row.isTrustedTier === 1) {
    trustTierBoost = 0.09;
    why.push("trusted_tier_boost");
  } else if (vectorScore > 0 || lexical > 0.3) {
    trustTierBoost = 0.02;
    why.push("broad_chunked_doc_admission");
  }

  let rerank =
    lexical * 0.42 +
    calibratedVectorFusionScore(vectorScore) * 0.23 +
    exactPhraseBoost +
    citationBoost +
    metadataBoost +
    sectionBoost +
    partyNameBoost +
    judgeNameBoost +
    trustTierBoost;
  if (hasIssueTerms && issueTermHits > 0) {
    const issueBoost = Math.min(0.2, issueTermHits * 0.06);
    rerank += issueBoost;
    why.push(`issue_term_overlap:${issueTermHits}`);
  }
  if (sentenceStyleReasoningQuery && primarySignals.length > 0) {
    if (primarySignalHits >= Math.min(2, primarySignals.length)) {
      rerank += 0.08;
      why.push(`sentence_primary_issue_boost:${primarySignalHits}`);
    } else if (primarySignalHits === 1) {
      rerank += primarySignals.length === 1 ? 0.08 : 0.03;
      why.push("sentence_primary_issue_partial_boost");
    } else if (issueTermHits > 0) {
      rerank -= primarySignals.length === 1 ? 0.16 : 0.1;
      why.push("sentence_primary_issue_missing_penalty");
    }
  }
  if (sentenceStyleReasoningQuery && sentenceIssueAnchors.length > 0) {
    if (sentenceIssueAnchorHits >= Math.min(2, sentenceIssueAnchors.length)) {
      rerank += 0.13;
      why.push(`sentence_issue_anchor_boost:${sentenceIssueAnchorHits}`);
    } else if (sentenceIssueAnchorHits === 1) {
      rerank += 0.06;
      why.push("sentence_issue_anchor_partial_boost");
    } else if (primarySignalHits > 0) {
      rerank -= 0.11;
      why.push("sentence_issue_anchor_missing_penalty");
    }
  }
  if (sentenceStyleReasoningQuery && sentenceIssueAnchorHits > 0 && findingsLikeChunk) {
    rerank += 0.08;
    why.push("sentence_issue_anchor_findings_boost");
  }
  if (sentenceStyleReasoningQuery && sentenceIssueAnchors.length > 0 && conclusionsLikeChunk && sentenceIssueAnchorHits === 0) {
    rerank -= 0.08;
    why.push("sentence_anchorless_conclusion_penalty");
  }
  if (sentenceStyleReasoningQuery && sentenceSecondaryTokens.length > 0) {
    if (sentenceSecondaryHits >= Math.min(2, sentenceSecondaryTokens.length)) {
      rerank += 0.09;
      why.push(`sentence_secondary_fact_boost:${sentenceSecondaryHits}`);
    } else if (sentenceSecondaryHits === 1) {
      rerank += 0.035;
      why.push("sentence_secondary_fact_partial_boost");
    } else if (primarySignalHits > 0) {
      rerank -= 0.09;
      why.push("sentence_secondary_fact_missing_penalty");
    }
  }
  if (sentenceStyleReasoningQuery && sentenceSecondaryHits > 0 && findingsLikeChunk) {
    rerank += 0.07;
    why.push("sentence_secondary_fact_findings_boost");
  }
  if (sentenceStyleReasoningQuery && sentenceFactualMetrics.matchedCount >= 2) {
    const factualCoverageBoost = Math.min(0.1, sentenceFactualMetrics.coverageRatio * 0.12);
    rerank += factualCoverageBoost + sentenceFactualMetrics.proximityBoost;
    why.push(`sentence_factual_coverage_boost:${sentenceFactualMetrics.coverageRatio.toFixed(2)}`);
    if (sentenceFactualMetrics.proximityBoost > 0) {
      why.push(`sentence_factual_proximity_boost:${sentenceFactualMetrics.proximityBoost.toFixed(2)}`);
    }
  }
  if (
    sentenceStyleReasoningQuery &&
    findingsLikeChunk &&
    primarySignalHits > 0 &&
    (sentenceIssueAnchorHits > 0 || sentenceSecondaryHits > 0)
  ) {
    rerank += 0.12;
    why.push("sentence_factual_findings_preference");
  }
  if (
    sentenceStyleReasoningQuery &&
    primarySignals.length === 1 &&
    primarySignalHits === 0 &&
    isHousingServicesDefinitionBoilerplate(searchableText, normalizedTextContext)
  ) {
    rerank -= 0.14;
    why.push("sentence_issue_boilerplate_penalty");
  }
  if (
    sentenceStyleReasoningQuery &&
    queryDerived.ownerMoveInQuery &&
    sentenceIssueAnchors.length > 0 &&
    sentenceIssueAnchorHits === 0 &&
    isOwnerMoveInLegalStandardBoilerplate(searchableText, normalizedTextContext)
  ) {
    rerank -= 0.18;
    why.push("owner_move_in_legal_standard_penalty");
  }
  if (
    sentenceStyleReasoningQuery &&
    sentenceSecondaryHits === 0 &&
    isHousingServicesDefinitionBoilerplate(searchableText, normalizedTextContext)
  ) {
    rerank -= 0.08;
    why.push("sentence_secondary_boilerplate_penalty");
  }
  if (
    sentenceStyleReasoningQuery &&
    conclusionsLikeChunk &&
    primarySignalHits > 0 &&
    sentenceIssueAnchorHits === 0 &&
    sentenceSecondaryHits === 0
  ) {
    rerank -= 0.16;
    why.push("sentence_generic_conclusion_penalty");
  }
  if (
    sentenceStyleReasoningQuery &&
    conclusionsLikeChunk &&
    primarySignalHits > 0 &&
    sentenceFactualMetrics.totalCount >= 2 &&
    sentenceFactualMetrics.coverageRatio < 0.34
  ) {
    rerank -= 0.08;
    why.push("sentence_low_factual_coverage_conclusion_penalty");
  }
  if (hasIssueTerms && /findings? of fact|background|history|order/i.test(row.sectionLabel)) {
    rerank += 0.08;
    why.push("issue_section_boost");
  }
  if (hasIssueTerms && conclusionsLikeChunk) {
    rerank += 0.1;
    why.push("issue_conclusions_priority_boost");
  } else if (hasIssueTerms && findingsLikeChunk) {
    rerank += 0.06;
    why.push("issue_findings_priority_boost");
  }
  if (hasIssueTerms && isIssuePreferredChunkType(normalizedChunkType) && issueTermHits > 0) {
    rerank += 0.06;
    why.push("issue_preferred_chunk_type_boost");
  }
  if (hasIssueTerms && isIssueDisfavoredChunkType(normalizedChunkType) && issueTermHits === 0) {
    rerank -= 0.18;
    why.push("issue_disfavored_chunk_penalty");
  }
  if (hasProceduralTerms && proceduralTermHits > 0) {
    const proceduralBoost = Math.min(0.18, proceduralTermHits * 0.05);
    rerank += proceduralBoost;
    why.push(`procedural_term_overlap:${proceduralTermHits}`);
  }
  if (hasProceduralTerms && /order|conclusions? of law|procedural|history|background/i.test(row.sectionLabel)) {
    rerank += 0.08;
    why.push("procedural_section_boost");
  }
  if (hasProceduralTerms && isLowValueIssueIntentChunkType(normalizedChunkType) && proceduralTermHits === 0) {
    rerank -= 0.2;
    why.push("procedural_low_value_chunk_penalty");
  }
  if (
    hasIssueTerms &&
    /conclusions? of law/i.test(row.sectionLabel) &&
    isCapitalImprovementBoilerplate(row.chunkText, { normalizedText: cachedNormalizedChunkText(row, context) }) &&
    issueTermHits === 0
  ) {
    rerank -= 0.22;
    why.push("capital_improvement_boilerplate_penalty");
  }
  if (queryDerived.queryMentionsMold && hasMoldCollision(searchableText, normalizedTextContext)) {
    rerank -= 0.3;
    why.push("mold_molding_collision_penalty");
  }
  if (queryDerived.queryMentionsMildew) {
    const normalizedMildewText = loweredSnippet;
    const mildewAuthorityLike =
      isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
      !/analysis_reasoning/i.test(String(row.sectionLabel || ""));
    const mildewAnalysisOnly = /analysis_reasoning/i.test(String(row.sectionLabel || ""));
    if (/\bmildew\b/.test(normalizedMildewText)) {
      if (mildewAuthorityLike) {
        rerank += 0.08;
        why.push("mildew_authority_priority_boost");
      }
      if (mildewAnalysisOnly) {
        rerank -= 0.08;
        why.push("mildew_analysis_penalty");
      }
    } else if ((vectorScore > 0.16 || lexical > 0.12) && mildewAnalysisOnly) {
      rerank -= 0.16;
      why.push("mildew_context_missing_analysis_penalty");
    }
  }
  if (queryDerived.coolingIssueQuery && issueTermHits === 0 && vectorScore > 0) {
    rerank -= 0.18;
    why.push("cooling_issue_evidence_penalty");
  }
  if (queryDerived.coolingIssueQuery && issueTermHits > 0 && /findings? of fact|order/i.test(row.sectionLabel)) {
    rerank += 0.08;
    why.push("cooling_issue_evidence_boost");
  }
  if (queryDerived.coolingIssueQuery && hasCoolingProxyDrift(searchableText, normalizedTextContext)) {
    rerank -= 0.24;
    why.push("cooling_proxy_drift_penalty");
  }
  if (phraseCoverage.totalCount >= 2 && phraseCoverage.matchedCount < Math.min(2, phraseCoverage.totalCount) && (lexical > 0.08 || vectorScore > 0.12)) {
    rerank -= 0.28;
    why.push(`phrase_concept_undercoverage_penalty:${phraseCoverage.matchedCount}/${phraseCoverage.totalCount}`);
  }
  if (hasHeatApplianceDrift(context.query, searchableText, normalizedTextContext)) {
    rerank -= 0.34;
    why.push("heat_appliance_drift_penalty");
  }
  if (hasWaterHeaterDrift(context.query, searchableText, normalizedTextContext)) {
    rerank -= 0.42;
    why.push("room_heat_water_heater_drift_penalty");
  }
  if (hasCapitalImprovementCostDrift(context.query, searchableText, normalizedTextContext)) {
    rerank -= phraseCoverage.matchedCount < phraseCoverage.totalCount ? 0.34 : 0.22;
    why.push("phrase_capital_improvement_cost_drift_penalty");
  }
  if (queryDerived.phraseEvidenceQuery && phraseCoverage.totalCount >= 2) {
    const concretePhraseFacts = hasConcretePhraseFactSignal(searchableText, normalizedTextContext);
    if (phraseCoverage.exactPhrase && concretePhraseFacts) {
      rerank += findingsLikeChunk ? 0.22 : 0.16;
      why.push("phrase_exact_fact_evidence_boost");
    } else if (phraseCoverage.matchedCount >= phraseCoverage.totalCount && concretePhraseFacts) {
      rerank += findingsLikeChunk ? 0.14 : 0.08;
      why.push("phrase_fact_evidence_boost");
    }
    if (isGenericHousingServiceStandard(searchableText, normalizedTextContext) && !concretePhraseFacts) {
      rerank -= conclusionsLikeChunk ? 0.18 : 0.1;
      why.push("phrase_generic_legal_standard_penalty");
    }
  }
  const leakWindowAdjustment = leakWindowContextAdjustment(context.query, searchableText, normalizedTextContext);
  if (leakWindowAdjustment.score !== 0) {
    rerank += leakWindowAdjustment.score;
    if (leakWindowAdjustment.reason) why.push(leakWindowAdjustment.reason);
  }
  if (queryDerived.roomHeatQuery && /\bspace heaters?\b|\bheating system\b|\bradiator\b|\bsteam heat\b|\broom temperature\b|\bminimum room temperature\b/.test(loweredSnippet)) {
    rerank += 0.14;
    why.push("room_heat_context_boost");
  }
  if (hasWrongContextForQuery(context.query, searchableText, normalizedTextContext)) {
    rerank -= 0.24;
    why.push("family_wrong_context_penalty");
  }
  if (queryDerived.accommodationQuery) {
    if (accommodationContext) {
      rerank += findingsLikeChunk ? 0.18 : 0.12;
      why.push("accommodation_context_boost");
      if (hasEmploymentAccommodationDrift(searchableText, normalizedTextContext)) {
        rerank -= 0.34;
        why.push("accommodation_employment_drift_penalty");
      }
    } else if (hasEmploymentAccommodationDrift(searchableText, normalizedTextContext)) {
      rerank -= 0.34;
      why.push("accommodation_employment_drift_penalty");
    } else if (vectorScore > 0.2 || lexical > 0.16) {
      rerank -= 0.22;
      why.push("accommodation_context_missing_penalty");
    }
  }
  if (queryDerived.lockBoxQuery) {
    const lockBoxContext = hasLockBoxContext(searchableText, normalizedTextContext);
    const lockBoxAuthorityLike =
      isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
      !/analysis_reasoning/i.test(String(row.sectionLabel || ""));
    if (lockBoxContext) {
      rerank += lockBoxAuthorityLike ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("lock_box_context_boost");
      if (lockBoxAuthorityLike) {
        rerank += 0.08;
        why.push("lock_box_authority_priority_boost");
      }
    } else if (lockBoxAuthorityLike && issueTermHits === 0) {
      rerank -= 0.22;
      why.push("lock_box_generic_authority_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("lock_box_context_missing_penalty");
    }
  }
  if (queryDerived.homeownersExemptionQuery) {
    if (hasHomeownersExemptionContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("homeowners_exemption_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("homeowners_exemption_context_missing_penalty");
    }
  }
  if (queryDerived.cameraPrivacyQuery) {
    if (hasCameraPrivacyContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("camera_privacy_context_boost");
    } else if (
      /privacy/.test(loweredSnippet) &&
      !/\bcamera\b|\bcameras\b|\bsurveillance\b|\bsecurity camera\b/.test(loweredSnippet) &&
      (vectorScore > 0.1 || lexical > 0.08)
    ) {
      rerank -= 0.24;
      why.push("camera_privacy_missing_camera_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("camera_privacy_context_missing_penalty");
    }
  }
  if (queryDerived.packageSecurityQuery) {
    const packageSecuritySensitiveDrift =
      /\bsecurity deposit\b|\bsecurity deposits\b|\bsocial security\b|\bsocial security number\b|\bdriver'?s license number\b/.test(
        loweredSnippet
      );
    const packageBoilerplateDrift =
      (/housing services are those services provided by the landlord|loss of any tenant housing services|housing services reasonably expected|planning code section 207|accessory dwelling unit|\badu\b/.test(
        loweredSnippet
      ) &&
        !/\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bsign for packages\b|\bdelivery person\b|\bextra keys?\b|\bapprehend\b/.test(
          loweredSnippet
        ));
    if (hasPackageDeliverySecurityContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.24 : findingsLikeChunk ? 0.16 : 0.12;
      why.push("package_security_delivery_context_boost");
      if (packageSecuritySensitiveDrift) {
        rerank -= 0.42;
        why.push("package_security_sensitive_drift_penalty");
      }
    } else if (hasPackageSecurityContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("package_security_context_boost");
      rerank -= 0.12;
      why.push("package_security_generic_context_penalty");
      if (packageSecuritySensitiveDrift) {
        rerank -= 0.42;
        why.push("package_security_sensitive_drift_penalty");
      }
      if (packageBoilerplateDrift) {
        rerank -= 0.34;
        why.push("package_security_boilerplate_drift_penalty");
      }
    } else if (packageSecuritySensitiveDrift) {
      rerank -= 0.32;
      why.push("package_security_security_deposit_penalty");
    } else if (packageBoilerplateDrift) {
      rerank -= 0.3;
      why.push("package_security_boilerplate_drift_penalty");
    } else if (/\bsecurity fee\b|\bsecurity fees\b|\bcharge for a security\b|\bunlawful charges? for security fees?\b/.test(loweredSnippet)) {
      rerank -= 0.28;
      why.push("package_security_security_fee_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("package_security_context_missing_penalty");
    }
  }
  if (queryDerived.dogQuery) {
    if (hasDogPolicyContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.26 : findingsLikeChunk ? 0.18 : 0.14;
      why.push("dog_policy_context_boost");
    } else if (hasDogParkContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.14 : findingsLikeChunk ? 0.1 : 0.06;
      why.push("dog_park_context_boost");
    } else if (hasDogContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.18 : findingsLikeChunk ? 0.12 : 0.08;
      why.push("dog_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("dog_context_missing_penalty");
    }
  }
  if (queryDerived.collegeQuery) {
    const collegeBondDrift =
      /\bcommunity college district\b|\bschool district\b|\bgeneral obligation bonds?\b|\bbond passthrough\b|\bpassthrough\b/.test(
        loweredSnippet
      ) && !hasCollegeContext(searchableText, normalizedTextContext);
    if (hasCollegeContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("college_context_boost");
    } else if (collegeBondDrift) {
      rerank -= 0.28;
      why.push("college_bond_drift_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("college_context_missing_penalty");
    }
  }
  if (queryDerived.selfEmployedQuery) {
    if (hasSelfEmployedContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("self_employed_context_boost");
    } else if (/\b1099\b|\btax return\b|\btax returns\b/.test(loweredSnippet)) {
      rerank += 0.06;
      why.push("self_employed_partial_evidence_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("self_employed_context_missing_penalty");
    }
  }
  if (queryDerived.adjudicatedQuery) {
    if (hasAdjudicatedContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("adjudicated_context_boost");
    } else if (/\bdecid(?:ed|e)\b|\bstate court\b/.test(loweredSnippet)) {
      rerank += 0.05;
      why.push("adjudicated_partial_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("adjudicated_context_missing_penalty");
    }
  }
  if (queryDerived.socialMediaQuery) {
    const socialSecurityDrift =
      /\bsocial security\b|\bsocial security number\b|\bsupplemental security income\b|\bssi\b/.test(loweredSnippet);
    if (hasSocialMediaContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("social_media_context_boost");
      if (socialSecurityDrift) {
        rerank -= 0.32;
        why.push("social_media_social_security_drift_penalty");
      }
    } else if (/\bfacebook\b|\binstagram\b|\bnextdoor\b/.test(loweredSnippet)) {
      rerank += 0.04;
      why.push("social_media_partial_platform_boost");
      if (socialSecurityDrift) {
        rerank -= 0.32;
        why.push("social_media_social_security_drift_penalty");
      }
    } else if (socialSecurityDrift) {
      rerank -= 0.32;
      why.push("social_media_social_security_drift_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("social_media_context_missing_penalty");
    }
  }
  if (queryDerived.caregiverQuery) {
    if (hasCaregiverContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("caregiver_context_boost");
    } else if (/\bcaregiver\b|\bcaregiving\b|\bcaretaker\b/.test(loweredSnippet)) {
      rerank += 0.05;
      why.push("caregiver_partial_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("caregiver_context_missing_penalty");
    }
  }
  if (queryDerived.poopQuery) {
    const normalizedPoopText = loweredSnippet;
    const poopAuthorityLike =
      isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
      !/analysis_reasoning/i.test(String(row.sectionLabel || ""));
    const poopAnalysisOnly = /analysis_reasoning/i.test(String(row.sectionLabel || ""));
    if (hasPoopContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("poop_context_boost");
      if (poopAuthorityLike) {
        rerank += 0.08;
        why.push("poop_authority_priority_boost");
      }
      if (poopAnalysisOnly) {
        rerank -= 0.08;
        why.push("poop_analysis_penalty");
      }
    } else if (/\bfeces\b|\bfaeces\b|\bdog waste\b|\banimal waste\b/.test(loweredSnippet)) {
      rerank += 0.05;
      why.push("poop_partial_context_boost");
      if (poopAuthorityLike) {
        rerank += 0.06;
        why.push("poop_authority_priority_boost");
      }
      if (poopAnalysisOnly) {
        rerank -= 0.06;
        why.push("poop_analysis_penalty");
      }
      if (/\brat feces\b|\brodent urine\/feces\b/.test(normalizedPoopText) && !/\bdog waste\b|\banimal waste\b|\bhuman feces\b|\bsewage\b/.test(normalizedPoopText)) {
        rerank -= 0.04;
        why.push("poop_rodent_only_penalty");
      }
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("poop_context_missing_penalty");
    }
  }
  if (queryDerived.mootQuery) {
    if (hasMootContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("moot_context_boost");
    } else if (/\bnull and void\b|\brescinded\b|\bdismissed\b/.test(loweredSnippet)) {
      rerank += 0.05;
      why.push("moot_partial_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("moot_context_missing_penalty");
    }
  }
  if (queryDerived.remoteWorkQuery) {
    if (hasRemoteWorkContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.16 : 0.1;
      why.push("remote_work_context_boost");
    } else if (/\bremote work\b|\bwork from home\b|\bworking from home\b/.test(loweredSnippet)) {
      rerank += 0.06;
      why.push("remote_work_partial_phrase_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("remote_work_context_missing_penalty");
    }
  }
  if (queryDerived.divorceQuery) {
    if (hasDivorceContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("divorce_context_boost");
    } else if (/\bspouse\b|\bhusband\b|\bwife\b/.test(loweredSnippet)) {
      rerank -= 0.2;
      why.push("divorce_generic_spouse_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("divorce_context_missing_penalty");
    }
  }
  if (queryDerived.intercomQuery) {
    if (hasIntercomContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.22 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("intercom_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("intercom_context_missing_penalty");
    }
  }
  if (queryDerived.garageSpaceQuery) {
    if (hasGarageSpaceContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("garage_space_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("garage_space_context_missing_penalty");
    }
  }
  if (queryDerived.commonAreasQuery) {
    if (hasCommonAreasContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("common_areas_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("common_areas_context_missing_penalty");
    }
  }
  if (queryDerived.stairsQuery) {
    if (hasStairsContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("stairs_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("stairs_context_missing_penalty");
    }
  }
  if (queryDerived.porchQuery) {
    if (hasPorchContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("porch_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("porch_context_missing_penalty");
    }
  }
  if (queryDerived.windowsQuery) {
    const windowsCapitalImprovementDrift =
      /\bcapital improvement\b|\bnew windows\b|\bcertified\b|\bpassthrough\b|\bamortiz/.test(loweredSnippet) &&
      !/\binoperable\b|\bbroken\b|\boperable\b|\bwindow latch\b|\bwindow sash\b|\bwould not open\b|\bwould not close\b|\bdraft\b|\bleak\b/.test(
        loweredSnippet
      );
    if (hasWindowsContext(searchableText, normalizedTextContext)) {
      rerank += conclusionsLikeChunk ? 0.2 : findingsLikeChunk ? 0.14 : 0.1;
      why.push("windows_context_boost");
    } else if (windowsCapitalImprovementDrift) {
      rerank -= 0.28;
      why.push("windows_capital_improvement_drift_penalty");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.18;
      why.push("windows_context_missing_penalty");
    }
  }
  if (queryDerived.coLivingQuery) {
    if (hasCoLivingContext(searchableText, normalizedTextContext)) {
      rerank += findingsLikeChunk ? 0.18 : conclusionsLikeChunk ? 0.14 : 0.1;
      why.push("co_living_context_boost");
    } else if (vectorScore > 0.16 || lexical > 0.12) {
      rerank -= 0.2;
      why.push("co_living_context_missing_penalty");
    }
  }
  if (queryDerived.buyoutPressureQuery) {
    if (hasBuyoutPressureContext(searchableText, normalizedTextContext)) {
      rerank += findingsLikeChunk ? 0.24 : 0.16;
      why.push("buyout_pressure_context_boost");
    } else if (hasBuyoutContext(searchableText, normalizedTextContext)) {
      rerank -= 0.24;
      why.push("buyout_pressure_missing_pressure_penalty");
    } else if ((lexical > 0.12 || vectorScore > 0.16) && /settlement|claims|paid\s*\$|paying\s*\$|agreement/.test(loweredSnippet)) {
      rerank -= 0.3;
      why.push("buyout_pressure_generic_settlement_penalty");
    }
  }
  if (queryDerived.section8Query && section8Context) {
    rerank += conclusionsLikeChunk ? 0.12 : 0.08;
    why.push("section8_context_boost");
  }
  if (queryDerived.unlawfulDetainerQuery && unlawfulDetainerContext) {
    rerank += conclusionsLikeChunk || findingsLikeChunk ? 0.16 : 0.1;
    why.push("unlawful_detainer_context_boost");
  }
  if (section8UdQuery) {
    if (section8Context && unlawfulDetainerContext) {
      rerank += conclusionsLikeChunk || findingsLikeChunk ? 0.24 : 0.16;
      why.push("section8_ud_joint_context_boost");
    } else if ((section8Context || unlawfulDetainerContext) && (lexical > 0.1 || vectorScore > 0.14)) {
      rerank -= 0.16;
      why.push("section8_ud_partial_context_penalty");
    }
    if (hasSection8RehabDrift(searchableText, normalizedTextContext)) {
      rerank -= 0.34;
      why.push("section8_rehab_drift_penalty");
    }
    if (hasSection827RentIncreaseDrift(searchableText, normalizedTextContext)) {
      rerank -= 0.42;
      why.push("section827_rent_increase_drift_penalty");
    }
  }
  if (ownerMoveInFollowThroughRequired) {
    if (ownerMoveInContext && ownerMoveInFollowThroughContext) {
      rerank += findingsLikeChunk ? 0.22 : 0.16;
      why.push("owner_move_in_follow_through_joint_context_boost");
    } else if (!ownerMoveInContext || !ownerMoveInFollowThroughContext) {
      rerank -= 0.3;
      why.push("owner_move_in_follow_through_missing_penalty");
    }
  }
  if ((hasIssueTerms || hasProceduralTerms) && isLowValueIssueIntentChunkType(normalizedChunkType) && lexical === 0 && vectorScore > 0) {
    rerank -= 0.26;
    why.push("low_value_issue_vector_penalty");
  }
  if (queryDerived.evictionProtectionQuery && issueTermHits > 0 && /conclusions? of law|order|decision/i.test(row.sectionLabel)) {
    rerank += 0.08;
    why.push("eviction_protection_authority_boost");
  }
  if (queryDerived.lockoutSpecificityRequired) {
    const hasLockoutContext = hasWrongfulEvictionLockoutContext(searchableText, normalizedTextContext);
    if (hasLockoutContext) {
      rerank += /conclusions? of law|findings? of fact|order/i.test(row.sectionLabel) ? 0.24 : 0.14;
      why.push("wrongful_eviction_lockout_required_boost");
    } else if (
      queryDerived.wrongfulEvictionIssueQuery &&
      (
        hasWrongfulEvictionContext(searchableText, normalizedTextContext) ||
        hasHarassmentContext(searchableText, normalizedTextContext) ||
        hasRepairNoticeContext(searchableText, normalizedTextContext)
      )
    ) {
      rerank -= 0.42;
      why.push("wrongful_eviction_lockout_required_penalty");
    }
  }
  if (queryDerived.wrongfulEvictionIssueQuery && hasWrongfulEvictionLockoutContext(searchableText, normalizedTextContext)) {
    rerank += /conclusions? of law|findings? of fact|order/i.test(row.sectionLabel) ? 0.14 : 0.08;
    why.push("wrongful_eviction_lockout_context_boost");
  }
  if (
    queryDerived.wrongfulEvictionIssueQuery &&
    sentenceStyleReasoningQuery &&
    !hasWrongfulEvictionLockoutContext(searchableText, normalizedTextContext) &&
    (hasHarassmentContext(searchableText, normalizedTextContext) || hasRepairNoticeContext(searchableText, normalizedTextContext))
  ) {
    rerank -= 0.18;
    why.push("wrongful_eviction_missing_lockout_penalty");
  }
  if (queryDerived.evictionProtectionQuery && issueTermHits === 0 && lexical > 0.5 && vectorScore === 0) {
    rerank -= 0.14;
    why.push("eviction_protection_lexical_only_penalty");
  }
  if (
    queryDerived.strongIssueEvidenceRequired &&
    !hasStrongIssueEvidence(context.query, row, issueTermHits, proceduralTermHits, context) &&
    lexical > 0.2
  ) {
    rerank -= 0.18;
    why.push("strong_issue_evidence_missing_penalty");
  }
  const intentChunkTypeBoost = intentBoostForChunkType(queryIntent, row.sectionLabel || "");
  rerank += intentChunkTypeBoost;
  if (intentChunkTypeBoost > 0) why.push(`intent_chunk_type_boost:${queryIntent}`);
  if (intentChunkTypeBoost < 0) why.push(`intent_chunk_type_penalty:${queryIntent}`);
  if (lexical > 0) why.push(vectorScore > 0 ? "lexical_and_vector_match" : "lexical_match");
  if (vectorScore > 0) why.push("vector_match");

  // Reduce lexical-only dominance for low-signal heading fragments unless query is direct citation lookup.
  const lexicalDominance = lexical > 0.25 && vectorScore === 0;
  if (context.queryType !== "citation_lookup" && lexicalDominance && isLowSignalStructuralChunkType(normalizedChunkType)) {
    rerank -= 0.12;
    why.push("lexical_low_signal_chunk_penalty");
  }

  const vectorDominance = vectorScore > 0.45 && lexical === 0;
  if (!structuralIntent && context.queryType !== "citation_lookup" && vectorDominance && isLowSignalTabularChunkType(normalizedChunkType)) {
    rerank -= 0.28;
    why.push("vector_tabular_chunk_penalty");
  }
  if (!structuralIntent && context.queryType !== "citation_lookup" && vectorDominance && hasMalformedDocxArtifact(row.chunkText)) {
    rerank -= 0.22;
    why.push("vector_docx_artifact_penalty");
  }
  if (!structuralIntent && context.queryType !== "citation_lookup" && hasSevereExtractionArtifact(row.chunkText)) {
    rerank -= 0.3;
    why.push("severe_extraction_artifact_penalty");
  }

  if (context.queryType === "citation_lookup") {
    rerank += citationBoost * 0.35;
  }
  if (context.queryType === "party_name") {
    rerank += partyNameBoost * 0.35;
  }
  if (context.queryType === "rules_ordinance" || context.queryType === "index_code") {
    rerank += metadataBoost * 0.25;
  }

  // Monotonic compression keeps ranking order stable while reducing score over-separation.
  // This improves score-shape stability across mixed corpora without weakening retrieval gates.
  rerank = Math.pow(Math.max(0, rerank), 0.6);

  return {
    lexicalScore: Number(lexical.toFixed(6)),
    vectorScore: Number(vectorScore.toFixed(6)),
    exactPhraseBoost: Number(exactPhraseBoost.toFixed(6)),
    citationBoost: Number(citationBoost.toFixed(6)),
    metadataBoost: Number(metadataBoost.toFixed(6)),
    sectionBoost: Number(sectionBoost.toFixed(6)),
    partyNameBoost: Number(partyNameBoost.toFixed(6)),
    judgeNameBoost: Number(judgeNameBoost.toFixed(6)),
    trustTierBoost: Number(trustTierBoost.toFixed(6)),
    rerankScore: Number(rerank.toFixed(6)),
    why: uniq(why)
  };
}

function buildDocumentEvidenceSummary(
  candidates: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext
) {
  const queryDerived = getQueryDerivedContext(context);
  const issueTerms = queryDerived.normalizedIssueTerms;
  const proceduralTerms = queryDerived.normalizedProceduralTerms;
  const primarySignals = queryDerived.primarySignals;
  const sentenceAnchors = queryDerived.normalizedSentenceIssueAnchors;
  const sentenceSecondaryTokens = queryDerived.normalizedSentenceSecondaryTokens;
  const sentenceStyle = queryDerived.sentenceStyleReasoningQuery;
  const phraseEvidenceQuery = queryDerived.phraseEvidenceQuery;

  const aggregatedText = candidates.map((candidate) => cachedNormalizedSearchableText(candidate.row, context)).join(" ");
  const phraseConceptContext = { normalizedQuery: queryDerived.normalizedQuery, normalizedGroups: queryDerived.normalizedPhraseConceptGroups };
  const aggregatedPhraseCoverage = phraseConceptCoverage(context.query, aggregatedText, { ...phraseConceptContext, normalizedText: aggregatedText });
  const uniqueIssueCoverage = issueTerms.filter((term) => aggregatedText.includes(term)).length;
  const uniqueProceduralCoverage = proceduralTerms.filter((term) => aggregatedText.includes(term)).length;
  const primaryCoverage = issueSignalHitCount(aggregatedText, primarySignals, {
    normalizedText: aggregatedText,
    normalizedSignals: queryDerived.normalizedPrimarySignals
  });
  const anchorCoverage = sentenceAnchors.filter((term) => aggregatedText.includes(term)).length;
  const secondaryCoverage = sentenceSecondaryTokens.filter((term) => aggregatedText.includes(term)).length;

  let docBoost = 0;
  const reasons: string[] = [];

  if (uniqueIssueCoverage > 1) {
    const boost = Math.min(0.08, uniqueIssueCoverage * 0.02);
    docBoost += boost;
    reasons.push(`document_issue_coverage_boost:${uniqueIssueCoverage}`);
  }
  if (uniqueProceduralCoverage > 1) {
    const boost = Math.min(0.06, uniqueProceduralCoverage * 0.02);
    docBoost += boost;
    reasons.push(`document_procedural_coverage_boost:${uniqueProceduralCoverage}`);
  }
  if (sentenceStyle) {
    if (primaryCoverage > 0) {
      const boost = Math.min(0.08, primaryCoverage * 0.03);
      docBoost += boost;
      reasons.push(`document_primary_signal_coverage:${primaryCoverage}`);
    }
    if (anchorCoverage > 0) {
      const boost = Math.min(0.12, anchorCoverage * 0.04);
      docBoost += boost;
      reasons.push(`document_anchor_coverage:${anchorCoverage}`);
    }
    if (secondaryCoverage > 0) {
      const boost = Math.min(0.08, secondaryCoverage * 0.03);
      docBoost += boost;
      reasons.push(`document_secondary_fact_coverage:${secondaryCoverage}`);
    }
  }
  if (phraseEvidenceQuery && aggregatedPhraseCoverage.totalCount >= 2) {
    const aggregateConcretePhraseFacts = hasConcretePhraseFactSignal(aggregatedText, { normalizedText: aggregatedText });
    if (aggregatedPhraseCoverage.exactPhrase && aggregateConcretePhraseFacts) {
      docBoost += 0.18;
      reasons.push("document_phrase_exact_fact_boost");
    } else if (aggregatedPhraseCoverage.matchedCount >= aggregatedPhraseCoverage.totalCount && aggregateConcretePhraseFacts) {
      docBoost += 0.1;
      reasons.push("document_phrase_fact_boost");
    }
  }

  let leadChunkId: string | null = null;
  let leadBoost = 0;
  let bestLeadScore = Number.NEGATIVE_INFINITY;
  let bestConclusionChunkId: string | null = null;
  let bestConclusionSupport = Number.NEGATIVE_INFINITY;
  let bestFindingsChunkId: string | null = null;
  let bestFindingsSupport = Number.NEGATIVE_INFINITY;
  for (const candidate of candidates) {
    const searchableText = cachedCombinedSearchableText(candidate.row, context);
    const normalizedText = cachedNormalizedSearchableText(candidate.row, context);
    const issueHits = issueTerms.filter((term) => normalizedText.includes(term)).length;
    const primaryHits = issueSignalHitCount(normalizedText, primarySignals, {
      normalizedText,
      normalizedSignals: queryDerived.normalizedPrimarySignals
    });
    const anchorHits = sentenceAnchors.filter((term) => normalizedText.includes(term)).length;
    const secondaryHits = sentenceSecondaryTokens.filter((term) => normalizedText.includes(term)).length;
    const factualMetrics = sentenceFactualTokenMetrics(context.query, searchableText, queryDerived.normalizedSentenceFactualTokens, {
      normalizedText
    });
    const phraseCoverage = phraseConceptCoverage(context.query, searchableText, { ...phraseConceptContext, normalizedText });
    const concretePhraseFacts = hasConcretePhraseFactSignal(searchableText, { normalizedText });
    const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
    const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");

    let leadScore = 0;
    if (primaryHits > 0) leadScore += primaryHits * 0.08;
    if (anchorHits > 0) leadScore += anchorHits * 0.11;
    if (secondaryHits > 0) leadScore += secondaryHits * 0.08;
    if (factualMetrics.matchedCount >= 2) {
      leadScore += Math.min(0.16, factualMetrics.coverageRatio * 0.14) + factualMetrics.proximityBoost;
    }
    if (sentenceStyle && findingsLike && (anchorHits > 0 || secondaryHits > 0)) leadScore += 0.12;
    if (sentenceStyle && conclusionsLike && anchorHits === 0 && secondaryHits === 0 && primaryHits > 0) leadScore -= 0.08;
    if (sentenceStyle && conclusionsLike && factualMetrics.totalCount >= 2 && factualMetrics.coverageRatio < 0.34 && primaryHits > 0) {
      leadScore -= 0.08;
    }
    if (phraseEvidenceQuery && phraseCoverage.totalCount >= 2) {
      if (phraseCoverage.exactPhrase && concretePhraseFacts) {
        leadScore += findingsLike ? 0.24 : 0.18;
      } else if (phraseCoverage.matchedCount >= phraseCoverage.totalCount && concretePhraseFacts) {
        leadScore += findingsLike ? 0.14 : 0.09;
      }
      if (isGenericHousingServiceStandard(searchableText, { normalizedText }) && !concretePhraseFacts) {
        leadScore -= conclusionsLike ? 0.14 : 0.08;
      }
    }

    if (conclusionsLike) {
      const conclusionSupport =
        issueHits * 0.08 +
        primaryHits * 0.12 +
        (factualMetrics.matchedCount > 0 ? factualMetrics.coverageRatio * 0.08 : 0);
      if (conclusionSupport > bestConclusionSupport) {
        bestConclusionSupport = conclusionSupport;
        bestConclusionChunkId = candidate.row.chunkId;
      }
    }
    if (findingsLike) {
      const findingsSupport =
        anchorHits * 0.12 +
        secondaryHits * 0.1 +
        (factualMetrics.matchedCount > 0 ? factualMetrics.coverageRatio * 0.14 + factualMetrics.proximityBoost : 0);
      if (findingsSupport > bestFindingsSupport) {
        bestFindingsSupport = findingsSupport;
        bestFindingsChunkId = candidate.row.chunkId;
      }
    }

    if (leadScore > bestLeadScore) {
      bestLeadScore = leadScore;
      leadChunkId = candidate.row.chunkId;
    }
  }

  if (bestConclusionSupport >= 0.14 && bestFindingsSupport >= 0.18) {
    docBoost += 0.16;
    reasons.push("document_conclusion_findings_stitch");
    if (sentenceStyle && bestFindingsChunkId && bestFindingsSupport >= Math.max(0.18, bestConclusionSupport + 0.04)) {
      leadChunkId = bestFindingsChunkId;
      leadBoost = Math.max(leadBoost, 0.16);
      reasons.push("document_findings_led_stitch");
    } else if (bestConclusionChunkId) {
      leadChunkId = bestConclusionChunkId;
      leadBoost = Math.max(leadBoost, 0.14);
    }
  }

  if (sentenceStyle && bestLeadScore >= 0.16 && leadChunkId) {
    leadBoost = Math.max(leadBoost, Math.min(0.16, bestLeadScore * 0.6));
  }

  return {
    docBoost: Number(docBoost.toFixed(6)),
    reasons,
    leadChunkId,
    leadBoost: Number(leadBoost.toFixed(6))
  };
}

function representativeChunkDisplayScore(
  candidate: { row: ChunkRow; diagnostics: RankingDiagnostics },
  context: SearchContext
): number {
  const queryDerived = getQueryDerivedContext(context);
  const searchableText = cachedCombinedSearchableText(candidate.row, context);
  const normalizedText = cachedNormalizedSearchableText(candidate.row, context);
  const sentenceStyle = queryDerived.sentenceStyleReasoningQuery;
  const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
  const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
  const primarySignals = queryDerived.primarySignals;
  const sentenceAnchors = queryDerived.normalizedSentenceIssueAnchors;
  const sentenceSecondaryTokens = queryDerived.normalizedSentenceSecondaryTokens;
  const factualMetrics = sentenceFactualTokenMetrics(context.query, searchableText, queryDerived.normalizedSentenceFactualTokens, {
    normalizedText
  });
  const issueTerms = queryDerived.normalizedIssueTerms;
  const proceduralTerms = queryDerived.normalizedProceduralTerms;

  const primaryHits = issueSignalHitCount(normalizedText, primarySignals, {
    normalizedText,
    normalizedSignals: queryDerived.normalizedPrimarySignals
  });
  const anchorHits = sentenceAnchors.filter((term) => normalizedText.includes(term)).length;
  const secondaryHits = sentenceSecondaryTokens.filter((term) => normalizedText.includes(term)).length;
  const issueHits = issueTerms.filter((term) => normalizedText.includes(term)).length;
  const proceduralHits = proceduralTerms.filter((term) => normalizedText.includes(term)).length;

  let score = candidate.diagnostics.rerankScore * 0.12;
  if (primaryHits > 0) score += primaryHits * 0.2;
  if (anchorHits > 0) score += anchorHits * 0.24;
  if (secondaryHits > 0) score += secondaryHits * 0.18;
  if (factualMetrics.matchedCount >= 2) {
    score += Math.min(0.2, factualMetrics.coverageRatio * 0.22) + factualMetrics.proximityBoost;
  }
  if (issueHits > 0) score += Math.min(0.12, issueHits * 0.04);
  if (proceduralHits > 0) score += Math.min(0.1, proceduralHits * 0.04);

  if (sentenceStyle) {
    score += sentencePhraseOverlapScore(context.query, searchableText, {
      queryTokens: queryDerived.sentencePhraseOverlapTokens,
      normalizedText
    });
    score += exactMultiWordPhraseScore(context.query, searchableText, {
      normalizedGroups: queryDerived.normalizedPhraseConceptGroups,
      normalizedQuery: queryDerived.normalizedQuery,
      normalizedText,
      phraseTokens: queryDerived.phraseTokens
    });
    if (conclusionsLike && primaryHits > 0) {
      score += factualMetrics.matchedCount > 0 || anchorHits > 0 || secondaryHits > 0 ? 0.12 : 0.03;
    }
    if (findingsLike && (anchorHits > 0 || secondaryHits > 0)) score += 0.18;
    if (findingsLike && primaryHits > 0) score += 0.08;
    if (findingsLike && factualMetrics.matchedCount >= 2) score += 0.1;
    if (conclusionsLike && anchorHits === 0 && secondaryHits === 0 && primaryHits > 0) score -= 0.16;
    if (conclusionsLike && factualMetrics.totalCount >= 2 && factualMetrics.coverageRatio < 0.34 && primaryHits > 0) score -= 0.1;
  } else {
    if (conclusionsLike && issueHits > 0) score += 0.08;
    if (findingsLike && issueHits > 0) score += 0.06;
  }

  if (isLowSignalStructuralChunkType(candidate.row.sectionLabel || "")) score -= 0.12;
  if (isLowSignalTabularChunkType(candidate.row.sectionLabel || "")) score -= 0.14;
  if (isHousingServicesDefinitionBoilerplate(searchableText, { normalizedText }) && secondaryHits === 0) score -= 0.12;
  if (queryDerived.ownerMoveInQuery && isOwnerMoveInLegalStandardBoilerplate(searchableText, { normalizedText }) && anchorHits === 0) score -= 0.14;

  return Number(score.toFixed(6));
}

export function toSearchResultPassage(
  candidate: { row: ChunkRow; diagnostics: RankingDiagnostics },
  context: SearchContext,
  options?: { kind?: "authority" | "supporting_fact" }
): SearchResultPassage {
  const snippet =
    options?.kind === "supporting_fact"
      ? chooseSupportingFactSnippet(candidate.row.chunkText, context)
      : chooseSnippet(candidate.row.chunkText, context);
  return {
    chunkId: candidate.row.chunkId,
    snippet,
    sectionLabel: candidate.row.sectionLabel,
    sectionHeading: candidate.row.sectionLabel,
    citationAnchor: candidate.row.citationAnchor,
    paragraphAnchor: candidate.row.paragraphAnchor,
    chunkType: normalizeChunkTypeLabel(candidate.row.sectionLabel),
    score: candidate.diagnostics.rerankScore
  };
}

function authorityPassageScore(candidate: { row: ChunkRow; diagnostics: RankingDiagnostics }, context: SearchContext): number {
  const queryDerived = getQueryDerivedContext(context);
  const searchableText = cachedCombinedSearchableText(candidate.row, context);
  const normalizedText = cachedNormalizedSearchableText(candidate.row, context);
  const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
  const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
  const primaryHits = issueSignalHitCount(normalizedText, queryDerived.primarySignals, {
    normalizedText,
    normalizedSignals: queryDerived.normalizedPrimarySignals
  });
  const issueHits = queryDerived.normalizedIssueTerms.filter((term) => normalizedText.includes(term)).length;
  const factualMetrics = sentenceFactualTokenMetrics(context.query, searchableText, queryDerived.normalizedSentenceFactualTokens, {
    normalizedText
  });
  const phraseCoverage = phraseConceptCoverage(context.query, searchableText, {
    normalizedQuery: queryDerived.normalizedQuery,
    normalizedGroups: queryDerived.normalizedPhraseConceptGroups,
    normalizedText
  });

  let score = candidate.diagnostics.rerankScore * 0.18;
  if (conclusionsLike) score += 0.34;
  if (findingsLike) score -= 0.04;
  if (primaryHits > 0) score += primaryHits * 0.14;
  if (issueHits > 0) score += Math.min(0.1, issueHits * 0.03);
  if (queryDerived.sentenceStyleReasoningQuery) {
    score += sentencePhraseOverlapScore(context.query, searchableText, {
      queryTokens: queryDerived.sentencePhraseOverlapTokens,
      normalizedText
    });
    score += exactMultiWordPhraseScore(context.query, searchableText, {
      normalizedGroups: queryDerived.normalizedPhraseConceptGroups,
      normalizedQuery: queryDerived.normalizedQuery,
      normalizedText,
      phraseTokens: queryDerived.phraseTokens
    });
    if (conclusionsLike && factualMetrics.matchedCount > 0) score += 0.08;
    if (conclusionsLike && factualMetrics.matchedCount === 0 && primaryHits > 0) score -= 0.06;
  }
  if (phraseCoverage.matchedCount >= 2) score += Math.min(0.22, phraseCoverage.coverageRatio * 0.12 + phraseCoverage.proximityBoost);
  const normalizedTextContext = { normalizedQuery: queryDerived.normalizedQuery, normalizedText };
  if (hasWaterHeaterDrift(context.query, searchableText, normalizedTextContext)) score -= 0.3;
  if (hasCapitalImprovementCostDrift(context.query, searchableText, normalizedTextContext)) {
    score -= phraseCoverage.matchedCount < phraseCoverage.totalCount ? 0.22 : 0.14;
  }
  score += leakWindowContextAdjustment(context.query, searchableText, normalizedTextContext).score * 0.7;
  if (isLowSignalStructuralChunkType(candidate.row.sectionLabel || "")) score -= 0.16;
  if (isLowSignalTabularChunkType(candidate.row.sectionLabel || "")) score -= 0.18;

  return Number(score.toFixed(6));
}

function hasHeatApplianceDrift(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): boolean {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedText = precomputed?.normalizedText ?? normalize(text || "");
  if (!/\b(?:heat|heating|heater|boiler|radiator\b)/.test(normalizedQuery)) return false;
  if (!/\b(?:oven|stove|range\b)/.test(normalizedText)) return false;
  return !/\bheater\b|\bboiler\b|\bradiator\b|\bsteam heat\b|\bheating system\b|\bpermanent heat\b|\broom temperature\b/.test(normalizedText);
}

function hasWaterHeaterDrift(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): boolean {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!/\b(?:heat|heating|heater|boiler|radiator|winter|cold\b)/.test(normalizedQuery)) return false;
  if (/\b(?:hot water|water heater|water heaters\b)/.test(normalizedQuery)) return false;
  const normalizedText = precomputed?.normalizedText ?? normalize(text || "");
  if (!/\bwater heaters?\b|\bhot water heaters?\b/.test(normalizedText)) return false;
  return !/\bspace heaters?\b|\broom temperature\b|\bpermanent heat\b|\bheating system\b|\bradiator\b|\bsteam heat\b|\bminimum room temperature\b/.test(
    normalizedText
  );
}

function leakWindowContextAdjustment(
  query: string,
  text: string,
  precomputed?: { normalizedQuery?: string; normalizedText?: string }
): { score: number; reason: string | null } {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!isLeakWindowQuery(query, { normalizedQuery })) return { score: 0, reason: null };
  const requiresBathroom = /\bbathroom\b|\bbath\b/.test(normalizedQuery);
  const normalizedTextContext = { normalizedText: precomputed?.normalizedText ?? normalize(text || "") };
  const leakWindow = hasLeakWindowContext(text, normalizedTextContext);
  const bathroom = hasBathroomLocationContext(text, normalizedTextContext);
  const bathroomWindow = hasBathroomWindowContext(text, normalizedTextContext);
  if (leakWindow && !requiresBathroom) {
    return { score: 0.28, reason: "leak_window_context_boost" };
  }
  if (leakWindow && bathroomWindow) {
    return { score: 0.28, reason: "leak_window_bathroom_context_boost" };
  }
  if (leakWindow) {
    if (requiresBathroom) {
      return bathroom
        ? { score: 0.06, reason: "leak_window_context_boost" }
        : { score: -0.12, reason: "leak_window_missing_bathroom_penalty" };
    }
    return { score: 0.18, reason: "leak_window_context_boost" };
  }
  return { score: -0.38, reason: "leak_window_split_evidence_penalty" };
}

function hasCapitalImprovementCostDrift(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): boolean {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedText = precomputed?.normalizedText ?? normalize(text || "");
  if (!/\b(?:heat|heating|heater|boiler|radiator|window|windows|leak|leaky|mold\b)/.test(normalizedQuery)) return false;
  return /\bcapital improvement\b|\bamortiz(?:e|ed|ation)?\b|\bcost of\b|\bcosts\b|\bcertified\b|\bpassthrough\b/.test(normalizedText);
}

function habitabilityCoverageSignals(
  text: string,
  query: string,
  precomputed?: { normalizedText?: string; requiredConditionSignals?: string[] }
): {
  conditionSignalHits: number;
  reportingHits: number;
  repairFailureHits: number;
} {
  const normalizedText = precomputed?.normalizedText ?? normalize(text || "");
  const requiredConditionSignals = precomputed?.requiredConditionSignals ?? requiredHabitabilityPrimarySignals(query);
  const conditionSignalHits = requiredConditionSignals.filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
  const reportingHits = [
    /\breport(?:ed|ing)?\b/g,
    /\bcomplain(?:ed|ing)?\b/g,
    /\bnotified\b/g,
    /\bnotice\b/g,
    /\brepair request\b/g,
    /\bwork order\b/g
  ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
  const repairFailureHits = [
    /\bfailed to repair\b/g,
    /\bdid not repair\b/g,
    /\brefused to repair\b/g,
    /\bnot repaired\b/g,
    /\bfailed to restore\b/g,
    /\bdid not restore\b/g,
    /\brestore service\b/g,
    /\bservice restoration\b/g,
    /\brestored service\b/g,
    /\brestore heat\b/g,
    /\brestore hot water\b/g
  ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
  return {
    conditionSignalHits,
    reportingHits,
    repairFailureHits
  };
}

function supportingFactAnchorDiagnostics(candidate: { row: ChunkRow; diagnostics: RankingDiagnostics }, context: SearchContext) {
  const queryDerived = getQueryDerivedContext(context);
  const searchableText = cachedCombinedSearchableText(candidate.row, context);
  const normalizedText = cachedNormalizedSearchableText(candidate.row, context);
  const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
  const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
  const sentenceStyle = queryDerived.sentenceStyleReasoningQuery;
  const primarySignals = queryDerived.primarySignals;
  const sentenceAnchors = queryDerived.normalizedSentenceIssueAnchors;
  const sentenceSecondaryTokens = queryDerived.normalizedSentenceSecondaryTokens;
  const primarySignalHits = issueSignalHitCount(normalizedText, primarySignals, {
    normalizedText,
    normalizedSignals: queryDerived.normalizedPrimarySignals
  });
  const anchorHits = sentenceAnchors.filter((term) => normalizedText.includes(term)).length;
  const secondaryHits = sentenceSecondaryTokens.filter((term) => normalizedText.includes(term)).length;
  const issueHits = queryDerived.normalizedIssueTerms.filter((term) => normalizedText.includes(term)).length;
  const factualMetrics = sentenceFactualTokenMetrics(context.query, searchableText, queryDerived.normalizedSentenceFactualTokens, {
    normalizedText
  });
  const phraseCoverage = phraseConceptCoverage(context.query, searchableText, {
    normalizedQuery: queryDerived.normalizedQuery,
    normalizedGroups: queryDerived.normalizedPhraseConceptGroups,
    normalizedText
  });
  const habitabilityServiceQuery = queryDerived.habitabilityServiceQuery;

  let factualAnchorScore = 0;
  if (primarySignalHits > 0) factualAnchorScore += Math.min(0.18, primarySignalHits * 0.07);
  if (anchorHits > 0) factualAnchorScore += anchorHits * 0.15;
  if (secondaryHits > 0) factualAnchorScore += secondaryHits * 0.12;
  if (issueHits > 0) factualAnchorScore += Math.min(0.1, issueHits * 0.03);
  if (factualMetrics.matchedCount >= 2) {
    factualAnchorScore += Math.min(0.22, factualMetrics.coverageRatio * 0.24) + factualMetrics.proximityBoost;
  }
  if (queryDerived.ownerMoveInQuery) {
    const occupancyHits = [
      /\boccup(?:y|ied|ancy)\b/g,
      /\bresid(?:e|ed|ency)\b/g,
      /\bmove(?:d)? in\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const failedFollowThroughHits = [
      /\bnever occupied\b/g,
      /\bdid not occupy\b/g,
      /\bfailed to occupy\b/g,
      /\bnever resided\b/g,
      /\bdid not reside\b/g,
      /\bnever moved in\b/g,
      /\bdid not move in\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    factualAnchorScore += Math.min(0.18, occupancyHits * 0.06);
    factualAnchorScore += Math.min(0.22, failedFollowThroughHits * 0.11);
    if (queryDerived.ownerMoveInFollowThroughRequired) {
      if (failedFollowThroughHits > 0) {
        factualAnchorScore += 0.18;
      } else {
        factualAnchorScore -= 0.16;
      }
    }
  }
  if (queryDerived.queryMentionsMold) {
    const moldHits = normalizedText.includes("mold") ? 1 : 0;
    const reportingHits = [
      /\breport(?:ed|ing)?\b/g,
      /\bcomplain(?:ed|ing)?\b/g,
      /\bnotified\b/g,
      /\bnotice\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const repairFailureHits = [
      /\bfailed to repair\b/g,
      /\bdid not repair\b/g,
      /\brefused to repair\b/g,
      /\brepair request\b/g,
      /\bnot repaired\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    if (moldHits && reportingHits > 0) factualAnchorScore += 0.14;
    if (moldHits && repairFailureHits > 0) factualAnchorScore += 0.18;
    factualAnchorScore += Math.min(0.08, reportingHits * 0.03);
    factualAnchorScore += Math.min(0.12, repairFailureHits * 0.04);
    if (moldHits === 0) factualAnchorScore -= 0.18;
  }
  if (habitabilityServiceQuery) {
    const requiredConditionSignals = queryDerived.requiredHabitabilitySignals;
    const conditionSignalHits = requiredConditionSignals.filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
    const conditionHits = [
      /\bmold\b/g,
      /\bhot water\b/g,
      /\bheat(?:ing)?\b/g,
      /\bheater\b/g,
      /\bboiler\b/g,
      /\bradiator\b/g,
      /\brodent\b/g,
      /\bcockroach\b/g,
      /\bbed bugs?\b/g,
      /\bventilation\b/g,
      /\bleak\b/g,
      /\bwater intrusion\b/g,
      /\bplumbing\b/g,
      /\bsewage\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const reportingNoticeHits = [
      /\breport(?:ed|ing)?\b/g,
      /\bcomplain(?:ed|ing)?\b/g,
      /\bnotified\b/g,
      /\bnotice\b/g,
      /\brepair request\b/g,
      /\bwork order\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const restorationHits = [
      /\bfailed to repair\b/g,
      /\bdid not repair\b/g,
      /\brefused to repair\b/g,
      /\bnot repaired\b/g,
      /\bfailed to restore\b/g,
      /\bdid not restore\b/g,
      /\brestore service\b/g,
      /\bservice restoration\b/g,
      /\brestored service\b/g,
      /\brestore heat\b/g,
      /\brestore hot water\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    factualAnchorScore += Math.min(0.12, conditionHits * 0.03);
    factualAnchorScore += Math.min(0.1, reportingNoticeHits * 0.03);
    factualAnchorScore += Math.min(0.16, restorationHits * 0.04);
    factualAnchorScore += Math.min(0.18, conditionSignalHits * 0.08);
    if (conditionHits > 0 && reportingNoticeHits > 0) factualAnchorScore += 0.14;
    if (conditionHits > 0 && restorationHits > 0) factualAnchorScore += 0.18;
    if (findingsLike && conditionHits > 0 && (reportingNoticeHits > 0 || restorationHits > 0)) factualAnchorScore += 0.12;
    if (requiredConditionSignals.length > 0 && conditionSignalHits === 0) factualAnchorScore -= 0.14;
  }
  if (queryDerived.harassmentRetaliationQuery) {
    const harassmentHits = [
      /\bharassment\b/g,
      /\bretaliation\b/g,
      /\btenant harassment\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    const entryNoticeHits = [
      /\bnotice\b/g,
      /\bnotices\b/g,
      /\bentry\b/g,
      /\bentries\b/g,
      /\benter(?:ed|ing)?\b/g,
      /\baccess\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    factualAnchorScore += Math.min(0.16, harassmentHits * 0.05);
    factualAnchorScore += Math.min(0.12, entryNoticeHits * 0.03);
    if (harassmentHits > 0 && entryNoticeHits > 0) factualAnchorScore += 0.14;
    if (findingsLike && harassmentHits > 0) factualAnchorScore += 0.08;
  }
  if (queryDerived.wrongfulEvictionQuery) {
    const lockoutHits = [
      /\blockout\b/g,
      /\blocked out\b/g,
      /\bchanged locks?\b/g,
      /\bdenied access\b/g,
      /\bself-help eviction\b/g,
      /\bself help eviction\b/g,
      /\bshut off utilities\b/g
    ].reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
    factualAnchorScore += Math.min(0.24, lockoutHits * 0.08);
  }

  let score = candidate.diagnostics.rerankScore * 0.1 + factualAnchorScore;
  if (findingsLike) score += 0.34;
  if (conclusionsLike) score -= 0.08;
  if (sentenceStyle && anchorHits === 0 && secondaryHits === 0 && factualMetrics.coverageRatio === 0 && factualAnchorScore < 0.08) {
    score -= 0.28;
  }
  if (phraseCoverage.matchedCount >= 2) {
    score += Math.min(0.36, phraseCoverage.coverageRatio * 0.22 + phraseCoverage.proximityBoost);
  }
  if (phraseCoverage.exactPhrase) score += 0.16;
  const normalizedTextContext = { normalizedQuery: queryDerived.normalizedQuery, normalizedText };
  if (hasWaterHeaterDrift(context.query, searchableText, normalizedTextContext)) score -= 0.32;
  if (hasCapitalImprovementCostDrift(context.query, searchableText, normalizedTextContext)) {
    score -= phraseCoverage.matchedCount < phraseCoverage.totalCount ? 0.24 : 0.16;
  }
  score += leakWindowContextAdjustment(context.query, searchableText, normalizedTextContext).score * 0.7;
  if (isLowSignalStructuralChunkType(candidate.row.sectionLabel || "")) score -= 0.16;
  if (isLowSignalTabularChunkType(candidate.row.sectionLabel || "")) score -= 0.18;

  return {
    score: Number(score.toFixed(6)),
    factualAnchorScore: Number(factualAnchorScore.toFixed(6)),
    anchorHits,
    secondaryHits,
    coverageRatio: Number(factualMetrics.coverageRatio.toFixed(4))
  };
}

export function pickPrimaryAuthorityCandidate(
  candidates: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext
) {
  const hasConclusionsLike = candidates.some((candidate) => isConclusionsLikeSectionLabel(candidate.row.sectionLabel || ""));
  const hasFindingsLike = candidates.some((candidate) => isFindingsLikeSectionLabel(candidate.row.sectionLabel || ""));
  return candidates
    .map((candidate) => {
      let score = authorityPassageScore(candidate, context);
      const conclusionsLike = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
      const findingsLike = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");
      if (hasConclusionsLike && hasFindingsLike) {
        if (conclusionsLike) score += 0.18;
        if (findingsLike) score -= 0.12;
      }
      if (hasConclusionsLike && !conclusionsLike) score -= 0.04;
      return { candidate, score };
    })
    .sort((a, b) => b.score - a.score)[0]?.candidate;
}

export function pickSupportingFactCandidate(
  candidates: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext,
  authorityChunkId?: string,
  source: SupportingFactDebug["source"] = "matched_pool"
) {
  const queryDerived = getQueryDerivedContext(context);
  const sentenceStyle = queryDerived.sentenceStyleReasoningQuery;
  const lockoutSpecificityRequired = queryDerived.lockoutSpecificityRequired;
  const ownerMoveInFollowThroughRequired = queryDerived.ownerMoveInFollowThroughRequired;
  const requiredConditionSignals = queryDerived.requiredHabitabilitySignals;
  const minimumAnchorScore = sentenceStyle ? 0.14 : 0.08;
  const scored = candidates
    .map((candidate) => ({ candidate, diagnostics: supportingFactAnchorDiagnostics(candidate, context) }))
    .sort((a, b) => b.diagnostics.score - a.diagnostics.score)
    .filter((item) => item.candidate.row.chunkId !== authorityChunkId);

  const acceptable = scored.find((item) => {
    const supportLikeSection = isSupportingFactSectionLabel(item.candidate.row.sectionLabel || "");
    const hasMeaningfulFactSignal =
      item.diagnostics.anchorHits > 0 ||
      item.diagnostics.secondaryHits > 0 ||
      item.diagnostics.coverageRatio >= 0.34 ||
      item.diagnostics.factualAnchorScore >= minimumAnchorScore;
    const candidateText = cachedCombinedSearchableText(item.candidate.row, context);
    const normalizedCandidateText = cachedNormalizedSearchableText(item.candidate.row, context);
    const hasRequiredLockoutSignal = !lockoutSpecificityRequired || hasWrongfulEvictionLockoutContext(candidateText);
    const hasRequiredOwnerMoveInFollowThrough =
      !ownerMoveInFollowThroughRequired || hasOwnerMoveInFollowThroughContext(candidateText);
    const hasRequiredConditionSignal =
      requiredConditionSignals.length === 0 ||
      requiredConditionSignals.some((signal) => textContainsIssueSignal(normalizedCandidateText, signal));

    if (!sentenceStyle) {
      return (supportLikeSection || hasMeaningfulFactSignal) && hasRequiredLockoutSignal && hasRequiredOwnerMoveInFollowThrough && hasRequiredConditionSignal;
    }

    if (source === "matched_pool") {
      return (
        supportLikeSection &&
        hasMeaningfulFactSignal &&
        hasRequiredLockoutSignal &&
        hasRequiredOwnerMoveInFollowThrough &&
        hasRequiredConditionSignal
      );
    }

    return (
      (supportLikeSection || hasMeaningfulFactSignal) &&
      hasRequiredLockoutSignal &&
      hasRequiredOwnerMoveInFollowThrough &&
      hasRequiredConditionSignal
    );
  });

  return acceptable;
}

export function shouldRetrySupportingFactFallback(
  layers: {
    primaryAuthorityPassage?: SearchResultPassage;
    supportingFactPassage?: SearchResultPassage;
    supportingFactDebug?: SupportingFactDebug;
  },
  context: SearchContext
): boolean {
  if (!layers.supportingFactPassage) return true;
  const queryDerived = getQueryDerivedContext(context);
  if (!queryDerived.sentenceStyleReasoningQuery) return false;
  const debug = layers.supportingFactDebug;
  if (!debug || debug.source !== "matched_pool") return false;
  return debug.anchorHits === 0 && debug.secondaryHits === 0 && debug.coverageRatio === 0 && debug.factualAnchorScore < 0.08;
}

export function buildDecisionDisplayLayers(
  candidates: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext,
  supportingFactSource: SupportingFactDebug["source"] = "matched_pool"
): {
  primaryAuthorityPassage?: SearchResultPassage;
  supportingFactPassage?: SearchResultPassage;
  supportingFactDebug?: SupportingFactDebug;
} {
  if (candidates.length === 0) return {};

  const primaryAuthorityCandidate = pickPrimaryAuthorityCandidate(candidates, context);
  const supportingFactSelection = pickSupportingFactCandidate(candidates, context, primaryAuthorityCandidate?.row.chunkId, supportingFactSource);
  const supportingFactCandidate = supportingFactSelection?.candidate;

  return {
    primaryAuthorityPassage: primaryAuthorityCandidate ? toSearchResultPassage(primaryAuthorityCandidate, context, { kind: "authority" }) : undefined,
    supportingFactPassage: supportingFactCandidate ? toSearchResultPassage(supportingFactCandidate, context, { kind: "supporting_fact" }) : undefined,
    supportingFactDebug: supportingFactSelection
      ? {
          source: supportingFactSource,
          factualAnchorScore: supportingFactSelection.diagnostics.factualAnchorScore,
          anchorHits: supportingFactSelection.diagnostics.anchorHits,
          secondaryHits: supportingFactSelection.diagnostics.secondaryHits,
          coverageRatio: supportingFactSelection.diagnostics.coverageRatio
        }
      : undefined
  };
}

function decisionLayerSnippetText(layers?: {
  primaryAuthorityPassage?: SearchResultPassage;
  supportingFactPassage?: SearchResultPassage;
  supportingFactDebug?: SupportingFactDebug;
}): string {
  return normalize(
    [
      layers?.primaryAuthorityPassage?.sectionLabel,
      layers?.primaryAuthorityPassage?.snippet,
      layers?.supportingFactPassage?.sectionLabel,
      layers?.supportingFactPassage?.snippet
    ]
      .filter(Boolean)
      .join(" ")
  );
}

function isGenericAweDecisionLayer(layers?: {
  primaryAuthorityPassage?: SearchResultPassage;
  supportingFactPassage?: SearchResultPassage;
  supportingFactDebug?: SupportingFactDebug;
}): boolean {
  const text = decisionLayerSnippetText(layers);
  if (!text) return false;
  return (text.includes("report of alleged wrongful eviction") || containsWholeWord(text, "awe")) && !hasWrongfulEvictionLockoutContext(text);
}

function decisionLayerFingerprint(layers?: {
  primaryAuthorityPassage?: SearchResultPassage;
  supportingFactPassage?: SearchResultPassage;
  supportingFactDebug?: SupportingFactDebug;
}): string {
  return decisionLayerSnippetText(layers)
    .replace(/\b(?:case\s+no\.?|no\.?)\s*[a-z]?\d+\b/g, " ")
    .replace(/\be\d+\b/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 180);
}

export function orderDecisionFirst(
  rows: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext,
  decisionLayerMap?: Map<
    string,
    { primaryAuthorityPassage?: SearchResultPassage; supportingFactPassage?: SearchResultPassage; supportingFactDebug?: SupportingFactDebug }
  >
) {
  const queryDerived = getQueryDerivedContext(context);
  const grouped = new Map<string, Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>>();
  for (const candidate of rows) {
    const current = grouped.get(candidate.row.documentId) || [];
    current.push(candidate);
    grouped.set(candidate.row.documentId, current);
  }

  const groups = Array.from(grouped.entries())
    .map(([documentId, candidates]) => {
      const evidence = buildDocumentEvidenceSummary(candidates, context);
      const adjustedCandidates = candidates.map((candidate) => {
        const leadRowBoost = evidence && evidence.leadChunkId === candidate.row.chunkId ? evidence.leadBoost : 0;
        if (!leadRowBoost && !(evidence?.docBoost)) return candidate;
        return {
          row: candidate.row,
          diagnostics: {
            ...candidate.diagnostics,
            rerankScore: Number((candidate.diagnostics.rerankScore + leadRowBoost).toFixed(6)),
            why: uniq([
              ...candidate.diagnostics.why,
              leadRowBoost > 0 ? `document_factual_lead_boost:${leadRowBoost.toFixed(3)}` : "",
              ...(evidence?.docBoost ? evidence.reasons : [])
            ].filter(Boolean))
          }
        };
      });

      const sorted = adjustedCandidates.slice().sort((a, b) => {
        const diff = b.diagnostics.rerankScore - a.diagnostics.rerankScore;
        if (diff !== 0) return diff;
        return b.row.createdAt.localeCompare(a.row.createdAt);
      });
      const top = sorted[0];
      const supportScore = sorted.slice(1, 3).reduce((sum, item, index) => {
        const weight = index === 0 ? 0.18 : 0.08;
        return sum + item.diagnostics.rerankScore * weight;
      }, 0);
      const uniqueChunkTypes = uniq(sorted.map((item) => normalizeChunkTypeLabel(item.row.sectionLabel || ""))).filter(Boolean).length;
      const strongEvidenceCount = sorted.filter(
        (item) =>
          item.diagnostics.why.some((reason) => reason.startsWith("issue_term_overlap:") || reason.startsWith("procedural_term_overlap:")) ||
          item.diagnostics.why.includes("issue_preferred_chunk_type_boost") ||
          item.diagnostics.why.includes("eviction_protection_authority_boost")
      ).length;
      const docBoost = Math.min(0.14, Math.max(0, sorted.length - 1) * 0.025) + Math.min(0.05, uniqueChunkTypes * 0.015);
      const evidenceBoost = Math.min(0.08, strongEvidenceCount * 0.025);
      const trustBoost = top?.row.isTrustedTier === 1 ? 0.03 : 0;
      const layers = decisionLayerMap?.get(documentId);
      const layerText = decisionLayerSnippetText(layers);
      const authorityText = normalize(
        [layers?.primaryAuthorityPassage?.sectionLabel, layers?.primaryAuthorityPassage?.snippet].filter(Boolean).join(" ")
      );
      const supportText = normalize(
        [layers?.supportingFactPassage?.sectionLabel, layers?.supportingFactPassage?.snippet].filter(Boolean).join(" ")
      );
      const layerTextContext = { normalizedText: layerText };
      const authorityTextContext = { normalizedText: authorityText };
      const supportTextContext = { normalizedText: supportText };
      const supportHasLockoutContext = hasWrongfulEvictionLockoutContext(supportText, supportTextContext);
      let layerBoost = 0;
      const layerReasons: string[] = [];
      if (layers) {
        const phraseConceptContext = { normalizedQuery: queryDerived.normalizedQuery, normalizedGroups: queryDerived.normalizedPhraseConceptGroups };
        const decisionLayerSentenceStyle = queryDerived.sentenceStyleReasoningQuery;
        if (isConclusionsLikeSectionLabel(layers.primaryAuthorityPassage?.sectionLabel || "")) {
          layerBoost += 0.14;
          layerReasons.push("decision_layer_conclusions_authority_boost");
        } else if (decisionLayerSentenceStyle && layers.primaryAuthorityPassage) {
          layerBoost -= 0.04;
          layerReasons.push("decision_layer_non_conclusions_authority_penalty");
        }
        if (layers.supportingFactPassage && isFindingsLikeSectionLabel(layers.supportingFactPassage.sectionLabel || "")) {
          layerBoost += 0.06;
          layerReasons.push("decision_layer_findings_support_boost");
        }
        if (layers.supportingFactDebug) {
          const debug = layers.supportingFactDebug;
          const habitabilitySupportWeight = queryDerived.habitabilityServiceQuery ? 1.35 : 1;
          const supportBoost = Math.min(
            queryDerived.habitabilityServiceQuery ? 0.36 : 0.26,
            (debug.factualAnchorScore * 0.18 + debug.anchorHits * 0.04 + debug.secondaryHits * 0.03 + debug.coverageRatio * 0.12) *
              habitabilitySupportWeight
          );
          if (supportBoost > 0) {
            layerBoost += supportBoost;
            layerReasons.push(`decision_layer_support_score_boost:${supportBoost.toFixed(3)}`);
          }
        }
        if (decisionLayerSentenceStyle && layers.primaryAuthorityPassage && layers.supportingFactPassage) {
          layerBoost += 0.04;
          layerReasons.push("decision_layer_dual_snippet_boost");
        }
        if (queryDerived.wrongfulEvictionIssueQuery && hasWrongfulEvictionLockoutContext(layerText, layerTextContext)) {
          layerBoost += 0.16;
          layerReasons.push("decision_layer_lockout_specific_boost");
        }
        if (queryDerived.lockoutSpecificityRequired) {
          if (supportHasLockoutContext) {
            layerBoost += 0.22;
            layerReasons.push("decision_layer_support_lockout_minimum_boost");
          } else {
            layerBoost -= 0.42;
            layerReasons.push("decision_layer_missing_lockout_support_penalty");
          }
          if (
            !supportHasLockoutContext &&
            (hasHarassmentContext(layerText, layerTextContext) || hasRepairNoticeContext(layerText, layerTextContext))
          ) {
            layerBoost -= 0.16;
            layerReasons.push("decision_layer_generic_awe_overlap_penalty");
          }
        }
        if (queryDerived.habitabilityServiceQuery) {
          const combinedHabitabilityText = `${authorityText} ${supportText}`.trim();
          const habitabilityCoverageContext = { requiredConditionSignals: queryDerived.requiredHabitabilitySignals };
          const authorityCoverage = habitabilityCoverageSignals(authorityText, context.query, {
            normalizedText: authorityText,
            ...habitabilityCoverageContext
          });
          const supportCoverage = habitabilityCoverageSignals(supportText, context.query, {
            normalizedText: supportText,
            ...habitabilityCoverageContext
          });
          const combinedCoverage = habitabilityCoverageSignals(combinedHabitabilityText, context.query, {
            normalizedText: combinedHabitabilityText,
            ...habitabilityCoverageContext
          });
          const layerPhraseCoverage = phraseConceptCoverage(context.query, layerText, { ...phraseConceptContext, normalizedText: layerText });

          if (supportCoverage.conditionSignalHits > 0) {
            layerBoost += 0.18;
            layerReasons.push("decision_layer_habitability_support_condition_boost");
          } else if (layers.supportingFactPassage) {
            layerBoost -= 0.12;
            layerReasons.push("decision_layer_habitability_missing_support_condition_penalty");
          }

          if (combinedCoverage.conditionSignalHits > 0 && combinedCoverage.reportingHits > 0) {
            layerBoost += 0.16;
            layerReasons.push("decision_layer_habitability_condition_notice_stitch");
          }
          if (combinedCoverage.conditionSignalHits > 0 && combinedCoverage.repairFailureHits > 0) {
            layerBoost += 0.18;
            layerReasons.push("decision_layer_habitability_condition_repair_stitch");
          }
          if (
            combinedCoverage.conditionSignalHits > 0 &&
            combinedCoverage.reportingHits > 0 &&
            combinedCoverage.repairFailureHits > 0
          ) {
            layerBoost += 0.22;
            layerReasons.push("decision_layer_habitability_full_fact_pattern_boost");
          }

          if (supportCoverage.reportingHits > 0 || supportCoverage.repairFailureHits > 0) {
            layerBoost += 0.08;
            layerReasons.push("decision_layer_habitability_support_fact_boost");
          }

          if (authorityCoverage.conditionSignalHits > 0 || authorityCoverage.repairFailureHits > 0) {
            layerBoost += 0.06;
            layerReasons.push("decision_layer_habitability_authority_alignment_boost");
          }

          if (layerPhraseCoverage.matchedCount >= 2) {
            layerBoost += Math.min(0.18, layerPhraseCoverage.coverageRatio * 0.08 + layerPhraseCoverage.proximityBoost * 0.6);
            layerReasons.push(`decision_layer_phrase_coverage:${layerPhraseCoverage.matchedCount}/${layerPhraseCoverage.totalCount}`);
          }
          if (layerPhraseCoverage.exactPhrase && hasConcretePhraseFactSignal(layerText, { normalizedText: layerText })) {
            layerBoost += 0.16;
            layerReasons.push("decision_layer_exact_phrase_evidence_boost");
          }
          const layerNormalizedTextContext = { normalizedQuery: queryDerived.normalizedQuery, normalizedText: layerText };
          if (hasWaterHeaterDrift(context.query, layerText, layerNormalizedTextContext)) {
            layerBoost -= 0.42;
            layerReasons.push("decision_layer_water_heater_drift_penalty");
          }
          if (hasCapitalImprovementCostDrift(context.query, layerText, layerNormalizedTextContext)) {
            layerBoost -= layerPhraseCoverage.matchedCount < layerPhraseCoverage.totalCount ? 0.32 : 0.2;
            layerReasons.push("decision_layer_capital_improvement_drift_penalty");
          }
          const layerLeakWindowAdjustment = leakWindowContextAdjustment(context.query, layerText, layerNormalizedTextContext);
          if (layerLeakWindowAdjustment.score !== 0) {
            layerBoost += layerLeakWindowAdjustment.score * 0.7;
            if (layerLeakWindowAdjustment.reason) layerReasons.push(`decision_layer_${layerLeakWindowAdjustment.reason}`);
          }
        }
        if (queryDerived.poopQuery) {
          const combinedPoopLayerText = `${authorityText} ${supportText}`.trim();
          const strongPoopLayer =
            hasStrongPoopDecisionContext(combinedPoopLayerText, { normalizedText: combinedPoopLayerText }) ||
            hasStrongPoopDecisionContext(layerText, { normalizedText: layerText });
          const weakRodentPoopLayer =
            hasWeakRodentPoopContext(combinedPoopLayerText, { normalizedText: combinedPoopLayerText }) ||
            hasWeakRodentPoopContext(layerText, { normalizedText: layerText });
          if (strongPoopLayer) {
            layerBoost += 0.42;
            layerReasons.push("decision_layer_poop_specificity_boost");
          } else if (weakRodentPoopLayer) {
            layerBoost -= 0.42;
            layerReasons.push("decision_layer_poop_rodent_only_penalty");
          }
        }
        if (queryDerived.lockBoxQuery) {
          const authorityHasLockBox = hasLockBoxContext(authorityText, authorityTextContext);
          const supportHasLockBox = hasLockBoxContext(supportText, supportTextContext);
          if (authorityHasLockBox) {
            layerBoost += 0.28;
            layerReasons.push("decision_layer_lock_box_authority_boost");
          } else if (supportHasLockBox) {
            layerBoost -= 0.14;
            layerReasons.push("decision_layer_lock_box_support_only_penalty");
          }
        }
        if (queryDerived.cameraPrivacyQuery) {
          const authorityHasCameraPrivacy = hasCameraPrivacyContext(authorityText, authorityTextContext);
          const supportHasCameraPrivacy = hasCameraPrivacyContext(supportText, supportTextContext);
          const authorityHasPrivacyOnly =
            /\bprivacy\b|\binvasion of privacy\b/.test(authorityText) &&
            !/\bcamera\b|\bcameras\b|\bsurveillance\b|\bsecurity camera\b|\bvideo camera\b|\bvideo monitoring\b/.test(authorityText);
          if (authorityHasCameraPrivacy) {
            layerBoost += 0.3;
            layerReasons.push("decision_layer_camera_privacy_authority_boost");
          } else if (supportHasCameraPrivacy) {
            layerBoost -= 0.16;
            layerReasons.push("decision_layer_camera_privacy_support_only_penalty");
          } else if (authorityHasPrivacyOnly) {
            layerBoost -= 0.22;
            layerReasons.push("decision_layer_camera_privacy_generic_privacy_penalty");
          }
        }
      }

      let displayRows = sorted;
      const displayScored = sorted
        .map((candidate) => ({
          candidate,
          displayScore: representativeChunkDisplayScore(candidate, context)
        }))
        .sort((a, b) => {
          const diff = b.displayScore - a.displayScore;
          if (diff !== 0) return diff;
          return b.candidate.diagnostics.rerankScore - a.candidate.diagnostics.rerankScore;
        });
      const displayLead = displayScored[0];
      if (displayLead) {
        const promotedCandidate =
          displayLead.candidate.row.chunkId === sorted[0]?.row.chunkId
            ? displayLead.candidate
            : {
                row: displayLead.candidate.row,
                diagnostics: {
                  ...displayLead.candidate.diagnostics,
                  why: uniq([
                    ...displayLead.candidate.diagnostics.why,
                    `representative_display_lead:${displayLead.displayScore.toFixed(3)}`
                  ])
                }
              };
        displayRows = [
          promotedCandidate,
          ...sorted.filter((candidate) => candidate.row.chunkId !== displayLead.candidate.row.chunkId)
        ];
      }

      return {
        documentId,
        docScore: Number(
          ((top?.diagnostics.rerankScore || 0) + supportScore + docBoost + evidenceBoost + trustBoost + (evidence?.docBoost || 0) + layerBoost).toFixed(6)
        ),
        rows: displayRows.map((candidate, index) =>
          index === 0 && layerReasons.length
            ? {
                row: candidate.row,
                diagnostics: {
                  ...candidate.diagnostics,
                  why: uniq([...candidate.diagnostics.why, ...layerReasons])
                }
              }
            : candidate
        ),
        hasStrongLockoutFacts:
          Boolean(
            layers?.supportingFactDebug &&
              supportHasLockoutContext &&
              layers.supportingFactDebug.factualAnchorScore >= 0.7
          ) || false,
        hasAnyLockoutFacts: supportHasLockoutContext || hasWrongfulEvictionLockoutContext(layerText, layerTextContext),
        hasPackageDeliveryEvidence:
          hasPackageDeliverySecurityContext(layerText, layerTextContext) ||
          /\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bsign for packages\b|\bdelivery person\b|\bextra keys?\b|\bapprehend\b/.test(
            layerText
          ),
        hasCameraPrivacyAuthorityEvidence: hasCameraPrivacyContext(authorityText, authorityTextContext),
        hasCameraPrivacySupportEvidence: hasCameraPrivacyContext(supportText, supportTextContext),
        isCameraPrivacyGenericLike:
          queryDerived.cameraPrivacyQuery &&
          (/\bprivacy\b|\binvasion of privacy\b/.test(layerText) &&
            !/\bcamera\b|\bcameras\b|\bsurveillance\b|\bsecurity camera\b|\bvideo camera\b|\bvideo monitoring\b/.test(layerText)),
        isPackageSecurityGenericLike:
          queryDerived.packageSecurityQuery &&
          (/housing services are those services provided by the landlord|loss of any tenant housing services|housing services reasonably expected|planning code section 207|accessory dwelling unit|\badu\b/.test(
            layerText
          ) &&
            !(
              hasPackageDeliverySecurityContext(layerText, layerTextContext) ||
              /\bpackage theft\b|\bstolen packages\b|\bmail theft\b|\bmailroom\b|\bsign for packages\b|\bdelivery person\b|\bextra keys?\b|\bapprehend\b/.test(
                layerText
              )
            )),
        hasStrongPoopEvidence: hasStrongPoopDecisionContext(layerText, { normalizedText: layerText }),
        hasWeakRodentPoopEvidence: hasWeakRodentPoopContext(layerText, { normalizedText: layerText }),
        isGenericAweLike: isGenericAweDecisionLayer(layers),
        genericAweFingerprint: decisionLayerFingerprint(layers)
      };
    });

  if (queryDerived.wrongfulEvictionIssueQuery) {
    const hasStrongerLockoutDecision = groups.some((group) => group.hasStrongLockoutFacts);
    if (hasStrongerLockoutDecision) {
      for (const group of groups) {
        if (!group.hasAnyLockoutFacts) {
          group.docScore = Number((group.docScore - 0.22).toFixed(6));
        }
        if (group.isGenericAweLike && !group.hasStrongLockoutFacts) {
          group.docScore = Number((group.docScore - 0.28).toFixed(6));
        }
        if (group.hasAnyLockoutFacts && !group.hasStrongLockoutFacts) {
          group.docScore = Number((group.docScore - 0.08).toFixed(6));
        }
      }
      const preSorted = groups
        .slice()
        .sort((a, b) => {
          const diff = b.docScore - a.docScore;
          if (diff !== 0) return diff;
          return a.documentId.localeCompare(b.documentId);
        });
      const seenGenericAweFingerprints = new Set<string>();
      for (const group of preSorted) {
        if (!group.isGenericAweLike) continue;
        const fingerprint = group.genericAweFingerprint;
        if (!fingerprint) continue;
        if (seenGenericAweFingerprints.has(fingerprint)) {
          group.docScore = Number((group.docScore - 0.26).toFixed(6));
          continue;
        }
        seenGenericAweFingerprints.add(fingerprint);
      }
    }
  }

  if (queryDerived.packageSecurityQuery) {
    const hasSpecificPackageDecision = groups.some((group) => group.hasPackageDeliveryEvidence);
    if (hasSpecificPackageDecision) {
      for (const group of groups) {
        if (group.hasPackageDeliveryEvidence) {
          group.docScore = Number((group.docScore + 0.18).toFixed(6));
        } else if (group.isPackageSecurityGenericLike) {
          group.docScore = Number((group.docScore - 0.42).toFixed(6));
        } else {
          group.docScore = Number((group.docScore - 0.12).toFixed(6));
        }
      }
    }
  }

  if (queryDerived.cameraPrivacyQuery) {
    const hasSpecificCameraPrivacyDecision = groups.some((group) => group.hasCameraPrivacyAuthorityEvidence);
    if (hasSpecificCameraPrivacyDecision) {
      for (const group of groups) {
        if (group.hasCameraPrivacyAuthorityEvidence) {
          group.docScore = Number((group.docScore + 0.26).toFixed(6));
        } else if (group.hasCameraPrivacySupportEvidence) {
          group.docScore = Number((group.docScore - 0.16).toFixed(6));
        } else if (group.isCameraPrivacyGenericLike) {
          group.docScore = Number((group.docScore - 0.32).toFixed(6));
        } else {
          group.docScore = Number((group.docScore - 0.12).toFixed(6));
        }
      }
    }
  }

  if (queryDerived.poopQuery) {
    const hasStrongPoopDecision = groups.some((group) => group.hasStrongPoopEvidence);
    if (hasStrongPoopDecision) {
      for (const group of groups) {
        if (group.hasStrongPoopEvidence) {
          group.docScore = Number((group.docScore + 0.34).toFixed(6));
        } else if (group.hasWeakRodentPoopEvidence) {
          group.docScore = Number((group.docScore - 0.44).toFixed(6));
        } else {
          group.docScore = Number((group.docScore - 0.12).toFixed(6));
        }
      }
    }
  }

  return groups
    .sort((a, b) => {
      const diff = b.docScore - a.docScore;
      if (diff !== 0) return diff;
      return a.documentId.localeCompare(b.documentId);
    })
    .flatMap((group) => group.rows);
}

function hasDecisionScopedSignal(diagnostics: RankingDiagnostics): boolean {
  return (
    diagnostics.lexicalScore > 0 ||
    diagnostics.vectorScore > 0 ||
    diagnostics.exactPhraseBoost > 0 ||
    diagnostics.citationBoost > 0 ||
    diagnostics.metadataBoost > 0 ||
    diagnostics.partyNameBoost > 0 ||
    diagnostics.judgeNameBoost > 0
  );
}

function hasRelaxedCombinedFilterRecoverySignal(
  row: ChunkRow,
  diagnostics: RankingDiagnostics,
  context: SearchContext
): boolean {
  if (hasDecisionScopedSignal(diagnostics)) return true;

  const conclusionsLikeChunk = isConclusionsLikeSectionLabel(row.sectionLabel || "");
  const findingsLikeChunk = isFindingsLikeSectionLabel(row.sectionLabel || "");
  const sectionPriorityChunk = conclusionsLikeChunk || findingsLikeChunk;

  if (!sectionPriorityChunk) return false;
  if (diagnostics.rerankScore >= 0.22) return true;
  if (chunkMatchesIssueTerms(row, context) || chunkMatchesProceduralTerms(row, context)) return true;
  if (diagnostics.metadataBoost > 0 || diagnostics.judgeNameBoost > 0) return true;
  return diagnostics.sectionBoost >= 0.14;
}

function applyCombinedFilterRecoveryBoost(
  candidate: { row: ChunkRow; diagnostics: RankingDiagnostics },
  context: SearchContext
) {
  const structuredFilterKinds = getQueryDerivedContext(context).activeStructuredFilterKinds;
  if (structuredFilterKinds.length < 2) return candidate;

  let recoveryBoost = 0;
  const reasons: string[] = [];
  const conclusionsLikeChunk = isConclusionsLikeSectionLabel(candidate.row.sectionLabel || "");
  const findingsLikeChunk = isFindingsLikeSectionLabel(candidate.row.sectionLabel || "");

  if (conclusionsLikeChunk) {
    recoveryBoost += 0.08;
    reasons.push("combined_filter_zero_hit_recovery_conclusions");
  } else if (findingsLikeChunk) {
    recoveryBoost += 0.04;
    reasons.push("combined_filter_zero_hit_recovery_findings");
  }
  if (candidate.diagnostics.metadataBoost > 0) {
    recoveryBoost += 0.04;
    reasons.push("combined_filter_zero_hit_recovery_metadata");
  }
  if (candidate.diagnostics.judgeNameBoost > 0) {
    recoveryBoost += 0.03;
    reasons.push("combined_filter_zero_hit_recovery_judge");
  }
  if (candidate.diagnostics.exactPhraseBoost > 0 || candidate.diagnostics.lexicalScore >= 0.2) {
    recoveryBoost += 0.03;
    reasons.push("combined_filter_zero_hit_recovery_text");
  }
  if (recoveryBoost <= 0) return candidate;

  return {
    row: candidate.row,
    diagnostics: {
      ...candidate.diagnostics,
      rerankScore: Number((candidate.diagnostics.rerankScore + recoveryBoost).toFixed(6)),
      why: uniq([...candidate.diagnostics.why, ...reasons])
    }
  };
}

function buildIssueFamilyFallbackCandidates(
  base: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext,
  explicitJudgeFilters: string[],
  section8UdDocumentSupportIds: Set<string> = new Set()
) {
  const queryDerived = getQueryDerivedContext(context);
  const guarded = base
    .filter(({ row }) => !queryDerived.judgeDrivenQuery || rowMatchesReferencedJudge(row, context.query, explicitJudgeFilters))
    .filter(({ row }) => {
      if (!queryDerived.strongIssueEvidenceRequired) return true;
      const searchableText = cachedCombinedSearchableText(row, context);
      const normalizedText = cachedNormalizedSearchableText(row, context);
      return !hasWrongContextForQuery(context.query, searchableText, { normalizedQuery: queryDerived.normalizedQuery, normalizedText });
    })
    .filter(
      ({ row, diagnostics }) =>
        !(
          !queryDerived.structuralIntent &&
          context.queryType !== "citation_lookup" &&
          hasSevereExtractionArtifact(row.chunkText) &&
          diagnostics.lexicalScore < 0.6
        )
    )
    .filter(
      ({ row, diagnostics }) =>
        !(
          !queryDerived.structuralIntent &&
          context.queryType !== "citation_lookup" &&
          diagnostics.lexicalScore === 0 &&
          diagnostics.vectorScore > 0 &&
          (isLowSignalVectorOnlyChunkType(row.sectionLabel || "") || hasMalformedDocxArtifact(row.chunkText))
        )
    );

  const familyMatches = guarded.filter(({ row, diagnostics }) => {
    const searchableText = cachedCombinedSearchableText(row, context);
    const normalizedText = cachedNormalizedSearchableText(row, context);

    if (queryDerived.ownerMoveInFollowThroughRequired) {
      const conclusionsOccupancyProxy =
        isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
        hasOwnerMoveInOccupancyStandardContext(searchableText, { normalizedText }) &&
        diagnostics.sectionBoost >= 0.14;
      return (
        (hasOwnerMoveInContext(searchableText, { normalizedText }) || conclusionsOccupancyProxy) &&
        (
          (
            (hasOwnerMoveInFollowThroughContext(searchableText, { normalizedText }) || normalizedText.includes("owner occupancy")) &&
            (diagnostics.lexicalScore >= 0.2 || diagnostics.vectorScore >= 0.55)
          ) ||
          conclusionsOccupancyProxy
        )
      );
    }

    if (queryDerived.section8UdQuery) {
      return (
        (
          hasSection8Context(searchableText, { normalizedText }) &&
          (hasUnlawfulDetainerContext(searchableText, { normalizedText }) || /\beviction\b/.test(normalizedText)) &&
          (diagnostics.lexicalScore >= 0.25 || diagnostics.vectorScore >= 0.55)
        ) ||
        (
          chunkQualifiesForSection8UdDocumentSupport(row, diagnostics, section8UdDocumentSupportIds, context) &&
          (diagnostics.lexicalScore >= 0.1 || diagnostics.vectorScore >= 0.45)
        )
      );
    }

    if (queryDerived.buyoutPressureQuery) {
      return (
        hasBuyoutContext(searchableText, { normalizedText }) &&
        (hasBuyoutPressureContext(searchableText, { normalizedText }) || /coerc|pressur|threat|harass/.test(normalizedText)) &&
        (diagnostics.lexicalScore >= 0.25 || diagnostics.vectorScore >= 0.55)
      );
    }

    return false;
  });

  return familyMatches.map((candidate) => ({
    row: candidate.row,
    diagnostics: {
      ...candidate.diagnostics,
      rerankScore: Number((candidate.diagnostics.rerankScore + 0.02).toFixed(6)),
      why: uniq([...candidate.diagnostics.why, "issue_family_zero_hit_fallback"])
    }
  }));
}

export function buildDecisionScopedCandidates(
  rows: ChunkRow[],
  vectorScores: Map<string, number>,
  context: SearchContext,
  decisionScopeDocumentIds: string[],
  explicitJudgeFilters: string[],
  options?: { relaxedCombinedFilterRecovery?: boolean }
) {
  const relaxedCombinedFilterRecovery = Boolean(options?.relaxedCombinedFilterRecovery);
  const queryDerived = getQueryDerivedContext(context);
  const section8UdDocumentSupportIds = queryDerived.section8UdQuery
    ? buildSection8UdDocumentSupportSet(rows, context)
    : new Set<string>();

  const base = rows
    .map((row) => {
      const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
      return { row, diagnostics };
    })
    .filter(({ row }) => decisionScopeDocumentIds.includes(row.documentId))
    .filter(({ row }) => rowMatchesQueryGuard(row, context.query, context))
    .filter(({ row }) => chunkTypeMatchesFilter(row.sectionLabel, context.filters.chunkType));

  if (!relaxedCombinedFilterRecovery) {
    const strict = base
      .filter(({ row }) => !queryDerived.judgeDrivenQuery || rowMatchesReferencedJudge(row, context.query, explicitJudgeFilters))
      .filter(
        ({ row, diagnostics }) =>
          !(
            queryDerived.conditionIssueQuery &&
            isIssueDisfavoredChunkType(row.sectionLabel || "") &&
            !chunkMatchesIssueTerms(row, context) &&
            diagnostics.lexicalScore < 0.2
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            (queryDerived.conditionIssueQuery || queryDerived.noticeProceduralQuery) &&
            isLowValueIssueIntentChunkType(row.sectionLabel || "") &&
            !chunkMatchesIssueTerms(row, context) &&
            !chunkMatchesProceduralTerms(row, context) &&
            diagnostics.lexicalScore < 0.24
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            queryDerived.coolingIssueQuery &&
            !chunkMatchesIssueTerms(row, context) &&
            ((diagnostics.lexicalScore === 0 && diagnostics.vectorScore > 0) || diagnostics.lexicalScore < 0.3) &&
            !/findings? of fact|order/i.test(row.sectionLabel || "")
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            queryDerived.coolingIssueQuery &&
            !chunkMatchesIssueTerms(row, context) &&
            diagnostics.lexicalScore < 0.35
          )
      )
      .filter(
        ({ row }) => {
          if (!queryDerived.strongIssueEvidenceRequired) return true;
          const searchableText = cachedCombinedSearchableText(row, context);
          const normalizedText = cachedNormalizedSearchableText(row, context);
          return !hasWrongContextForQuery(context.query, searchableText, { normalizedQuery: queryDerived.normalizedQuery, normalizedText });
        }
      )
      .filter(
        ({ row, diagnostics }) =>
          !(() => {
            if (!queryDerived.strongIssueEvidenceRequired) return false;
            const normalizedText = cachedNormalizedSearchableText(row, context);
            const issueHits = queryDerived.normalizedIssueTerms.filter((term) => normalizedText.includes(term)).length;
            const proceduralHits = queryDerived.normalizedProceduralTerms.filter((term) => normalizedText.includes(term)).length;
            // NS-36: the lexical bar for killing a row is 0.7 only because a live vector channel gets
            // its own say (vectorScore >= 0.72 saves the row). When the vector channel produced NO
            // signal at all (unavailable binding or zero matches — vectorScores is empty), that
            // alternative is structurally unmeetable and the un-corroborated 0.7 bar eliminated every
            // lexical rescue row for vector-first issue queries. Without vector corroboration, only
            // clearly-weak rows (lexical < 0.35) are dropped; the -0.18 strong-evidence penalty and
            // ranking handle the rest.
            const lexicalKillBar = vectorScores.size === 0 ? 0.35 : 0.7;
            return (
              !hasStrongIssueEvidence(context.query, row, issueHits, proceduralHits, context) &&
              !(queryDerived.section8UdQuery && chunkQualifiesForSection8UdDocumentSupport(row, diagnostics, section8UdDocumentSupportIds, context)) &&
              diagnostics.lexicalScore < lexicalKillBar &&
              diagnostics.vectorScore < 0.72
            );
          })()
      )
      .filter(
        ({ row, diagnostics }) =>
          !(() => {
            if (!queryDerived.ownerMoveInFollowThroughRequired) return false;
            const searchableText = cachedCombinedSearchableText(row, context);
            const normalizedText = cachedNormalizedSearchableText(row, context);
            const conclusionsOccupancyProxy =
              isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
              hasOwnerMoveInOccupancyStandardContext(searchableText, { normalizedText }) &&
              diagnostics.sectionBoost >= 0.14;
            return (
              ((!hasOwnerMoveInContext(searchableText, { normalizedText }) && !conclusionsOccupancyProxy) ||
                (!hasOwnerMoveInFollowThroughContext(searchableText, { normalizedText }) && !conclusionsOccupancyProxy)) &&
              diagnostics.lexicalScore < 0.88 &&
              diagnostics.vectorScore < 0.82
            );
          })()
      )
      .filter(
        ({ row, diagnostics }) => {
          const searchableText = cachedCombinedSearchableText(row, context);
          const normalizedText = cachedNormalizedSearchableText(row, context);
          return !(
            queryDerived.section8UdQuery &&
            (!hasSection8Context(searchableText, { normalizedText }) ||
              !hasUnlawfulDetainerContext(searchableText, { normalizedText })) &&
            !chunkQualifiesForSection8UdDocumentSupport(row, diagnostics, section8UdDocumentSupportIds, context) &&
            diagnostics.lexicalScore < 0.92 &&
            diagnostics.vectorScore < 0.84
          );
        }
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            !queryDerived.structuralIntent &&
            context.queryType !== "citation_lookup" &&
            hasSevereExtractionArtifact(row.chunkText) &&
            diagnostics.lexicalScore < 0.6
          )
      )
      .filter(
        ({ row, diagnostics }) =>
          !(
            !queryDerived.structuralIntent &&
            context.queryType !== "citation_lookup" &&
            diagnostics.lexicalScore === 0 &&
            diagnostics.vectorScore > 0 &&
            (isLowSignalVectorOnlyChunkType(row.sectionLabel || "") || hasMalformedDocxArtifact(row.chunkText))
          )
      )
      .filter(({ diagnostics }) => hasDecisionScopedSignal(diagnostics));

    if (strict.length > 0) return strict;
    return buildIssueFamilyFallbackCandidates(base, context, explicitJudgeFilters, section8UdDocumentSupportIds);
  }

  return base
    .filter(
      ({ row, diagnostics }) =>
        !(
          !queryDerived.structuralIntent &&
          context.queryType !== "citation_lookup" &&
          hasSevereExtractionArtifact(row.chunkText) &&
          diagnostics.lexicalScore < 0.4 &&
          diagnostics.sectionBoost < 0.1
        )
    )
    .filter(
      ({ row, diagnostics }) =>
        !(
          !queryDerived.structuralIntent &&
          context.queryType !== "citation_lookup" &&
          diagnostics.lexicalScore === 0 &&
          diagnostics.vectorScore > 0 &&
          hasMalformedDocxArtifact(row.chunkText) &&
          !isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
          !isFindingsLikeSectionLabel(row.sectionLabel || "")
        )
    )
    .filter(({ row, diagnostics }) => hasRelaxedCombinedFilterRecoverySignal(row, diagnostics, context))
    .map((candidate) => applyCombinedFilterRecoveryBoost(candidate, context));
}

export function diversify(rows: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>, context: SearchContext, limit: number) {
  const maxPerDocument =
    context.filters.documentId
      ? Math.max(3, limit)
      : context.queryType === "rules_ordinance" ||
          context.queryType === "index_code" ||
          context.queryType === "citation_lookup" ||
          context.queryType === "keyword" ||
          // exact_phrase caps at 1 like keyword: result rows are case-level (the decision-layer
          // overlay replaces chunkId/anchor/snippet with the document's authority passage), so a
          // second chunk of the same document renders as an identical duplicate row (NS-03).
          context.queryType === "exact_phrase"
        ? 1
        : 2;
  const perDoc = new Map<string, number>();
  const seenAnchors = new Set<string>();
  const selectedAnchorOrdinalsByDoc = new Map<string, number[]>();
  const out: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }> = [];

  for (const candidate of rows) {
    if (seenAnchors.has(candidate.row.citationAnchor)) continue;
    const count = perDoc.get(candidate.row.documentId) ?? 0;
    if (count >= maxPerDocument) continue;
    if (context.queryType === "citation_lookup") {
      const candidateOrdinal = parseAnchorOrdinal(candidate.row.citationAnchor || "");
      const selectedOrdinals = selectedAnchorOrdinalsByDoc.get(candidate.row.documentId) || [];
      if (
        candidateOrdinal != null &&
        selectedOrdinals.some((ord) => Math.abs(Number(ord) - candidateOrdinal) <= 1)
      ) {
        continue;
      }
      candidate.diagnostics.why = uniq([...candidate.diagnostics.why, "citation_doc_cap=1", "citation_anchor_neighbor_guard=1"]);
    }

    seenAnchors.add(candidate.row.citationAnchor);
    perDoc.set(candidate.row.documentId, count + 1);
    const ordinal = parseAnchorOrdinal(candidate.row.citationAnchor || "");
    if (ordinal != null) {
      selectedAnchorOrdinalsByDoc.set(candidate.row.documentId, [...(selectedAnchorOrdinalsByDoc.get(candidate.row.documentId) || []), ordinal]);
    }
    out.push(candidate);

    if (out.length >= limit) break;
  }

  return out;
}

export function applyLowSignalStructuralGuard(
  rows: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>,
  context: SearchContext,
  limit: number
) {
  const queryDerived = getQueryDerivedContext(context);
  if (queryDerived.structuralIntent || context.queryType === "citation_lookup") {
    return rows.slice(0, limit);
  }

  const maxLowSignalInTop = Math.max(1, Math.floor(limit / 5));
  let lowSignalCount = 0;
  const preferred: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }> = [];
  const overflow: Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }> = [];

  for (const candidate of rows) {
    const lowSignal = isLowSignalStructuralChunkType(candidate.row.sectionLabel || "");
    if (!lowSignal) {
      preferred.push(candidate);
      continue;
    }
    if (lowSignalCount < maxLowSignalInTop) {
      preferred.push(candidate);
      lowSignalCount += 1;
      continue;
    }
    overflow.push(candidate);
  }

  return [...preferred, ...overflow].slice(0, limit);
}
