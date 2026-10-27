// Shared pure evidence and ranking policies; request-owned caches stay on SearchContext.
import { canonicalizeJudgeName, inferJudgeFromTextFragments, judgeSearchTerms, normalizeJudgeLookupKey, queryReferencesJudge, sanitizeDisplayJudgeName } from "./judges";
import { normalizeFilterValue } from "./legal-references";
import { meaningfulPhraseTokens, phraseConceptCoverage, phraseConceptVariantsForToken, wholePhraseIndexInNormalizedText } from "./search-concepts";
import { buildAdaptiveRecallConfig, getQueryDerivedContext, isConclusionsLikeSectionLabel, isFindingsLikeSectionLabel, normalizeChunkTypeLabel, requiredHabitabilityPrimarySignals, sentenceIssueAnchorTerms, sentenceSecondaryFactTokens, textContainsIssueSignal } from "./search-query-analysis";
import { hasAccommodationContext, hasAdjudicatedContext, hasBathroomLocationContext, hasBathroomWindowContext, hasBuyoutContext, hasBuyoutPressureContext, hasCameraPrivacyContext, hasCaregiverContext, hasCoLivingContext, hasCollegeContext, hasCommonAreasContext, hasCoolingProxyDrift, hasDivorceContext, hasDogContext, hasGarageSpaceContext, hasHarassmentContext, hasHomeownersExemptionContext, hasIntercomContext, hasLeakWindowContext, hasMootContext, hasNuisanceContext, hasOwnerMoveInContext, hasOwnerMoveInFollowThroughContext, hasOwnerMoveInOccupancyStandardContext, hasOwnerMoveInPhrase, hasPackageDeliverySecurityContext, hasPetPolicyDrift, hasPoopContext, hasRemoteWorkContext, hasRentReductionContext, hasRepairNoticeContext, hasSection827RentIncreaseDrift, hasSection8Context, hasSelfEmployedContext, hasSocialMediaContext, hasStairsContext, hasUnlawfulDetainerContext, hasWrongfulEvictionContext, hasWrongfulEvictionLockoutContext, hasWrongfulEvictionPhrase, isAccommodationQuery, isAdjudicatedQuery, isBuyoutQuery, isCapitalImprovementBoilerplate, isCaregiverQuery, isCollegeQuery, isCoolingIssueQuery, isDivorceQuery, isDogQuery, isEvictionProtectionQuery, isLeakWindowQuery, isMootQuery, isNuisanceQuery, isPackageSecurityQuery, isPoopQuery, isRemoteWorkQuery, isRentReductionQuery, isSection8UnlawfulDetainerQuery, isSelfEmployedQuery, isShortAlphabeticQuery, isSocialMediaQuery } from "./search-query-classification";
import { containsWholeWord, meaningfulLexicalTokens, normalize, normalizeWhitespace, uniq, wholeWordCountRegex, wholeWordRegex } from "./search-text";
import type { ChunkRow, QueryIntent, RankingDiagnostics, RowMetadata, SearchContext, SearchResultPassage, SupportingFactDebug } from "./search-types";



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


export const VECTOR_SKIP_BROAD_ISSUE_TERMS = [
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


export const VECTOR_FIRST_ISSUE_TERMS = [
  "harassment",
  "buyout",
  "capital improvement"
] as const;


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


export function marketConditionReasoningScore(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): number {
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


export function cachedRowMetadata(row: ChunkRow, context: SearchContext): RowMetadata {
  const cached = context.rowMetadataCache?.get(row.chunkId);
  if (cached) return cached;
  const metadata = buildRowMetadata(row);
  if (!context.rowMetadataCache) context.rowMetadataCache = new Map();
  context.rowMetadataCache.set(row.chunkId, metadata);
  return metadata;
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


export function sentenceFactualTokenMetrics(
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


export function issueSignalHitCount(text: string, signals: string[], precomputed?: { normalizedText?: string; normalizedSignals?: string[] }): number {
  return signals.filter((signal, index) =>
    textContainsIssueSignal(text, signal, {
      normalizedText: precomputed?.normalizedText,
      normalizedSignal: precomputed?.normalizedSignals?.[index]
    })
  ).length;
}


export function rowMatchesReferencedJudge(row: ChunkRow, query: string, explicitJudgeFilters?: string[]): boolean {
  const rowJudge = canonicalizeJudgeName(row.authorName);
  if (!rowJudge) return false;
  const candidates = explicitJudgeFilters && explicitJudgeFilters.length > 0 ? explicitJudgeFilters : queryReferencesJudge(query);
  return candidates.some((judge) => normalizeJudgeLookupKey(judge) === normalizeJudgeLookupKey(rowJudge));
}


export function hasWrongContextForQuery(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): boolean {
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


export function hasStrongIssueEvidence(
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


export function chunkMatchesIssueTerms(row: ChunkRow, context: SearchContext): boolean {
  const queryDerived = getQueryDerivedContext(context);
  const issueTerms = queryDerived.issueTerms;
  if (!issueTerms.length) return false;
  const text = cachedNormalizedSearchableText(row, context);
  return issueTerms.some((term) => text.includes(term));
}


export function chunkMatchesProceduralTerms(row: ChunkRow, context: SearchContext): boolean {
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


export function sectionPriorityBoost(sectionLabel: string): { boost: number; why: string | null } {
  if (isConclusionsLikeSectionLabel(sectionLabel)) {
    return { boost: 0.24, why: "section_priority_conclusions_of_law" };
  }
  if (isFindingsLikeSectionLabel(sectionLabel)) {
    return { boost: 0.14, why: "section_priority_findings_of_fact" };
  }
  return { boost: 0, why: null };
}


export function intentBoostForChunkType(intent: QueryIntent, chunkType: string): number {
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


export function isLowSignalStructuralChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}


export function isLowSignalTabularChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /^(base|monthly|total|cost|allowable_rent_increase_s|cap_imp_pass_through)$/.test(t);
}


export function isIssuePreferredChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /^(findings_of_fact|order|conclusions_of_law|procedural_history|background)$/.test(t);
}


export function isIssueDisfavoredChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return /^(minute_order|allowable_rent_increase_s|cap_imp_pass_through|base|monthly|total|cost|introduction|body)$/.test(t);
}


export function isLowValueIssueIntentChunkType(chunkType: string): boolean {
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


export function isLowSignalVectorOnlyChunkType(chunkType: string): boolean {
  const t = normalizeChunkTypeLabel(chunkType);
  if (!t) return false;
  return isLowSignalTabularChunkType(t) || /^(introduction|body|minute_order)$/.test(t);
}


export function hasMalformedDocxArtifact(text: string): boolean {
  return /<w:[^>]+>/.test(String(text || ""));
}


export function hasSevereExtractionArtifact(text: string): boolean {
  const raw = String(text || "");
  if (!raw) return false;
  return (
    /<w:[^>]+>/.test(raw) ||
    /w:(?:rPr|tabs|spacing|ind|rFonts|sz|lang|tab)\b/.test(raw) ||
    /<\/w:[^>]+>/.test(raw)
  );
}


export function parseAnchorOrdinal(anchor: string): number | null {
  const raw = String(anchor || "");
  if (!raw) return null;
  const match = raw.match(/(?:^|[#:_-])p(\d+)\b/i);
  if (!match) return null;
  const parsed = Number.parseInt(match[1] || "", 10);
  return Number.isFinite(parsed) ? parsed : null;
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


export function hasHeatApplianceDrift(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): boolean {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedText = precomputed?.normalizedText ?? normalize(text || "");
  if (!/\b(?:heat|heating|heater|boiler|radiator\b)/.test(normalizedQuery)) return false;
  if (!/\b(?:oven|stove|range\b)/.test(normalizedText)) return false;
  return !/\bheater\b|\bboiler\b|\bradiator\b|\bsteam heat\b|\bheating system\b|\bpermanent heat\b|\broom temperature\b/.test(normalizedText);
}


export function hasWaterHeaterDrift(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): boolean {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  if (!/\b(?:heat|heating|heater|boiler|radiator|winter|cold\b)/.test(normalizedQuery)) return false;
  if (/\b(?:hot water|water heater|water heaters\b)/.test(normalizedQuery)) return false;
  const normalizedText = precomputed?.normalizedText ?? normalize(text || "");
  if (!/\bwater heaters?\b|\bhot water heaters?\b/.test(normalizedText)) return false;
  return !/\bspace heaters?\b|\broom temperature\b|\bpermanent heat\b|\bheating system\b|\bradiator\b|\bsteam heat\b|\bminimum room temperature\b/.test(
    normalizedText
  );
}


export function leakWindowContextAdjustment(
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


export function hasCapitalImprovementCostDrift(query: string, text: string, precomputed?: { normalizedQuery?: string; normalizedText?: string }): boolean {
  const normalizedQuery = precomputed?.normalizedQuery ?? normalize(query || "");
  const normalizedText = precomputed?.normalizedText ?? normalize(text || "");
  if (!/\b(?:heat|heating|heater|boiler|radiator|window|windows|leak|leaky|mold\b)/.test(normalizedQuery)) return false;
  return /\bcapital improvement\b|\bamortiz(?:e|ed|ation)?\b|\bcost of\b|\bcosts\b|\bcertified\b|\bpassthrough\b/.test(normalizedText);
}


export function habitabilityCoverageSignals(
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


export function decisionLayerSnippetText(layers?: {
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


export function isGenericAweDecisionLayer(layers?: {
  primaryAuthorityPassage?: SearchResultPassage;
  supportingFactPassage?: SearchResultPassage;
  supportingFactDebug?: SupportingFactDebug;
}): boolean {
  const text = decisionLayerSnippetText(layers);
  if (!text) return false;
  return (text.includes("report of alleged wrongful eviction") || containsWholeWord(text, "awe")) && !hasWrongfulEvictionLockoutContext(text);
}
