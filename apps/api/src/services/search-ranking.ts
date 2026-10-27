// Per-row scoring; consumes request budget and delegates evidence policy.
import { canonicalizeJudgeName, normalizeJudgeLookupKey } from "./judges";
import { phraseConceptCoverage, sentencePhraseOverlapScore } from "./search-concepts";
import { cachedNormalizedChunkText, getQueryDerivedContext, isConclusionsLikeSectionLabel, isFindingsLikeSectionLabel, normalizeChunkTypeLabel } from "./search-query-analysis";
import { hasAccommodationContext, hasAdjudicatedContext, hasBuyoutContext, hasBuyoutPressureContext, hasCameraPrivacyContext, hasCaregiverContext, hasCoLivingContext, hasCollegeContext, hasCommonAreasContext, hasConcretePhraseFactSignal, hasCoolingProxyDrift, hasDivorceContext, hasDogContext, hasDogParkContext, hasDogPolicyContext, hasEmploymentAccommodationDrift, hasGarageSpaceContext, hasHarassmentContext, hasHomeownersExemptionContext, hasIntercomContext, hasLockBoxContext, hasMoldCollision, hasMootContext, hasOwnerMoveInContext, hasOwnerMoveInFollowThroughContext, hasPackageDeliverySecurityContext, hasPackageSecurityContext, hasPoopContext, hasPorchContext, hasRemoteWorkContext, hasRepairNoticeContext, hasSection827RentIncreaseDrift, hasSection8Context, hasSection8RehabDrift, hasSelfEmployedContext, hasSocialMediaContext, hasStairsContext, hasUnlawfulDetainerContext, hasWindowsContext, hasWrongfulEvictionContext, hasWrongfulEvictionLockoutContext, isCapitalImprovementBoilerplate, isGenericHousingServiceStandard, isHousingServicesDefinitionBoilerplate, isOwnerMoveInLegalStandardBoilerplate } from "./search-query-classification";
import { uniq } from "./search-text";
import type { ChunkRow, RankingDiagnostics, SearchContext } from "./search-types";
import { cachedCombinedSearchableText, cachedNormalizedSearchableText, cachedRowMetadata, exactMultiWordPhraseScore, hasCapitalImprovementCostDrift, hasHeatApplianceDrift, hasMalformedDocxArtifact, hasSevereExtractionArtifact, hasStrongIssueEvidence, hasWaterHeaterDrift, hasWrongContextForQuery, intentBoostForChunkType, isIssueDisfavoredChunkType, isIssuePreferredChunkType, isLowSignalStructuralChunkType, isLowSignalTabularChunkType, isLowValueIssueIntentChunkType, issueSignalHitCount, leakWindowContextAdjustment, lexicalScore, marketConditionReasoningScore, rowMatchesReferencedJudge, sectionPriorityBoost, sentenceFactualTokenMetrics } from './search-evidence-policy';



// NS-16: bge-base cosines live in ~0.55-0.95, so the raw score's fusion contribution (×0.23) had a
// differentiation spread of ~0.08 — in practice "+0.15 if retrieved at all", not a ranking signal.
// This FIXED affine calibration spreads that band across [0, 1] before the fusion weight. Fixed (not
// per-result-set min-max) so a chunk's score is independent of what else was retrieved — stable
// across pagination and cap changes. Applied ONLY to the fusion term: every guard threshold in the
// codebase (0.72/0.82/0.84/0.45/...) still reads the raw cosine and keeps its tuned meaning. Zero
// stays zero, so environments with an inert vector channel are byte-identical.
function calibratedVectorFusionScore(rawCosine: number): number {
  if (rawCosine <= 0) return 0;
  const calibrated = (rawCosine - 0.55) / 0.35;
  return Math.max(0, Math.min(1, calibrated));
}


export function scoreRow(row: ChunkRow, vectorScore: number, context: SearchContext): RankingDiagnostics {
  context.workBudget?.consume("scoringOperations");
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
