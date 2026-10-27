// Candidate assembly, ordering and decision presentation; no persistence.
import { phraseConceptCoverage, sentencePhraseOverlapScore } from "./search-concepts";
import { getQueryDerivedContext, isConclusionsLikeSectionLabel, isFindingsLikeSectionLabel, isSupportingFactSectionLabel, normalizeChunkTypeLabel, textContainsIssueSignal } from "./search-query-analysis";
import { hasBuyoutContext, hasBuyoutPressureContext, hasCameraPrivacyContext, hasConcretePhraseFactSignal, hasHarassmentContext, hasLockBoxContext, hasOwnerMoveInContext, hasOwnerMoveInFollowThroughContext, hasOwnerMoveInOccupancyStandardContext, hasPackageDeliverySecurityContext, hasRepairNoticeContext, hasSection8Context, hasStrongPoopDecisionContext, hasUnlawfulDetainerContext, hasWeakRodentPoopContext, hasWrongfulEvictionLockoutContext, isGenericHousingServiceStandard, isHousingServicesDefinitionBoilerplate, isOwnerMoveInLegalStandardBoilerplate } from "./search-query-classification";
import { normalize, uniq } from "./search-text";
import type { ChunkRow, RankingDiagnostics, SearchContext, SearchResultPassage, SupportingFactDebug } from "./search-types";
import { buildSection8UdDocumentSupportSet, cachedCombinedSearchableText, cachedNormalizedSearchableText, chunkMatchesIssueTerms, chunkMatchesProceduralTerms, chunkQualifiesForSection8UdDocumentSupport, chunkTypeMatchesFilter, decisionLayerSnippetText, exactMultiWordPhraseScore, habitabilityCoverageSignals, hasCapitalImprovementCostDrift, hasMalformedDocxArtifact, hasSevereExtractionArtifact, hasStrongIssueEvidence, hasWaterHeaterDrift, hasWrongContextForQuery, isGenericAweDecisionLayer, isIssueDisfavoredChunkType, isLowSignalStructuralChunkType, isLowSignalTabularChunkType, isLowSignalVectorOnlyChunkType, isLowValueIssueIntentChunkType, issueSignalHitCount, leakWindowContextAdjustment, parseAnchorOrdinal, rowMatchesQueryGuard, rowMatchesReferencedJudge, sentenceFactualTokenMetrics } from './search-evidence-policy';
import { toSearchResultPassage } from './search-snippets';
import { scoreRow } from './search-ranking';



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
        // NS-20: equal scores tie-broke on ingestion timestamp alone — identical within a batch, so
      // the residual order was map-insertion order and re-ingesting a document reshuffled results.
      // chunkId is content-stable and makes equal-score ordering deterministic forever.
      const createdDiff = b.row.createdAt.localeCompare(a.row.createdAt);
      if (createdDiff !== 0) return createdDiff;
      return a.row.chunkId.localeCompare(b.row.chunkId);
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

  const decisionScopeDocumentIdSet = new Set(decisionScopeDocumentIds);
  const base = rows
    .map((row) => {
      const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
      return { row, diagnostics };
    })
    .filter(({ row }) => decisionScopeDocumentIdSet.has(row.documentId))
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
    const anchorIdentity=JSON.stringify([candidate.row.documentId,candidate.row.citationAnchor||candidate.row.chunkId]);
    if (seenAnchors.has(anchorIdentity)) continue;
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

    seenAnchors.add(anchorIdentity);
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
