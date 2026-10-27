// Pure source-passage and snippet presentation.
import { phraseConceptCoverage } from "./search-concepts";
import { getQueryDerivedContext, isConclusionsLikeSectionLabel, isFindingsLikeSectionLabel, normalizeChunkTypeLabel } from "./search-query-analysis";
import { hasAccommodationContext, hasBuyoutContext, hasHarassmentContext, hasRepairNoticeContext, hasSection8Context, hasUnlawfulDetainerContext, hasWrongfulEvictionContext, hasWrongfulEvictionLockoutContext, isHousingServicesDefinitionBoilerplate } from "./search-query-classification";
import { containsWholeWord, normalize, uniq } from "./search-text";
import type { ChunkRow, RankingDiagnostics, SearchContext, SearchResultPassage, SupportingFactDebug } from "./search-types";
import { habitabilityCoverageSignals, isGenericAweDecisionLayer, sentenceFactualTokenMetrics } from './search-evidence-policy';



function chooseSnippet(text: string, context: SearchContext): string {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  const maxSnippetChars = Math.max(120, Math.min(1200, Number(context.snippetMaxLength || 260)));
  const queryDerived = getQueryDerivedContext(context);

  if (queryDerived.packageSecurityQuery) {
    const packageTargets = preparedSnippetTargets(context, "package", () => [
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
    ]);

    return chooseSnippetForTargets(normalized, packageTargets, maxSnippetChars, true);
  }

  if (queryDerived.leakWindowQuery) {
    const leakWindowTargets = preparedSnippetTargets(context, "leakWindow", () => [
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
    ]);

    return chooseSnippetForTargets(normalized, leakWindowTargets, maxSnippetChars, true);
  }

  const targets = preparedSnippetTargets(context, "default", () => [
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
  ]);

  return chooseSnippetForTargets(normalized, targets, maxSnippetChars, true);
}


// NS-40: every snippet-target list is a pure function of the query context, but it was rebuilt
// (uniq + trim + length-sort) for EVERY result row. Prepared lists are cached per request keyed on
// the context object (WeakMap — lifecycle follows the request, no type changes).
const preparedSnippetTargetsCache = new WeakMap<SearchContext, Map<string, string[]>>();


function preparedSnippetTargets(context: SearchContext, key: string, build: () => Array<string | null | undefined>): string[] {
  let byKey = preparedSnippetTargetsCache.get(context);
  if (!byKey) {
    byKey = new Map();
    preparedSnippetTargetsCache.set(context, byKey);
  }
  const hit = byKey.get(key);
  if (hit) return hit;
  const prepared = uniq(build())
    .filter((value): value is string => Boolean(value))
    .map((value) => String(value || "").trim())
    .filter(Boolean)
    .sort((a, b) => b.length - a.length);
  byKey.set(key, prepared);
  return prepared;
}


export function chooseSnippetForTargets(normalizedText: string, rawTargets: string[], maxSnippetChars: number, prepared = false): string {
  const normalized = String(normalizedText || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "";
  const targets = prepared
    ? rawTargets
    : uniq(rawTargets)
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

  return chooseSnippetForTargets(normalized, preparedSnippetTargets(context, "facts", () => Array.from(factTargets)), maxSnippetChars, true);
}


export function buildLayeredResultSnippet(
  context: SearchContext,
  primaryAuthorityPassage?: SearchResultPassage,
  supportingFactPassage?: SearchResultPassage,
  supportingFactDebug?: SupportingFactDebug,
  fallbackText?: string
): string {
  const chosen = buildLayeredResultSnippetInner(context, primaryAuthorityPassage, supportingFactPassage, supportingFactDebug, fallbackText);
  // Literal-intent guarantee: when the user quoted exact text, the displayed snippet must SHOW that
  // text — the whole point of the blurb is judging relevance before opening the document. The inner
  // selection optimizes for issue coverage and can lose the quoted words even when its inputs held
  // them (measured: the fact+authority combine branch budget-truncates the authority passage at
  // ~80 chars, chopping a quoted boilerplate chain mid-word). Post-check the FINAL string; if the
  // quoted spans aren't visible, fall back to the first source that does contain them, windowed to
  // the display budget around the spans.
  const literalSpans = (context.literalSpans || []).map((span) => normalize(span)).filter(Boolean);
  if (literalSpans.length === 0) return chosen;
  const showsAllSpans = (text: string) => Boolean(text) && literalSpans.every((span) => normalize(text).includes(span));
  if (showsAllSpans(chosen)) return chosen;
  const maxSnippetChars = Math.max(120, Math.min(1200, Number(context.snippetMaxLength || 260)));
  for (const source of [primaryAuthorityPassage?.snippet, supportingFactPassage?.snippet, fallbackText]) {
    const text = String(source || "").replace(/\s+/g, " ").trim();
    if (!text || !showsAllSpans(text)) continue;
    if (text.length <= maxSnippetChars) return text;
    const literalWindow = chooseSnippetForTargets(
      text,
      preparedSnippetTargets(context, "literal-spans", () => literalSpans),
      maxSnippetChars,
      true
    );
    if (literalWindow && showsAllSpans(literalWindow)) return literalWindow;
    // Spans too far apart for one window (multi-span queries like "denied" "rats" where the words
    // sit in different sentences of the chunk): show a mini-window around EACH span, joined with
    // ellipses, so the reader sees every quoted term in its own context.
    const composed = composeLiteralSpanSnippet(text, literalSpans, maxSnippetChars);
    if (composed && showsAllSpans(composed)) return composed;
  }
  return chosen;
}


function composeLiteralSpanSnippet(text: string, spans: string[], maxSnippetChars: number): string {
  const lowered = text.toLowerCase();
  const perSpanBudget = Math.max(48, Math.floor(maxSnippetChars / spans.length) - 2);
  const parts: string[] = [];
  for (const span of spans) {
    const index = lowered.indexOf(span);
    if (index < 0) return "";
    const pad = Math.max(0, Math.floor((perSpanBudget - span.length) / 2));
    let start = Math.max(0, index - pad);
    let end = Math.min(text.length, index + span.length + pad);
    while (start > 0 && /\S/.test(text[start - 1] || "")) start -= 1;
    while (end < text.length && /\S/.test(text[end] || "")) end += 1;
    parts.push(`${start > 0 ? "…" : ""}${text.slice(start, end).trim()}${end < text.length ? "…" : ""}`);
  }
  return parts.join(" ");
}


function buildLayeredResultSnippetInner(
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
