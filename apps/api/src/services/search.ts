import {
  searchDebugRequestSchema,
  searchDebugResponseSchema,
  searchRequestSchema,
  searchResponseSchema,
  type SearchRequest
} from "@beedle/shared";
import type { Env } from "../lib/types";
import type {
  ChunkRow,
  RankingDiagnostics,
  SearchContext,
  SearchResultPassage,
  SupportingFactDebug,
} from "./search-types";
import {
  buildAdaptiveRecallConfig,
  buildQueryDerivedContext,
  buildSearchScope,
  getQueryDerivedContext,
  isConclusionsLikeSectionLabel,
  isFindingsLikeSectionLabel,
  isRetryableSearchError,
  normalizeChunkTypeLabel
} from "./search-query-analysis";
import {
  ensureSearchRuntimeIndexes,
  fetchAuthorityChunksByDocumentIds,
  fetchChunksByDocumentIds,
  fetchChunksByIds,
  fetchFtsMatchingDocumentIds,
  fetchHabitabilityCandidateDocumentIds,
  fetchIssueCandidateDocumentIds,
  fetchKeywordCandidateDocumentIds,
  fetchLockoutCandidateDocumentIds,
  fetchScopedDocumentIds,
  fetchSupportingFactChunksByDocumentIds,
  FTS_SEARCH_ERROR_RESULT,
  ftsSearch,
  lexicalSearch,
  lexicalSearchWholeWord,
  searchFtsAvailable,
  vectorSearchWithDiagnostics
} from "./search-fts";
import {
  KEYWORD_RECALL_UNIVERSE_MAX,
  applyLowSignalStructuralGuard,
  buildCitationFamilySignature,
  buildDecisionDisplayLayers,
  buildDecisionScopedCandidates,
  buildLayeredResultSnippet,
  buildSection8UdDocumentSupportSet,
  cachedCombinedSearchableText,
  cachedNormalizedSearchableText,
  chooseVectorQuery,
  chunkQualifiesForSection8UdDocumentSupport,
  chunkTypeMatchesFilter,
  countBy,
  diversify,
  enhanceQueryWithIndexCodeContext,
  expandQueryForRetrieval,
  hasAnyExactIndexCodeCoverage,
  inferDocumentJudgeNames,
  keywordExecutionTerms,
  orderDecisionFirst,
  phraseFtsSearchLimit,
  pickPrimaryAuthorityCandidate,
  pickSupportingFactCandidate,
  rowMatchesQueryGuard,
  scoreRow,
  shouldRetrySupportingFactFallback,
  shouldSkipVectorSearch,
  toSearchResultPassage
} from "./search-scoring";
import {
  normalize,
  spellCorrectQuery,
  uniq,
  wholeQueryQuotedPhrase
} from "./search-text";
import {
  anyTokenFtsQuery,
  phraseSearchFtsQuery,
  prefixedFtsTermsQuery,
  relaxedPhraseFtsQuery,
  sectionReferenceFtsQuery
} from "./search-concepts";
import {
  hasAccommodationContext,
  hasAdjudicatedContext,
  hasBuyoutContext,
  hasBuyoutPressureContext,
  hasCaregiverContext,
  hasCoLivingContext,
  hasCollegeContext,
  hasDivorceContext,
  hasHomeownersExemptionContext,
  hasMootContext,
  hasOwnerMoveInContext,
  hasOwnerMoveInFollowThroughContext,
  hasOwnerMoveInOccupancyStandardContext,
  hasPoopContext,
  hasRemoteWorkContext,
  hasSection8Context,
  hasSelfEmployedContext,
  hasSocialMediaContext,
  hasUnlawfulDetainerContext
} from "./search-query-classification";
import { sanitizeDisplayJudgeName } from "./judges";
import { effectiveSourceLink } from "./storage";

async function runSearchInternal(
  env: Env,
  parsed: SearchRequest,
  queryType: SearchContext["queryType"],
  includeDiagnostics: boolean,
  internalOptions?: { spellCorrected?: boolean }
) {
  await ensureSearchRuntimeIndexes(env);
  const logStage = (stage: string, details: Record<string, unknown> = {}) => {
    if (!includeDiagnostics) return;
    try {
      console.info("[search-debug]", JSON.stringify({ query: parsed.query, stage, ...details }));
    } catch {
      console.info("[search-debug]", stage);
    }
  };
  // NS-03: a query that is entirely one double-quoted phrase is explicit exact-match intent
  // (Westlaw/Lexis convention). The public route hardcodes keyword, and tokenize strips quotes before
  // any later stage sees them — so the upgrade happens here, where both the public path and the
  // debug endpoint's production-parity mode (queryType "keyword") flow through it. Responses keep
  // echoing the user's original quoted query.
  const requestedQueryType = queryType;
  const requestedQuery = parsed.query;
  if (queryType === "keyword") {
    const quotedPhrase = wholeQueryQuotedPhrase(parsed.query);
    if (quotedPhrase) {
      parsed = { ...parsed, query: quotedPhrase };
      queryType = "exact_phrase";
      logStage("quoted_phrase_upgrade", { phrase: quotedPhrase });
    }
  }
  const totalStartedAt = Date.now();
  let scopeBuildMs = 0;
  let lexicalScopeFetchMs = 0;
  let lexicalSearchMs = 0;
  let vectorSearchMs = 0;
  let vectorChunkFetchMs = 0;
  let initialScoringMs = 0;
  let decisionScopeFetchMs = 0;
  let decisionScopeBuildMs = 0;
  let finalizeResultsMs = 0;
  let lexicalScopeDocumentCount = 0;
  let lexicalRowCount = 0;
  let mergedChunkCount = 0;
  let scoredCount = 0;
  let rerankedCount = 0;
  let decisionScopeDocumentCount = 0;
  let decisionScopeChunkCount = 0;
  const pageWindow = parsed.offset + parsed.limit + parsed.limit;
  const effectiveQuery = enhanceQueryWithIndexCodeContext(parsed.query, parsed.filters);
  const retrievalQuery = expandQueryForRetrieval(effectiveQuery);
  const vectorQuery = chooseVectorQuery(effectiveQuery);
  const context: SearchContext = {
    query: effectiveQuery,
    retrievalQuery,
    vectorQuery,
    queryType,
    filters: parsed.filters,
    snippetMaxLength: parsed.snippetMaxLength
  };
  context.derived = buildQueryDerivedContext(context);
  const queryDerived = getQueryDerivedContext(context);
  const ownerMoveInIssueSearch = queryDerived.retrievalOwnerMoveInIssueQuery;
  const wrongfulEvictionIssueSearch = queryDerived.retrievalWrongfulEvictionIssueQuery;
  const infestationAliasIssueSearch = queryDerived.retrievalInfestationAliasQuery;
  const vectorFirstIssueSearch = queryDerived.vectorFirstIssueQuery;
  const keywordFamilyRecallQuery = queryType === "keyword" && queryDerived.keywordFamilyRecallQuery;
  const lockoutSpecificityRequired = queryDerived.retrievalLockoutSpecificityRequired;
  const habitabilitySpecificityRequired = queryDerived.retrievalHabitabilitySpecificityRequired;
  const directLexicalIssueSearch = ownerMoveInIssueSearch || wrongfulEvictionIssueSearch || infestationAliasIssueSearch;
  const requestedJudges = queryDerived.explicitJudgeFilters;
  const requestedCodes = queryDerived.indexCodeFilterContext.requestedCodes;
  const activeStructuredKinds = queryDerived.activeStructuredFilterKinds;
  // NS-05: phrase-evidence queries keep the phrase-FTS candidate path even under structured filters
  // (judge/index_code/rules/ordinance/party/date). The search scope's WHERE already carries every
  // filter, so the FTS query intersects with it directly; the previous behavior fell back to an
  // arbitrarily-truncated fetchScopedDocumentIds slice (measured: "breach of quiet enjoyment" + a
  // judge with 233 docs searched only 96 of them and missed the densest on-topic decisions).
  // Keyword-family + judge queries are unaffected: bypassScopedKeywordRecall takes precedence.
  const phraseFtsCandidateSearch =
    (queryType === "keyword" || queryType === "exact_phrase") &&
    queryDerived.phraseEvidenceQuery;
  const bypassScopedKeywordRecall = keywordFamilyRecallQuery && requestedJudges.length > 0;
  const exactIndexCodeCoverage = requestedCodes.length > 0 ? await hasAnyExactIndexCodeCoverage(env, parsed.filters) : false;
  const useSoftIndexCodeScope = requestedCodes.length > 0 && !exactIndexCodeCoverage;
  const scopeBuildStartedAt = Date.now();
  const { where, params } = buildSearchScope(parsed, parsed.corpusMode, { useSoftIndexCodeScope });
  const recallConfig = buildAdaptiveRecallConfig(parsed, pageWindow, { activeStructuredFilterKinds: activeStructuredKinds });
  scopeBuildMs = Date.now() - scopeBuildStartedAt;
  logStage("scope_build", {
    ms: scopeBuildMs,
    hasStructuredFilters: recallConfig.hasStructuredFilters,
    issueGuidedSearch: recallConfig.issueGuidedSearch,
    shortBroadIssueSearch: recallConfig.shortBroadIssueSearch,
    ownerMoveInIssueSearch,
    wrongfulEvictionIssueSearch,
    infestationAliasIssueSearch,
    keywordFamilyRecallQuery,
    bypassScopedKeywordRecall,
    lockoutSpecificityRequired,
    habitabilitySpecificityRequired,
    exactIndexCodeCoverage,
    useSoftIndexCodeScope
  });
  const normalizedEffectiveQuery = normalize(effectiveQuery || "");
  const keywordTermsOverride =
    queryType === "keyword"
      ? keywordExecutionTerms(effectiveQuery, {
          normalizedQuery: normalizedEffectiveQuery,
          normalizedGroups: queryDerived.normalizedPhraseConceptGroups
        })
      : undefined;
  logStage("lexical_scope_fetch_start", {
    issueGuidedSearch: recallConfig.issueGuidedSearch,
    shortBroadIssueSearch: recallConfig.shortBroadIssueSearch
  });
  const lexicalScopeStartedAt = Date.now();
  // NS-11: the family+judge universe qualifies documents by an FTS match on the query's execution
  // terms before the recency-ordered cap — so the cap trims the least-recent MATCHES instead of
  // hiding older matching decisions behind non-matching recent ones. Falls back to the pure
  // recency universe when FTS is unavailable or finds nothing (conservative: old behavior).
  const keywordScopedUniverseDocumentIds =
    bypassScopedKeywordRecall
      ? await (async () => {
          const universeCap = Math.min(
            KEYWORD_RECALL_UNIVERSE_MAX,
            Math.max(recallConfig.lexicalScopeDocumentLimit * 8, recallConfig.decisionScopeDocumentLimit * 10, pageWindow * 20, 400)
          );
          if (searchFtsAvailable && (keywordTermsOverride?.length ?? 0) > 0) {
            const matchingUniverse = await fetchFtsMatchingDocumentIds(
              env,
              where,
              params,
              prefixedFtsTermsQuery(keywordTermsOverride ?? []),
              universeCap
            );
            if (matchingUniverse.length > 0) {
              logStage("keyword_universe_fts_matched", { matchingDocumentCount: matchingUniverse.length });
              return matchingUniverse;
            }
          }
          return fetchScopedDocumentIds(
            env,
            where,
            params,
            universeCap
          );
        })()
      : [];
  let lexicalScopeDocumentIds = bypassScopedKeywordRecall
    ? await fetchKeywordCandidateDocumentIds(
        env,
        where,
        params,
        retrievalQuery,
        Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 2, pageWindow * 3, 60),
        keywordScopedUniverseDocumentIds
      )
    : phraseFtsCandidateSearch
      ? []
    : lockoutSpecificityRequired
    ? await fetchLockoutCandidateDocumentIds(
        env,
        where,
        params,
        retrievalQuery,
        Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 3, pageWindow * 3, 18)
      )
    : habitabilitySpecificityRequired
      ? await fetchHabitabilityCandidateDocumentIds(
          env,
          where,
          params,
          retrievalQuery,
          Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 3, pageWindow * 3, 18)
        )
    : recallConfig.issueGuidedSearch && !directLexicalIssueSearch
      ? await fetchIssueCandidateDocumentIds(
          env,
          where,
          params,
          retrievalQuery,
          Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 2, pageWindow * 2)
        )
      : [];
  if (bypassScopedKeywordRecall && lexicalScopeDocumentIds.length === 0 && keywordScopedUniverseDocumentIds.length > 0) {
    lexicalScopeDocumentIds = keywordScopedUniverseDocumentIds.slice(
      0,
      Math.max(recallConfig.lexicalScopeDocumentLimit, recallConfig.decisionScopeDocumentLimit * 2, pageWindow * 3, 60)
    );
  }
  if (!phraseFtsCandidateSearch && !bypassScopedKeywordRecall && lexicalScopeDocumentIds.length === 0 && recallConfig.hasStructuredFilters) {
    lexicalScopeDocumentIds = await fetchScopedDocumentIds(
      env,
      where,
      params,
      recallConfig.lexicalScopeDocumentLimit
    );
  }
  if (
    !phraseFtsCandidateSearch &&
    !bypassScopedKeywordRecall &&
    lexicalScopeDocumentIds.length === 0 &&
    recallConfig.issueGuidedSearch &&
    !directLexicalIssueSearch
  ) {
    if (!vectorFirstIssueSearch) {
      lexicalScopeDocumentIds = await fetchScopedDocumentIds(
        env,
        where,
        params,
        Math.max(recallConfig.decisionScopeDocumentLimit, pageWindow * 2)
      );
    }
  }
  lexicalScopeDocumentCount = lexicalScopeDocumentIds.length;
  lexicalScopeFetchMs = Date.now() - lexicalScopeStartedAt;
  logStage("lexical_scope_fetch", { ms: lexicalScopeFetchMs, lexicalScopeDocumentCount });
  logStage("lexical_search_start", {
    skipLexicalForVectorFirstIssueSearch:
      recallConfig.issueGuidedSearch &&
      vectorFirstIssueSearch &&
      lexicalScopeDocumentIds.length === 0
  });
  const lexicalSearchStartedAt = Date.now();
  const skipLexicalForVectorFirstIssueSearch =
    recallConfig.issueGuidedSearch &&
    vectorFirstIssueSearch &&
    lexicalScopeDocumentIds.length === 0;
  const allowDocumentChunkLexicalSearch =
    recallConfig.issueGuidedSearch || (queryType === "keyword" && lexicalScopeDocumentIds.length > 0);
  // NS-08: a dotted section reference in the query ("37.9(a)(2)", "1942.4") becomes a mandatory FTS
  // phrase arm — its sub-tokens die in lexical tokenization, but the FTS index holds them as adjacent
  // tokens, so the quoted phrase matches the exact reference. Detection is pin-inert (no golden or
  // judged query contains a dotted ref). When concept groups also exist they AND with the reference.
  const sectionReferenceQuery = queryType === "keyword" ? sectionReferenceFtsQuery(parsed.query) : "";
  const conceptPhraseFtsQuery = phraseSearchFtsQuery(effectiveQuery, {
    normalizedQuery: normalizedEffectiveQuery,
    normalizedGroups: queryDerived.normalizedPhraseConceptGroups,
    phraseTokens: queryDerived.phraseTokens
  });
  const phraseFtsQuery = sectionReferenceQuery
    ? [sectionReferenceQuery, conceptPhraseFtsQuery ? `(${conceptPhraseFtsQuery})` : ""].filter(Boolean).join(" AND ")
    : conceptPhraseFtsQuery;
  const phraseFtsEligible =
    !skipLexicalForVectorFirstIssueSearch &&
    (queryType === "keyword" || queryType === "exact_phrase") &&
    phraseFtsQuery.length > 0;
  // NS-30: keyword queries that never qualify for the phrase FTS path and execute a SINGLE lexical
  // term (bare tokens like "rent", misspellings like "habitibility") previously fell straight through
  // to the full-corpus substring scan — 25-40s each. Serve their candidates from the FTS index
  // instead: recall via the prefix-quoted term, then the scan's own match clause, weighted-instr rank
  // expression, and tiebreak order applied to the recalled rows (scanParityRankTerms), which
  // reproduces the scan's output for this class. Multi-term vocabularies (curated families like
  // mold => mold/molds/mildew) stay on the scan: rows matched only via title/author sit at the top of
  // the scan's slate (weights 2.4/1.9) but are unreachable through the FTS index (those columns are
  // not indexed), which was measured to move golden-pinned results. The scan also remains the
  // fallback when FTS is unavailable.
  const keywordFtsFirstQuery =
    !skipLexicalForVectorFirstIssueSearch &&
    queryType === "keyword" &&
    !phraseFtsEligible &&
    searchFtsAvailable &&
    keywordTermsOverride?.length === 1
      ? prefixedFtsTermsQuery(keywordTermsOverride)
      : "";
  let lexicalRows: ChunkRow[] = [];
  if (phraseFtsEligible) {
    lexicalRows = await ftsSearch(
      env,
      where,
      params,
      effectiveQuery,
      phraseFtsSearchLimit(recallConfig, pageWindow),
      lexicalScopeDocumentIds,
      { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, ftsQuery: phraseFtsQuery }
    );
    logStage("phrase_fts_search", { enabled: searchFtsAvailable, rowCount: lexicalRows.length });
  } else if (keywordFtsFirstQuery) {
    lexicalRows = await ftsSearch(
      env,
      where,
      params,
      effectiveQuery,
      recallConfig.lexicalSearchLimit,
      lexicalScopeDocumentIds,
      { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, ftsQuery: keywordFtsFirstQuery, scanParityRankTerms: keywordTermsOverride }
    );
    logStage("keyword_fts_first_search", { enabled: searchFtsAvailable, rowCount: lexicalRows.length });
  }
  if (!skipLexicalForVectorFirstIssueSearch && lexicalRows.length === 0) {
    // NS-04/NS-07 (relaxed phrase tiers): long natural-language queries (5+ meaningful tokens) whose
    // full AND-across-concept-groups FTS query matched nothing retry with only the 4, then 3, most
    // selective groups — the constraint core ("landlord raise rent twice year" → landlord/twice/
    // raise/...) — before any substring scan. Gated to 5+ tokens: no golden query has more than 4
    // meaningful tokens (verified against the fixture), so pinned outputs cannot take these tiers.
    if (queryType === "keyword" && phraseFtsEligible && searchFtsAvailable && (queryDerived.phraseTokens?.length ?? 0) >= 5) {
      for (const keepGroups of [4, 3]) {
        const relaxedFtsQuery = relaxedPhraseFtsQuery(
          effectiveQuery,
          {
            normalizedGroups: queryDerived.normalizedPhraseConceptGroups,
            phraseTokens: queryDerived.phraseTokens
          },
          keepGroups
        );
        if (!relaxedFtsQuery) continue;
        lexicalRows = await ftsSearch(
          env,
          where,
          params,
          effectiveQuery,
          recallConfig.lexicalSearchLimit,
          lexicalScopeDocumentIds,
          { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, ftsQuery: relaxedFtsQuery }
        );
        logStage("relaxed_phrase_fts_search", { keepGroups, rowCount: lexicalRows.length });
        if (lexicalRows.length > 0) break;
      }
    }
  }
  if (!skipLexicalForVectorFirstIssueSearch && lexicalRows.length === 0) {
    // NS-29/NS-30 (futility): the lexicalSearch fallback below is an unindexable substring scan over
    // the full corpus — the 25-30s query class. If the FTS-first path already asked the index with the
    // scan's own (prefix-widened) vocabulary and got nothing, the scan cannot match either and is
    // skipped outright. Otherwise, for phrase-eligible keyword queries whose AND-of-groups FTS query
    // came back empty, probe the index with OR-of-prefix-variants (LIMIT 1): zero matches prove
    // futility the same way; any match runs the scan unchanged — the probe never supplies candidates,
    // so ranked output stays golden-pinned. Futility is only claimed on a GENUINE zero-match — an
    // errored FTS call returns the FTS_SEARCH_ERROR_RESULT sentinel (and a structural failure flips
    // searchFtsAvailable), and both re-checks below then fall back to the scan as before.
    let scanProvenFutile = false;
    if (keywordFtsFirstQuery && searchFtsAvailable && lexicalRows !== FTS_SEARCH_ERROR_RESULT) {
      scanProvenFutile = true;
      logStage("zero_hit_scan_skipped", { via: "keyword_fts_first" });
    } else if (sectionReferenceQuery && searchFtsAvailable && lexicalRows !== FTS_SEARCH_ERROR_RESULT) {
      // NS-08: the query names an exact section reference and the FTS phrase for it matched nothing —
      // the corpus provably does not cite that reference (adjacent-token phrases cover every literal
      // occurrence), so probing/scanning for token fragments would only surface noise.
      scanProvenFutile = true;
      logStage("zero_hit_scan_skipped", { via: "section_reference" });
    } else {
      const futilityProbeFtsQuery =
        queryType === "keyword" && searchFtsAvailable && phraseFtsEligible
          ? anyTokenFtsQuery(effectiveQuery, {
              normalizedGroups: queryDerived.normalizedPhraseConceptGroups,
              phraseTokens: queryDerived.phraseTokens
            })
          : "";
      if (futilityProbeFtsQuery) {
        const probeRows = await ftsSearch(env, where, params, effectiveQuery, 1, lexicalScopeDocumentIds, {
          allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch,
          ftsQuery: futilityProbeFtsQuery
        });
        scanProvenFutile = probeRows.length === 0 && probeRows !== FTS_SEARCH_ERROR_RESULT && searchFtsAvailable;
        logStage("any_token_fts_probe", { matched: probeRows.length > 0 });
        if (scanProvenFutile) logStage("zero_hit_scan_skipped", { via: "any_token_probe" });
        // NS-30c: the probe matched, so the corpus contains SOME variant token — previously that
        // meant paying the full 25-30s substring scan even when only a handful of chunks match
        // (measured: a query whose misspelling literally appears in 4 corpus chunks scanned for
        // 30s to find them). Fetch those rows from the FTS index in scan-parity mode instead (the
        // scan's own match clause, rank expression, and tiebreaks over the FTS-recalled rows). If
        // parity-FTS returns nothing despite the probe match (title/author-only corpus presence,
        // which the FTS index cannot recall), the scan below still runs.
        if (!scanProvenFutile && probeRows.length > 0 && searchFtsAvailable) {
          lexicalRows = await ftsSearch(
            env,
            where,
            params,
            effectiveQuery,
            recallConfig.lexicalSearchLimit,
            lexicalScopeDocumentIds,
            {
              allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch,
              ftsQuery: futilityProbeFtsQuery,
              scanParityRankTerms: keywordTermsOverride
            }
          );
          logStage("any_token_fts_parity_fetch", { rowCount: lexicalRows.length });
        }
      }
    }
    if (!scanProvenFutile && lexicalRows.length === 0) {
      lexicalRows = await lexicalSearch(
        env,
        where,
        params,
        retrievalQuery,
        recallConfig.lexicalSearchLimit,
        lexicalScopeDocumentIds,
        { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, termsOverride: keywordTermsOverride }
      );
    }
  }
  const keywordProvisionalFallbackEligible =
    parsed.corpusMode === "trusted_only" &&
    queryType === "keyword" &&
    (queryDerived.literalKeywordQuery || queryDerived.retrievalInfestationAliasQuery || queryDerived.curatedKeywordFamilyQuery) &&
    lexicalRows.length < Math.min(Math.max(parsed.limit, 8), 18);
  if (keywordProvisionalFallbackEligible) {
    const { where: provisionalWhere, params: provisionalParams } = buildSearchScope(parsed, "trusted_plus_provisional", {
      useSoftIndexCodeScope
    });
    // NS-30: when the primary candidates came from the FTS index, the provisional-scope retry uses it
    // too — otherwise a sparse trusted_only query pays the full substring scan it just avoided.
    const provisionalLexicalRows =
      keywordFtsFirstQuery && searchFtsAvailable
        ? await ftsSearch(
            env,
            provisionalWhere,
            provisionalParams,
            effectiveQuery,
            recallConfig.lexicalSearchLimit,
            lexicalScopeDocumentIds,
            { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, ftsQuery: keywordFtsFirstQuery, scanParityRankTerms: keywordTermsOverride }
          )
        : await lexicalSearch(
            env,
            provisionalWhere,
            provisionalParams,
            retrievalQuery,
            recallConfig.lexicalSearchLimit,
            lexicalScopeDocumentIds,
            { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, termsOverride: keywordTermsOverride }
          );
    const mergedLexicalRows = new Map<string, ChunkRow>();
    for (const row of lexicalRows) mergedLexicalRows.set(row.chunkId, row);
    for (const row of provisionalLexicalRows) {
      if (!mergedLexicalRows.has(row.chunkId)) mergedLexicalRows.set(row.chunkId, row);
    }
    lexicalRows = Array.from(mergedLexicalRows.values())
      .sort((a, b) => {
        const rankDiff = Number(b.lexicalRank || 0) - Number(a.lexicalRank || 0);
        if (rankDiff !== 0) return rankDiff;
        const trustDiff = Number(b.isTrustedTier || 0) - Number(a.isTrustedTier || 0);
        if (trustDiff !== 0) return trustDiff;
        const searchableDiff = String(b.searchableAt || "").localeCompare(String(a.searchableAt || ""));
        if (searchableDiff !== 0) return searchableDiff;
        return Number(a.orderRank || 0) - Number(b.orderRank || 0);
      })
      .slice(0, recallConfig.lexicalSearchLimit);
    logStage("keyword_provisional_lexical_fallback", {
      baseLexicalRowCount: lexicalRowCount,
      provisionalLexicalRowCount: provisionalLexicalRows.length,
      mergedLexicalRowCount: lexicalRows.length
    });
  }
  lexicalRowCount = lexicalRows.length;
  lexicalSearchMs = Date.now() - lexicalSearchStartedAt;
  logStage("lexical_search", { ms: lexicalSearchMs, lexicalRowCount, skipLexicalForVectorFirstIssueSearch });
  logStage("vector_search_start");
  const vectorSearchStartedAt = Date.now();
  const phraseFtsHasEnoughEvidence =
    phraseFtsEligible &&
    lexicalRows.length >= Math.min(Math.max(parsed.limit, 8), 18) &&
    !activeStructuredKinds.length;
  const vectorRuntime = shouldSkipVectorSearch(effectiveQuery, parsed.filters, queryType) || phraseFtsHasEnoughEvidence
    ? {
        scores: new Map<string, number>(),
        aiAvailable: Boolean(env.AI),
        vectorQueryAttempted: false,
        vectorMatchCount: 0,
        vectorErrored: false,
        vectorErrorMessage: ""
      }
    : await vectorSearchWithDiagnostics(env, [vectorQuery, retrievalQuery], recallConfig.vectorSearchLimit);
  vectorSearchMs = Date.now() - vectorSearchStartedAt;
  logStage("vector_search", {
    ms: vectorSearchMs,
    vectorQueryAttempted: vectorRuntime.vectorQueryAttempted,
    vectorMatchCount: vectorRuntime.vectorMatchCount,
    vectorErrored: vectorRuntime.vectorErrored,
    phraseFtsHasEnoughEvidence
  });
  const vectorScores = vectorRuntime.scores;

  const merged = new Map<string, ChunkRow>();
  for (const row of lexicalRows) merged.set(row.chunkId, row);

  const vectorChunkFetchStartedAt = Date.now();
  const extraVectorIds = Array.from(vectorScores.keys()).filter((id) => !merged.has(id));
  logStage("vector_chunk_fetch_start", { extraVectorIdCount: extraVectorIds.length });
  const issueSpecificScopeRequired = lockoutSpecificityRequired || habitabilitySpecificityRequired;
  const vectorRows = (await fetchChunksByIds(env, extraVectorIds, where, params)).filter((row) =>
    !issueSpecificScopeRequired || lexicalScopeDocumentIds.length === 0 || lexicalScopeDocumentIds.includes(row.documentId)
  );
  vectorChunkFetchMs = Date.now() - vectorChunkFetchStartedAt;
  for (const row of vectorRows) merged.set(row.chunkId, row);
  if (skipLexicalForVectorFirstIssueSearch && merged.size === 0 && queryType === "keyword") {
    // Vector-first issue queries (harassment/buyout/capital-improvement class) skip lexical recall
    // entirely, so whenever the vector channel yields nothing — unavailable AI binding, missing
    // embedding coverage, or zero matches — the query silently returned EMPTY. Rescue order:
    // (1) the phrase AND-of-concept-groups FTS query — selective (all concepts in one chunk), fast,
    //     and its slate spreads across documents;
    // (2) the OR-of-prefix execution terms in scan-parity mode — needed for bare tokens (no phrase
    //     query exists), but for multi-token forms an ultra-common token (landlord*) makes it match
    //     half the corpus and the title-weighted rank collapses the slate onto 1-2 documents.
    // The NS-36 guard fix (dead-vector kill bar) keeps the surviving rows from being eliminated.
    // With a healthy vector channel this block never runs.
    let rescueRows: ChunkRow[] = [];
    if (searchFtsAvailable && phraseFtsQuery.length > 0) {
      rescueRows = await ftsSearch(
        env,
        where,
        params,
        effectiveQuery,
        phraseFtsSearchLimit(recallConfig, pageWindow),
        lexicalScopeDocumentIds,
        { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, ftsQuery: phraseFtsQuery }
      );
      logStage("vector_first_phrase_rescue", { rowCount: rescueRows.length });
    }
    if (rescueRows.length === 0 && searchFtsAvailable && (keywordTermsOverride?.length ?? 0) > 0) {
      rescueRows = await ftsSearch(
        env,
        where,
        params,
        effectiveQuery,
        recallConfig.lexicalSearchLimit,
        lexicalScopeDocumentIds,
        {
          allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch,
          ftsQuery: prefixedFtsTermsQuery(keywordTermsOverride ?? []),
          scanParityRankTerms: keywordTermsOverride
        }
      );
    }
    const ftsAnswered = searchFtsAvailable && rescueRows !== FTS_SEARCH_ERROR_RESULT && (keywordTermsOverride?.length ?? 0) > 0;
    if (rescueRows.length === 0 && !ftsAnswered) {
      rescueRows = await lexicalSearch(
        env,
        where,
        params,
        retrievalQuery,
        recallConfig.lexicalSearchLimit,
        lexicalScopeDocumentIds,
        { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, termsOverride: keywordTermsOverride }
      );
    }
    for (const row of rescueRows) merged.set(row.chunkId, row);
    logStage("vector_first_lexical_rescue", { rowCount: rescueRows.length });
  }
  if (merged.size === 0 && recallConfig.issueGuidedSearch && lexicalScopeDocumentIds.length > 0) {
    const recoveryDocLimit = recallConfig.shortBroadIssueSearch
      ? recallConfig.hasStructuredFilters
        ? 4
        : 6
      : recallConfig.hasStructuredFilters
        ? 6
        : 8;
    const recoveryDocumentIds = lexicalScopeDocumentIds.slice(
      0,
      Math.min(
        recoveryDocLimit,
        Math.max(1, Math.max(recallConfig.decisionScopeDocumentLimit, pageWindow))
      )
    );
    const recoveryRows = await fetchChunksByDocumentIds(env, recoveryDocumentIds, where, params);
    for (const row of recoveryRows) merged.set(row.chunkId, row);
    logStage("short_broad_zero_hit_recovery", {
      recoveryDocumentCount: recoveryDocumentIds.length,
      recoveryRowCount: recoveryRows.length
    });
  }
  mergedChunkCount = merged.size;
  logStage("vector_chunk_fetch", { ms: vectorChunkFetchMs, mergedChunkCount });

  const explicitJudgeFilters = requestedJudges;

  const initialScoringStartedAt = Date.now();
  logStage("initial_scoring_start", { mergedChunkCount: merged.size });
  const scoreMergedRows = (rows: ChunkRow[]) =>
    rows
      .map((row) => {
        const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
        return { row, diagnostics };
      })
      .filter(({ row }) => rowMatchesQueryGuard(row, effectiveQuery, context))
      .filter(({ row }) => chunkTypeMatchesFilter(row.sectionLabel, parsed.filters.chunkType));

  let scored = scoreMergedRows(Array.from(merged.values()));
  const wholeWordKeywordRescueEligible =
    queryType === "keyword" &&
    scored.length === 0 &&
    lexicalRows.length > 0 &&
    (queryDerived.literalKeywordQuery || queryDerived.curatedKeywordFamilyQuery);
  if (wholeWordKeywordRescueEligible) {
    const { where: provisionalWhere, params: provisionalParams } = buildSearchScope(parsed, "trusted_plus_provisional", {
      useSoftIndexCodeScope
    });
    const wholeWordRows = await lexicalSearchWholeWord(
      env,
      provisionalWhere,
      provisionalParams,
      effectiveQuery,
      recallConfig.lexicalSearchLimit,
      lexicalScopeDocumentIds,
      { allowActiveDocumentChunkSearch: allowDocumentChunkLexicalSearch, termsOverride: keywordTermsOverride }
    );
    merged.clear();
    for (const row of wholeWordRows) merged.set(row.chunkId, row);
    mergedChunkCount = merged.size;
    scored = wholeWordRows
      .map((row) => {
        const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
        return { row, diagnostics };
      })
      .filter(({ row }) => chunkTypeMatchesFilter(row.sectionLabel, parsed.filters.chunkType));
    logStage("whole_word_keyword_rescue", {
      wholeWordRowCount: wholeWordRows.length,
      mergedChunkCount,
      rescuedScoredCount: scored.length
    });
  }
  scoredCount = scored.length;
  initialScoringMs = Date.now() - initialScoringStartedAt;
  logStage("initial_scoring", { ms: initialScoringMs, scoredCount: scored.length });

  // Index-code filters should admit decisions at the document scope, then let the query rank
  // the best passages inside those decisions. A hard chunk-level compatibility filter causes
  // good decisions to disappear when the right passage does not itself mention the selected code.
  const scoredWithIndexCompatibility = scored;

  const docHitCounts = scoredWithIndexCompatibility.reduce<Map<string, number>>((acc, candidate) => {
    acc.set(candidate.row.documentId, (acc.get(candidate.row.documentId) ?? 0) + 1);
    return acc;
  }, new Map());

  const docAwareScored = scoredWithIndexCompatibility.map((candidate) => {
    const docHitCount = docHitCounts.get(candidate.row.documentId) ?? 1;
    const docCoverageBoost = queryDerived.phraseEvidenceQuery
      ? Math.min(0.04, Math.max(0, docHitCount - 1) * 0.01)
      : Math.min(0.12, Math.max(0, docHitCount - 1) * 0.025);
    if (docCoverageBoost <= 0) return candidate;
    return {
      row: candidate.row,
      diagnostics: {
        ...candidate.diagnostics,
        rerankScore: Number((candidate.diagnostics.rerankScore + docCoverageBoost).toFixed(6)),
        why: uniq([...candidate.diagnostics.why, `document_multi_match_boost:${docHitCount}`])
      }
    };
  });

  const rerankedBase = queryType === "citation_lookup"
    ? (() => {
        const byFamily = countBy(docAwareScored.map(({ row }) => buildCitationFamilySignature(row)));
        return docAwareScored.map((candidate) => {
          const familyKey = buildCitationFamilySignature(candidate.row);
          const familyCount = Number(byFamily[familyKey] || 0);
          const familyPenalty = familyCount > 1 ? Number(Math.min(0.24, (familyCount - 1) * 0.03).toFixed(6)) : 0;
          const adjusted = Number(Math.max(0, candidate.diagnostics.rerankScore - familyPenalty).toFixed(6));
          return {
            row: candidate.row,
            diagnostics: {
              ...candidate.diagnostics,
              rerankScore: adjusted,
              why: uniq([
                ...candidate.diagnostics.why,
                familyPenalty > 0 ? `citation_family_repeat_penalty:${familyKey}:${familyPenalty.toFixed(3)}` : ""
              ].filter(Boolean))
            }
          };
        });
      })()
    : docAwareScored;

  const reranked = rerankedBase
    .sort((a, b) => {
      const diff = b.diagnostics.rerankScore - a.diagnostics.rerankScore;
      if (diff !== 0) return diff;
      // NS-20: equal scores tie-broke on ingestion timestamp alone — identical within a batch, so
      // the residual order was map-insertion order and re-ingesting a document reshuffled results.
      // chunkId is content-stable and makes equal-score ordering deterministic forever.
      const createdDiff = b.row.createdAt.localeCompare(a.row.createdAt);
      if (createdDiff !== 0) return createdDiff;
      return a.row.chunkId.localeCompare(b.row.chunkId);
    });
  rerankedCount = reranked.length;

  // NS-36 instrumentation: this span (issue-family synthetic seed fetches, up to a dozen serial
  // fetchKeywordCandidateDocumentIds calls, plus the decision-scope fallback fetch below) was the
  // only un-timed region of the pipeline and hid a measured 16-60s crawl on some issue queries.
  const issueSeedPrepStartedAt = Date.now();
  const ownerMoveInFollowThroughDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const ownerMoveInFollowThroughSyntheticSeedIds =
    queryDerived.ownerMoveInFollowThroughRequired
      ? await fetchKeywordCandidateDocumentIds(
          env,
          where,
          params,
          "owner occupancy principal residence",
          ownerMoveInFollowThroughDecisionScopeLimit
        )
      : [];

  const buyoutPressureDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const buyoutPressureSyntheticSeedIds =
    queryDerived.buyoutPressureQuery
      ? await fetchKeywordCandidateDocumentIds(
          env,
          where,
          params,
          "buyout payment vacate threats intimidation coercion",
          buyoutPressureDecisionScopeLimit
        )
      : [];

  const section8UnlawfulDetainerDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const section8UnlawfulDetainerSyntheticSeedIds =
    queryDerived.section8UdQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "section 8 eviction action",
            section8UnlawfulDetainerDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "housing choice voucher unlawful detainer complaint",
            section8UnlawfulDetainerDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "section 8 unlawful detainer complaint",
            section8UnlawfulDetainerDecisionScopeLimit
          ))
        ]).slice(0, section8UnlawfulDetainerDecisionScopeLimit)
      : [];

  const homeownersExemptionDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const homeownersExemptionSyntheticSeedIds =
    queryDerived.homeownersExemptionQuery
      ? await fetchKeywordCandidateDocumentIds(
          env,
          where,
          params,
          "homeowner property tax exemption principal residence",
          homeownersExemptionDecisionScopeLimit
        )
      : [];

  const coLivingDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const coLivingSyntheticSeedIds =
    queryDerived.coLivingQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "separate tenancy individual room",
            coLivingDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "separate rental agreements common areas",
            coLivingDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "separately rented rooms common areas",
            coLivingDecisionScopeLimit
          ))
        ]).slice(0, coLivingDecisionScopeLimit)
      : [];

  const collegeDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const collegeSyntheticSeedIds =
    queryDerived.collegeQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "temporarily living in student housing attend school",
            collegeDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "school breaks intends to return to live full-time in the unit",
            collegeDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "attend college temporary absence permanent residence",
            collegeDecisionScopeLimit
          ))
        ]).slice(0, collegeDecisionScopeLimit)
      : [];

  const selfEmployedDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const selfEmployedSyntheticSeedIds =
    queryDerived.selfEmployedQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "self employed 1099 tax returns",
            selfEmployedDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "self-employed files tax returns using the subject unit as address",
            selfEmployedDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "1099 reporting income principal residence address",
            selfEmployedDecisionScopeLimit
          ))
        ]).slice(0, selfEmployedDecisionScopeLimit)
      : [];

  const adjudicatedDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const adjudicatedSyntheticSeedIds =
    queryDerived.adjudicatedQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "already decided precluded",
            adjudicatedDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "properly adjudicated in state court",
            adjudicatedDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "previously decided final decision",
            adjudicatedDecisionScopeLimit
          ))
        ]).slice(0, adjudicatedDecisionScopeLimit)
      : [];

  const socialMediaDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const socialMediaSyntheticSeedIds =
    queryDerived.socialMediaQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "social media principal residence",
            socialMediaDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "facebook roommate search",
            socialMediaDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "instagram occupancy principal residence",
            socialMediaDecisionScopeLimit
          ))
        ]).slice(0, socialMediaDecisionScopeLimit)
      : [];

  const caregiverDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const caregiverSyntheticSeedIds =
    queryDerived.caregiverQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "primary caregiver principal residence",
            caregiverDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "caregiver return to live in the unit",
            caregiverDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "caregiving principal residence family schedule",
            caregiverDecisionScopeLimit
          ))
        ]).slice(0, caregiverDecisionScopeLimit)
      : [];

  const poopDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const poopSyntheticSeedIds =
    queryDerived.poopQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "dog waste backyard",
            poopDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "human feces hallway sewage",
            poopDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "animal waste common areas",
            poopDecisionScopeLimit
          ))
        ]).slice(0, poopDecisionScopeLimit)
      : [];

  const mootDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const mootSyntheticSeedIds =
    queryDerived.mootQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "rendered moot null and void",
            mootDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "rescinded administratively dismissed",
            mootDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "failure to repair claim is moot",
            mootDecisionScopeLimit
          ))
        ]).slice(0, mootDecisionScopeLimit)
      : [];

  const remoteWorkDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const remoteWorkSyntheticSeedIds =
    queryDerived.remoteWorkQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "work from home construction noise",
            remoteWorkDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "unable to work from home quiet enjoyment",
            remoteWorkDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "power was turned off unable to work from home",
            remoteWorkDecisionScopeLimit
          ))
        ]).slice(0, remoteWorkDecisionScopeLimit)
      : [];

  const divorceDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const divorceSyntheticSeedIds =
    queryDerived.divorceQuery
      ? uniq([
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "divorced separated spouse moved out",
            divorceDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "marital issues live separately",
            divorceDecisionScopeLimit
          )),
          ...(await fetchKeywordCandidateDocumentIds(
            env,
            where,
            params,
            "divorce separation residence occupancy",
            divorceDecisionScopeLimit
          ))
        ]).slice(0, divorceDecisionScopeLimit)
      : [];

  const legacyPestIssueDecisionScopeLimit = Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit));
  const legacyPestSeedQuery =
    /\b(?:cockroach|cockroaches|roach|roaches)\b/.test(queryDerived.normalizedQuery) && requestedCodes.includes("G44")
      ? "cockroach infestation"
      : /\b(?:rodent|rodents|rat|rats|mouse|mice)\b/.test(queryDerived.normalizedQuery) && requestedCodes.includes("G76")
        ? "rodent infestation"
        : "";
  const relaxedLegacyPestParsed = legacyPestSeedQuery
    ? { ...parsed, filters: { ...parsed.filters, indexCodes: [] } }
    : null;
  const relaxedLegacyPestScope = relaxedLegacyPestParsed
    ? buildSearchScope(relaxedLegacyPestParsed, parsed.corpusMode, { useSoftIndexCodeScope: false })
    : null;
  const legacyPestSyntheticSeedIds =
    legacyPestSeedQuery && relaxedLegacyPestScope
      ? await fetchKeywordCandidateDocumentIds(
          env,
          relaxedLegacyPestScope.where,
          relaxedLegacyPestScope.params,
          legacyPestSeedQuery,
          legacyPestIssueDecisionScopeLimit
        )
      : [];

  const issueFamilyDecisionScopeSeedIds =
    queryDerived.ownerMoveInFollowThroughRequired
      ? uniq([
          ...reranked
            .filter(({ row, diagnostics }) => {
              const searchableText = cachedCombinedSearchableText(row, context);
              const normalizedText = cachedNormalizedSearchableText(row, context);
              const conclusionsOccupancyProxy =
                isConclusionsLikeSectionLabel(row.sectionLabel || "") &&
                hasOwnerMoveInOccupancyStandardContext(searchableText, { normalizedText });
              return (
                (hasOwnerMoveInContext(searchableText, { normalizedText }) || conclusionsOccupancyProxy) &&
                (hasOwnerMoveInFollowThroughContext(searchableText, { normalizedText }) || conclusionsOccupancyProxy) &&
                (diagnostics.vectorScore > 0 || diagnostics.sectionBoost >= 0.14 || diagnostics.lexicalScore >= 0.1)
              );
            })
            .map((candidate) => candidate.row.documentId),
          ...ownerMoveInFollowThroughSyntheticSeedIds
        ]).slice(0, ownerMoveInFollowThroughDecisionScopeLimit)
      : queryDerived.accommodationQuery
        ? uniq(
            reranked
              .filter(({ row, diagnostics }) => {
                const searchableText = cachedCombinedSearchableText(row, context);
                const normalizedText = cachedNormalizedSearchableText(row, context);
                return (
                  hasAccommodationContext(searchableText, { normalizedText }) &&
                  (diagnostics.vectorScore > 0 ||
                    diagnostics.lexicalScore >= 0.12 ||
                    diagnostics.sectionBoost >= 0.1 ||
                    /\b(?:reasonable accommodation|service animal|support animal|emotional support animal|assistance animal\b)/.test(
                      normalizedText
                    ))
                );
              })
              .map((candidate) => candidate.row.documentId)
          ).slice(0, Math.max(4, Math.min(8, recallConfig.decisionScopeDocumentLimit)))
      : queryDerived.buyoutPressureQuery
        ? uniq([
            ...reranked
              .filter(({ row, diagnostics }) => {
                const searchableText = cachedCombinedSearchableText(row, context);
                const normalizedText = cachedNormalizedSearchableText(row, context);
                return (
                  hasBuyoutContext(searchableText, { normalizedText }) &&
                  (hasBuyoutPressureContext(searchableText, { normalizedText }) || diagnostics.vectorScore > 0) &&
                  (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.15 || diagnostics.sectionBoost >= 0.12)
                );
              })
              .map((candidate) => candidate.row.documentId),
            ...buyoutPressureSyntheticSeedIds
          ]).slice(0, buyoutPressureDecisionScopeLimit)
        : queryDerived.section8UdQuery
          ? uniq([
              ...reranked
                .filter(({ row, diagnostics }) => {
                  const searchableText = cachedCombinedSearchableText(row, context);
                  const normalizedText = cachedNormalizedSearchableText(row, context);
                  return (
                    hasSection8Context(searchableText, { normalizedText }) &&
                    (hasUnlawfulDetainerContext(searchableText, { normalizedText }) || /\beviction\b/.test(normalizedText) || diagnostics.vectorScore > 0) &&
                    (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.15 || diagnostics.sectionBoost >= 0.12)
                  );
                })
                .map((candidate) => candidate.row.documentId),
              ...section8UnlawfulDetainerSyntheticSeedIds
            ]).slice(0, section8UnlawfulDetainerDecisionScopeLimit)
        : queryDerived.homeownersExemptionQuery
          ? uniq([
              ...reranked
                .filter(({ row, diagnostics }) => {
                  const searchableText = cachedCombinedSearchableText(row, context);
                  const normalizedText = cachedNormalizedSearchableText(row, context);
                  return (
                    hasHomeownersExemptionContext(searchableText, { normalizedText }) &&
                    (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                  );
                })
                .map((candidate) => candidate.row.documentId),
              ...homeownersExemptionSyntheticSeedIds
            ]).slice(0, homeownersExemptionDecisionScopeLimit)
          : queryDerived.divorceQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasDivorceContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...divorceSyntheticSeedIds
              ]).slice(0, divorceDecisionScopeLimit)
          : queryDerived.adjudicatedQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasAdjudicatedContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...adjudicatedSyntheticSeedIds
              ]).slice(0, adjudicatedDecisionScopeLimit)
          : queryDerived.socialMediaQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasSocialMediaContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...socialMediaSyntheticSeedIds
              ]).slice(0, socialMediaDecisionScopeLimit)
          : queryDerived.caregiverQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasCaregiverContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...caregiverSyntheticSeedIds
              ]).slice(0, caregiverDecisionScopeLimit)
          : queryDerived.poopQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasPoopContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...poopSyntheticSeedIds
              ]).slice(0, poopDecisionScopeLimit)
          : queryDerived.mootQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasMootContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...mootSyntheticSeedIds
              ]).slice(0, mootDecisionScopeLimit)
          : queryDerived.selfEmployedQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasSelfEmployedContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...selfEmployedSyntheticSeedIds
              ]).slice(0, selfEmployedDecisionScopeLimit)
          : queryDerived.remoteWorkQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasRemoteWorkContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...remoteWorkSyntheticSeedIds
              ]).slice(0, remoteWorkDecisionScopeLimit)
          : queryDerived.collegeQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasCollegeContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...collegeSyntheticSeedIds
              ]).slice(0, collegeDecisionScopeLimit)
          : queryDerived.coLivingQuery
            ? uniq([
                ...reranked
                  .filter(({ row, diagnostics }) => {
                    const searchableText = cachedCombinedSearchableText(row, context);
                    const normalizedText = cachedNormalizedSearchableText(row, context);
                    return (
                      hasCoLivingContext(searchableText, { normalizedText }) &&
                      (diagnostics.vectorScore > 0 || diagnostics.lexicalScore >= 0.12 || diagnostics.sectionBoost >= 0.1)
                    );
                  })
                  .map((candidate) => candidate.row.documentId),
                ...coLivingSyntheticSeedIds
              ]).slice(0, coLivingDecisionScopeLimit)
          : legacyPestSyntheticSeedIds.slice(0, legacyPestIssueDecisionScopeLimit);
  logStage("issue_seed_scope_prep", {
    ms: Date.now() - issueSeedPrepStartedAt,
    issueFamilySeedCount: issueFamilyDecisionScopeSeedIds.length
  });
  // NS-19: the decision-layer boosts (±0.16 to ±0.42 — the largest movers in the ranker) are only
  // computed for documents inside this scope, so slicing at the bare limit silently dropped docs
  // that would WIN once their findings/conclusions layers were scored. Admit 3× the limit; the
  // per-document fallback fetches and the final output caps already bound cost and result size.
  // exact_phrase keeps the tight scope: containment IS the relevance definition there, and widening
  // measurably let layer-boosted topical docs displace phrase-containing docs (eval-caught).
  const decisionScopeAdmissionLimit =
    queryType === "exact_phrase" ? recallConfig.decisionScopeDocumentLimit : recallConfig.decisionScopeDocumentLimit * 3;
  const topDecisionIds = uniq(orderDecisionFirst(reranked, context).map((candidate) => candidate.row.documentId)).slice(
    0,
    decisionScopeAdmissionLimit
  );
  const issueSpecificSeedDecisionIds =
    issueSpecificScopeRequired && lexicalScopeDocumentIds.length > 0
      ? lexicalScopeDocumentIds.slice(0, Math.max(recallConfig.decisionScopeDocumentLimit, pageWindow * 2))
      : [];
  let decisionScopeDocumentIds = uniq([...issueFamilyDecisionScopeSeedIds, ...issueSpecificSeedDecisionIds, ...topDecisionIds]);
  if (!bypassScopedKeywordRecall && recallConfig.fallbackDocumentLimit > 0) {
    const fallbackFetchStartedAt = Date.now();
    const fallbackDocumentIds = await fetchScopedDocumentIds(env, where, params, recallConfig.fallbackDocumentLimit);
    for (const documentId of fallbackDocumentIds) {
      if (!decisionScopeDocumentIds.includes(documentId)) {
        decisionScopeDocumentIds.push(documentId);
      }
    }
    logStage("decision_scope_fallback_fetch", {
      ms: Date.now() - fallbackFetchStartedAt,
      fallbackDocumentCount: fallbackDocumentIds.length
    });
  }
  decisionScopeDocumentCount = decisionScopeDocumentIds.length;
  const decisionScopeFetchStartedAt = Date.now();
  logStage("decision_scope_fetch_start", { decisionScopeDocumentCount });
  const useMergedOnlyDecisionScope = recallConfig.issueGuidedSearch || queryType === "keyword";
  let decisionScopeRows = useMergedOnlyDecisionScope
    ? Array.from(merged.values()).filter((row) => decisionScopeDocumentIds.includes(row.documentId))
    : await fetchChunksByDocumentIds(env, decisionScopeDocumentIds, where, params);
  const supplementalIssueSeedDocumentIds = uniq([
    ...ownerMoveInFollowThroughSyntheticSeedIds,
    ...buyoutPressureSyntheticSeedIds,
    ...section8UnlawfulDetainerSyntheticSeedIds,
    ...homeownersExemptionSyntheticSeedIds,
    ...divorceSyntheticSeedIds,
    ...adjudicatedSyntheticSeedIds,
    ...socialMediaSyntheticSeedIds,
    ...caregiverSyntheticSeedIds,
    ...poopSyntheticSeedIds,
    ...mootSyntheticSeedIds,
    ...selfEmployedSyntheticSeedIds,
    ...remoteWorkSyntheticSeedIds,
    ...collegeSyntheticSeedIds,
    ...coLivingSyntheticSeedIds
  ]);
  const relaxedSupplementalIssueSeedDocumentIds = uniq(legacyPestSyntheticSeedIds);
  if (useMergedOnlyDecisionScope && (supplementalIssueSeedDocumentIds.length > 0 || relaxedSupplementalIssueSeedDocumentIds.length > 0)) {
    const presentDecisionScopeDocumentIds = new Set(decisionScopeRows.map((row) => row.documentId));
    const seenDecisionScopeChunkIds = new Set(decisionScopeRows.map((row) => row.chunkId));
    const missingSupplementalIssueSeedDocumentIds = supplementalIssueSeedDocumentIds.filter(
      (documentId) => decisionScopeDocumentIds.includes(documentId) && !presentDecisionScopeDocumentIds.has(documentId)
    );
    if (missingSupplementalIssueSeedDocumentIds.length > 0) {
      const supplementalIssueRows = await fetchChunksByDocumentIds(env, missingSupplementalIssueSeedDocumentIds, where, params);
      for (const row of supplementalIssueRows) {
        if (seenDecisionScopeChunkIds.has(row.chunkId)) continue;
        decisionScopeRows.push(row);
        seenDecisionScopeChunkIds.add(row.chunkId);
      }
      logStage("synthetic_issue_seed_scope_fetch", {
        missingSyntheticIssueSeedDocumentCount: missingSupplementalIssueSeedDocumentIds.length,
        supplementalRowCount: supplementalIssueRows.length
      });
    }
    const missingRelaxedSupplementalIssueSeedDocumentIds = relaxedSupplementalIssueSeedDocumentIds.filter(
      (documentId) => decisionScopeDocumentIds.includes(documentId) && !presentDecisionScopeDocumentIds.has(documentId)
    );
    if (missingRelaxedSupplementalIssueSeedDocumentIds.length > 0 && relaxedLegacyPestScope) {
      const relaxedSupplementalIssueRows = await fetchChunksByDocumentIds(
        env,
        missingRelaxedSupplementalIssueSeedDocumentIds,
        relaxedLegacyPestScope.where,
        relaxedLegacyPestScope.params
      );
      for (const row of relaxedSupplementalIssueRows) {
        if (seenDecisionScopeChunkIds.has(row.chunkId)) continue;
        decisionScopeRows.push(row);
        seenDecisionScopeChunkIds.add(row.chunkId);
      }
      logStage("legacy_issue_seed_scope_fetch", {
        missingLegacyIssueSeedDocumentCount: missingRelaxedSupplementalIssueSeedDocumentIds.length,
        supplementalRowCount: relaxedSupplementalIssueRows.length
      });
    }
  }
  decisionScopeFetchMs = Date.now() - decisionScopeFetchStartedAt;
  logStage("decision_scope_fetch", {
    ms: decisionScopeFetchMs,
    decisionScopeDocumentCount,
    decisionScopeRowCount: decisionScopeRows.length,
    useMergedOnlyDecisionScope
  });
  const decisionScopeMerged = new Map<string, ChunkRow>();
  for (const row of decisionScopeRows) {
    decisionScopeMerged.set(row.chunkId, row);
  }
  for (const row of merged.values()) {
    if (decisionScopeDocumentIds.includes(row.documentId)) {
      decisionScopeMerged.set(row.chunkId, row);
    }
  }
  decisionScopeChunkCount = decisionScopeMerged.size;

  const decisionScopeBuildStartedAt = Date.now();
  logStage("decision_scope_build_start", { decisionScopeChunkCount });
  let decisionScoped = buildDecisionScopedCandidates(
    Array.from(decisionScopeMerged.values()),
    vectorScores,
    context,
    decisionScopeDocumentIds,
    explicitJudgeFilters
  );
  const usedCombinedFilterZeroHitRecovery = decisionScoped.length === 0 && recallConfig.hasCombinedStructuredFilters;
  if (usedCombinedFilterZeroHitRecovery) {
    decisionScoped = buildDecisionScopedCandidates(
      Array.from(decisionScopeMerged.values()),
      vectorScores,
      context,
      decisionScopeDocumentIds,
      explicitJudgeFilters,
      { relaxedCombinedFilterRecovery: true }
    );
  }

  const section8UdDecisionScopedDocumentSupportIds = queryDerived.section8UdQuery
    ? buildSection8UdDocumentSupportSet(Array.from(decisionScopeMerged.values()), context)
    : new Set<string>();

  const scopedDocHitCounts = decisionScoped.reduce<Map<string, number>>((acc, candidate) => {
    acc.set(candidate.row.documentId, (acc.get(candidate.row.documentId) ?? 0) + 1);
    return acc;
  }, new Map());

  const decisionScopedDocAware = decisionScoped.map((candidate) => {
    const docHitCount = scopedDocHitCounts.get(candidate.row.documentId) ?? 1;
    const docCoverageBoost = queryDerived.phraseEvidenceQuery
      ? Math.min(0.04, Math.max(0, docHitCount - 1) * 0.01)
      : Math.min(0.12, Math.max(0, docHitCount - 1) * 0.025);
    const section8UdDocumentBoost =
      queryDerived.section8UdQuery &&
      chunkQualifiesForSection8UdDocumentSupport(candidate.row, candidate.diagnostics, section8UdDecisionScopedDocumentSupportIds, context)
        ? isFindingsLikeSectionLabel(candidate.row.sectionLabel || "")
          ? 0.18
          : 0.1
        : 0;
    if (docCoverageBoost <= 0 && section8UdDocumentBoost <= 0) return candidate;
    return {
      row: candidate.row,
      diagnostics: {
        ...candidate.diagnostics,
        rerankScore: Number((candidate.diagnostics.rerankScore + docCoverageBoost + section8UdDocumentBoost).toFixed(6)),
        why: uniq([
          ...candidate.diagnostics.why,
          ...(docCoverageBoost > 0 ? [`document_multi_match_boost:${docHitCount}`] : []),
          ...(section8UdDocumentBoost > 0
            ? [isFindingsLikeSectionLabel(candidate.row.sectionLabel || "") ? "section8_ud_document_findings_boost" : "section8_ud_document_support_boost"]
            : [])
        ])
      }
    };
  });
  decisionScopeBuildMs = Date.now() - decisionScopeBuildStartedAt;
  logStage("decision_scope_build", { ms: decisionScopeBuildMs, decisionScopeChunkCount, decisionScopedCount: decisionScoped.length });

  const finalizeResultsStartedAt = Date.now();
  const decisionFirst = orderDecisionFirst(
    decisionScopedDocAware.sort((a, b) => {
      const diff = b.diagnostics.rerankScore - a.diagnostics.rerankScore;
      if (diff !== 0) return diff;
      // NS-20: equal scores tie-broke on ingestion timestamp alone — identical within a batch, so
      // the residual order was map-insertion order and re-ingesting a document reshuffled results.
      // chunkId is content-stable and makes equal-score ordering deterministic forever.
      const createdDiff = b.row.createdAt.localeCompare(a.row.createdAt);
      if (createdDiff !== 0) return createdDiff;
      return a.row.chunkId.localeCompare(b.row.chunkId);
    }),
    context
  );
  const decisionLayerMap = new Map<
    string,
    { primaryAuthorityPassage?: SearchResultPassage; supportingFactPassage?: SearchResultPassage; supportingFactDebug?: SupportingFactDebug }
  >();
  const decisionFirstByDecision = new Map<string, Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>>();
  for (const candidate of decisionFirst) {
    const current = decisionFirstByDecision.get(candidate.row.documentId) || [];
    current.push(candidate);
    decisionFirstByDecision.set(candidate.row.documentId, current);
  }
  for (const [documentId, candidates] of decisionFirstByDecision.entries()) {
    decisionLayerMap.set(documentId, buildDecisionDisplayLayers(candidates, context));
  }
  const decisionLayerChunkCache = new Map<string, ChunkRow[]>();
  const authorityFallbackDocumentIds = Array.from(decisionLayerMap.entries())
    .filter(([, layers]) => !isConclusionsLikeSectionLabel(layers.primaryAuthorityPassage?.sectionLabel || ""))
    .map(([documentId]) => documentId)
    .slice(0, parsed.limit + parsed.offset + 10);
  if (authorityFallbackDocumentIds.length > 0) {
    const authorityFallbackRows = await fetchAuthorityChunksByDocumentIds(
      env,
      authorityFallbackDocumentIds,
      where,
      params,
      decisionLayerChunkCache
    );
    const fallbackByDecision = new Map<string, Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>>();
    for (const row of authorityFallbackRows) {
      const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
      const current = fallbackByDecision.get(row.documentId) || [];
      current.push({ row, diagnostics });
      fallbackByDecision.set(row.documentId, current);
    }
    for (const [documentId, fallbackCandidates] of fallbackByDecision.entries()) {
      const existing = decisionLayerMap.get(documentId) || {};
      const authorityCandidate = pickPrimaryAuthorityCandidate(fallbackCandidates, context);
      if (!authorityCandidate) continue;
      const primaryAuthorityPassage = toSearchResultPassage(authorityCandidate, context, { kind: "authority" });
      let supportingFactPassage = existing.supportingFactPassage;
      let supportingFactDebug = existing.supportingFactDebug;
      if (supportingFactPassage?.chunkId === primaryAuthorityPassage.chunkId) {
        supportingFactPassage = undefined;
        supportingFactDebug = undefined;
      }
      decisionLayerMap.set(documentId, {
        primaryAuthorityPassage,
        supportingFactPassage,
        supportingFactDebug
      });
    }
  }
  const missingSupportingFactDocumentIds = Array.from(decisionLayerMap.entries())
    .filter(([, layers]) => shouldRetrySupportingFactFallback(layers, context))
    .map(([documentId]) => documentId)
    .slice(0, parsed.limit + parsed.offset + 10);
  if (missingSupportingFactDocumentIds.length > 0) {
    const supportingFactFallbackRows = await fetchSupportingFactChunksByDocumentIds(
      env,
      missingSupportingFactDocumentIds,
      where,
      params,
      context,
      decisionLayerChunkCache
    );
    const fallbackByDecision = new Map<string, Array<{ row: ChunkRow; diagnostics: RankingDiagnostics }>>();
    for (const row of supportingFactFallbackRows) {
      const diagnostics = scoreRow(row, vectorScores.get(row.chunkId) ?? 0, context);
      const current = fallbackByDecision.get(row.documentId) || [];
      current.push({ row, diagnostics });
      fallbackByDecision.set(row.documentId, current);
    }
    for (const [documentId, fallbackCandidates] of fallbackByDecision.entries()) {
      const existing = decisionLayerMap.get(documentId) || {};
      const supportingFactSelection = pickSupportingFactCandidate(
        fallbackCandidates,
        context,
        existing.primaryAuthorityPassage?.chunkId,
        "fallback_findings_background_pool"
      );
      const supportingFactCandidate = supportingFactSelection?.candidate;
      if (supportingFactCandidate && supportingFactSelection) {
        decisionLayerMap.set(documentId, {
          primaryAuthorityPassage: existing.primaryAuthorityPassage,
          supportingFactPassage: toSearchResultPassage(supportingFactCandidate, context, { kind: "supporting_fact" }),
          supportingFactDebug: {
            source: "fallback_findings_background_pool",
            factualAnchorScore: supportingFactSelection.diagnostics.factualAnchorScore,
            anchorHits: supportingFactSelection.diagnostics.anchorHits,
            secondaryHits: supportingFactSelection.diagnostics.secondaryHits,
            coverageRatio: supportingFactSelection.diagnostics.coverageRatio
          }
        });
      }
    }
  }
  const decisionFirstLayerAware = orderDecisionFirst(decisionFirst, context, decisionLayerMap);
  const diversified = diversify(decisionFirstLayerAware, context, pageWindow * 2);
  const guarded = applyLowSignalStructuralGuard(diversified, context, pageWindow);
  const inferredJudgeNamesByDocument = inferDocumentJudgeNames(decisionScopeRows);

  const allResultRows = guarded.map(({ row, diagnostics }) => {
    const layers = decisionLayerMap.get(row.documentId);
    const primaryAuthorityPassage = layers?.primaryAuthorityPassage;
    return {
      documentId: row.documentId,
      chunkId: primaryAuthorityPassage?.chunkId ?? row.chunkId,
      title: row.title,
      citation: row.citation,
      authorName: sanitizeDisplayJudgeName(row.authorName) || inferredJudgeNamesByDocument.get(row.documentId) || null,
      decisionDate: row.decisionDate || null,
      fileType: row.fileType,
      snippet: buildLayeredResultSnippet(
        context,
        primaryAuthorityPassage,
        layers?.supportingFactPassage,
        layers?.supportingFactDebug,
        row.chunkText
      ),
      sectionLabel: primaryAuthorityPassage?.sectionLabel || row.sectionLabel,
      sourceFileRef: row.sourceFileRef,
      sourceLink: effectiveSourceLink(env, row.documentId, row.sourceLink),
      citationAnchor: primaryAuthorityPassage?.citationAnchor || row.citationAnchor,
      sectionHeading: primaryAuthorityPassage?.sectionHeading || row.sectionLabel,
      paragraphAnchor: primaryAuthorityPassage?.paragraphAnchor || row.paragraphAnchor,
      corpusTier: row.isTrustedTier === 1 ? "trusted" : "provisional",
      chunkType: primaryAuthorityPassage?.chunkType || normalizeChunkTypeLabel(row.sectionLabel),
      retrievalReason: diagnostics.why.slice(0, 4),
      matchedPassage: toSearchResultPassage({ row, diagnostics }, context),
      ...layers,
      score: diagnostics.rerankScore,
      lexicalScore: diagnostics.lexicalScore,
      vectorScore: diagnostics.vectorScore,
      ...(includeDiagnostics ? { diagnostics } : {})
    };
  });
  if (allResultRows.length === 0 && !internalOptions?.spellCorrected) {
    // NS-01: zero results is the only condition under which the spell map applies — a valid query
    // (even one containing a mapped string as a real name) has results and never reaches this. The
    // corrected query re-runs the FULL pipeline so scoring, guards, and family expansion all see the
    // corrected terms; the spellCorrected flag makes the retry single-shot.
    const correctedQuery = spellCorrectQuery(parsed.query);
    if (correctedQuery !== parsed.query) {
      logStage("zero_hit_spell_correction", { correctedQuery });
      return runSearchInternal(env, { ...parsed, query: correctedQuery }, requestedQueryType, includeDiagnostics, {
        spellCorrected: true
      });
    }
  }
  const pagedRows = allResultRows.slice(parsed.offset, parsed.offset + parsed.limit);
  const hasMore = allResultRows.length > parsed.offset + parsed.limit;
  finalizeResultsMs = Date.now() - finalizeResultsStartedAt;
  const totalMs = Date.now() - totalStartedAt;
  logStage("finalize_results", { ms: finalizeResultsMs, totalMs, resultCount: allResultRows.length });

  if (includeDiagnostics) {
    const tierCounts = pagedRows.reduce(
      (acc, row) => {
        if (row.corpusTier === "trusted") acc.trusted += 1;
        else acc.provisional += 1;
        return acc;
      },
      { trusted: 0, provisional: 0 }
    );
    return searchDebugResponseSchema.parse({
      query: requestedQuery,
      queryType,
      debugProfile: {
        endpoint: "admin_retrieval_debug",
        requestedQueryType,
        productionSearchQueryType: "keyword",
        // Production hardcodes keyword and then applies the same quoted-phrase upgrade, so a keyword
        // request matches the production path even when it was upgraded to exact_phrase here.
        matchesProductionSearchPath: requestedQueryType === "keyword"
      },
      corpusMode: parsed.corpusMode,
      offset: parsed.offset,
      limit: parsed.limit,
      hasMore,
      combinedFilterZeroHitRecoveryUsed: usedCombinedFilterZeroHitRecovery,
      tierCounts,
      filters: parsed.filters,
      runtimeDiagnostics: {
        aiAvailable: vectorRuntime.aiAvailable,
        vectorQueryAttempted: vectorRuntime.vectorQueryAttempted,
        vectorMatchCount: vectorRuntime.vectorMatchCount,
        vectorErrored: vectorRuntime.vectorErrored,
        vectorErrorMessage: vectorRuntime.vectorErrorMessage,
        vectorNamespace: env.VECTOR_NAMESPACE,
        lexicalScopeDocumentCount,
        lexicalRowCount,
        mergedChunkCount,
        scoredCount,
        rerankedCount,
        decisionScopeDocumentCount,
        decisionScopeChunkCount,
        stageTimingsMs: {
          scopeBuild: scopeBuildMs,
          lexicalScopeFetch: lexicalScopeFetchMs,
          lexicalSearch: lexicalSearchMs,
          vectorSearch: vectorSearchMs,
          vectorChunkFetch: vectorChunkFetchMs,
          initialScoring: initialScoringMs,
          decisionScopeFetch: decisionScopeFetchMs,
          decisionScopeBuild: decisionScopeBuildMs,
          finalizeResults: finalizeResultsMs,
          total: totalMs
        }
      },
      total: allResultRows.length,
      results: pagedRows
    });
  }

  const tierCounts = pagedRows.reduce(
    (acc, row) => {
      if (row.corpusTier === "trusted") acc.trusted += 1;
      else acc.provisional += 1;
      return acc;
    },
    { trusted: 0, provisional: 0 }
  );
  return searchResponseSchema.parse({
    query: requestedQuery,
    corpusMode: parsed.corpusMode,
    offset: parsed.offset,
    limit: parsed.limit,
    hasMore,
    tierCounts,
    total: allResultRows.length,
    results: pagedRows
  });
}

export async function search(env: Env, input: unknown) {
  const parsed = searchRequestSchema.parse(input);
  try {
    return await runSearchInternal(env, parsed, "keyword", false);
  } catch (error) {
    if (isRetryableSearchError(error)) {
      return runSearchInternal(env, parsed, "keyword", false);
    }
    throw error;
  }
}

export async function searchDebug(env: Env, input: unknown) {
  const parsed = searchDebugRequestSchema.parse(input);
  try {
    return await runSearchInternal(env, parsed, parsed.queryType, true);
  } catch (error) {
    if (isRetryableSearchError(error)) {
      return runSearchInternal(env, parsed, parsed.queryType, true);
    }
    throw error;
  }
}
