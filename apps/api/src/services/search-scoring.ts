// Compatibility exports: implementations live behind the named policy/stage boundaries.
export { KEYWORD_RECALL_UNIVERSE_MAX, shouldSkipVectorSearch, enhanceQueryWithIndexCodeContext, keywordExecutionTerms, expandQueryForRetrieval, chooseVectorQuery } from './search-query-plan';
export { inferDocumentJudgeNames, chunkTypeMatchesFilter, exactMultiWordPhraseScore, cachedCombinedSearchableText, cachedNormalizedSearchableText, rowMatchesQueryGuard, buildSection8UdDocumentSupportSet, chunkQualifiesForSection8UdDocumentSupport, countBy, buildCitationFamilySignature, lexicalScore, phraseFtsSearchLimit } from './search-evidence-policy';
export { chooseSnippetForTargets, buildLayeredResultSnippet, toSearchResultPassage } from './search-snippets';
export { scoreRow } from './search-ranking';
export { hasAnyExactIndexCodeCoverage } from './search-scope-repository';
export { pickPrimaryAuthorityCandidate, pickSupportingFactCandidate, shouldRetrySupportingFactFallback, buildDecisionDisplayLayers, orderDecisionFirst, buildDecisionScopedCandidates, diversify, applyLowSignalStructuralGuard } from './search-candidates';
