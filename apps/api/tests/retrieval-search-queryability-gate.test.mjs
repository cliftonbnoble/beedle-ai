import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

test("search scope allows activated retrieval chunks to satisfy decision QC/approval gates", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  assert.match(
    src,
    /EXISTS \(SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active\.document_id = d\.id AND rs_active\.active = 1\)/,
    "Expected active retrieval chunk existence clause"
  );

  assert.match(
    src,
    /const hasBasicChunkedDecisionClause =[\s\S]*document_chunks c_basic[\s\S]*d\.source_r2_key IS NOT NULL[\s\S]*COALESCE\(d\.title, ''\) != ''/,
    "Expected broad provisional admission clause for chunked decisions with provenance"
  );

  assert.match(
    src,
    /corpusMode === "trusted_plus_provisional"[\s\S]*\(d\.file_type != 'decision_docx' OR \$\{hasBasicChunkedDecisionClause\} OR \$\{hasActiveRetrievalChunkClause\}\)/,
    "Expected trusted+provisional mode to allow basic chunked-searchable decisions"
  );

  assert.match(
    src,
    /corpusMode === "trusted_plus_provisional"[\s\S]*: `\(d\.file_type != 'decision_docx' OR \$\{hasActiveRetrievalChunkClause\}\)`/,
    "Expected trusted-only mode to restrict decision docs to activated trusted chunks"
  );

  assert.match(
    src,
    /if \(parsed\.filters\.fileType\) \{[\s\S]*clauses\.push\("d\.file_type = \?"\)[\s\S]*\} else \{[\s\S]*clauses\.push\("d\.file_type = 'decision_docx'"\)/,
    "Expected product search to default to decision documents unless fileType is explicitly requested"
  );

  assert.match(
    src,
    /\(d\.file_type != 'decision_docx' OR d\.approved_at IS NOT NULL OR \$\{hasActiveRetrievalChunkClause\}\)/,
    "Expected approvedOnly gate to allow activated retrieval docs"
  );
});

test("lexical and vector candidate paths read from active retrieval_search_chunks", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  assert.match(src, /function lexicalTerms\(query: string\): string\[]/, "Expected token-aware lexical term builder");
  assert.match(src, /function rowMatchesQueryGuard\(row: ChunkRow, query: string\): boolean/, "Expected short-query guard for lexical noise");
  assert.match(src, /const lexicalRows = await lexicalSearch\(env, where, params, retrievalQuery, parsed\.limit \* 12\)/, "Expected larger lexical candidate pool for document recall");
  assert.match(src, /document_multi_match_boost:/, "Expected document-level boost for multiple relevant chunk hits");
  assert.match(src, /trusted_tier_boost/, "Expected trusted tier boost to keep activated docs ranked above broad provisional docs");
  assert.match(src, /broad_chunked_doc_admission/, "Expected lightweight boost for broad chunked document admission");
  assert.match(src, /judge_name_filter_match/, "Expected judge-name filter to contribute to ranking");
  assert.match(src, /judge_name_query_match/, "Expected direct judge-name queries to contribute to ranking");
  assert.match(src, /judge_only_author_match_boost/, "Expected judge-only queries to strongly prefer author matches");
  assert.match(src, /judge_only_author_mismatch_penalty/, "Expected judge-only queries to penalize mismatched authors");
  assert.match(src, /d\.author_name as authorName/, "Expected chunk retrieval to hydrate author_name for judge-aware search");
  assert.doesNotMatch(
    src,
    /FROM document_chunks c[\s\S]*\$\{where\}[\s\S]*AND d\.searchable_at IS NOT NULL[\s\S]*AND \$\{primaryMatch\.clause\}/,
    "Expected broad chunked provisional docs to participate without a searchable_at hard gate"
  );
  assert.match(src, /if \(!terms\.length\) return \[\];/, "Expected broad stopword-only queries to avoid junk lexical matches");
  assert.match(src, /function buildLexicalRankExpr\(/, "Expected lexical candidate ranking expression builder");
  assert.match(src, /ORDER BY lexicalRank DESC, searchableAt DESC, orderRank ASC/, "Expected lexical candidate ordering to prioritize match quality before recency");
  assert.match(
    src,
    /buildLexicalMatchClause\("c\.chunk_text", "d\.citation", "d\.title", "d\.author_name", terms\)/,
    "Expected lexical search to include author_name for judge-aware term matching"
  );
  assert.match(src, /FROM retrieval_search_chunks rs[\s\S]*AND rs\.active = 1/, "Expected lexical retrieval chunk union with active filter");
  assert.match(src, /FROM retrieval_search_chunks rs[\s\S]*AND rs\.chunk_id IN \(\$\{placeholders\}\)/, "Expected vector fetch to include activated retrieval chunks");
});

test("runtime ranking applies low-signal structural guards for non-structural intents", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  assert.match(src, /function expandQueryForRetrieval\(query: string\)/);
  assert.doesNotMatch(src, /add\("tenant", "landlord", "unit", "apartment", "building"\)/);
  assert.match(src, /function chooseVectorQuery\(originalQuery: string\)/);
  assert.match(src, /function inferIssueTerms\(query: string\)/);
  assert.match(src, /function inferProceduralTerms\(query: string\)/);
  assert.match(src, /function isNoticeProceduralQuery\(query: string\): boolean/);
  assert.match(src, /function isCoolingIssueQuery\(query: string\): boolean/);
  assert.match(src, /function isEvictionProtectionQuery\(query: string\): boolean/);
  assert.match(src, /function isBuyoutQuery\(query: string\): boolean/);
  assert.match(src, /function isRentReductionQuery\(query: string\): boolean/);
  assert.match(src, /function isNuisanceQuery\(query: string\): boolean/);
  assert.match(src, /function requiresStrongIssueEvidence\(query: string\): boolean/);
  assert.match(src, /\.normalize\("NFD"\)\s*\.replace\(\/\[\\u0300-\\u036f\]\/g, ""\)/, "Expected accent-insensitive normalization");
  assert.match(src, /function isJudgeDrivenQuery\(query: string\): boolean/);
  assert.match(src, /function rowMatchesReferencedJudge\(row: ChunkRow, query: string, explicitJudgeFilters\?: string\[\]\): boolean/);
  assert.match(src, /function containsWholeWord\(text: string, term: string\): boolean/);
  assert.match(src, /function hasMoldCollision\(text: string\): boolean/);
  assert.match(src, /function hasCoolingProxyDrift\(text: string\): boolean/);
  assert.match(src, /function hasBuyoutContext\(text: string\): boolean/);
  assert.match(src, /function hasOwnerMoveInContext\(text: string\): boolean/);
  assert.match(src, /function hasWrongfulEvictionContext\(text: string\): boolean/);
  assert.match(src, /function hasHarassmentContext\(text: string\): boolean/);
  assert.match(src, /function hasRentReductionContext\(text: string\): boolean/);
  assert.match(src, /function hasRepairNoticeContext\(text: string\): boolean/);
  assert.match(src, /function hasNuisanceContext\(text: string\): boolean/);
  assert.match(src, /function hasWrongContextForQuery\(query: string, text: string\): boolean/);
  assert.match(src, /function hasStrongIssueEvidence\(query: string, row: ChunkRow, issueTermHits: number, proceduralTermHits: number\): boolean/);
  assert.match(src, /function chunkMatchesIssueTerms\(row: ChunkRow, query: string\): boolean/);
  assert.match(src, /function chunkMatchesProceduralTerms\(row: ChunkRow, query: string\): boolean/);
  assert.match(src, /function isCapitalImprovementBoilerplate\(text: string\)/);
  assert.match(src, /function isLowSignalTabularChunkType\(chunkType: string\)/);
  assert.match(src, /function isIssuePreferredChunkType\(chunkType: string\): boolean/);
  assert.match(src, /function isIssueDisfavoredChunkType\(chunkType: string\): boolean/);
  assert.match(src, /function isLowValueIssueIntentChunkType\(chunkType: string\): boolean/);
  assert.match(src, /function isLowSignalVectorOnlyChunkType\(chunkType: string\)/);
  assert.match(src, /function hasMalformedDocxArtifact\(text: string\)/);
  assert.match(src, /function hasSevereExtractionArtifact\(text: string\): boolean/);
  assert.match(src, /const retrievalQuery = expandQueryForRetrieval\(parsed\.query\)/);
  assert.match(src, /const vectorQuery = chooseVectorQuery\(parsed\.query\)/);
  assert.match(src, /const vectorRuntime = await vectorSearchWithDiagnostics\(env, \[vectorQuery, retrievalQuery\], parsed\.limit\)/);
  assert.match(src, /topK: Math\.min\(25, Math\.max\(limit \* 2, 10\)\)/);
  assert.match(src, /returnMetadata: true/);
  assert.match(src, /capital_improvement_boilerplate_penalty/);
  assert.match(src, /mold_molding_collision_penalty/);
  assert.match(src, /procedural_term_overlap:/);
  assert.match(src, /procedural_section_boost/);
  assert.match(src, /procedural_low_value_chunk_penalty/);
  assert.match(src, /cooling_issue_evidence_penalty/);
  assert.match(src, /cooling_issue_evidence_boost/);
  assert.match(src, /cooling_proxy_drift_penalty/);
  assert.match(src, /family_wrong_context_penalty/);
  assert.match(src, /strong_issue_evidence_missing_penalty/);
  assert.match(src, /eviction_protection_authority_boost/);
  assert.match(src, /eviction_protection_lexical_only_penalty/);
  assert.match(src, /low_value_issue_vector_penalty/);
  assert.match(src, /vector_tabular_chunk_penalty/);
  assert.match(src, /vector_docx_artifact_penalty/);
  assert.match(src, /severe_extraction_artifact_penalty/);
  assert.match(src, /issue_term_overlap:/);
  assert.match(src, /issue_section_boost/);
  assert.match(src, /issue_preferred_chunk_type_boost/);
  assert.match(src, /issue_disfavored_chunk_penalty/);
  assert.match(src, /const topDecisionIds = uniq\(orderDecisionFirst\(reranked\)\.map\(\(candidate\) => candidate\.row\.documentId\)\)/);
  assert.match(src, /const decisionFirst = orderDecisionFirst\(\s*decisionScopedDocAware\.sort/);
  assert.match(src, /async function fetchChunksByDocumentIds\(/);
  assert.match(src, /const decisionScopeRows = await fetchChunksByDocumentIds\(env, topDecisionIds, where, params\)/);
  assert.match(src, /const decisionScopedDocAware = decisionScoped\.map\(/);
  assert.match(src, /diagnostics\.lexicalScore > 0[\s\S]*diagnostics\.partyNameBoost > 0/);
  assert.match(src, /diagnostics\.lexicalScore === 0[\s\S]*isLowSignalVectorOnlyChunkType\(row\.sectionLabel \|\| ""\) \|\| hasMalformedDocxArtifact\(row\.chunkText\)/);
  assert.match(src, /isConditionIssueQuery\(context\.query\)[\s\S]*isIssueDisfavoredChunkType\(row\.sectionLabel \|\| ""\)[\s\S]*!chunkMatchesIssueTerms\(row, context\.query\)/);
  assert.match(src, /isNoticeProceduralQuery\(context\.query\)[\s\S]*isLowValueIssueIntentChunkType\(row\.sectionLabel \|\| ""\)[\s\S]*!chunkMatchesProceduralTerms\(row, context\.query\)/);
  assert.match(src, /isCoolingIssueQuery\(context\.query\)[\s\S]*diagnostics\.vectorScore > 0[\s\S]*!\/findings\? of fact\|order\/i\.test/);
  assert.match(src, /isCoolingIssueQuery\(context\.query\)[\s\S]*!chunkMatchesIssueTerms\(row, context\.query\)[\s\S]*diagnostics\.lexicalScore < 0\.35/);
  assert.match(src, /requiresStrongIssueEvidence\(context\.query\)[\s\S]*hasWrongContextForQuery\(context\.query, combinedSearchableText\(row\)\)/);
  assert.match(src, /requiresStrongIssueEvidence\(context\.query\)[\s\S]*!hasStrongIssueEvidence\(/);
  assert.match(src, /hasSevereExtractionArtifact\(row\.chunkText\)[\s\S]*diagnostics\.lexicalScore < 0\.6/);
  assert.match(src, /function isLowSignalStructuralChunkType\(chunkType: string\)/);
  assert.match(src, /context\.filters\.documentId[\s\S]*Math\.max\(3, limit\)/);
  assert.match(src, /function applyLowSignalStructuralGuard\(/);
  assert.match(src, /maxLowSignalInTop = Math\.max\(1, Math\.floor\(limit \/ 5\)\)/);
  assert.match(src, /if \(isStructuralIntent\(context\) \|\| context\.queryType === "citation_lookup"\)/);
});

test("search results expose corpus mode and tier labeling for trusted/provisional visibility", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  assert.match(src, /buildSearchScope\(parsed, parsed\.corpusMode\)/, "Expected corpus mode to drive search scope");
  assert.match(src, /corpusTier: row\.isTrustedTier === 1 \? "trusted" : "provisional"/, "Expected per-result corpus tier labeling");
  assert.match(src, /runtimeDiagnostics:/, "Expected debug search response to expose runtime vector diagnostics");
  assert.match(src, /vectorQueryAttempted:/, "Expected runtime diagnostics to show whether vector search ran");
  assert.match(src, /corpusMode: parsed\.corpusMode/, "Expected response corpus mode echo");
  assert.match(src, /tierCounts/, "Expected tier counts in search response payload");
});

test("search scope supports decision drill-down filtering and larger detail snippets", async () => {
  const src = await fs.readFile(searchServicePath, "utf8");

  assert.match(src, /if \(parsed\.filters\.documentId\) \{/);
  assert.match(src, /clauses\.push\("d\.id = \?"\)/);
  assert.match(src, /const judgeFilters = requestedJudgeFilters\(parsed\.filters\)/);
  assert.match(src, /if \(judgeFilters\.length > 0\) \{/);
  assert.match(src, /lower\(coalesce\(d\.author_name, ''\)\) = lower\(\?\)/);
  assert.match(src, /snippetMaxLength: parsed\.snippetMaxLength/);
  assert.match(src, /const maxSnippetChars = Math\.max\(120, Math\.min\(1200, Number\(context\.snippetMaxLength \|\| 260\)\)\)/);
});
