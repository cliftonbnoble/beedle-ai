import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

test("search scope delegates publication and trust to the shared policy", async () => {
  const scope = await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8");
  const policy = await fs.readFile(path.resolve(process.cwd(), "src/services/publication-policy.ts"), "utf8");
  assert.match(scope, /import \{ searchPublicationSql \} from "\.\/publication-policy"/);
  assert.match(scope, /const clauses: string\[\] = \[searchPublicationSql\(\{ \.\.\.parsed, corpusMode \}\)\]/);
  assert.doesNotMatch(scope, /hasBasicChunkedDecisionClause/);
  assert.match(policy, /ingestion_state/);
  assert.match(policy, /approved_revision/);
  assert.match(policy, /rejected_at/);
  // The SQL behavior matrix in publication-behavior.test.mjs verifies lifecycle
  // eligibility. Active retrieval rows alone must no longer bypass that policy.
  assert.match(scope, /clauses.push\("d.file_type = 'decision_docx'"\)/);
});

test("lexical and vector candidate paths read from active retrieval_search_chunks", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const searchQueryAnalysisSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8");

  assert.match(searchQueryAnalysisSrc, /function lexicalTerms\(query: string\): string\[]/, "Expected token-aware lexical term builder");
  assert.match(src, /function rowMatchesQueryGuard\(row: ChunkRow, query: string, context: SearchContext\): boolean/, "Expected short-query guard for lexical noise");
  assert.match(src, /lexicalRows = await lexicalSearch\(\s*env,\s*where,\s*params,\s*retrievalQuery,\s*recallConfig\.lexicalSearchLimit/, "Expected configured lexical candidate pool for document recall");
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
  const lexicalSqlSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-lexical-sql.ts"), "utf8");
  assert.match(lexicalSqlSrc, /export function buildLexicalRankExpr\(/, "Expected lexical candidate ranking expression builder (extracted to search-lexical-sql)");
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
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const searchQueryAnalysisSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8");
  const searchTextSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-text.ts"), "utf8");

  assert.doesNotMatch(src, /add\("tenant", "landlord", "unit", "apartment", "building"\)/);
  assert.doesNotMatch(src, /function isNoticeProceduralQuery\(query: string\): boolean/);
  assert.doesNotMatch(src, /function isConditionIssueQuery\(query: string\): boolean/);
  assert.match(searchQueryAnalysisSrc, /conditionIssueQuery: issueTerms\.length > 0/);
  assert.match(searchQueryAnalysisSrc, /noticeProceduralQuery: proceduralTerms\.length > 0/);
  assert.match(searchTextSrc, /\.normalize\("NFD"\)\s*\.replace\(\/\[\\u0300-\\u036f\]\/g, ""\)/, "Expected accent-insensitive normalization");
  assert.match(src, /const retrievalQuery = expandQueryForRetrieval\(effectiveQuery\)/);
  assert.match(src, /const vectorQuery = chooseVectorQuery\(effectiveQuery\)/);
  assert.match(src, /vectorSearchWithDiagnostics\(env,\s*\[effectiveQuery,\s*vectorQuery\],\s*recallConfig\.vectorSearchLimit,\s*\{\s*filters:\s*parsed\.filters,\s*corpusMode:\s*parsed\.corpusMode,\s*where,\s*params\s*\}\)/);
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
  assert.match(src, /const topDecisionIds = uniq\(orderDecisionFirst\(reranked, context\)\.map\(\(candidate\) => candidate\.row\.documentId\)\)/);
  assert.match(src, /const decisionFirst = orderDecisionFirst\(\s*decisionScopedDocAware\.sort/);
  assert.match(src, /async function fetchChunksByDocumentIds\(/);
  assert.match(src, /fetchChunksByDocumentIds\(env, decisionScopeDocumentIds, where, params\)/);
  assert.match(src, /const decisionScopedDocAware = decisionScoped\.map\(/);
  assert.match(src, /diagnostics\.lexicalScore > 0[\s\S]*diagnostics\.partyNameBoost > 0/);
  assert.match(src, /diagnostics\.lexicalScore === 0[\s\S]*isLowSignalVectorOnlyChunkType\(row\.sectionLabel \|\| ""\) \|\| hasMalformedDocxArtifact\(row\.chunkText\)/);
  assert.match(src, /queryDerived\.conditionIssueQuery[\s\S]*isIssueDisfavoredChunkType\(row\.sectionLabel \|\| ""\)[\s\S]*!chunkMatchesIssueTerms\(row, context\)/);
  assert.match(src, /queryDerived\.noticeProceduralQuery[\s\S]*isLowValueIssueIntentChunkType\(row\.sectionLabel \|\| ""\)[\s\S]*!chunkMatchesProceduralTerms\(row, context\)/);
  assert.match(src, /queryDerived\.coolingIssueQuery[\s\S]*diagnostics\.vectorScore > 0[\s\S]*!\/findings\? of fact\|order\/i\.test/);
  assert.match(src, /queryDerived\.coolingIssueQuery[\s\S]*!chunkMatchesIssueTerms\(row, context\)[\s\S]*diagnostics\.lexicalScore < 0\.35/);
  assert.match(src, /queryDerived\.strongIssueEvidenceRequired[\s\S]*hasWrongContextForQuery\(context\.query, searchableText, \{ normalizedQuery: queryDerived\.normalizedQuery, normalizedText \}\)/);
  assert.match(src, /queryDerived\.strongIssueEvidenceRequired[\s\S]*!hasStrongIssueEvidence\(/);
  assert.match(src, /hasSevereExtractionArtifact\(row\.chunkText\)[\s\S]*diagnostics\.lexicalScore < 0\.6/);
  assert.match(src, /context\.filters\.documentId[\s\S]*Math\.max\(3, limit\)/);
  assert.match(src, /function applyLowSignalStructuralGuard\(/);
  assert.match(src, /maxLowSignalInTop = Math\.max\(1, Math\.floor\(limit \/ 5\)\)/);
  assert.match(src, /function applyLowSignalStructuralGuard[\s\S]*const queryDerived = getQueryDerivedContext\(context\)[\s\S]*if \(queryDerived\.structuralIntent \|\| context\.queryType === "citation_lookup"\)/);
});

test("search results expose corpus mode and tier labeling for trusted/provisional visibility", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");

  assert.match(src, /buildSearchScope\(parsed, parsed\.corpusMode, \{ useSoftIndexCodeScope \}\)/, "Expected corpus mode to drive search scope");
  assert.match(src, /corpusTier: eligible\.get\(row\.documentId\)(?:\?\.trusted)? === 1 \? "trusted" : "provisional"/, "Expected per-result corpus tier labeling");
  assert.match(src, /runtimeDiagnostics:/, "Expected debug search response to expose runtime vector diagnostics");
  assert.match(src, /vectorQueryAttempted:/, "Expected runtime diagnostics to show whether vector search ran");
  assert.match(src, /corpusMode: parsed\.corpusMode/, "Expected response corpus mode echo");
  assert.match(src, /tierCounts/, "Expected tier counts in search response payload");
});

test("search scope supports decision drill-down filtering and larger detail snippets", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const searchQueryAnalysisSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8");

  assert.match(searchQueryAnalysisSrc, /if \(parsed\.filters\.documentId\) \{/);
  assert.match(searchQueryAnalysisSrc, /clauses\.push\("d\.id = \?"\)/);
  assert.match(searchQueryAnalysisSrc, /const judgeFilters = requestedJudgeFilters\(parsed\.filters\)/);
  assert.match(searchQueryAnalysisSrc, /if \(judgeFilters\.length > 0\) \{/);
  assert.match(searchQueryAnalysisSrc, /lower\(coalesce\(d\.author_name, ''\)\) = lower\(\?\)/);
  assert.match(src, /snippetMaxLength: parsed\.snippetMaxLength/);
  assert.match(src, /const maxSnippetChars = Math\.max\(120, Math\.min\(1200, Number\(context\.snippetMaxLength \|\| 260\)\)\)/);
});
