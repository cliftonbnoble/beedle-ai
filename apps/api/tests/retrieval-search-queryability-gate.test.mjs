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

test("lexical and vector candidate paths use bounded indexed recall and active retrieval chunks", async () => {
  const fts = await fs.readFile(path.resolve(process.cwd(), "src/services/search-fts.ts"), "utf8");
  const indexed = await fs.readFile(path.resolve(process.cwd(), "src/services/search-indexed-lexical.ts"), "utf8");
  const search = await fs.readFile(searchServicePath, "utf8");
  // Behavioral coverage lives in search-indexed-recall-behavior and vector-filter-behavior.
  // These integration pins ensure orchestration still delegates to those tested boundaries.
  assert.match(search, /indexedLexicalSearch\(env,\s*where,\s*params/);
  assert.match(fts, /return indexedLexicalSearch\(env,\s*where,\s*params,\s*terms,\s*limit/);
  assert.match(fts, /if\s*\(!terms\.length\)\s*return \[\];/);
  assert.match(indexed, /requireSubstringIndex\(env\)/);
  assert.match(indexed, /substringRecallLimits=\{body:1600,metadataDocuments:200,metadataChunksPerDocument:2\}/);
  assert.match(indexed, /m\.source_kind='retrieval'[\s\S]*a\.chunk_id=m\.chunk_id AND a\.active=1/);
  assert.match(indexed, /b\.lexicalRank-a\.lexicalRank/);
  assert.match(indexed, /d\.author_name AS authorName/);
  assert.match(fts, /FROM retrieval_search_chunks rs[\s\S]*AND rs\.chunk_id IN \(\$\{placeholders\}\)/);
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
