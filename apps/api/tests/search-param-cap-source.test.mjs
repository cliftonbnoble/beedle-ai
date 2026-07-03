import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const searchServicePath = path.resolve(process.cwd(), "src/services/search.ts");

// SEARCH-05 param-cap sweep: D1 rejects a prepared statement with more than ~100 bound parameters. Every
// lexical recall query binds several parameters per query term, so its term expansion must be capped or a
// broad curated family (optionally combined with a filter that inflates the WHERE binds) overflows the
// limit and 400s. A single shared helper caps each query's terms to fit the remaining budget; because the
// cap equals D1's limit it is a no-op for any query already under it (so no ranking regression — the
// golden net stays byte-identical), and it guarantees no lexical statement can exceed the limit. The
// degrade-on-"too many SQL variables" path (search-query-degradation-source) remains a backstop.
test("all lexical recall queries cap term expansion under D1's bound-parameter limit", async () => {
  const src = (await Promise.all((await fs.readdir(path.resolve(process.cwd(), "src/services"))).filter((f) => /^search.*\.ts$/.test(f)).sort().map((f) => fs.readFile(path.resolve(process.cwd(), "src/services", f), "utf8")))).join("\n").replace(/^export /gm, "");
  const searchQueryAnalysisSrc = await fs.readFile(path.resolve(process.cwd(), "src/services/search-query-analysis.ts"), "utf8");

  // The limit constant and the shared cap helper exist, with the correct math.
  assert.match(searchQueryAnalysisSrc, /const D1_MAX_BOUND_PARAMS = 100;/);
  assert.match(
    src,
    /function boundLexicalTermsForD1\(terms: string\[\], perTerm: number, fixedParams: number\): string\[\] \{\s*\n\s*const maxTerms = Math\.max\(1, Math\.floor\(\(D1_MAX_BOUND_PARAMS - fixedParams\) \/ perTerm\)\);\s*\n\s*return terms\.length > maxTerms \? terms\.slice\(0, maxTerms\) : terms;/
  );

  // Applied at every lexical query-building site, with the per-term cost matching that statement's shape.
  // fetchKeywordCandidateDocumentIds + the issue-candidate phrase query bind 9 params/term (match + rank).
  assert.match(src, /const terms = boundLexicalTermsForD1\(keywordCandidateTerms\(query, normalizedQueryContext\), 9, documentScopeParams\.length \+ 1\)/);
  assert.match(src, /const phraseLexicalTerms = boundLexicalTermsForD1\(lexicalTerms\(phraseQuery\), 9, params\.length \+ 1\)/);
  // lexicalSearch + lexicalSearchWholeWord bind 18 params/term (four match/rank builders) + scope twice.
  const boundedTerms18 = /const boundedTerms = boundLexicalTermsForD1\(terms, 18, documentScopeParams\.length \* 2 \+ 1\)/g;
  assert.equal((src.match(boundedTerms18) || []).length, 2, "expected the 18-params/term cap in both lexicalSearch and lexicalSearchWholeWord");

  // Those four-builder statements consume the bounded terms — assert both functions' primary builders use
  // boundedTerms (fetchKeywordCandidateDocumentIds instead caps its own `terms` at the definition above).
  const primaryBuildersBounded = /buildLexicalMatchClause\("c\.chunk_text", "d\.citation", "d\.title", "d\.author_name", boundedTerms\)/;
  assert.match(src, primaryBuildersBounded);
  assert.match(src, /buildWholeWordLexicalMatchClause\("c\.chunk_text", "d\.citation", "d\.title", "d\.author_name", boundedTerms\)/);
});
