import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
test('indexed lexical recall binds vocabulary and document scopes once',async()=>{
  const src=await fs.readFile('src/services/search-indexed-lexical.ts','utf8');
  assert.match(src,/json_each\(\?\)/);assert.match(src,/bind\(\.\.\.scopedParams,match\)/);
  const fts=await fs.readFile('src/services/search-fts.ts','utf8');
  assert.match(fts,/boundLexicalTermsForD1\(options.scanParityRankTerms,9,documentScopeParams.length\+2\)/);
  assert.doesNotMatch(fts,/maxScopedLexicalDocumentBatchSize|maxKeywordCandidateDocumentBatchSize/);
});
