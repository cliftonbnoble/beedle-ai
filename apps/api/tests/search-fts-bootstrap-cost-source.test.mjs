import { readServiceBoundary } from './helpers/service-boundary-source.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
test('search requests use explicit read-only readiness without isolate-owned I/O or sticky failures',async()=>{
 const fts=await fs.readFile('src/services/search-fts.ts','utf8');const readiness=await fs.readFile('src/services/search-readiness.ts','utf8');const search=await readServiceBoundary('src/services/search.ts');
 assert.doesNotMatch(fts,/CREATE |ensureSearchFts|searchRuntimeIndexesEnsured|ftsVocabAvailable|ftsVocabEnsured|searchFtsAvailable =/);
 assert.match(readiness,/FROM sqlite_master/);assert.match(readiness,/search_schema_readiness/);assert.match(search,/await searchReadyEnvironment\(searchBudgetEnvironment\(original\)\)/);
 assert.doesNotMatch(readiness,/CREATE |INSERT |UPDATE |DELETE |let .*Promise/);
});
