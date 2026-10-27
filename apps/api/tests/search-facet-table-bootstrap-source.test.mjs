import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
test('facet schema belongs to migration 0027 and bootstrap writes are absent from search',async()=>{
 const request=await fs.readFile('src/services/search-fts.ts','utf8');const migration=await fs.readFile('migrations/0027_search_readiness.sql','utf8');
 assert.doesNotMatch(request,/CREATE (TABLE|INDEX|TRIGGER|VIRTUAL)|INSERT OR IGNORE|ensureDocumentFacetTables|documentFacetTablesEnsured|searchRuntimeIndexesPromise/);
 for(const name of ['document_index_codes','document_rules_sections','document_ordinance_sections','idx_document_index_codes_normalized','idx_document_rules_sections_normalized','idx_document_ordinance_sections_normalized','documents_ai_document_facets','documents_au_document_facets','documents_ad_document_facets'])assert.ok(migration.includes(name),name);
});
test('facet reconciliation retains exact normalization, scoped to a bounded maintenance batch',async()=>{
 const maintenance=await fs.readFile('src/services/search-facet-statements.ts','utf8');const builder=await fs.readFile('src/services/search-facet-build.ts','utf8');
 for(const token of ["'ic-%'","'rule %'","'ordinance %'","'ordinance.%'"])assert.ok(maintenance.includes(token),token);
 assert.equal((maintenance.match(/WHERE d.id IN/g)||[]).length,3);assert.match(builder,/LIMIT 100/);assert.match(builder,/await env.DB.batch\(statements\)/);
});
