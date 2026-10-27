import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import ts from 'typescript';
import {loadTs} from './helpers/load-ts.mjs';
import {queryCases,rows} from './helpers/search-boundary-cases.mjs';
const q=loadTs('src/services/search-query-analysis.ts'),s=loadTs('src/services/search-scoring.ts');
const {searchRequestSchema}=loadTs('../../packages/shared/src/index.ts');
// Captured from the verified Medium baseline (15f5ea5), before relocating any implementation.
const baseline=JSON.parse(fs.readFileSync(new URL('./fixtures/search-boundary-parity.json',import.meta.url),'utf8'));
test('query planning, scope, adaptive limits, ranking reasons and source snippets retain the pre-extraction behavior',()=>{
 const actual=[];
 for(const [query,filters] of queryCases){
  const parsed=searchRequestSchema.parse({query,filters});
  const effective=s.enhanceQueryWithIndexCodeContext(query,parsed.filters);
  const context={query:effective,retrievalQuery:s.expandQueryForRetrieval(effective),vectorQuery:s.chooseVectorQuery(effective),queryType:'keyword',filters:parsed.filters,snippetMaxLength:260};
  context.derived=q.buildQueryDerivedContext(context);
  const candidates=rows.map((row,i)=>({row,diagnostics:s.scoreRow(row,i?0.78:0,context)}));
  const passages=candidates.map(x=>s.toSearchResultPassage(x,context));
  actual.push({query,filters:parsed.filters,scope:q.buildSearchScope(parsed,parsed.corpusMode),recall:q.buildAdaptiveRecallConfig(parsed,80),expanded:context.retrievalQuery,vector:context.vectorQuery,scores:candidates.map(x=>x.diagnostics),passages,snippet:s.buildLayeredResultSnippet(context,...passages)});
 }
 assert.deepEqual(JSON.parse(JSON.stringify(actual)),baseline);
});
const pure=['search-query-plan','search-evidence-policy','search-filter-policy','search-snippets','reviewer-policy'];
const modules=[...pure,'search-ranking','search-candidates','search-scoring','search-query-analysis','search-scope-repository','search-pipeline','search','reviewer-read-model','reviewer-contracts','admin-ingestion'];
test('policy modules have no binding access and stage dependencies remain acyclic',()=>{
 const edges=new Map();
 for(const name of modules){const file=new URL('../src/services/'+name+'.ts',import.meta.url),src=fs.readFileSync(file,'utf8'),ast=ts.createSourceFile(file.pathname,src,ts.ScriptTarget.Latest,true),deps=[];
  for(const node of ast.statements){if(!ts.isImportDeclaration(node)&&!ts.isExportDeclaration(node))continue;if(!node.moduleSpecifier||node.isTypeOnly||node.importClause?.isTypeOnly)continue;const target=path.basename(node.moduleSpecifier.text);if(modules.includes(target))deps.push(target);}
  edges.set(name,deps);
  if(pure.includes(name)){function visit(node){if(ts.isPropertyAccessExpression(node))assert.ok(!['DB','SOURCE_BUCKET','AI','VECTOR_INDEX','VECTOR_JOBS_QUEUE'].includes(node.name.text),name+' accesses '+node.name.text);if(ts.isCallExpression(node)&&ts.isIdentifier(node.expression))assert.notEqual(node.expression.text,'fetch',name+' performs network I/O');if(ts.isImportSpecifier(node))assert.notEqual(node.name.text,'Env',name+' depends on runtime bindings');ts.forEachChild(node,visit);}visit(ast);assert.ok(!deps.some(dep=>['search','search-pipeline','admin-ingestion','reviewer-read-model','search-scope-repository'].includes(dep)),name+' imports an I/O boundary');}
 }
 function visit(name,stack=[]){assert.ok(!stack.includes(name),'Cycle: '+[...stack,name].join(' -> '));for(const dependency of edges.get(name)||[])visit(dependency,[...stack,name]);}
 for(const name of modules)visit(name);
});
