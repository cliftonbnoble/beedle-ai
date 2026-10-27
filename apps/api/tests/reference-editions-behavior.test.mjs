import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {loadTs} from './helpers/load-ts.mjs';
import {memoryD1} from './helpers/d1-memory.mjs';
import {catalogPayload} from './helpers/catalog-fixture.mjs';
const {rebuildLegalReferences,buildDocumentReferenceValidationStatements}=loadTs('src/services/legal-references.ts');
const {resolveReferenceEdition,readReferenceEdition}=loadTs('src/services/reference-editions.ts');
const {retrieveRequestedLaw}=loadTs('src/services/draft-law.ts');
const {assembleDraft}=loadTs('src/services/draft-conclusions.ts');
const {restoreLegalCatalog,catalogState}=loadTs('src/services/legal-catalog.ts');
const {ingestDocumentBytes}=loadTs('src/services/ingest.ts');
const {updateIngestionMetadata,getIngestionDocumentDetail,reprocessIngestionDocument}=loadTs('src/services/admin-ingestion.ts');
const {restoreDocumentGeneration}=loadTs('src/services/document-generations.ts');
const {handleGetProvision}=loadTs('src/routes/provision.ts');
const {draftConclusionsRequestSchema,legalReferenceRebuildRequestSchema,legalResearchContextSchema}=loadTs('../../packages/shared/src/index.ts');
function descriptor(jurisdiction='Test',from='2010-01-01',to='2020-01-01'){
 const source=kind=>({uri:`https://example.invalid/synthetic/${kind}`,sha256:'a'.repeat(64),retrieved_at:'2026-01-01T00:00:00.000Z'});
 return {policy:'reviewed_interval',jurisdiction,effective_from:from,effective_to:to,sources:{index_codes:source('codes'),rules:source('rules'),ordinance:source('ordinance')},reviewed_by:'Synthetic declared fixture reviewer',reviewed_at:'2026-01-01T00:00:00.000Z',notes:'Synthetic test metadata; no human legal evaluation is claimed.'};
}
async function setup(){const {DB,sqlite}=memoryD1();const objects=new Map();return {sqlite,env:{DB,SOURCE_PROXY_BASE_URL:'https://api.example',R2_PUBLIC_BASE_URL:'https://example.invalid',SOURCE_BUCKET:{async put(key,bytes){objects.set(key,new Uint8Array(bytes));},async get(key){const bytes=objects.get(key);return bytes?{size:bytes.length,arrayBuffer:async()=>bytes.slice().buffer}:null;},async delete(key){objects.delete(key);}}}};}
async function publish(env,label,edition){const payload=catalogPayload(label);if(edition)payload.edition=edition;return (await rebuildLegalReferences(env,payload)).publication.version;}
const law=(context,extra={})=>draftConclusionsRequestSchema.parse({rules_sections:['1.2'],law_context:context,...extra});
async function provision(env,url){const u=new URL(url);return handleGetProvision(Object.assign(new Request(url),{params:{kind:u.pathname.split('/').at(-2),id:decodeURIComponent(u.pathname.split('/').at(-1))}}),env);}
async function doc(env,date='2019-04-01'){
 const source='# Index Codes\n\n100\n\n# Rules\n\nRule 1.2\n\n# Ordinance\n\nOrdinance 37.8\n\n# Findings of Fact\n\nThe tenant gave written notice of needed repairs and the landlord received it. The parties disputed the record.\n\n# Conclusions of Law\n\nThe remedy requires review of the evidence under Rule 1.2 and Ordinance 37.8.';
 return (await ingestDocumentBytes(env,{title:'Edition validation',citation:'T-18',jurisdiction:'Test',decisionDate:date,fileType:'decision_docx',performVectorUpsert:false,sourceFile:{filename:'edition.md',mimeType:'text/markdown'}},new TextEncoder().encode(source))).documentId;
}

test('same citation resolves to exact immutable text across dates and jurisdictions; end dates are exclusive',async()=>{
 const {env,sqlite}=await setup();try{
  const old=await publish(env,'Old Test',descriptor()),current=await publish(env,'New Test',descriptor('Test','2020-01-01',null));await publish(env,'Other place',descriptor('Other','2010-01-01',null));
  for(const [jurisdiction,date,text,id] of [['Test','2019-12-31','Old Test',old],['Test','2020-01-01','New Test',current],['Other','2019-12-31','Other place',null]]){
   const result=await retrieveRequestedLaw(env,law({jurisdiction,as_of_date:date}));assert.equal(result.authorities.length,1);const source=result.authorities[0];assert.match(source.snippet,new RegExp(text));assert.equal(source.reference_edition.status,'matched');assert.equal(source.reference_edition.date_basis,'user_supplied');assert.equal(source.reference_edition.review_required,true);assert.match(source.reference_edition.body_sha256,/^[a-f0-9]{64}$/);if(id)assert.equal(source.reference_edition.edition_id,id);
  }
  const absent=await retrieveRequestedLaw(env,law({jurisdiction:'Test',as_of_date:'2009-12-31'}));assert.equal(absent.authorities.length,0);assert.equal(absent.edition.status,'unavailable');assert.match(absent.warnings.join(' '),/No current-catalog text/);
 }finally{sqlite.close();}
});

test('overlapping editions abstain, explicit choices recheck jurisdiction/date, and repeated identical imports are equivalent',async()=>{
 const {env,sqlite}=await setup();try{
  const first=await publish(env,'First',descriptor('Test','2010-01-01',null));await publish(env,'Second',descriptor('Test','2015-01-01',null));
  const context={jurisdiction:'Test',as_of_date:'2019-01-01'};
  assert.equal((await retrieveRequestedLaw(env,law(context))).edition.status,'ambiguous');assert.equal((await retrieveRequestedLaw(env,law(context))).authorities.length,0);
  assert.equal((await retrieveRequestedLaw(env,law({...context,edition_id:first}))).authorities[0].reference_edition.edition_id,first);
  assert.equal((await retrieveRequestedLaw(env,law({...context,jurisdiction:'Other',edition_id:first}))).edition.reason,'jurisdiction_mismatch');
  assert.equal((await retrieveRequestedLaw(env,law({...context,as_of_date:'2009-01-01',edition_id:first}))).edition.reason,'outside_reviewed_interval');
  await publish(env,'First',descriptor('Test','2010-01-01',null));
  assert.equal((await resolveReferenceEdition(env,{jurisdiction:'Test',as_of_date:'2012-01-01'})).status,'matched','identical payload/descriptor imports do not introduce false ambiguity');
 }finally{sqlite.close();}
});

test('legacy/unknown dates and unreviewed editions stay unknown without inventing source hashes or dates',async()=>{
 const {env,sqlite}=await setup();try{
  const id=await publish(env,'Unreviewed');const edition=await readReferenceEdition(env,id);assert.equal(edition.descriptor.sources,null);assert.equal(edition.descriptor.effective_from,null);
  const compatible=await retrieveRequestedLaw(env,law(undefined));assert.equal(compatible.authorities.length,1);assert.equal(compatible.authorities[0].reference_edition.status,'unknown');
  const explicitUnknown=await retrieveRequestedLaw(env,law({edition_id:id}));assert.equal(explicitUnknown.authorities[0].reference_edition.status,'unknown');
  const dated=await retrieveRequestedLaw(env,law({jurisdiction:'Test',as_of_date:'2019-01-01'}));assert.equal(dated.authorities.length,0);
  assert.equal((await resolveReferenceEdition(env,undefined)).edition_id,null);
 }finally{sqlite.close();}
});

test('edition metadata and source rows remain immutable; historical source links replay after replacement',async()=>{
 const {env,sqlite}=await setup();try{
  const old=await publish(env,'Historical',descriptor());const source=(await retrieveRequestedLaw(env,law({jurisdiction:'Test',as_of_date:'2019-01-01'}))).authorities[0];
  assert.throws(()=>sqlite.prepare("UPDATE legal_reference_editions SET descriptor_json='{}' WHERE catalog_id=?").run(old),/immutable/);assert.throws(()=>sqlite.prepare('DELETE FROM legal_reference_editions WHERE catalog_id=?').run(old),/immutable/);
  await publish(env,'Replacement',descriptor('Test','2020-01-01',null));const response=await provision(env,source.source_link);assert.equal(response.status,200);const body=await response.json();assert.match(body.body,/Historical/);assert.equal(body.catalogVersion,old);assert.equal(body.bodySha256,source.reference_edition.body_sha256);
  assert.equal((await provision(env,source.source_link.replace(/sha256=[^&]+/,'sha256=wrong'))).status,409);
  assert.equal((await provision(env,source.source_link.replace(/catalog=[^&]+/,'catalog=missing'))).status,404);
 }finally{sqlite.close();}
});

test('removed/repealed provisions and missing subsections never borrow current or parent text',async()=>{
 const {env,sqlite}=await setup();try{
  await publish(env,'Historical',descriptor());const payload=catalogPayload('After repeal');payload.edition=descriptor('Test','2020-01-01',null);payload.rules_sections=[{section_number:'1.3',body_text:'Replacement procedure requires review.'}];await rebuildLegalReferences(env,payload);
  const current=await retrieveRequestedLaw(env,law({jurisdiction:'Test',as_of_date:'2021-01-01'}));assert.deepEqual(current.authorities,[]);assert.match(current.warnings.join(' '),/no parent or different edition/);
  const old=await retrieveRequestedLaw(env,law({jurisdiction:'Test',as_of_date:'2019-01-01'}));assert.equal(old.authorities.length,1);
  const child=await retrieveRequestedLaw(env,law({jurisdiction:'Test',as_of_date:'2019-01-01'},{rules_sections:['1.2(a)']}));assert.equal(child.authorities.length,0);
 }finally{sqlite.close();}
});

test('document validation binds exact historical bodies and date edits preserve the earlier validation context',async()=>{
 const {env,sqlite}=await setup();try{
  const old=await publish(env,'Old document law',descriptor()),current=await publish(env,'New document law',descriptor('Test','2020-01-01',null));const id=await doc(env);let detail=await getIngestionDocumentDetail(env,id);
  assert.equal(detail.referenceEditionContext.edition_id,old);assert.equal(detail.referenceEditionContext.date_basis,'decision_date_proxy');assert.equal(detail.referenceEditionContext.as_of_date,'2019-04-01');
  const initial=sqlite.prepare("SELECT edition_id,edition_body_sha256 FROM document_reference_links WHERE document_id=? AND reference_type='rules_section'").get(id);assert.equal(initial.edition_id,old);assert.match(initial.edition_body_sha256,/^[a-f0-9]{64}$/);
  detail=await updateIngestionMetadata(env,id,{expectedRevision:detail.documentRevision,mutationId:crypto.randomUUID(),decision_date:'2021-04-01'});assert.equal(detail.referenceEditionContext.edition_id,current);
  const history=sqlite.prepare('SELECT edition_context_json,links_json FROM document_reference_validation_history WHERE document_id=? ORDER BY rowid DESC LIMIT 1').get(id);assert.equal(JSON.parse(history.edition_context_json).edition_id,old);assert.ok(JSON.parse(history.links_json).some(link=>link.edition===old&&link.editionBodySha256===initial.edition_body_sha256));
  const receipt=sqlite.prepare('SELECT before_json,after_json FROM reviewer_mutations WHERE document_id=?').get(id);assert.equal(JSON.parse(receipt.before_json).reference_edition_context.edition_id,old);assert.equal(JSON.parse(receipt.after_json).reference_edition_context.edition_id,current);
 }finally{sqlite.close();}
});

test('reprocessing and v3 restore preserve edition bindings and context with their complete source generation',async()=>{
 const {env,sqlite}=await setup();try{
  const old=await publish(env,'Historical',descriptor());await publish(env,'Current',descriptor('Test','2020-01-01',null));const id=await doc(env);const before=await getIngestionDocumentDetail(env,id);
  const reprocessed=await reprocessIngestionDocument(env,id);const retained=reprocessed.reprocessArtifacts.previousGenerationId;assert.equal(sqlite.prepare('SELECT manifest_version FROM document_generations WHERE id=?').get(retained).manifest_version,3);
  const source=sqlite.prepare('SELECT edition_body_sha256 FROM staged_document_reference_links WHERE generation_id=? AND reference_type=\'rules_section\'').get(retained);assert.ok(source.edition_body_sha256);
  await restoreDocumentGeneration(env,id,retained,reprocessed.documentRevision);const restored=await getIngestionDocumentDetail(env,id);assert.equal(restored.referenceEditionContext.edition_id,old);assert.equal(restored.referenceEditionContext.as_of_date,before.referenceEditionContext.as_of_date);assert.equal(restored.approvedAt,null);
 }finally{sqlite.close();}
});

test('registry epoch detects an A to B to A switch before publishing validation',async()=>{
 const {env,sqlite}=await setup();try{
  const first=await publish(env,'First',descriptor());const id=await doc(env);const statements=await buildDocumentReferenceValidationStatements(env,id,{indexCodes:['100'],rulesSections:['1.2'],ordinanceSections:['37.8']});
  await publish(env,'Overlap',descriptor('Test','2015-01-01',null));await restoreLegalCatalog(env,{versionId:first,expectedEpoch:(await catalogState(env)).epoch});
  const before=sqlite.prepare('SELECT reference_edition_context_json,document_revision FROM documents WHERE id=?').get(id);
  await assert.rejects(env.DB.batch(statements),/catalog_revision_conflict/);assert.deepEqual(sqlite.prepare('SELECT reference_edition_context_json,document_revision FROM documents WHERE id=?').get(id),before);
 }finally{sqlite.close();}
});

test('invalid calendar dates, incomplete reviewed assertions and partial context reject before database writes',async()=>{
 let calls=0;const env={DB:{prepare(){calls++;throw new Error('No work expected');}}};
 for(const edition of [{...descriptor(),effective_from:'2023-02-29'},{...descriptor(),effective_to:'2010-01-01'},{...descriptor(),sources:null},{...descriptor(),reviewed_by:null}])await assert.rejects(rebuildLegalReferences(env,{...catalogPayload(),edition}));
 assert.equal(calls,0);assert.equal(legalResearchContextSchema.safeParse({jurisdiction:'Test'}).success,false);assert.equal(legalResearchContextSchema.safeParse({jurisdiction:'Test',as_of_date:'2024-02-29'}).success,true);assert.equal(legalResearchContextSchema.safeParse({jurisdiction:'Test',as_of_date:'2100-02-29'}).success,false);
 assert.equal(legalReferenceRebuildRequestSchema.safeParse({...catalogPayload(),edition:{...descriptor(),invented:true}}).success,false);
});

test('populated migration preserves legacy text and labels absent edition provenance unknown',()=>{
 const {DB,sqlite}=memoryD1({migrationThrough:30});void DB;try{
  sqlite.exec("INSERT INTO legal_catalog_versions(id,base_epoch,state,created_at) VALUES('old-fixture',0,'staging','old')");
  sqlite.prepare('INSERT INTO legal_catalog_rows VALUES(?,?,?,?)').run('old-fixture','legal_rules_sections','old-rule',JSON.stringify({id:'old-rule',citation:'1.2',body_text:'Retained historical bytes.',active:1}));
  sqlite.exec("UPDATE legal_catalog_versions SET state='retained',published_at='old' WHERE id='old-fixture'");sqlite.exec(readFileSync(new URL('../migrations/0031_reference_editions.sql',import.meta.url),'utf8'));
  const legacy=sqlite.prepare("SELECT descriptor_json,descriptor_sha256 FROM legal_reference_editions WHERE catalog_id='old-fixture'").get();assert.equal(legacy.descriptor_sha256,null);const descriptor=JSON.parse(legacy.descriptor_json);assert.equal(descriptor.policy,'unknown');assert.equal(descriptor.sources,null);assert.equal(descriptor.effective_from,null);assert.match(sqlite.prepare("SELECT payload_json FROM legal_catalog_rows WHERE catalog_id='old-fixture'").get().payload_json,/Retained historical bytes/);
 }finally{sqlite.close();}
});

test('saved drafts and every review export retain the exact edition/context and body hash after publication changes',async()=>{
 const {env,sqlite}=await setup();try{
  const old=await publish(env,'Saved historical text',descriptor());const context={jurisdiction:'Test',as_of_date:'2019-02-01'};const parsed=law(context);const lawResult=await retrieveRequestedLaw(env,parsed);
  const research={query_summary:'Synthetic law research',similar_cases:[],relevant_law:lawResult.authorities,outcome_guidance:{direction:'unclear',rationale:'Human review required'},reasoning_themes:[],vulnerabilities:lawResult.warnings.map(impact=>({issue:'Law review',impact,citation_ids:[]})),strengthening_suggestions:[],confidence:'low',citations:[],guardrails:[]};
  const {draft}=await assembleDraft(parsed,research,env);const saved=JSON.parse(JSON.stringify(draft));await publish(env,'Later text',descriptor('Test','2020-01-01',null));
  assert.equal(saved.supporting_authorities[0].reference_edition.edition_id,old);assert.equal(saved.provenance.sources[0].reference_edition.body_sha256,lawResult.authorities[0].reference_edition.body_sha256);assert.deepEqual(saved.research_context.law_context,context);
  const {renderReviewPackage}=loadTs('src/services/draft-review-package.ts');for(const format of ['markdown','text','html']){const content=renderReviewPackage(saved,'Historical review',format);assert.ok(content.includes(old));assert.ok(content.includes(lawResult.authorities[0].reference_edition.body_sha256));assert.ok(content.includes('2019-02-01'));}
  const source=await(await provision(env,saved.supporting_authorities[0].source_link)).json();assert.match(source.body,/Saved historical text/);
 }finally{sqlite.close();}
});

test('selection changes during immutable provision retrieval return a conflict instead of an unpinned context claim',async()=>{
 const {env,sqlite}=await setup();try{
  await publish(env,'First selection',descriptor());const prepare=env.DB.prepare;let switched=false;
  env.DB.prepare=sql=>{const statement=prepare(sql);if(!sql.includes('SELECT row_id AS id,payload_json AS payload'))return statement;
   const bind=statement.bind;statement.bind=function(...args){const bound=bind.apply(this,args),all=bound.all;bound.all=async()=>{const rows=await all.call(bound);if(!switched){switched=true;await publish({...env,DB:{...env.DB,prepare}},'Concurrent overlap',descriptor('Test','2015-01-01',null));}return rows;};return bound;};return statement;};
  await assert.rejects(retrieveRequestedLaw(env,law({jurisdiction:'Test',as_of_date:'2019-01-01'})),/registry changed/);assert.equal(switched,true);
 }finally{sqlite.close();}
});

test('v2 generation restore keeps its original catalog but marks previously uncaptured editions unknown',async()=>{
 const {env,sqlite}=await setup();try{
  await publish(env,'Version 2 fixture',descriptor());const id=await doc(env);const result=await reprocessIngestionDocument(env,id);const retained=result.reprocessArtifacts.previousGenerationId;
  const {generationTables}=loadTs('src/services/generation-tables.ts'),{sha256Bytes}=loadTs('src/lib/digests.ts');const manifest={};
  for(const [table,columns] of Object.entries(generationTables)){const rows=sqlite.prepare(`SELECT ${columns.filter(column=>!['edition_id','edition_provision_id','edition_body_sha256','applicability_status'].includes(column)).join(',')} FROM staged_${table} WHERE generation_id=? ORDER BY id`).all(retained);manifest[table]={count:rows.length,sha256:await sha256Bytes(new TextEncoder().encode(JSON.stringify(rows)))};}
  sqlite.prepare("UPDATE document_generations SET manifest_version=2,manifest_json=?,snapshot_json=json_remove(snapshot_json,'$.reference_edition_context_json') WHERE id=?").run(JSON.stringify(manifest),retained);
  sqlite.prepare("UPDATE staged_document_reference_links SET edition_id=NULL,edition_provision_id=NULL,edition_body_sha256=NULL,applicability_status='unknown' WHERE generation_id=?").run(retained);
  await restoreDocumentGeneration(env,id,retained,result.documentRevision);const detail=await getIngestionDocumentDetail(env,id);assert.equal(detail.referenceEditionContext.status,'unknown');assert.ok(detail.referenceEditionBindings.every(binding=>binding.applicabilityStatus==='unknown'));assert.equal(detail.approvedAt,null);
 }finally{sqlite.close();}
});

test('explicit supersession corrects an open interval without erasing history or resurrecting it through identical reimport',async()=>{
 const {env,sqlite}=await setup();try{
  const open=descriptor('Test','2010-01-01',null),old=await publish(env,'Original open interval',open);const source=(await retrieveRequestedLaw(env,law({jurisdiction:'Test',as_of_date:'2019-01-01'}))).authorities[0];
  const current=await publish(env,'New law',descriptor('Test','2020-01-01',null));
  const corrected=await publish(env,'Corrected historic interval',{...descriptor(),supersedes:[old]});
  assert.equal((await resolveReferenceEdition(env,{jurisdiction:'Test',as_of_date:'2019-01-01'})).edition_id,corrected);assert.equal((await resolveReferenceEdition(env,{jurisdiction:'Test',as_of_date:'2021-01-01'})).edition_id,current);
  assert.equal((await resolveReferenceEdition(env,{jurisdiction:'Test',as_of_date:'2019-01-01',edition_id:old})).reason,'edition_applicability_assertion_superseded');
  const replay=await provision(env,source.source_link);assert.equal(replay.status,200);const historical=await replay.json();assert.match(historical.body,/Original open interval/);assert.deepEqual(historical.edition.supersededBy,[corrected]);assert.match(historical.limitation,/superseded/);
  const duplicate=await publish(env,'Original open interval',open);assert.equal((await readReferenceEdition(env,duplicate)).supersededBy[0],corrected);assert.equal((await resolveReferenceEdition(env,{jurisdiction:'Test',as_of_date:'2019-01-01'})).edition_id,corrected);
  assert.throws(()=>sqlite.exec('DELETE FROM legal_reference_edition_supersessions'),/immutable/);
 }finally{sqlite.close();}
});

test('unpublished correction edges have no effect and cross-jurisdiction supersession fails without changing the active catalog',async()=>{
 const {env,sqlite}=await setup();try{
  const old=await publish(env,'Existing',descriptor('Test','2010-01-01',null));const before=await catalogState(env);const batch=env.DB.batch;
  env.DB.batch=async statements=>{if(statements.some(s=>s.sql.includes('INSERT INTO legal_catalog_guards')))throw new Error('Synthetic publication interruption');return batch(statements);};
  await assert.rejects(publish(env,'Correction',{...descriptor(),supersedes:[old]}),/publication interruption/);env.DB.batch=batch;
  assert.equal((await resolveReferenceEdition(env,{jurisdiction:'Test',as_of_date:'2019-01-01'})).edition_id,old);assert.deepEqual((await readReferenceEdition(env,old)).supersededBy,[]);
  await assert.rejects(publish(env,'Wrong jurisdiction',{...descriptor('Other'),supersedes:[old]}),/same jurisdiction/);assert.deepEqual(await catalogState(env),before);
 }finally{sqlite.close();}
});

test('a known historical edition validates an old citation even when it is absent from the active catalog',async()=>{
 const {env,sqlite}=await setup();try{
  const historical=await publish(env,'Historical citation',descriptor());const payload={...catalogPayload('Current different rule'),edition:descriptor('Test','2020-01-01',null)};payload.rules_sections=[{section_number:'1.3',body_text:'Current replacement procedure.'}];const active=(await rebuildLegalReferences(env,payload)).publication.version;
  const id=await doc(env);const link=sqlite.prepare("SELECT is_valid,catalog_version,edition_id FROM document_reference_links WHERE document_id=? AND reference_type='rules_section'").get(id);
  assert.equal(link.is_valid,1);assert.equal(link.catalog_version,historical);assert.equal(link.edition_id,historical);assert.equal(sqlite.prepare('SELECT reference_catalog_version FROM documents WHERE id=?').get(id).reference_catalog_version,active);
  const detail=await getIngestionDocumentDetail(env,id);assert.ok(detail.validReferences.rulesSections.includes('1.2'));assert.ok(!detail.extractionWarnings.some(warning=>warning.includes('Unknown rules references')));
 }finally{sqlite.close();}
});

test('edition publication during model generation cannot return a newly stale applicability claim',async()=>{
 const {env,sqlite}=await setup();const original=globalThis.fetch;try{
  await publish(env,'Before generation',descriptor());const parsed=law({jurisdiction:'Test',as_of_date:'2019-01-01'}),selected=await retrieveRequestedLaw(env,parsed);
  const research={query_summary:'Synthetic research',similar_cases:[],relevant_law:selected.authorities,outcome_guidance:{direction:'unclear',rationale:'Review'},reasoning_themes:[],vulnerabilities:[],strengthening_suggestions:[],confidence:'low',citations:[],guardrails:[]};
  globalThis.fetch=async()=>{await publish(env,'Correction during model',descriptor('Test','2015-01-01',null));return Response.json({choices:[{message:{content:'1. Review the captured source.'},finish_reason:'stop'}]});};
  await assert.rejects(assembleDraft(parsed,research,{...env,LLM_API_KEY:'synthetic',LLM_MODEL:'synthetic',LLM_BASE_URL:'https://example.invalid'}),/registry changed/);
 }finally{globalThis.fetch=original;sqlite.close();}
});
