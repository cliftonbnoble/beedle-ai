import {readyReviewerQueue} from "./helpers/reviewer-queue.mjs";
import test from 'node:test';
import assert from 'node:assert/strict';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
const {listIngestionDocuments}=loadTs('src/services/admin-ingestion.ts');
// Synthetic reviewed policy lets this scope test contain accepted high-score
// rows. Production's rent-board taxonomy remains explicitly unreviewed.
const {getActiveTaxonomyConfig}=loadTs('src/services/template-config.ts');
Object.assign(getActiveTaxonomyConfig().inference_policy,{jurisdictions:['Test'],review_status:'reviewed_for_inference',reviewed_by:'Synthetic scope fixture',reviewed_at:'2026-10-26T00:00:00Z',evaluation_ref:'test-only'});
const scores=[
 {name:'fallback-high',fallback:true,confidence:0.9,low:true},
 {name:'scored-low',fallback:false,confidence:0.2,low:true},
 {name:'boundary',fallback:false,confidence:0.55,low:false},
 {name:'high',fallback:false,confidence:0.9,low:false},
 {name:'null',fallback:false,confidence:null,low:true},
 {name:'missing',fallback:false,low:true},
 {name:'zero',fallback:false,confidence:0,low:true},
 {name:'fallback-missing',fallback:true,low:true},
 {name:'bad-bool',fallback:false,confidence:false,low:true},
 {name:'bad-string',fallback:false,confidence:'0.2',low:true}
];
async function setup(){
 const {DB,sqlite}=memoryD1();const rows=[];let count=0;
 for(const fileType of ['decision_docx','law_pdf'])for(const status of ['pending','approved','rejected'])for(const published of [false,true])for(const fixture of [false,true])for(const taxonomy of scores){
  const id='row-'+count++;const title=(fixture?'Fixture ':'')+'Rent Board hearing '+id;const citation='RB-'+(count-1);const warnings=count%2===0?['Needs source check']:[];
  const tax={caseTypeId:"zoning_variance",caseTypeLabel:"Zoning Variance",inferenceVersion:"taxonomy-evidence-2",configVersion:"1.1.0",reviewStatus:"reviewed_for_inference",fallback:taxonomy.fallback,...('confidence'in taxonomy?{confidence:taxonomy.confidence}:{})};const metadata=JSON.stringify({taxonomy:tax,originalFilename:fixture?'fixture-case.docx':'received-case.docx'});
  sqlite.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,metadata_json,approved_at,rejected_at,searchable_at,extraction_warnings_json,extraction_confidence) VALUES(?,?,'Test',?,?,'source','https://example.invalid','2024-10-01','2024-10-01',?,?,?,?,?,0.8)`).run(id,fileType,title,citation,metadata,status==='approved'?'now':null,status==='rejected'?'now':null,published?'now':null,JSON.stringify(warnings));
  rows.push({id,fileType,status,published,fixture,taxonomy,warnings,title,citation});
 }
 return {env:{DB,SOURCE_PROXY_BASE_URL:'https://api.example'},sqlite,rows};
}
function matches(row,options){return row.taxonomy.low && (!options.fileType||row.fileType===options.fileType) && (!options.realOnly||!row.fixture) && (!options.hasWarnings||row.warnings.length>0) && (!options.query||row.title.includes(options.query)||row.citation.includes(options.query)) && (!options.status||options.status==='all'||(options.status==='searchable'?row.published:options.status==='staged'?!row.published:row.status===options.status));}

async function collect(env,options){let cursor;let first;const documents=[];for(let n=0;n<20;n++){const page=await listIngestionDocuments(env,{...options,limit:100,cursor});first??=page;documents.push(...page.documents);cursor=page.nextCursor;if(!cursor)return {...first,documents};}throw Error('Fixture pagination did not finish');}

test('fallback, weak and unknown taxonomy inputs obey every surrounding reviewer scope',async()=>{
 const {env,sqlite,rows}=await setup();try{
  await readyReviewerQueue(env);
  for(const status of ['all','pending','approved','rejected','staged','searchable'])for(const fileType of [undefined,'decision_docx','law_pdf'])for(const realOnly of [false,true]){
   const options={status,fileType,realOnly,lowConfidenceTaxonomyOnly:true,limit:1000};const result=await collect(env,options);const expected=rows.filter(row=>matches(row,options)).map(row=>row.id).sort();assert.deepEqual(result.documents.map(row=>row.id).sort(),expected,JSON.stringify(options));assert.ok(result.documents.every(row=>row.lowConfidenceTaxonomy));assert.equal(result.summary.withLowConfidenceTaxonomy,expected.length);
  }
 }finally{sqlite.close();}
});
test('later query predicates and derived-count queries keep the same conjunction semantics',async()=>{
 const {env,sqlite,rows}=await setup();try{
  await readyReviewerQueue(env);
  for(const sort of ['createdAtDesc','reviewerEffortAsc']){
   const options={status:'pending',fileType:'decision_docx',realOnly:true,hasWarnings:true,lowConfidenceTaxonomyOnly:true,query:'RB-1',sort,limit:1000};const result=await collect(env,options);const expected=rows.filter(row=>matches(row,options)).map(row=>row.id).sort();assert.deepEqual(result.documents.map(row=>row.id).sort(),expected);if(result.summary.derivedProcessingApplied)assert.equal(result.summary.derivedPrefilterCount,expected.length);
  }
 }finally{sqlite.close();}
});
