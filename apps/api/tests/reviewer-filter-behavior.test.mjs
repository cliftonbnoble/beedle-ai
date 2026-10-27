import test from 'node:test';
import assert from 'node:assert/strict';
import {memoryD1} from './helpers/d1-memory.mjs';
import {loadTs} from './helpers/load-ts.mjs';
const {listIngestionDocuments}=loadTs('src/services/admin-ingestion.ts');
const scores=[
 {name:'fallback-high',fallback:true,confidence:0.9,low:true},
 {name:'scored-low',fallback:false,confidence:0.2,low:true},
 {name:'boundary',fallback:false,confidence:0.45,low:false},
 {name:'high',fallback:false,confidence:0.9,low:false},
 {name:'null',fallback:false,confidence:null,low:false},
 {name:'missing',fallback:false,low:false},
 {name:'zero',fallback:false,confidence:0,low:true},
 {name:'fallback-missing',fallback:true,low:true},
 {name:'bad-bool',fallback:false,confidence:false,low:false},
 {name:'bad-string',fallback:false,confidence:'0.2',low:false}
];
async function setup(){
 const {DB,sqlite}=memoryD1();const rows=[];let count=0;
 for(const fileType of ['decision_docx','law_pdf'])for(const status of ['pending','approved','rejected'])for(const published of [false,true])for(const fixture of [false,true])for(const taxonomy of scores){
  const id='row-'+count++;const title=(fixture?'Fixture ':'')+'Rent Board hearing '+id;const citation='RB-'+(count-1);const warnings=count%2===0?['Needs source check']:[];
  const tax={fallback:taxonomy.fallback,...('confidence'in taxonomy?{confidence:taxonomy.confidence}:{})};const metadata=JSON.stringify({taxonomy:tax,originalFilename:fixture?'fixture-case.docx':'received-case.docx'});
  sqlite.prepare(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,metadata_json,approved_at,rejected_at,searchable_at,extraction_warnings_json,extraction_confidence) VALUES(?,?,'Test',?,?,'source','https://example.invalid','2024-10-01','2024-10-01',?,?,?,?,?,0.8)`).run(id,fileType,title,citation,metadata,status==='approved'?'now':null,status==='rejected'?'now':null,published?'now':null,JSON.stringify(warnings));
  rows.push({id,fileType,status,published,fixture,taxonomy,warnings,title,citation});
 }
 return {env:{DB,SOURCE_PROXY_BASE_URL:'https://api.example'},sqlite,rows};
}
function matches(row,options){return row.taxonomy.low && (!options.fileType||row.fileType===options.fileType) && (!options.realOnly||!row.fixture) && (!options.hasWarnings||row.warnings.length>0) && (!options.query||row.title.includes(options.query)||row.citation.includes(options.query)) && (!options.status||options.status==='all'||(options.status==='searchable'?row.published:options.status==='staged'?!row.published:row.status===options.status));}

test('both low-confidence OR branches obey every surrounding reviewer scope',async()=>{
 const {env,sqlite,rows}=await setup();try{
  for(const status of ['all','pending','approved','rejected','staged','searchable'])for(const fileType of [undefined,'decision_docx','law_pdf'])for(const realOnly of [false,true]){
   const options={status,fileType,realOnly,lowConfidenceTaxonomyOnly:true,limit:1000};const result=await listIngestionDocuments(env,options);const expected=rows.filter(row=>matches(row,options)).map(row=>row.id).sort();assert.deepEqual(result.documents.map(row=>row.id).sort(),expected,JSON.stringify(options));assert.ok(result.documents.every(row=>row.lowConfidenceTaxonomy));assert.equal(result.summary.withLowConfidenceTaxonomy,expected.length);
  }
 }finally{sqlite.close();}
});
test('later query predicates and derived-count queries keep the same conjunction semantics',async()=>{
 const {env,sqlite,rows}=await setup();try{
  for(const sort of ['createdAtDesc','reviewerEffortAsc']){
   const options={status:'pending',fileType:'decision_docx',realOnly:true,hasWarnings:true,lowConfidenceTaxonomyOnly:true,query:'RB-1',sort,limit:1000};const result=await listIngestionDocuments(env,options);const expected=rows.filter(row=>matches(row,options)).map(row=>row.id).sort();assert.deepEqual(result.documents.map(row=>row.id).sort(),expected);if(result.summary.derivedProcessingApplied)assert.equal(result.summary.derivedPrefilterCount,expected.length);
  }
 }finally{sqlite.close();}
});
