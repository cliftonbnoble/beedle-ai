import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { loadTs } from './helpers/load-ts.mjs';
import { memoryD1 } from './helpers/d1-memory.mjs';
const {parseMarkdownDocument,parseRestoredMarkdownDocument}=loadTs('src/services/parser.ts');
const {detectSourceFormat}=loadTs('src/services/source-format.ts');
const {reprocessIngestionDocument}=loadTs('src/services/admin-ingestion.ts');
const sha=x=>createHash('sha256').update(x).digest('hex');
const bytes=s=>new TextEncoder().encode(s);
const sentence='Synthetic evidence records written notice and the response without asserting any governing law.';

test('the recovery view preserves original bytes and every non-whitespace character across form-feed pages',async()=>{
  const original=bytes('# Findings\n\n'+sentence+'\f'+sentence+' Second page.');const before=original.slice();
  assert.throws(()=>detectSourceFormat(original,'source.md','decision_docx'),/Markdown/);
  const result=await parseRestoredMarkdownDocument(original,sha(original));
  assert.deepEqual(original,before);assert(result.plainText.includes('Second page'));assert.equal(result.extraction.parser,'recovered-markdown/1');
  const proof=result.extraction.restoration;assert.equal(proof.sourceSha256,sha(original));assert.equal(proof.formFeedPageBreaks,1);
  assert.equal(proof.nonWhitespaceSha256,sha(bytes(new TextDecoder().decode(original).replace(/\s/gu,''))));
  assert.equal(proof.viewSha256,sha(bytes(new TextDecoder().decode(original).replace(/\f/g,'\n\n'))));
  assert.throws(()=>detectSourceFormat(original,'source.md','decision_docx'),/Markdown/);
});

test('the larger recovery bound does not change ordinary intake limits before or after use',async()=>{
  const original=bytes('# Findings\n\n'+(sentence+'\n\n').repeat(3000));assert(original.length>250000&&original.length<500000);
  assert.throws(()=>parseMarkdownDocument(original),/bounded intake/);assert((await parseRestoredMarkdownDocument(original,sha(original))).plainText.length>250000);assert.throws(()=>parseMarkdownDocument(original),/bounded intake/);
});

test('wrong digests, malformed UTF8, other controls, markup, unreadable glyphs and larger inputs remain held',async()=>{
  const valid=bytes(sentence);await assert.rejects(parseRestoredMarkdownDocument(valid,'0'.repeat(64)),/digest/);
  for(const value of [new Uint8Array([0xc3,0x28]),bytes(sentence+'\0'),bytes(sentence+'\v'),bytes('<html>'+sentence+'</html>'),bytes(('\u02c7\u02db\u00af\uf8ff').repeat(1000)),bytes('a'.repeat(500001)),new Uint8Array(1000001)]){
    await assert.rejects(parseRestoredMarkdownDocument(value,sha(value)));
  }
});

function fixture(source) {
  const {DB,sqlite}=memoryD1();const original=bytes(source);const objects=new Map([['legacy.md',{bytes:original}]]);const id='synthetic-restoration';
  sqlite.prepare("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,metadata_json,created_at,updated_at) VALUES(?,'decision_docx','Test','Synthetic recovery','T-RECOVER','legacy.md','https://example.invalid',?,'now','now')").run(id,JSON.stringify({originalFilename:'legacy.md'}));
  const env={DB,SOURCE_BUCKET:{async get(key){const x=objects.get(key);return x?{size:x.bytes.length,arrayBuffer:async()=>x.bytes.slice().buffer}:null;},async put(key,value){objects.set(key,{bytes:new Uint8Array(value)});},async delete(){throw Error('Original deletion forbidden');}},VECTOR_JOBS_QUEUE:{async send(){}},VECTOR_NAMESPACE:'test',AI_EMBEDDING_MODEL:'deferred',AI_EMBEDDING_DIMENSIONS:'768',R2_PUBLIC_BASE_URL:'https://example.invalid',SOURCE_PROXY_BASE_URL:'https://api.example'};
  const plan={policy:'recovered-markdown-v1',documentId:id,expectedRevision:1,sourceSha256:sha(original),planSha256:'a'.repeat(64)};
  return{env,sqlite,original,objects,id,plan};
}

test('a planned larger generation stages all paragraphs, keeps its original source, and grants no publication or approval',async()=>{
  const source='# Findings\n\n'+Array.from({length:5901},(_,i)=>'Synthetic evidence paragraph '+i+' records notice.').join('\n\n');const x=fixture(source);
  try{
    await assert.rejects(reprocessIngestionDocument(x.env,x.id),/current revision was preserved/);
    const result=await reprocessIngestionDocument(x.env,x.id,x.plan);assert(result.reprocessArtifacts.paragraphCountAfter>=5901);
    const row=x.sqlite.prepare('SELECT * FROM documents WHERE id=?').get(x.id);assert.equal(row.ingestion_state,'ready');assert.equal(row.approved_at,null);assert.equal(row.approved_revision,null);assert.equal(row.qc_required_confirmed,0);assert.equal(row.published_revision,null);assert.equal(row.searchable_at,null);
    assert.equal(JSON.parse(row.metadata_json).restorationPlanSha256,x.plan.planSha256);assert.equal(JSON.parse(row.metadata_json).extraction.restoration.sourceSha256,x.plan.sourceSha256);
    assert.deepEqual(x.objects.get('legacy.md').bytes,x.original);assert.deepEqual(x.objects.get(row.source_r2_key).bytes,x.original);
    assert.equal(x.sqlite.prepare('PRAGMA foreign_key_check').all().length,0);
  }finally{x.sqlite.close();}
});

test('a stale plan or different source fails before staging and the recovery generation still has a finite paragraph bound',async()=>{
  const x=fixture(sentence+'\f'+sentence);
  try{
    const before=x.sqlite.prepare('SELECT total_changes() n').get().n;
    for(const plan of [{...x.plan,expectedRevision:2},{...x.plan,sourceSha256:'0'.repeat(64)},{...x.plan,documentId:'different'},{...x.plan,planSha256:'invalid'}])await assert.rejects(reprocessIngestionDocument(x.env,x.id,plan));
    assert.equal(x.sqlite.prepare('SELECT total_changes() n').get().n,before);assert.equal(x.objects.size,1);
  }finally{x.sqlite.close();}
  const large=fixture('# Findings\n\n'+Array.from({length:8001},(_,i)=>'Synthetic paragraph '+i+' records evidence.').join('\n\n'));
  try{
    await assert.rejects(reprocessIngestionDocument(large.env,large.id,large.plan),/at most 8000 paragraphs/);
    assert.equal(large.sqlite.prepare('SELECT ingestion_state FROM documents WHERE id=?').get(large.id).ingestion_state,'legacy');assert.equal(large.sqlite.prepare('SELECT COUNT(*) n FROM document_generations').get().n,0);assert.equal(large.objects.size,1);
  }finally{large.sqlite.close();}
});
