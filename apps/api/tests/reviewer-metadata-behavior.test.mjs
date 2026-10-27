import test from 'node:test';
import assert from 'node:assert/strict';
import { memoryD1 } from './helpers/d1-memory.mjs';
import { loadTs } from './helpers/load-ts.mjs';
import { reviewPatch } from '../scripts/lib/reviewer-mutation-client.mjs';
const {updateIngestionMetadata, rejectIngestionDocument}=loadTs('src/services/admin-ingestion.ts');
const {handleUpdateIngestionMetadata,handleRejectIngestionDocument}=loadTs('src/routes/admin-ingestion.ts');
const {preserveReviewedMetadata,readReviewerOverrides}=loadTs('src/services/reviewer-overrides.ts');

function setup() {
  const {DB,sqlite}=memoryD1();
  sqlite.exec(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,
    ingestion_state,ingestion_completed_at,expected_chunk_count,index_codes_json,rules_sections_json,ordinance_sections_json,
    qc_has_index_codes,qc_has_rules_section,qc_has_ordinance_section,qc_passed,qc_required_confirmed,reference_catalog_version,
    author_name,case_number,decision_date,outcome_label)
    VALUES('doc','decision_docx','Test','Reviewed case','T-1','source','https://example.invalid','now','now','ready','now',0,
    '["100"]','["1.2"]','["37.8"]',1,1,1,1,1,'legacy','Old author','Old case','2026-01-01','deny');`);
  const env={DB,SOURCE_PROXY_BASE_URL:'https://api.example'};
  const row=()=>sqlite.prepare('SELECT * FROM documents WHERE id=?').get('doc');
  const patch=(fields={},revision=row().document_revision,mutationId=crypto.randomUUID())=>reviewPatch(fields,revision,mutationId);
  const receipts=()=>sqlite.prepare('SELECT * FROM reviewer_mutations ORDER BY created_at').all();
  const visible=()=>JSON.stringify({row:row(),links:sqlite.prepare('SELECT * FROM document_reference_links ORDER BY id').all(),
    issues:sqlite.prepare('SELECT * FROM document_reference_issues ORDER BY id').all(),receipts:receipts()});
  return {env,sqlite,row,patch,receipts,visible};
}
function request(payload){return new Request('https://api.example/metadata',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(payload)});}

test('nullable PATCH clears each value, records reviewer provenance, and preserves explicit clears on reprocessing',async()=>{
  const {env,sqlite,row,patch,receipts}=setup();
  try {
    for(const field of ['author_name','case_number','decision_date']) {
      const before=row();const result=await updateIngestionMetadata(env,'doc',patch({[field]:null}));
      assert.equal(row()[field],null);assert.equal(result.qcRequiredConfirmed,0);assert.ok(result.documentRevision>before.document_revision);
      for(const other of ['author_name','case_number','decision_date'].filter(other=>other!==field))assert.equal(row()[other],before[other]);
    }
    const overrides=readReviewerOverrides(row().reviewer_overrides_json);
    assert.equal(overrides.author.value,null);assert.equal(overrides.caseNumber.value,null);assert.equal(overrides.decisionDate.value,null);
    const preserved=preserveReviewedMetadata({author:'Extractor author',caseNumber:'Extractor case',decisionDate:'2026-02-02'},
      {author:null,caseNumber:null,decisionDate:null},undefined,overrides,row().document_revision);
    assert.equal(preserved.metadata.author,null);assert.equal(preserved.metadata.caseNumber,null);assert.equal(preserved.metadata.decisionDate,null);
    assert.equal(receipts().length,3);assert.equal(receipts()[0].actor_id,null);
    assert.equal(JSON.parse(receipts()[0].before_json).author_name,'Old author');assert.equal(JSON.parse(receipts()[0].after_json).author_name,null);
  } finally {sqlite.close();}
});

test('confirmation uses retained arrays; explicit false clears it; omitted confirmation survives a true no-op',async()=>{
  const {env,sqlite,row,patch}=setup();
  try {
    await updateIngestionMetadata(env,'doc',patch({confirm_required_metadata:false}));
    assert.equal(row().qc_required_confirmed,0);assert.equal(row().qc_confirmed_at,null);
    await updateIngestionMetadata(env,'doc',patch({confirm_required_metadata:true}));
    assert.equal(row().qc_required_confirmed,1);assert.ok(row().qc_confirmed_at);
    await updateIngestionMetadata(env,'doc',patch({author_name:'Reviewed author',confirm_required_metadata:true}));
    const revision=row().document_revision;
    await updateIngestionMetadata(env,'doc',patch({author_name:'Reviewed author'}));
    assert.equal(row().qc_required_confirmed,1);assert.equal(row().document_revision,revision);
    await updateIngestionMetadata(env,'doc',patch({case_number:'Reviewed new case'}));
    assert.equal(row().qc_required_confirmed,0);
  } finally {sqlite.close();}
});

test('empty required groups reject confirmation without writes; explicit empty array clears and revokes approval',async()=>{
  const {env,sqlite,row,patch,visible}=setup();
  try {
    const before=visible();
    await assert.rejects(updateIngestionMetadata(env,'doc',patch({index_codes:[],confirm_required_metadata:true})),/all three/);
    assert.equal(visible(),before);
    sqlite.exec("UPDATE documents SET approved_at='now',approved_revision=document_revision,approval_policy_version='reviewer-policy-1'");
    await updateIngestionMetadata(env,'doc',patch({index_codes:[]}));
    assert.equal(row().index_codes_json,'[]');assert.equal(row().qc_passed,0);assert.equal(row().qc_required_confirmed,0);assert.equal(row().approved_at,null);
    const result=await updateIngestionMetadata(env,'doc',patch({index_codes:['not-in-catalog'],confirm_required_metadata:true}));
    assert.equal(result.approvalReadiness.eligible,false);assert.equal(result.approvedAt,null);
    assert.ok(sqlite.prepare('SELECT COUNT(*) AS n FROM document_reference_issues').get().n>0);
  } finally {sqlite.close();}
});

test('two reviewers of one revision have one winner, including rejection and writes racing the commit',async()=>{
  for(const contender of ['metadata','reject','late-read']) {
    const {env,sqlite,row,patch,receipts}=setup();
    try {
      const expected=row().document_revision;
      const save=patch({author_name:'Winning author'},expected);
      const second=contender==='reject'?()=>rejectIngestionDocument(env,'doc',patch({reason:'A stale rejection'},expected))
        :()=>updateIngestionMetadata(env,'doc',patch({author_name:'Losing author'},expected));
      if(contender==='late-read') {
        const batch=env.DB.batch.bind(env.DB);let injected=false;
        env.DB.batch=statements=>{if(!injected){injected=true;sqlite.exec("UPDATE documents SET author_name='Intervening reviewer'");}return batch(statements);};
        await assert.rejects(updateIngestionMetadata(env,'doc',save),/Document changed/);
        assert.equal(row().author_name,'Intervening reviewer');assert.equal(receipts().length,0);
      } else {
        const result=await Promise.allSettled([updateIngestionMetadata(env,'doc',save),second()]);
        assert.equal(result.filter(item=>item.status==='fulfilled').length,1);
        assert.match(String(result.find(item=>item.status==='rejected').reason),/Document changed/);
        const firstWon=result[0].status==='fulfilled';
        assert.equal(row().author_name,firstWon?'Winning author':contender==='reject'?'Old author':'Losing author');
        assert.equal(Boolean(row().rejected_at),!firstWon && contender==='reject');
        assert.equal(receipts().length,1);assert.equal(receipts()[0].expected_revision,expected);
        assert.equal(receipts()[0].action,!firstWon && contender==='reject'?'reject':'metadata');
      }
    } finally {sqlite.close();}
  }
});

test('metadata/reference/receipt failures roll back everything; lost acknowledgment recovers the committed receipt',async()=>{
  for(const mode of ['failure','lost-ack']) {
    const {env,sqlite,row,patch,receipts,visible}=setup();
    try {
      const before=visible();const batch=env.DB.batch.bind(env.DB);let first=true;
      env.DB.batch=async statements=>{
        if(first){first=false;if(mode==='failure')return batch([...statements,env.DB.prepare('INSERT INTO absent_table VALUES(1)')]);
          await batch(statements);throw new Error('Lost response after commit');}
        return batch(statements);
      };
      const payload=patch({author_name:'Atomic author',rules_sections:['unknown'],confirm_required_metadata:true});
      if(mode==='failure') {await assert.rejects(updateIngestionMetadata(env,'doc',payload));assert.equal(visible(),before);}
      else {
        const result=await updateIngestionMetadata(env,'doc',payload);
        assert.equal(result.mutationReceipt.replayed,true);assert.equal(row().author_name,'Atomic author');assert.equal(receipts().length,1);
        const committed=visible();await updateIngestionMetadata(env,'doc',payload);assert.equal(visible(),committed);
      }
      assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM document_revision_guards').get().n,0);
    } finally {sqlite.close();}
  }
});

test('duplicate delivery converges; changed payload or action cannot reuse a receipt; replay never overwrites later edits',async()=>{
  const {env,sqlite,row,patch,receipts}=setup();
  try {
    const payload=patch({author_name:'First correction'});
    const responses=await Promise.all([updateIngestionMetadata(env,'doc',payload),updateIngestionMetadata(env,'doc',payload)]);
    assert.equal(responses[0].mutationReceipt.appliedRevision,responses[1].mutationReceipt.appliedRevision);assert.equal(receipts().length,1);
    await assert.rejects(updateIngestionMetadata(env,'doc',{...payload,author_name:'Different correction'}),/already used/);
    await assert.rejects(rejectIngestionDocument(env,'doc',{expectedRevision:payload.expectedRevision,mutationId:payload.mutationId,reason:'Different action'}),/already used/);
    await updateIngestionMetadata(env,'doc',patch({author_name:'Later reviewer'}));const revision=row().document_revision;
    const replay=await updateIngestionMetadata(env,'doc',payload);assert.equal(row().author_name,'Later reviewer');assert.equal(row().document_revision,revision);
    assert.equal(replay.mutationReceipt.appliedRevision,responses[0].mutationReceipt.appliedRevision);assert.equal(replay.documentRevision,revision);
  } finally {sqlite.close();}
});

test('HTTP contracts reject missing/stale revisions and empty patches before writes, and rejection is guarded and retryable',async()=>{
  const {env,sqlite,row,patch,visible}=setup();
  try {
    const before=visible();
    for(const payload of [{},{author_name:'Legacy payload'},patch({}),patch({author_name:'Stale'},row().document_revision+1)]) {
      const response=await handleUpdateIngestionMetadata(request(payload),env,'doc');assert.ok([400,409].includes(response.status));assert.equal(visible(),before);
    }
    const payload=patch({reason:'Reviewed and rejected'});
    const response=await handleRejectIngestionDocument(request(payload),env,'doc');assert.equal(response.status,200);
    assert.equal(row().qc_required_confirmed,0);assert.equal(row().searchable_at,null);assert.equal(row().approved_revision,null);
    const after=visible();assert.equal((await handleRejectIngestionDocument(request(payload),env,'doc')).status,200);assert.equal(visible(),after);
    assert.equal((await handleRejectIngestionDocument(request(patch({reason:'Missing document'})),env,'absent')).status,404);
    assert.throws(()=>reviewPatch({author_name:'No new revision'},undefined),/Regenerate/);
  } finally {sqlite.close();}
});

test('omitted and identical reference values preserve historical JSON formatting without revoking a true no-op confirmation',async()=>{
 for(const explicit of [false,true]){
  const {env,sqlite,row,patch}=setup();
  try{
   const overrides={author:{value:'Old author',provenance:'reviewer',recordedAt:'2026-01-01',basedOnRevision:1},
    indexCodes:{value:['100'],provenance:'reviewer',recordedAt:'2026-01-01',basedOnRevision:1},
    rulesSections:{value:['1.2'],provenance:'reviewer',recordedAt:'2026-01-01',basedOnRevision:1},
    ordinanceSections:{value:['37.8'],provenance:'reviewer',recordedAt:'2026-01-01',basedOnRevision:1}};
   sqlite.prepare(`UPDATE documents SET index_codes_json='[ "100" ]',rules_sections_json='[ "1.2" ]',ordinance_sections_json='[ "37.8" ]',reviewer_overrides_json=? WHERE id='doc'`).run(JSON.stringify(overrides,null,2));
   sqlite.prepare("UPDATE documents SET qc_required_confirmed=1,qc_confirmed_at='retained-confirmation' WHERE id='doc'").run();
   const before=row();
   await updateIngestionMetadata(env,'doc',patch({author_name:'Old author',...(explicit?{index_codes:['100'],rules_sections:['1.2'],ordinance_sections:['37.8']}:{})}));
   const after=row();assert.equal(after.document_revision,before.document_revision);assert.equal(after.qc_required_confirmed,1);assert.equal(after.qc_confirmed_at,'retained-confirmation');
   for(const field of ['index_codes_json','rules_sections_json','ordinance_sections_json','reviewer_overrides_json'])assert.equal(after[field],before[field],field);
  }finally{sqlite.close();}
 }
});
