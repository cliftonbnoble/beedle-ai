import { searchCorpusRevision } from "./search-sessions";
import type { DatabaseEnv } from '../lib/types';
import { RequestConflictError, RequestValidationError, SearchIndexUnavailableError } from '../lib/http';
import { recoveryTriggerDrops, recoveryIndexCreates, recoverySourceTriggers } from './search-rebuild-statements';

export type CoverageKind='metadata'|'document'|'retrieval';
type State={generation:string;status:'idle'|'running'|'completed';phase:string;after_id:string;visited:number;upper_metadata:string;upper_document:string;upper_retrieval:string;upper_body_mapping:string;upper_metadata_mapping:string;owner_token:string|null;lease_expires_at:string|null;last_error:string|null;archived_generation:string|null};
const phases=['metadata','document','retrieval','prune_body','prune_metadata','verify_metadata','verify_document','verify_retrieval','completed'] as const;
const indexes=[['search_body_substrings','search_previous_body_fts'],['search_metadata_substrings','search_previous_metadata_fts'],['search_chunks_fts','search_previous_phrase_fts']] as const;
const descriptors={metadata:{table:'documents',key:'id',map:'search_document_identity'},document:{table:'document_chunks',key:'id',map:'search_body_identity'},retrieval:{table:'retrieval_search_chunks',key:'chunk_id',map:'search_body_identity'}} as const;
export async function indexRecoveryStatus(env:DatabaseEnv){
 const state=await env.DB.prepare('SELECT * FROM search_index_rebuild WHERE singleton=1').first<State>();
 if(!state)throw new SearchIndexUnavailableError('Search recovery migration is required');return state;
}
async function schemaNames(env:DatabaseEnv){return new Set((await env.DB.prepare("SELECT name FROM sqlite_master WHERE type IN ('table','view')").all<{name:string}>()).results.map(row=>row.name));}
export async function inspectIndexRecovery(env:DatabaseEnv){const state=await indexRecoveryStatus(env);const names=await schemaNames(env);return {...state,archives:indexes.filter(([,archive])=>names.has(archive)).map(([,archive])=>archive),batchSize:100,requiresMaintenance:true};}
export async function startIndexRecovery(env:DatabaseEnv,input:{dryRun:boolean;generation?:string;expectedGeneration?:string;replaceArchive:boolean;restart:boolean}){
 const state=await indexRecoveryStatus(env);const names=await schemaNames(env);
 if(input.dryRun)return {dryRun:true,...await inspectIndexRecovery(env)};
 if(!input.generation || !input.expectedGeneration)throw new RequestValidationError('generation and expectedGeneration are required for index recovery');
 if(input.generation===state.generation)return {dryRun:false,...await inspectIndexRecovery(env)};
 if(await env.DB.prepare('SELECT generation FROM search_index_generations WHERE generation=?').bind(input.generation).first())throw new RequestConflictError('A historical index generation cannot be reused. Choose a fresh generation ID.');
 if(state.generation!==input.expectedGeneration)throw new RequestConflictError('Index generation changed. Inspect recovery status before starting.');
 if(state.status==='running'&&!input.restart)throw new RequestConflictError('Index recovery is already running; resume it or explicitly restart it.');
 if(input.restart&&state.status!=='running')throw new RequestConflictError('Only an unfinished recovery can be restarted.');
 const missing=['documents','document_chunks','retrieval_search_chunks','search_body_identity','search_document_identity','search_body_content','search_document_content','search_phrase_content'].filter(name=>!names.has(name));
 if(missing.length)throw new SearchIndexUnavailableError('Source/identity schema is incomplete. Complete migrations before index recovery.');
 const archives=indexes.filter(([,archive])=>names.has(archive));
 if(!input.restart&&archives.length&&!input.replaceArchive)throw new RequestConflictError('A previous index archive exists. Review it and explicitly set replaceArchive before a new recovery.');
 const token=crypto.randomUUID();
 const statements=[env.DB.prepare('INSERT INTO search_index_start_guards(token,expected_generation,new_generation,restart) VALUES(?,?,?,?)').bind(token,input.expectedGeneration,input.generation,input.restart?1:0),...recoveryTriggerDrops.map(sql=>env.DB.prepare(sql)),env.DB.prepare('DROP TABLE IF EXISTS search_chunks_fts_vocab')];
 for(const [current,archive]of indexes){
  if(input.restart){if(names.has(current))statements.push(env.DB.prepare(`DROP TABLE ${current}`));}
  else{
   if(names.has(archive))statements.push(env.DB.prepare(`DROP TABLE ${archive}`));
   if(names.has(current))statements.push(env.DB.prepare(`ALTER TABLE ${current} RENAME TO ${archive}`));
  }
 }
 statements.push(env.DB.prepare('DROP TABLE IF EXISTS search_phrase_fts'),...recoveryIndexCreates.map(sql=>env.DB.prepare(sql)));
 statements.push(env.DB.prepare("UPDATE search_index_generations SET status='superseded',completed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE generation=? AND status='running'").bind(state.generation),env.DB.prepare("INSERT INTO search_index_generations(generation,previous_generation,status,started_at) VALUES(?,?,'running',strftime('%Y-%m-%dT%H:%M:%fZ','now'))").bind(input.generation,state.generation));
 statements.push(env.DB.prepare(`UPDATE search_index_rebuild SET generation=?,archived_generation=?,status='running',phase='metadata',after_id='',visited=0,owner_token=NULL,lease_expires_at=NULL,last_error=NULL,completed_at=NULL,started_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
 upper_metadata=COALESCE((SELECT max(id) FROM documents),''),upper_document=COALESCE((SELECT max(id) FROM document_chunks),''),upper_retrieval=COALESCE((SELECT max(chunk_id) FROM retrieval_search_chunks),''),
 upper_body_mapping=CAST(COALESCE((SELECT max(row_id) FROM search_body_identity),0) AS TEXT),upper_metadata_mapping=CAST(COALESCE((SELECT max(row_id) FROM search_document_identity),0) AS TEXT) WHERE singleton=1`).bind(input.generation,input.restart?state.archived_generation:state.generation));
 statements.push(...recoverySourceTriggers.map(sql=>env.DB.prepare(sql)),
  env.DB.prepare("UPDATE search_index_build SET phase='documents',after_id='',owner_token=NULL,lease_expires_at=NULL WHERE singleton=1"),
  env.DB.prepare("UPDATE search_phrase_build SET phase='document',after_id='',legacy_frozen=1,owner_token=NULL,lease_expires_at=NULL WHERE singleton=1"),
  env.DB.prepare("UPDATE search_facet_build SET phase='pending',after_id='',visited=0,owner_token=NULL,lease_expires_at=NULL WHERE singleton=1"),
  env.DB.prepare('UPDATE vector_lifecycle_epoch SET revision=revision+1 WHERE singleton=1'),env.DB.prepare('DELETE FROM search_index_start_guards WHERE token=?').bind(token));
 try{await env.DB.batch(statements);}catch(error){
  try{if((await indexRecoveryStatus(env)).generation===input.generation)return {dryRun:false,...await inspectIndexRecovery(env)};}catch{}
  if(String(error).includes('search_index_rebuild_start_conflict'))throw new RequestConflictError('Index generation or active maintenance lease changed. Inspect status and retry.');throw error;
 }
 return {dryRun:false,...await inspectIndexRecovery(env)};
}
export async function indexCoveragePage(env:DatabaseEnv,kind:CoverageKind,after='',upper?:string,allowMaintainedLiveWrites=false){
 const state=await indexRecoveryStatus(env);const revision=await searchCorpusRevision(env);const descriptor=descriptors[kind];
 if(state.generation==='legacy'){
  const bootstrap=await env.DB.prepare("SELECT (SELECT phase FROM search_index_build WHERE singleton=1) AS body,(SELECT phase FROM search_phrase_build WHERE singleton=1) AS phrase").first<{body:string;phrase:string}>();
  if(bootstrap?.body!=='ready'||(kind!=='metadata'&&bootstrap?.phrase!=='ready'))return {kind,after,nextAfter:after,checked:0,done:false,consistent:false,corpusRevision:String(revision),generation:state.generation,issueCount:1,revisionVerified:false,deepIntegrityChecked:false,issues:[{id:'',reason:'bootstrap_incomplete'}]};
 }
 const metadata=kind==='metadata';const mapJoin=metadata?'m.document_id=c.id':`m.source_kind='${kind}' AND m.chunk_id=c.${descriptor.key}`;
 const names=await schemaNames(env);
 const receipts=metadata?['search_metadata_substrings_docsize']:['search_body_substrings_docsize','search_chunks_fts_docsize'];
 if(receipts.some(name=>!names.has(name)))return {kind,after,nextAfter:after,checked:0,done:false,consistent:false,corpusRevision:String(revision),generation:state.generation,issueCount:1,revisionVerified:false,deepIntegrityChecked:false,issues:[{id:'',reason:'index_receipt_schema_missing'}]};
 type Row={id:string;mapping:string|null;identity_ok:number;flags_ok:number;generation_ok:number;revision_ok:number;receipt_one:string|null;receipt_two:string|null};
 const rows=(await env.DB.prepare(`SELECT c.${descriptor.key} AS id,CAST(m.row_id AS TEXT) AS mapping,
 ${metadata?'1':'m.document_id=c.document_id'} AS identity_ok,
 ${metadata?'m.indexed=1':'m.indexed=1 AND m.phrase_indexed=1'} AS flags_ok,
 m.index_generation=? AS generation_ok,m.indexed_revision=m.content_revision AS revision_ok,
 CAST(a.id AS TEXT) AS receipt_one,${metadata?'CAST(a.id AS TEXT)':'CAST(b.id AS TEXT)'} AS receipt_two
 FROM ${descriptor.table} c LEFT JOIN ${descriptor.map} m ON ${mapJoin}
 LEFT JOIN ${receipts[0]} a ON a.id=m.row_id ${metadata?'':`LEFT JOIN ${receipts[1]} b ON b.id=m.row_id`}
 WHERE c.${descriptor.key}>? ${upper===undefined?'':`AND c.${descriptor.key}<=?`} ORDER BY c.${descriptor.key} LIMIT 100`).bind(state.generation,after,...(upper===undefined?[]:[upper])).all<Row>()).results;
 const issues=rows.flatMap(row=>{
  const reason=!row.mapping?'mapping_missing':!row.identity_ok?'source_identity_mismatch':!row.flags_ok?'index_flag_missing':!row.receipt_one?'body_or_metadata_entry_missing':!row.receipt_two?'phrase_entry_missing':state.generation!=='legacy'&&!row.generation_ok?'generation_mismatch':state.generation!=='legacy'&&!row.revision_ok?'source_revision_mismatch':null;
  return reason?[{id:row.id,reason}]:[];
 });
 const current=await indexRecoveryStatus(env);const currentRevision=await searchCorpusRevision(env);
 return {kind,after,nextAfter:rows.at(-1)?.id||after,checked:rows.length,done:rows.length<100,consistent:current.generation===state.generation&&(allowMaintainedLiveWrites||currentRevision===revision),corpusRevision:String(currentRevision),sourceChangedDuringCheck:currentRevision!==revision,generation:state.generation,issues:issues.slice(0,20),issueCount:issues.length,revisionVerified:state.generation!=='legacy',deepIntegrityChecked:false};
}
export async function advanceIndexRecovery(env:DatabaseEnv,expectedGeneration?:string){
 const owner=crypto.randomUUID();const previous=await indexRecoveryStatus(env);
 if(expectedGeneration&&previous.generation!==expectedGeneration)throw new RequestConflictError('Index generation changed. Inspect status before resuming.');
 if(previous.status!=='running')return previous;
 const state=await env.DB.prepare(`UPDATE search_index_rebuild SET owner_token=?,lease_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','+60 seconds') WHERE singleton=1 AND generation=? AND status='running' AND (owner_token IS NULL OR lease_expires_at<=strftime('%Y-%m-%dT%H:%M:%fZ','now')) RETURNING *`).bind(owner,previous.generation).first<State>();
 if(!state)throw new RequestConflictError('Index recovery is already being advanced');
 try{
  const token=crypto.randomUUID();const statements=[env.DB.prepare('INSERT INTO search_index_rebuild_guards(token,owner_token,generation) VALUES(?,?,?)').bind(token,owner,state.generation)];
  let nextPhase=state.phase;let after=state.after_id;let visited=0;
  if(state.phase.startsWith('verify_')){
   const kind=state.phase.slice(7) as CoverageKind;const page=await indexCoveragePage(env,kind,after,state[`upper_${kind}`],true);
   if(!page.consistent || page.issues.length)throw new SearchIndexUnavailableError('Index coverage verification failed. Inspect coverage and explicitly restart this unfinished recovery.');
   visited=page.checked;after=page.nextAfter;if(page.done){nextPhase=phases[phases.indexOf(state.phase as typeof phases[number])+1]!;after='';}
  }else if(state.phase.startsWith('prune_')){
   const metadata=state.phase==='prune_metadata';const table=metadata?'search_document_identity':'search_body_identity';const upper=metadata?state.upper_metadata_mapping:state.upper_body_mapping;
   const rows=(await env.DB.prepare(`SELECT CAST(row_id AS TEXT) AS id FROM ${table} WHERE row_id>CAST(? AS INTEGER) AND row_id<=CAST(? AS INTEGER) ORDER BY row_id LIMIT 100`).bind(after||'0',upper).all<{id:string}>()).results;const ids=JSON.stringify(rows.map(row=>row.id));
   const orphan=metadata?'NOT EXISTS(SELECT 1 FROM documents d WHERE d.id=search_document_identity.document_id)':`NOT EXISTS(SELECT 1 FROM document_chunks c WHERE search_body_identity.source_kind='document' AND c.id=search_body_identity.chunk_id) AND NOT EXISTS(SELECT 1 FROM retrieval_search_chunks c WHERE search_body_identity.source_kind='retrieval' AND c.chunk_id=search_body_identity.chunk_id)`;
   if(rows.length)statements.push(env.DB.prepare(`DELETE FROM ${table} WHERE row_id IN (SELECT CAST(value AS INTEGER) FROM json_each(?)) AND ${orphan}`).bind(ids));
   visited=rows.length;after=rows.at(-1)?.id||after;if(rows.length<100){nextPhase=metadata?'verify_metadata':'prune_metadata';after='';}
  }else{
   const kind=state.phase as CoverageKind;const descriptor=descriptors[kind];if(!descriptor)throw new SearchIndexUnavailableError('Unknown recovery checkpoint phase');
   const rows=(await env.DB.prepare(`SELECT ${descriptor.key} AS id FROM ${descriptor.table} WHERE ${descriptor.key}>? AND ${descriptor.key}<=? ORDER BY ${descriptor.key} LIMIT 100`).bind(after,state[`upper_${kind}`]).all<{id:string}>()).results;
   const ids=JSON.stringify(rows.map(row=>row.id));const metadata=kind==='metadata';
   if(rows.length){
    if(metadata)statements.push(env.DB.prepare('INSERT OR IGNORE INTO search_document_identity(document_id) SELECT id FROM documents WHERE id IN (SELECT value FROM json_each(?))').bind(ids),
     env.DB.prepare("INSERT INTO search_metadata_substrings(rowid,title,citation,author) SELECT m.row_id,d.title,d.citation,COALESCE(d.author_name,'') FROM search_document_identity m JOIN documents d ON d.id=m.document_id WHERE m.index_generation<>? AND d.id IN (SELECT value FROM json_each(?))").bind(state.generation,ids),
     env.DB.prepare('UPDATE search_document_identity SET index_generation=?,indexed_revision=content_revision,indexed=1 WHERE document_id IN (SELECT value FROM json_each(?))').bind(state.generation,ids));
    else{
     statements.push(env.DB.prepare(`INSERT INTO search_body_identity(source_kind,chunk_id,document_id) SELECT ?,${descriptor.key},document_id FROM ${descriptor.table} WHERE ${descriptor.key} IN (SELECT value FROM json_each(?)) ON CONFLICT(source_kind,chunk_id) DO UPDATE SET document_id=excluded.document_id`).bind(kind,ids));
     for(const index of ['search_body_substrings','search_chunks_fts'])statements.push(env.DB.prepare(`INSERT INTO ${index}(rowid,section_label,chunk_text) SELECT m.row_id,c.section_label,c.chunk_text FROM search_body_identity m JOIN ${descriptor.table} c ON c.${descriptor.key}=m.chunk_id WHERE m.source_kind=? AND m.index_generation<>? AND c.${descriptor.key} IN (SELECT value FROM json_each(?))`).bind(kind,state.generation,ids));
     statements.push(env.DB.prepare('UPDATE search_body_identity SET index_generation=?,indexed_revision=content_revision,indexed=1,phrase_indexed=1 WHERE source_kind=? AND chunk_id IN (SELECT value FROM json_each(?))').bind(state.generation,kind,ids));
    }
   }
   visited=rows.length;after=rows.at(-1)?.id||after;if(rows.length<100){nextPhase=kind==='metadata'?'document':kind==='document'?'retrieval':'prune_body';after='';}
  }
  if(nextPhase==='completed')statements.push(env.DB.prepare("UPDATE search_index_generations SET status='completed',completed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE generation=?").bind(state.generation),env.DB.prepare("UPDATE search_index_build SET phase='ready',after_id='' WHERE singleton=1"),env.DB.prepare("UPDATE search_phrase_build SET phase='ready',after_id='',legacy_frozen=1 WHERE singleton=1"),env.DB.prepare('UPDATE vector_lifecycle_epoch SET revision=revision+1 WHERE singleton=1'));
  statements.push(env.DB.prepare("UPDATE search_index_rebuild SET phase=?,after_id=?,visited=visited+?,status=?,last_error=NULL,completed_at=CASE WHEN ?='completed' THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') ELSE NULL END WHERE singleton=1 AND owner_token=? AND generation=?").bind(nextPhase,after,visited,nextPhase==='completed'?'completed':'running',nextPhase,owner,state.generation),env.DB.prepare('DELETE FROM search_index_rebuild_guards WHERE token=?').bind(token));
  await env.DB.batch(statements);return await indexRecoveryStatus(env);
 }catch(error){
  try{const current=await indexRecoveryStatus(env);if(current.generation===state.generation&&(current.phase!==state.phase||current.after_id!==state.after_id||current.visited!==state.visited))return current;}catch{}
  await env.DB.prepare("UPDATE search_index_rebuild SET last_error=? WHERE singleton=1 AND owner_token=? AND generation=?").bind(error instanceof SearchIndexUnavailableError?'coverage_verification_failed':'checkpoint_failed',owner,state.generation).run().catch(()=>{});
  if(String(error).includes('search_index_rebuild_lease_lost'))throw new RequestConflictError('Index recovery owner or generation changed. Inspect status before retrying.');
  throw error;
 }
 finally{await env.DB.prepare('UPDATE search_index_rebuild SET owner_token=NULL,lease_expires_at=NULL WHERE singleton=1 AND owner_token=? AND generation=?').bind(owner,state.generation).run().catch(()=>{});}
}
