import type { Env } from '../lib/types';
import { SearchIndexUnavailableError } from '../lib/http';
export const searchSchemaVersion=28;
export const requiredSearchObjects=[
 'documents','document_sections','section_paragraphs','document_chunks','retrieval_search_chunks','document_reference_links','legal_catalog_state','vector_lifecycle_epoch','vector_lifecycle_dirty',
 'search_index_generations','search_index_rebuild','search_index_rebuild_guards','search_index_start_guards','search_index_rebuild_guard_check','search_index_start_guard_check','search_classic_body_rebuild_fence','search_classic_phrase_rebuild_fence','search_classic_facet_rebuild_fence',
 'search_body_substrings_docsize','search_metadata_substrings_docsize','search_chunks_fts_docsize',
 'search_schema_readiness','search_facet_build','search_index_build','search_phrase_build',
 'search_chunks_fts','search_chunks_fts_vocab','search_body_substrings','search_metadata_substrings',
 'search_body_identity','search_document_identity','search_phrase_content','search_body_content','search_document_content','search_sessions','search_session_capacity',
 'document_index_codes','document_rules_sections','document_ordinance_sections',
 'idx_documents_author_name_lookup','idx_documents_search_runtime','idx_retrieval_search_chunks_doc','idx_document_chunks_doc',
 'idx_document_index_codes_normalized','idx_document_rules_sections_normalized','idx_document_ordinance_sections_normalized',
 'documents_substring_insert','documents_substring_update','documents_substring_delete',
 'search_body_rowid_immutable','search_document_rowid_immutable','search_session_reserved_insert','search_session_reserved_delete','search_session_immutable','search_sessions_expiry',
 'search_reference_insert','search_reference_update','search_reference_delete','search_phrase_cutover_epoch',
 'vector_epoch_insert','vector_epoch_update','vector_lifecycle_document_insert','vector_lifecycle_document_update','vector_lifecycle_document_delete',
 'documents_ai_document_facets','documents_au_document_facets','documents_ad_document_facets',
 'document_chunks_substring_insert','document_chunks_substring_update','document_chunks_substring_delete',
 'retrieval_search_chunks_substring_insert','retrieval_search_chunks_substring_update','retrieval_search_chunks_substring_delete'
] as const;
const searchViewNames=new Set(['search_phrase_content','search_body_content','search_document_content']);
const searchTableNames=new Set(['search_index_generations','search_index_rebuild','search_index_rebuild_guards','search_index_start_guards','search_body_substrings_docsize','search_metadata_substrings_docsize','search_chunks_fts_docsize','documents','document_sections','section_paragraphs','document_chunks','retrieval_search_chunks','document_reference_links','legal_catalog_state','vector_lifecycle_epoch','vector_lifecycle_dirty','search_schema_readiness','search_facet_build','search_index_build','search_phrase_build','search_chunks_fts','search_chunks_fts_vocab','search_body_substrings','search_metadata_substrings','search_body_identity','search_document_identity','search_sessions','search_session_capacity','document_index_codes','document_rules_sections','document_ordinance_sections']);
function requiredObjectType(name:string){return searchViewNames.has(name)?'view':searchTableNames.has(name)?'table':name.startsWith('idx_')||name==='search_sessions_expiry'?'index':'trigger';}
export function isSearchSchemaError(error:unknown){return /no such table|no such module|no such column|malformed database schema/i.test(error instanceof Error?error.message:String(error));}
export async function inspectSearchReadiness(env:Env){
 const objects=(await env.DB.prepare('SELECT name,type FROM sqlite_master WHERE name IN (SELECT value FROM json_each(?))').bind(JSON.stringify(requiredSearchObjects)).all<{name:string;type:string}>()).results;
 const names=new Map(objects.map(row=>[row.name,row.type]));const missing=requiredSearchObjects.filter(name=>names.get(name)!==requiredObjectType(name));
 if(missing.length)return {ready:false,reason:'schema_missing',schemaVersion:0,missing};
 const state=await env.DB.prepare(`SELECT schema_version AS schemaVersion,
 (SELECT phase FROM search_index_build WHERE singleton=1) AS substrings,
 (SELECT phase FROM search_phrase_build WHERE singleton=1) AS phrases,
 (SELECT status FROM search_index_rebuild WHERE singleton=1) AS recovery,
 (SELECT CASE WHEN generation='legacy' AND status='idle' THEN 1 WHEN status='completed' AND EXISTS(SELECT 1 FROM search_index_generations g WHERE g.generation=search_index_rebuild.generation AND g.status='completed') THEN 1 ELSE 0 END FROM search_index_rebuild WHERE singleton=1) AS recoveryValid,
 (SELECT phase FROM search_facet_build WHERE singleton=1) AS facets FROM search_schema_readiness WHERE singleton=1`).first<{schemaVersion:number;substrings:string;phrases:string;facets:string;recovery:string;recoveryValid:number}>();
 const ready=Boolean(state && state.schemaVersion>=searchSchemaVersion && state.substrings==='ready' && state.phrases==='ready' && state.facets==='ready' && state.recoveryValid===1);
 return {ready,reason:ready?null:'maintenance_required',schemaVersion:state?.schemaVersion||0,missing,...state};
}
export async function searchReadyEnvironment(env:Env):Promise<Env>{
 try{
  const state=await inspectSearchReadiness(env);
  if(!state.ready)throw new SearchIndexUnavailableError(state.reason==='schema_missing'?'Search schema is incomplete. Complete migrations before serving search.':'Search index maintenance is incomplete. Complete the bounded builders before serving search.');
  return {...env,SEARCH_CAPABILITIES:{schemaVersion:state.schemaVersion,fts:true,vocabulary:true}};
 }catch(error){
  if(error instanceof SearchIndexUnavailableError)throw error;
  env.SEARCH_WORK_BUDGET?.assertAvailable();
  throw new SearchIndexUnavailableError(isSearchSchemaError(error)?'Search schema is incomplete. Complete migrations before serving search.':'Search readiness is temporarily unavailable. Please retry.');
 }
}
