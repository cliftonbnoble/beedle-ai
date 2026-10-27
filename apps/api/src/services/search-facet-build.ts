import type { DatabaseEnv } from '../lib/types';
import { RequestConflictError } from '../lib/http';
import { facetBackfillStatements } from './search-facet-statements';
type State={phase:'pending'|'ready';after_id:string;visited:number};
export async function facetBuildStatus(env:DatabaseEnv){return env.DB.prepare('SELECT * FROM search_facet_build WHERE singleton=1').first<State>();}
export async function advanceFacetBuild(env:DatabaseEnv){
 const owner=crypto.randomUUID();
 const state=await env.DB.prepare(`UPDATE search_facet_build SET owner_token=?,lease_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','+60 seconds') WHERE singleton=1 AND (owner_token IS NULL OR lease_expires_at<=strftime('%Y-%m-%dT%H:%M:%fZ','now')) RETURNING *`).bind(owner).first<State>();
 if(!state)throw new RequestConflictError('Facet builder is already running');
 try{
  if(state.phase==='ready')return state;
  const rows=(await env.DB.prepare('SELECT id FROM documents WHERE id>? ORDER BY id LIMIT 100').bind(state.after_id).all<{id:string}>()).results;
  const ids=JSON.stringify(rows.map(row=>row.id));const token=crypto.randomUUID();
  const statements=[env.DB.prepare('INSERT INTO search_facet_build_guards(token,owner_token) VALUES(?,?)').bind(token,owner)];
  if(rows.length){
   for(const table of ['document_index_codes','document_rules_sections','document_ordinance_sections'])statements.push(env.DB.prepare(`DELETE FROM ${table} WHERE document_id IN (SELECT value FROM json_each(?))`).bind(ids));
   statements.push(...facetBackfillStatements.map(sql=>env.DB.prepare(sql).bind(ids)));
  }
  statements.push(env.DB.prepare("UPDATE search_facet_build SET phase=?,after_id=?,visited=visited+?,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE singleton=1 AND owner_token=?").bind(rows.length?'pending':'ready',rows.at(-1)?.id||state.after_id,rows.length,owner),env.DB.prepare('DELETE FROM search_facet_build_guards WHERE token=?').bind(token));
  await env.DB.batch(statements);return await facetBuildStatus(env);
 }finally{await env.DB.prepare('UPDATE search_facet_build SET owner_token=NULL,lease_expires_at=NULL WHERE singleton=1 AND owner_token=?').bind(owner).run();}
}
