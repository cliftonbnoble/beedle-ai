import type { DatabaseEnv } from '../lib/types';
import { RequestConflictError, SearchIndexUnavailableError } from '../lib/http';

type Build={phase:'documents'|'document'|'retrieval'|'ready';after_id:string;visited:number;owner_token:string|null;lease_expires_at:string|null};
export async function searchIndexStatus(env:DatabaseEnv) {
  const state=await env.DB.prepare('SELECT * FROM search_index_build WHERE singleton=1').first<Build>();
  if(!state)throw new SearchIndexUnavailableError('Search index migration is required');
  return state;
}
export async function requireSubstringIndex(env:DatabaseEnv) {
  if((await searchIndexStatus(env)).phase!=='ready')throw new SearchIndexUnavailableError('Search index backfill must complete before search is enabled');
}
// Each call visits at most 100 source identities. Source values are read inside
// the guarded atomic batch, so concurrent edits cannot replay stale text.
export async function advanceSearchIndex(env:DatabaseEnv) {
  const owner=crypto.randomUUID();
  const state=await env.DB.prepare(`UPDATE search_index_build SET owner_token=?,lease_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','+60 seconds') WHERE singleton=1 AND (owner_token IS NULL OR lease_expires_at<=strftime('%Y-%m-%dT%H:%M:%fZ','now')) RETURNING *`).bind(owner).first<Build>();
  if(!state)throw new RequestConflictError('Search index builder is already running');
  try {
    if(state.phase==='ready')return state;
    const metadata=state.phase==='documents';
    const table=metadata?'documents':state.phase==='document'?'document_chunks':'retrieval_search_chunks';
    const idColumn=state.phase==='retrieval'?'chunk_id':'id';
    const rows=(await env.DB.prepare(`SELECT ${idColumn} AS id FROM ${table} WHERE ${idColumn}>? ORDER BY ${idColumn} LIMIT 100`).bind(state.after_id).all<{id:string}>()).results;
    const after=rows.at(-1)?.id||state.after_id;
    const ids=JSON.stringify(rows.map(row=>row.id));
    const guard=crypto.randomUUID();
    const statements=[env.DB.prepare('INSERT INTO search_index_build_guards(token,owner_token) VALUES(?,?)').bind(guard,owner)];
    if(rows.length) {
      if(metadata)statements.push(
        env.DB.prepare('INSERT OR IGNORE INTO search_document_identity(document_id) SELECT id FROM documents WHERE id IN (SELECT value FROM json_each(?))').bind(ids),
        env.DB.prepare(`INSERT INTO search_metadata_substrings(rowid,title,citation,author) SELECT m.row_id,d.title,d.citation,COALESCE(d.author_name,'') FROM search_document_identity m JOIN documents d ON d.id=m.document_id WHERE m.indexed=0 AND d.id IN (SELECT value FROM json_each(?))`).bind(ids),
        env.DB.prepare('UPDATE search_document_identity SET indexed=1 WHERE document_id IN (SELECT value FROM json_each(?))').bind(ids)
      );
      else statements.push(
        env.DB.prepare(`INSERT OR IGNORE INTO search_body_identity(source_kind,chunk_id,document_id) SELECT ?,${idColumn},document_id FROM ${table} WHERE ${idColumn} IN (SELECT value FROM json_each(?))`).bind(state.phase,ids),
        env.DB.prepare(`INSERT INTO search_body_substrings(rowid,section_label,chunk_text) SELECT m.row_id,c.section_label,c.chunk_text FROM search_body_identity m JOIN ${table} c ON c.${idColumn}=m.chunk_id WHERE m.source_kind=? AND m.indexed=0 AND c.${idColumn} IN (SELECT value FROM json_each(?))`).bind(state.phase,ids),
        env.DB.prepare('UPDATE search_body_identity SET indexed=1 WHERE source_kind=? AND chunk_id IN (SELECT value FROM json_each(?))').bind(state.phase,ids)
      );
    }
    let phase:Build["phase"]=state.phase;let cursor=after;
    if(!rows.length){phase=state.phase==='documents'?'document':state.phase==='document'?'retrieval':'ready';cursor='';}
    // Coverage proof runs once on transition, never on the request hot path.
    if(phase==='ready') {
      const missing=await env.DB.prepare(`SELECT
        (SELECT count(*) FROM documents d LEFT JOIN search_document_identity m ON m.document_id=d.id AND m.indexed=1 WHERE m.row_id IS NULL)+
        (SELECT count(*) FROM document_chunks c LEFT JOIN search_body_identity m ON m.source_kind='document' AND m.chunk_id=c.id AND m.indexed=1 WHERE m.row_id IS NULL)+
        (SELECT count(*) FROM retrieval_search_chunks c LEFT JOIN search_body_identity m ON m.source_kind='retrieval' AND m.chunk_id=c.chunk_id AND m.indexed=1 WHERE m.row_id IS NULL) AS missing`).first<number>('missing');
      if(missing)throw new SearchIndexUnavailableError('Search index coverage is incomplete; restart the build');
    }
    statements.push(env.DB.prepare("UPDATE search_index_build SET phase=?,after_id=?,visited=visited+?,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE singleton=1 AND owner_token=?").bind(phase,cursor,rows.length,owner),env.DB.prepare('DELETE FROM search_index_build_guards WHERE token=?').bind(guard));
    await env.DB.batch(statements);
    return await searchIndexStatus(env);
  }finally{await env.DB.prepare('UPDATE search_index_build SET owner_token=NULL,lease_expires_at=NULL WHERE singleton=1 AND owner_token=?').bind(owner).run();}
}
