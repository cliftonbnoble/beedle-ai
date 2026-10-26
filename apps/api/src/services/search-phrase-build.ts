import type { Env } from '../lib/types';
import { RequestConflictError, SearchIndexUnavailableError } from '../lib/http';
import { requireSubstringIndex } from './search-index-build';
import { phraseCutoverSql } from './search-phrase-cutover';
type Build={phase:'document'|'retrieval'|'ready';after_id:string;visited:number;legacy_frozen:number};
export async function phraseIndexStatus(env:Env){return env.DB.prepare('SELECT * FROM search_phrase_build WHERE singleton=1').first<Build>();}
// Shadow writes use the same source transactions as body postings. Existing
// readers keep the canonical old index until this builder completes coverage.
export async function advancePhraseIndex(env:Env) {
  await requireSubstringIndex(env);
  const owner=crypto.randomUUID();
  const state=await env.DB.prepare(`UPDATE search_phrase_build SET owner_token=?,lease_expires_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','+60 seconds') WHERE singleton=1 AND (owner_token IS NULL OR lease_expires_at<=strftime('%Y-%m-%dT%H:%M:%fZ','now')) RETURNING *`).bind(owner).first<Build>();
  if(!state)throw new RequestConflictError('Phrase index builder is already running');
  try {
    if(state.phase==='ready')return state;
    const table=state.phase==='document'?'document_chunks':'retrieval_search_chunks';
    const key=state.phase==='document'?'id':'chunk_id';
    const rows=(await env.DB.prepare(`SELECT ${key} AS id FROM ${table} WHERE ${key}>? ORDER BY ${key} LIMIT 100`).bind(state.after_id).all<{id:string}>()).results;
    const ids=JSON.stringify(rows.map(row=>row.id));const token=crypto.randomUUID();
    const statements=[env.DB.prepare('INSERT INTO search_phrase_build_guards(token,owner_token) VALUES(?,?)').bind(token,owner)];
    if(rows.length)statements.push(
      env.DB.prepare(`INSERT INTO search_phrase_fts(rowid,section_label,chunk_text) SELECT m.row_id,c.section_label,c.chunk_text FROM search_body_identity m JOIN ${table} c ON c.${key}=m.chunk_id WHERE m.source_kind=? AND m.phrase_indexed=0 AND c.${key} IN (SELECT value FROM json_each(?))`).bind(state.phase,ids),
      env.DB.prepare('UPDATE search_body_identity SET phrase_indexed=1 WHERE source_kind=? AND chunk_id IN (SELECT value FROM json_each(?))').bind(state.phase,ids),
      env.DB.prepare("UPDATE search_phrase_build SET after_id=?,visited=visited+?,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE singleton=1 AND owner_token=?").bind(rows.at(-1)!.id,rows.length,owner)
    );
    else if(state.phase==='document')statements.push(env.DB.prepare("UPDATE search_phrase_build SET phase='retrieval',after_id='' WHERE singleton=1 AND owner_token=?").bind(owner));
    else {
      const missing=await env.DB.prepare(`SELECT
       (SELECT count(*) FROM document_chunks c LEFT JOIN search_body_identity m ON m.source_kind='document' AND m.chunk_id=c.id AND m.phrase_indexed=1 WHERE m.row_id IS NULL)+
       (SELECT count(*) FROM retrieval_search_chunks c LEFT JOIN search_body_identity m ON m.source_kind='retrieval' AND m.chunk_id=c.chunk_id AND m.phrase_indexed=1 WHERE m.row_id IS NULL) AS missing`).first<number>('missing');
      if(missing)throw new SearchIndexUnavailableError('Phrase index coverage is incomplete');
      // Lease guard, pointer swap, replacement triggers and ready marker are one
      // atomic transaction. A lost acknowledgment replays as ready, never swaps twice.
      statements.push(...phraseCutoverSql.map(sql=>env.DB.prepare(sql)));
    }
    statements.push(env.DB.prepare('DELETE FROM search_phrase_build_guards WHERE token=?').bind(token));
    await env.DB.batch(statements);return await phraseIndexStatus(env);
  }finally{await env.DB.prepare('UPDATE search_phrase_build SET owner_token=NULL,lease_expires_at=NULL WHERE singleton=1 AND owner_token=?').bind(owner).run();}
}
