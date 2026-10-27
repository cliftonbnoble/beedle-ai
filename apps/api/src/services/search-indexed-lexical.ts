import type { Env } from '../lib/types';
import type { ChunkRow } from './search-types';
import { RequestValidationError } from '../lib/http';
import { trustLabelSql } from './publication-policy';
import { requireSubstringIndex } from './search-index-build';

export const substringRecallLimits={body:1600,metadataDocuments:200,metadataChunksPerDocument:2} as const;
const quote=(value:string)=>'"'+value.replace(/"/g,'""')+'"';
export async function indexedLexicalSearch(env:Env,where:string,params:Array<string|number>,terms:string[],limit:number,scopedDocumentIds:string[]=[],options:{allowActiveDocumentChunkSearch?:boolean;wholeWord?:boolean;metadataOnly?:boolean}={}) {
  const supported=[...new Set(terms.filter(term=>Array.from(term).length>=3))];
  if(!supported.length)throw new RequestValidationError('Text search requires at least three characters; use structured filters for shorter identifiers');
  await requireSubstringIndex(env);
  const match=supported.map(quote).join(' OR ');
  const scope=where+(scopedDocumentIds.length?' AND d.id IN (SELECT value FROM json_each(?))':'');
  const scopedParams=[...params,...(scopedDocumentIds.length?[JSON.stringify(scopedDocumentIds)]:[])];
  // Numbered bindings reuse the scope in both independent indexed channels.
  let position=0;const numberedScope=scope.replace(/\?/g,()=>`?${++position}`);
  const matchPosition=++position;const primary=options.allowActiveDocumentChunkSearch?'1=1':`NOT EXISTS(SELECT 1 FROM retrieval_search_chunks a WHERE a.document_id=d.id AND a.active=1)`;
  const rows=await env.DB.prepare(`WITH
    body AS MATERIALIZED (SELECT m.row_id FROM search_body_substrings JOIN search_body_identity m ON m.row_id=search_body_substrings.rowid JOIN documents d ON d.id=m.document_id ${numberedScope} AND search_body_substrings MATCH ?${matchPosition} AND ${options.metadataOnly?'0':'1'} AND ((m.source_kind='document' AND ${primary}) OR (m.source_kind='retrieval' AND EXISTS(SELECT 1 FROM retrieval_search_chunks a WHERE a.chunk_id=m.chunk_id AND a.active=1))) ORDER BY search_body_substrings.rowid LIMIT ${substringRecallLimits.body}),
    metadata AS MATERIALIZED (SELECT d.id FROM search_metadata_substrings JOIN search_document_identity m ON m.row_id=search_metadata_substrings.rowid JOIN documents d ON d.id=m.document_id ${numberedScope} AND search_metadata_substrings MATCH ?${matchPosition} ORDER BY search_metadata_substrings.rowid LIMIT ${substringRecallLimits.metadataDocuments}),
    candidates AS (SELECT row_id FROM body UNION SELECT m.row_id FROM metadata JOIN search_body_identity m ON m.row_id IN (SELECT x.row_id FROM search_body_identity x WHERE x.document_id=metadata.id AND ((x.source_kind='document' AND NOT EXISTS(SELECT 1 FROM retrieval_search_chunks a WHERE a.document_id=x.document_id AND a.active=1)) OR (x.source_kind='retrieval' AND EXISTS(SELECT 1 FROM retrieval_search_chunks a WHERE a.chunk_id=x.chunk_id AND a.active=1))) ORDER BY x.row_id LIMIT ${substringRecallLimits.metadataChunksPerDocument}))
    SELECT m.chunk_id AS chunkId,d.id AS documentId,d.document_revision AS documentRevision,d.retrieval_revision AS retrievalRevision,d.title,d.citation,d.author_name AS authorName,d.decision_date AS decisionDate,d.file_type AS fileType,d.source_r2_key AS sourceFileRef,d.source_link AS sourceLink,d.index_codes_json AS indexCodesJson,d.rules_sections_json AS rulesSectionsJson,d.ordinance_sections_json AS ordinanceSectionsJson,COALESCE(c.section_label,r.section_label) AS sectionLabel,COALESCE(c.paragraph_anchor,r.paragraph_anchor) AS paragraphAnchor,COALESCE(c.citation_anchor,r.citation_anchor) AS citationAnchor,COALESCE(c.chunk_text,r.chunk_text) AS chunkText,COALESCE(c.created_at,r.created_at) AS createdAt,${trustLabelSql()} AS isTrustedTier,d.searchable_at AS searchableAt,CASE WHEN m.source_kind='document' THEN c.chunk_order ELSE 999999 END AS orderRank
    FROM candidates JOIN search_body_identity m USING(row_id) JOIN documents d ON d.id=m.document_id LEFT JOIN document_chunks c ON m.source_kind='document' AND c.id=m.chunk_id LEFT JOIN retrieval_search_chunks r ON m.source_kind='retrieval' AND r.chunk_id=m.chunk_id`).bind(...scopedParams,match).all<ChunkRow>();
  const fold=(value:string)=>value.replace(/[A-Z]/g,c=>c.toLowerCase());
  const word=(value:string)=>' '+fold(value).replace(/[\n\r.,;:()\-/]/g,' ')+' ';
  const contains=(value:string,term:string,whole=true)=>options.wholeWord&&whole?word(value).includes(' '+fold(term)+' '):fold(value).includes(fold(term));
  const ranked=rows.results.filter(row=>supported.some(term=>[row.chunkText,row.title,row.authorName||''].some(value=>contains(value,term))||contains(row.citation,term,false))).map(row=>{
    env.SEARCH_WORK_BUDGET?.consume('scoringOperations');
    return {...row,lexicalRank:supported.reduce((rank,term)=>rank+(contains(row.title,term)?2.4:0)+(contains(row.citation,term,false)?2:0)+(contains(row.authorName||'',term)?1.9:0)+(contains(row.sectionLabel||'',term)?1.4:0)+(contains(row.chunkText,term)?1:0),0)};
  });
  return ranked.sort((a,b)=>b.lexicalRank-a.lexicalRank||String(b.searchableAt||'').localeCompare(String(a.searchableAt||''))||Number(a.orderRank||0)-Number(b.orderRank||0)||a.chunkId.localeCompare(b.chunkId)).slice(0,limit);
}
