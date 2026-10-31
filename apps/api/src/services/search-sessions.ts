import { VECTOR_PIPELINE_VERSION, CHUNKER_VERSION, RETRIEVAL_CHUNKER_VERSION } from "./vector-profile";
import type { Env, DatabaseEnv } from '../lib/types';
import type { SearchRequest } from '@beedle/shared';
import { RequestConflictError, RequestValidationError, SearchIndexUnavailableError } from '../lib/http';
import { assertCorpusPartitionRevisions } from './corpus-partitions';
import {corpusPartitionPolicy} from '../../scripts/lib/corpus-partition-contract.mjs';
export const searchRankingVersion='search-ranking-m08-v1';
export const rankingVersionFor=(env:Env)=>env.CORPUS_PARTITIONS_JSON?corpusPartitionPolicy.rankingVersion:searchRankingVersion;
export const searchSessionPolicy={scope:'public-published-corpus-v1',ttlMs:600_000,maxBytes:524_288,totalBytes:16*1024*1024,totalSessions:256,maxResults:200,cleanupBatch:32,candidatePageSize:20} as const;
type Row={id:string;secret:string;request_hash:string;scope:string;ranking_version:string;corpus_revision:number;payload_json:string;expires_at:number};
type Snapshot={results:Array<{documentId:string;corpusTier:string;[key:string]:unknown}>;[key:string]:unknown};
type Input=SearchRequest&{queryType?:string};
const encode=new TextEncoder();
function randomKey(){return [...crypto.getRandomValues(new Uint8Array(32))].map(n=>n.toString(16).padStart(2,'0')).join('');}
async function hmac(secret:string,value:string){const key=await crypto.subtle.importKey('raw',encode.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);return [...new Uint8Array(await crypto.subtle.sign('HMAC',key,encode.encode(value)))].map(n=>n.toString(16).padStart(2,'0')).join('');}
function canonical(value:unknown):string {
  if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';
  if(value&&typeof value==='object')return '{'+Object.entries(value).filter(([,v])=>v!==undefined).sort(([a],[b])=>a<b?-1:a>b?1:0).map(([k,v])=>JSON.stringify(k)+':'+canonical(v)).join(',')+'}';
  return JSON.stringify(value);
}
function requestIdentity(input:Input,kind:'search'|'debug',env:Env) {
  return canonical({kind,query:input.query,filters:input.filters,corpusMode:input.corpusMode,snippetMaxLength:input.snippetMaxLength,queryType:input.queryType||'keyword',scope:searchSessionPolicy.scope,rankingVersion:rankingVersionFor(env),partitionManifest:env.CORPUS_PARTITIONS_JSON,retrievalProfile:{model:env.AI_EMBEDDING_MODEL,dimensions:env.AI_EMBEDDING_DIMENSIONS,namespace:env.VECTOR_NAMESPACE,semanticAvailable:Boolean(env.AI&&env.VECTOR_INDEX),lifecycleReady:env.VECTOR_LIFECYCLE_READY,filtersReady:env.VECTOR_FILTERS_READY,pipeline:VECTOR_PIPELINE_VERSION,chunker:CHUNKER_VERSION,retrievalChunker:RETRIEVAL_CHUNKER_VERSION},sourceProxy:env.SOURCE_PROXY_BASE_URL,environment:env.APP_ENV});
}
export async function searchCorpusRevision(env:DatabaseEnv & Pick<Env,'CORPUS_PARTITIONS_JSON'|'CORPUS_COORDINATOR_DB'>) {
  const partitioned=await assertCorpusPartitionRevisions(env);if(partitioned!==null)return partitioned;
  const value=await env.DB.prepare('SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1').first<number>('revision');
  if(!Number.isSafeInteger(value))throw new SearchIndexUnavailableError('Search corpus revision is unavailable');return value!;
}
export async function expireSearchSessions(env:Env,now=Date.now()) {
  await env.DB.prepare('DELETE FROM search_sessions WHERE id IN (SELECT id FROM search_sessions WHERE expires_at<=? ORDER BY expires_at,id LIMIT 32)').bind(now).run();
}
async function cursor(row:Row,position:number) {
  const signature=await hmac(row.secret,`${row.id}|${position}|${row.request_hash}|${row.corpus_revision}|${row.ranking_version}|${row.scope}`);
  return `s1.${row.id}.${position}.${signature}`;
}
function expired():never{throw new RequestConflictError('This search has expired or the corpus changed. Run the search again.');}
export async function createSearchSession(env:Env,input:Input,kind:'search'|'debug',snapshot:Snapshot,revision:number) {
  await expireSearchSessions(env);
  // Store public evidence and numeric diagnostics, without the raw query or filters.
  const {query:_query,filters:_filters,...payload}=snapshot;
  const json=JSON.stringify(payload);const bytes=encode.encode(json).byteLength;
  if(bytes>searchSessionPolicy.maxBytes)throw new SearchIndexUnavailableError('Search result snapshot is too large. Narrow the query and try again.');
  const now=Date.now();const secret=randomKey();const id=crypto.randomUUID();
  const row:Row={id,secret,request_hash:await hmac(secret,requestIdentity(input,kind,env)),scope:searchSessionPolicy.scope,ranking_version:rankingVersionFor(env),corpus_revision:revision,payload_json:json,expires_at:now+searchSessionPolicy.ttlMs};
  const inserted=await env.DB.prepare(`INSERT INTO search_sessions(id,secret,request_hash,scope,ranking_version,corpus_revision,payload_json,payload_bytes,created_at,expires_at)
   SELECT ?,?,?,?,?,?,?,?,?,? WHERE (SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1)=? AND EXISTS(SELECT 1 FROM search_session_capacity WHERE singleton=1 AND reserved_count<? AND reserved_bytes+?<=?) RETURNING id`).bind(id,secret,row.request_hash,row.scope,row.ranking_version,revision,json,bytes,now,row.expires_at,revision,searchSessionPolicy.totalSessions,bytes,searchSessionPolicy.totalBytes).first();
  if(!inserted){if(await searchCorpusRevision(env)!==revision)expired();throw new SearchIndexUnavailableError('Search pagination is temporarily at capacity. Please retry shortly.');}
  return row;
}
export async function readSearchSession(env:Env,input:Input,kind:'search'|'debug') {
  const parts=/^s1\.([a-f0-9-]{36})\.(\d{1,4})\.([a-f0-9]{64})$/.exec(input.cursor||'');
  if(!parts)throw new RequestValidationError('Invalid search cursor');
  const row=await env.DB.prepare('SELECT * FROM search_sessions WHERE id=? AND expires_at>?').bind(parts[1],Date.now()).first<Row>();
  if(!row||row.scope!==searchSessionPolicy.scope||row.ranking_version!==rankingVersionFor(env))expired();
  if(row.request_hash!==await hmac(row.secret,requestIdentity(input,kind,env)))throw new RequestConflictError('Search cursor does not match this query, filters, or result format. Run the search again.');
  if(await cursor(row,Number(parts[2]))!==input.cursor)throw new RequestValidationError('Invalid search cursor');
  if(await searchCorpusRevision(env)!==row.corpus_revision)expired();
  const snapshot=JSON.parse(row.payload_json) as Snapshot;
  if(kind==='debug')snapshot.filters=input.filters;
  const position=Number(parts[2]);if(position>snapshot.results.length)throw new RequestValidationError('Invalid search cursor position');
  return {row,snapshot,position};
}
export async function searchSnapshotPage(env:Env,input:Input,snapshot:Snapshot,position:number,revision:number,row?:Row) {
  const results=snapshot.results.slice(position,position+input.limit);const next=position+results.length;const hasMore=next<snapshot.results.length;
  const nextCursor=hasMore&&row?await cursor(row,next):null;
  if(await searchCorpusRevision(env)!==revision || (row && row.expires_at<=Date.now()))expired();
  env.SEARCH_WORK_BUDGET?.assertAvailable();
  return {...snapshot,query:input.query,...('filters' in snapshot?{filters:input.filters}:{}),offset:position,limit:input.limit,total:snapshot.results.length,results,hasMore:hasMore&&Boolean(row),nextCursor,sessionExpiresAt:row?new Date(row.expires_at).toISOString():null,countKind:'bounded_candidates' as const,corpusExhaustive:false as const,candidateExhausted:!hasMore,rankingVersion:rankingVersionFor(env),corpusRevision:String(revision),tierCounts:{trusted:results.filter(result=>result.corpusTier==='trusted').length,provisional:results.filter(result=>result.corpusTier!=='trusted').length}};
}
