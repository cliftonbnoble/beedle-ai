import type { SearchRequest } from '@beedle/shared';
import type { Env } from '../lib/types';
import type { SearchContext } from './search-types';
import { SearchIndexUnavailableError } from '../lib/http';
import { corpusPartitions, corpusPartitionIndex } from './corpus-partitions';
import { searchReadyEnvironment } from './search-readiness';
import { searchPublicationSql } from './publication-policy';
import { runSearchInternal } from './search-pipeline';
import { searchSessionPolicy } from './search-sessions';
import { buildSearchScope, requestedIndexCodeFilters } from './search-filter-policy';
import { hasAnyExactIndexCodeCoverage } from './search-scope-repository';
import { enhanceQueryWithIndexCodeContext, expandQueryForRetrieval } from './search-scoring';
import { tokenize } from './search-text';

type Input=SearchRequest&{queryType?:SearchContext['queryType']};
type Snapshot=Awaited<ReturnType<typeof runSearchInternal>>;

// Every selected database must finish successfully. A failed partition is never
// presented as an empty result. The merge is deterministic, independent of I/O
// completion order, and explicitly versioned separately from single-DB ranking.
export function mergePartitionSearchResults(snapshots:Snapshot[]) {
  const results=snapshots.flatMap(snapshot=>snapshot.results);
  const identities=new Set<string>();
  for(const row of results){
    if(identities.has(row.documentId))throw new SearchIndexUnavailableError('Restored corpus contains overlapping document identities.');
    identities.add(row.documentId);
  }
  return results.sort((a,b)=>b.score-a.score||a.documentId.localeCompare(b.documentId)).slice(0,searchSessionPolicy.maxResults);
}

export async function runPartitionedSearch(env:Env,input:Input,kind:'search'|'debug') {
  const layout=corpusPartitions(env);
  if(!layout||!env.SEARCH_WORK_BUDGET)throw new SearchIndexUnavailableError('Restored corpus search is not configured.');
  if(env.VECTOR_LIFECYCLE_READY==='true'||env.VECTOR_FILTERS_READY==='true'||env.AI||env.VECTOR_INDEX)
    throw new SearchIndexUnavailableError('The restored corpus release requires its verified lexical-only configuration.');
  const selected=input.filters.documentIds?.length?new Set(await Promise.all(input.filters.documentIds.map(corpusPartitionIndex))):null;
  const parts=layout.partitions.filter(part=>!selected||selected.has(part.index));
  const ready:Env[]=[];
  for(let at=0;at<parts.length;at+=4)ready.push(...await Promise.all(parts.slice(at,at+4).map(part=>searchReadyEnvironment({...env,
    DB:part.database,CORPUS_PARTITIONS_JSON:undefined,CORPUS_COORDINATOR_DB:undefined,CORPUS_LOCAL_PARTITION:part.index}))));
  // The legacy soft index-code fallback is a corpus-wide decision, never a
  // separate fallback in partitions that happen not to contain that code.
  const codes=requestedIndexCodeFilters(input.filters);
  let exactCoverage=codes.length>0;
  for(const code of codes){
    let found=false;
    for(let at=0;at<ready.length&&!found;at+=4){
      const checks=await Promise.all(ready.slice(at,at+4).map(child=>hasAnyExactIndexCodeCoverage(child,{...input.filters,indexCode:undefined,indexCodes:[code]})));
      found=checks.some(Boolean);
    }
    if(!found){exactCoverage=false;break;}
  }
  const scope=buildSearchScope(input,input.corpusMode,{useSoftIndexCodeScope:codes.length>0&&!exactCoverage&&!input.filters.indexCodeOperator});
  const terms=[...new Set(tokenize(expandQueryForRetrieval(enhanceQueryWithIndexCodeContext(input.query,input.filters))).filter(term=>Array.from(term).length>=3))].slice(0,16);
  const match=terms.map(term=>'"'+term.replaceAll('"','""')+'"').join(' OR ');
  const candidates:Array<{documentId:string;decisionDate:string}>=[];
  // Select only identities/dates before fetching passage text. Running 32 full
  // corpus pipelines would multiply text I/O; one global finite candidate set
  // bounds that work while every partition still participates in discovery.
  for(let at=0;at<ready.length;at+=4){
    const batch=await Promise.all(ready.slice(at,at+4).map(async child=>{
      const text=match?`AND (d.id IN (SELECT m.document_id FROM search_body_substrings
        JOIN search_body_identity m ON m.row_id=search_body_substrings.rowid WHERE search_body_substrings MATCH ?
        AND ((m.source_kind='document' AND NOT EXISTS(SELECT 1 FROM retrieval_search_chunks a WHERE a.document_id=m.document_id AND a.active=1))
          OR (m.source_kind='retrieval' AND EXISTS(SELECT 1 FROM retrieval_search_chunks a WHERE a.chunk_id=m.chunk_id AND a.active=1))))
        OR d.id IN (SELECT m.document_id FROM search_metadata_substrings JOIN search_document_identity m
          ON m.row_id=search_metadata_substrings.rowid WHERE search_metadata_substrings MATCH ?))`:'';
      const rows=(await child.DB.prepare(`SELECT d.id AS documentId,COALESCE(d.decision_date,'') AS decisionDate FROM documents d
        ${scope.where} ${text} ORDER BY COALESCE(d.decision_date,'') DESC,d.id LIMIT ?`)
        .bind(...scope.params,...(match?[match,match]:[]),searchSessionPolicy.maxResults).all<{documentId:string;decisionDate:string}>()).results;
      for(const row of rows)if(await corpusPartitionIndex(row.documentId)!==child.CORPUS_LOCAL_PARTITION)
        throw new SearchIndexUnavailableError('Restored corpus document routing is inconsistent.');
      return rows;
    }));
    candidates.push(...batch.flat());
  }
  const ids=candidates.sort((a,b)=>b.decisionDate.localeCompare(a.decisionDate)||a.documentId.localeCompare(b.documentId))
    .slice(0,searchSessionPolicy.maxResults).map(row=>row.documentId);
  const byPartition=new Map<number,string[]>();
  for(const id of ids){const index=await corpusPartitionIndex(id);const list=byPartition.get(index)??[];list.push(id);byPartition.set(index,list);}
  const snapshots:Snapshot[]=[];
  // Four complete pipelines at a time bound transient text/candidate memory.
  for(let at=0;at<parts.length;at+=4){
    const batch=await Promise.all(parts.slice(at,at+4).map(async part=>{
      const documentIds=byPartition.get(part.index);if(!documentIds?.length)return null;
      const {budget,database}=env.SEARCH_WORK_BUDGET!.forkDatabase(part.database);
      // The child retains the existing per-pipeline SQL/scoring limits, while
      // charging every operation exactly once to the shared aggregate budget.
      const child={...ready.find(child=>child.CORPUS_LOCAL_PARTITION===part.index)!,DB:database,
        CORPUS_EXACT_INDEX_COVERAGE:exactCoverage,SEARCH_WORK_BUDGET:budget};
      const published=await child.DB.prepare(`SELECT 1 AS present FROM documents d WHERE ${searchPublicationSql(input)} LIMIT 1`).first();
      if(!published)return null;
      return runSearchInternal(child,{...input,filters:{...input.filters,documentIds},cursor:undefined,offset:0,limit:searchSessionPolicy.candidatePageSize},input.queryType||'keyword',kind==='debug');
    }));
    for(const snapshot of batch)if(snapshot)snapshots.push(snapshot);
    env.SEARCH_WORK_BUDGET.assertAvailable();
  }
  return {snapshots,results:mergePartitionSearchResults(snapshots)};
}
