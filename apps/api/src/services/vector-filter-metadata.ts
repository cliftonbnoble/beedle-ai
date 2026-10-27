import indexManifest from "../../vector-metadata-indexes.json";
import type { Env } from "../lib/types";
import type { SearchRequest } from "@beedle/shared";
import { sha256Bytes } from "../lib/digests";
import { canonicalJson } from "./activation-contract";
import { trustedDocumentSql } from "./publication-policy";
import { normalizeChunkTypeLabel, requestedJudgeFilters, requestedIndexCodeFilters } from "./search-query-analysis";
export const VECTOR_FILTER_VERSION=3;
export const vectorMetadataIndexes=indexManifest;
export const vectorFilterFields=['docKey','judgeKey','jurisdictionKey','fileType','trusted','decisionDate','series','cohortKey','sectionKey','scopeDigest'] as const;
export const vectorScopePolicy={maxPartitions:4,maxCohorts:112,maxFilterBytes:1900} as const;
export const vectorFilterHash=(value:string)=>sha256Bytes(new TextEncoder().encode(value));
const asciiLower=(value:string)=>value.replace(/[A-Z]/g,char=>char.toLowerCase());
export type VectorSearchScope={filters:SearchRequest['filters'];corpusMode:SearchRequest['corpusMode'];where:string;params:Array<string|number>};

export async function projectVectorFilterMetadata(env:Env,documentId:string,revision:number) {
  const doc=await env.DB.prepare(`SELECT d.id,lower(COALESCE(d.author_name,'')) AS judge,d.jurisdiction,d.file_type AS fileType,d.valid_decision_date AS decisionDate,
    ${trustedDocumentSql()} AS trusted,
    CASE WHEN upper(substr(trim(COALESCE(d.citation,'')),1,1)) IN ('T','L') THEN upper(substr(trim(d.citation),1,1))
      WHEN upper(substr(trim(COALESCE(d.case_number,'')),1,1)) IN ('T','L') THEN upper(substr(trim(d.case_number),1,1)) ELSE upper(substr(trim(COALESCE(d.title,'')),1,1)) END AS series
    FROM documents d WHERE id=?`).bind(documentId).first<{id:string;judge:string;jurisdiction:string;fileType:string;decisionDate:string|null;trusted:number;series:string}>();
  if(!doc) return null;
  const queries=[
    "SELECT code,normalized_code FROM document_index_codes WHERE document_id=? ORDER BY normalized_code,code LIMIT 1001",
    "SELECT section,normalized_section FROM document_rules_sections WHERE document_id=? ORDER BY normalized_section,section LIMIT 1001",
    "SELECT section,normalized_section FROM document_ordinance_sections WHERE document_id=? ORDER BY normalized_section,section LIMIT 1001",
    "SELECT reference_type,normalized_value,canonical_value FROM document_reference_links WHERE document_id=? AND is_valid=1 ORDER BY reference_type,normalized_value,canonical_value LIMIT 1001"
  ];
  const sets=await Promise.all(queries.map(query=>env.DB.prepare(query).bind(documentId).all()));
  const exact=sets.every(set=>(set.results||[]).length<=1000);
  // The cohort includes every input used by reference/index-code SQL filters.
  // Identical cohorts therefore have identical membership, independent of IDs.
  const cohortKey=await vectorFilterHash(exact?canonicalJson(sets.map(set=>set.results||[])):`unrepresentable:${documentId}`);
  const projection:Record<string,string|boolean>={docKey:await vectorFilterHash(doc.id),judgeKey:await vectorFilterHash(doc.judge),jurisdictionKey:await vectorFilterHash(doc.jurisdiction),
    fileType:doc.fileType,trusted:Boolean(doc.trusted),series:doc.series,cohortKey};
  if(doc.decisionDate!==null) projection.decisionDate=doc.decisionDate;
  await env.DB.prepare(`INSERT INTO vector_document_metadata(document_id,root_revision,cohort_key,cohort_exact,metadata_json) VALUES(?,?,?,?,?)
    ON CONFLICT(document_id) DO UPDATE SET root_revision=excluded.root_revision,cohort_key=excluded.cohort_key,cohort_exact=excluded.cohort_exact,metadata_json=excluded.metadata_json`)
    .bind(documentId,revision,cohortKey,Number(exact),canonicalJson(projection)).run();
  return projection;
}
export async function vectorEntryFilterProjection(document:Record<string,string|boolean>,sectionLabel:string,docDigest:string) {
  const projection={...document,sectionKey:await vectorFilterHash(normalizeChunkTypeLabel(sectionLabel)),docDigest};
  const json=canonicalJson(projection);
  return {projection,json,digest:await vectorFilterHash(json)};
}
export function stripVectorFilterMetadata(metadata:Record<string,VectorizeVectorMetadata>={}) {
  return Object.fromEntries(Object.entries(metadata).filter(([key])=>!vectorFilterFields.includes(key as typeof vectorFilterFields[number])));
}

export async function planVectorFilters(env:Env,scope?:VectorSearchScope):Promise<{filters:VectorizeVectorMetadataFilter[];reason:string|null}> {
  if(!scope) return {filters:[{live:true}],reason:null};
  if(env.VECTOR_FILTERS_READY!=='true') return {filters:[],reason:'vector_filter_indexes_unready'};
  const {filters}=scope;
  const base:VectorizeVectorMetadataFilter={live:true,fileType:filters.fileType||'decision_docx'};
  if(scope.corpusMode==='trusted_only'||filters.approvedOnly) base.trusted=true;
  const selectedIds=[...new Set(filters.documentIds||[])];
  if(filters.documentId && selectedIds.length && !selectedIds.includes(filters.documentId))return {filters:[],reason:null};
  if(filters.documentId)base.docKey=await vectorFilterHash(filters.documentId);
  else if(selectedIds.length)base.docKey={$in:await Promise.all(selectedIds.map(id=>vectorFilterHash(id)))};
  if(filters.jurisdiction)base.jurisdictionKey=await vectorFilterHash(filters.jurisdiction);
  const judges=requestedJudgeFilters(filters);
  if(judges.length)base.judgeKey={$in:await Promise.all(judges.map(judge=>vectorFilterHash(asciiLower(judge))))};
  if(filters.decisionSeries)base.series=filters.decisionSeries;
  if(filters.fromDate||filters.toDate)base.decisionDate={...(filters.fromDate?{$gte:filters.fromDate}:{}),...(filters.toDate?{$lte:filters.toDate}:{})};
  if(filters.chunkType)base.sectionKey=await vectorFilterHash(normalizeChunkTypeLabel(filters.chunkType));
  const references=requestedIndexCodeFilters(filters).length>0||Boolean(filters.rulesSection||filters.ordinanceSection);
  if(new TextEncoder().encode(JSON.stringify(base)).length>=vectorScopePolicy.maxFilterBytes)return {filters:[],reason:'vector_filter_exceeds_byte_budget'};
  if(!references&&!filters.partyName) return {filters:[base],reason:null};
  // Unsupported contains filters and oversized cohorts use exact D1 document
  // membership. Scalar constraints remain in every provider partition.
  const unsupported=filters.partyName||Boolean(await env.DB.prepare(`SELECT 1 FROM documents d JOIN vector_document_metadata m ON m.document_id=d.id
    ${scope.where} AND m.cohort_exact=0 LIMIT 1`).bind(...scope.params).first());
  const rows=(await env.DB.prepare(unsupported
    ? `SELECT d.id AS value FROM documents d ${scope.where} ORDER BY d.id LIMIT ?`
    : `SELECT DISTINCT m.cohort_key AS value FROM documents d JOIN vector_document_metadata m ON m.document_id=d.id ${scope.where} ORDER BY m.cohort_key LIMIT ?`)
    .bind(...scope.params,vectorScopePolicy.maxCohorts+1).all<{value:string}>()).results||[];
  if(rows.length>vectorScopePolicy.maxCohorts) return {filters:[],reason:'vector_scope_exceeds_partition_budget'};
  if(!rows.length)return {filters:[],reason:null};
  const values=unsupported?await Promise.all(rows.map(row=>vectorFilterHash(row.value))):rows.map(row=>row.value);
  const key=unsupported?'docKey':'cohortKey';
  const partitions:VectorizeVectorMetadataFilter[]=[];let group:string[]=[];
  for(const value of values) {
    const next={...base,[key]:{$in:[...group,value]}};
    if(new TextEncoder().encode(JSON.stringify(next)).length>=vectorScopePolicy.maxFilterBytes) {
      if(!group.length)return {filters:[],reason:'vector_filter_exceeds_byte_budget'};
      partitions.push({...base,[key]:{$in:group}});group=[];
    }
    group.push(value);
  }
  if(group.length)partitions.push({...base,[key]:{$in:group}});
  if(partitions.length>vectorScopePolicy.maxPartitions)return {filters:[],reason:'vector_scope_exceeds_partition_budget'};
  return {filters:partitions,reason:null};
}

export function vectorScopeMetadataMatches(metadata:Record<string,VectorizeVectorMetadata>|undefined,json:string) {
  const expected:Record<string,string|boolean>=JSON.parse(json);
  return vectorFilterFields.filter(key=>key!=='scopeDigest').every(key=>metadata?.[key]===expected[key])&&metadata?.docDigest===expected.docDigest;
}
