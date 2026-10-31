// Pure declarations shared by the Worker and CLI; no I/O or schema changes.
export const partitionIntegrityCommands={
  metadata:{command:"INSERT INTO search_metadata_substrings(search_metadata_substrings,rank) VALUES('integrity-check',1)",sha256:'999e91bb137511dcb40d4c2c2dfcbcf7d571ab4c908a381b0ff34ec6d4d3a291'},
  body:{command:"INSERT INTO search_body_substrings(search_body_substrings,rank) VALUES('integrity-check',1)",sha256:'a7eb803969549c851d2d423ff1831e22daaca44900ea3c0bc09e9beea0690173'},
  phrase:{command:"INSERT INTO search_chunks_fts(search_chunks_fts,rank) VALUES('integrity-check',1)",sha256:'07b944135270ff3eb2cde7364edb9f855488b6f1d58286510b6603a8c6607d80'},
  physical:{command:'PRAGMA quick_check',sha256:'c21fff1dea8cab2b5d33f21a187befd59cf7318858d8f844e4e268095c22bcce'},
  foreign_keys:{command:'PRAGMA foreign_key_check',sha256:'75d969201666ceebff397862f6a8b8d2eec57903f8ffbfbe0095a39cb3576f19'},
  source_sha:{command:'sha256-original-source-readback-v1',sha256:'9c94e294d89317c6db78d494eb00a4be9ebc73802cfa6af1f60b439cef7d8ad2'}
};
export const corpusPartitionPolicy={id:'restored-corpus-32-v1',partitions:32,readOnly:true,lexicalOnly:true,
  routing:'sha256-utf8-first-u32be-modulo-32',candidateDocuments:200,fanout:4,rankingVersion:'partitioned-search-m08-v1',
  integrityCommands:Object.fromEntries(Object.entries(partitionIntegrityCommands).map(([kind,row])=>[kind,row.sha256]))};
const digest=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const integer=(value,min=0)=>Number.isSafeInteger(value)&&value>=min;
const keys=(value,expected)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join('|')===expected.slice().sort().join('|');
export const corpusPartitionBinding=index=>index===0?'DB':`CORPUS_DB_${String(index).padStart(2,'0')}`;
export function parseCorpusPartitionManifest(text){
  if(typeof text!=='string'||text.length>20_000)throw Error('Invalid corpus partition manifest');
  const m=JSON.parse(text);
  if(!keys(m,['version','partitionCount','readOnly','sourceManifestSha256','catalogVersion','catalogSha256','restoreOwnerId','restorePlanSha256','documents','publishedDocuments','partitions'])||
    m.version!==1||m.partitionCount!==32||m.readOnly!==true||!digest(m.sourceManifestSha256)||!digest(m.catalogSha256)||!digest(m.restorePlanSha256)||
    typeof m.restoreOwnerId!=='string'||!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(m.restoreOwnerId)||
    typeof m.catalogVersion!=='string'||m.catalogVersion.length<1||m.catalogVersion.length>200||
    !integer(m.documents)||!integer(m.publishedDocuments)||m.publishedDocuments>m.documents||!Array.isArray(m.partitions)||m.partitions.length!==32)
    throw Error('Invalid corpus partition manifest');
  let documents=0,published=0;
  for(let index=0;index<32;index++){
    const p=m.partitions[index];
    if(!keys(p,['index','binding','documents','publishedDocuments','revision'])||p.index!==index||p.binding!==corpusPartitionBinding(index)||
      !integer(p.documents)||!integer(p.publishedDocuments)||p.publishedDocuments>p.documents||!integer(p.revision,1))throw Error('Invalid corpus partition mapping');
    documents+=p.documents;published+=p.publishedDocuments;
  }
  if(!integer(documents)||!integer(published)||documents!==m.documents||published!==m.publishedDocuments)throw Error('Incomplete corpus partition counts');
  return m;
}
export function partitionResourceBindings(target,manifest){
  if(target.d1_databases.length!==32)throw Error('Only the 32 declared corpus database bindings are allowed');
  const selected=manifest.partitions.map(part=>{
    const matches=target.d1_databases.filter(binding=>binding.binding===part.binding);
    if(matches.length!==1||typeof matches[0].database_name!=='string'||!matches[0].database_name||
      !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(matches[0].database_id))throw Error('Incomplete partition resource declaration');
    return matches[0];
  });
  if(new Set(selected.map(row=>row.database_id)).size!==32||new Set(selected.map(row=>row.database_name)).size!==32)throw Error('Aliased corpus partition resources');
  return selected;
}
// Full commands are executed separately once by the owned verifier. This probe
// only reads their durable acknowledged receipts at the exact release epoch.
export const partitionProofQuery=`SELECT o.*,
  (SELECT active_version FROM legal_catalog_state WHERE singleton=1) AS active_catalog,
  (SELECT COUNT(*) FROM documents) AS actual_documents,
  (SELECT COUNT(*) FROM documents WHERE searchable_at IS NOT NULL AND published_revision=document_revision AND ingestion_state='ready') AS actual_published,
  (SELECT COUNT(*) FROM documents WHERE approved_at IS NOT NULL OR approved_revision IS NOT NULL OR qc_required_confirmed!=0) AS human_approval_rows,
  (SELECT COUNT(*) FROM beedle_restore_integrity p WHERE p.owner_id=o.owner_id AND p.plan_sha256=o.plan_sha256
    AND p.corpus_revision=(SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1)
    AND p.kind IN ('metadata','body','phrase','physical','foreign_keys','source_sha') AND p.status='passed' AND p.engine_acknowledged=1
    AND p.command_sha256=CASE p.kind ${Object.entries(partitionIntegrityCommands).map(([kind,row])=>`WHEN '${kind}' THEN '${row.sha256}'`).join(' ')} END
    AND length(p.result_sha256)=64) AS acknowledged_proofs
  FROM beedle_restore_owner o WHERE singleton=1`;
export function partitionProofProblems(manifest,index,row){
  const p=manifest.partitions[index];
  if(!row||row.owner_id!==manifest.restoreOwnerId||row.plan_sha256!==manifest.restorePlanSha256||
    row.source_manifest_sha256!==manifest.sourceManifestSha256||row.catalog_sha256!==manifest.catalogSha256||
    row.partition_index!==index||row.partition_count!==32||row.phase!=='sealed'||row.expected_documents!==p.documents||
    row.expected_published_documents!==p.publishedDocuments||row.actual_documents!==p.documents||row.actual_published!==p.publishedDocuments||
    row.active_catalog!==manifest.catalogVersion||row.human_approval_rows!==0||row.acknowledged_proofs!==6)return ['partition:'+index+':restoration_proof'];
  return [];
}
export const corpusReviewCacheProofQuery=`SELECT c.*,
  (SELECT active_version FROM legal_catalog_state WHERE singleton=1) AS activeCatalog,
  (SELECT COUNT(*) FROM beedle_corpus_review_rows) AS indexedCount,
  (SELECT COALESCE(SUM(member_count),0) FROM beedle_corpus_review_groups) AS groupCount,
  (SELECT COUNT(*) FROM beedle_corpus_review_rows q LEFT JOIN beedle_corpus_review_groups g USING(group_key) WHERE g.group_key IS NULL) AS missingGroups
  FROM beedle_corpus_review_control c WHERE singleton=1`;
export async function manifestTextFingerprint(text){
  return [...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text)))].map(n=>n.toString(16).padStart(2,'0')).join('');
}
export function corpusReviewCacheProblems(manifest,manifestSha,row,policy){
  return !row||row.phase!=='ready'||row.epoch!==manifest.partitions[0].revision||row.policy_version!==policy||
    row.catalog_version!==row.activeCatalog||row.catalog_version!==manifest.catalogVersion||row.manifest_sha256!==manifestSha||
    !digest(row.rows_sha256)||row.source_count!==manifest.documents||row.indexedCount!==manifest.documents||row.groupCount!==manifest.documents||row.missingGroups!==0
    ?['corpus_review_cache']:[];
}
