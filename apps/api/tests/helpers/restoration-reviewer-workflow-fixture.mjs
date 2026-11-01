import {loadTs} from './load-ts.mjs';
import {sourceExecutionWorkflowFixture} from './restoration-source-execution-workflow-fixture.mjs';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';
import {restorationSchemaFingerprint} from '../../scripts/lib/owned-restoration-resources.mjs';

export async function restorationReviewerFixture(mode='seeded'){
  const x=await sourceExecutionWorkflowFixture(mode),env={DB:x.env.DB};
  const recovery=loadTs('src/services/search-index-recovery.ts'),facets=loadTs('src/services/search-facet-build.ts');
  const generation=crypto.randomUUID();
  await recovery.startIndexRecovery(env,{dryRun:false,generation,expectedGeneration:'legacy',replaceArchive:true,restart:false});
  for(let count=0;(await recovery.indexRecoveryStatus(env)).status!=='completed';count++){
    if(count>100)throw Error('Synthetic recovery bound');await recovery.advanceIndexRecovery(env,generation);
  }
  for(let count=0;(await facets.facetBuildStatus(env)).phase!=='ready';count++){
    if(count>100)throw Error('Synthetic facets bound');await facets.advanceFacetBuild(env);
  }
  const rows=()=>x.sqlite.prepare("SELECT name,type,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name").all();
  x.plan.beforeSchemaSha256=await restorationSchemaFingerprint(rows());let after;
  x.sqlite.exec('BEGIN');try{for(const sql of loadTs('src/services/restoration-source-execution-control.ts').restorationSourceExecutionSql)x.sqlite.exec(sql);after=await restorationSchemaFingerprint(rows());}finally{x.sqlite.exec('ROLLBACK');}
  x.plan.sourceSpecJson=JSON.stringify({...JSON.parse(x.plan.sourceSpecJson),schemaSha256:after});x.env.OPERATION_JSON=JSON.stringify(x.plan);
  await loadTs('src/services/restoration-source-execution-workflow.ts').runSourceExecutionWorkflow(x.env,
    {instanceId:x.execution.executionId,payload:{operationPlanSha256:await fingerprint(x.plan)}},{async do(_name,_options,callback){return callback();}});
  const service=loadTs('src/services/restoration-reviewer-workflow.ts'),one=sql=>x.sqlite.prepare(sql).get(),all=sql=>x.sqlite.prepare(sql).all();
  const now=Date.now(),plan={version:1,operationId:crypto.randomUUID(),source:x.identity,schemaSha256:await restorationSchemaFingerprint(rows()),
    ownerSha256:await fingerprint(one('SELECT * FROM beedle_restore_owner')),
    journalsSha256:await fingerprint(await service.restorationSourceJournalSnapshot(env.DB)),
    indexControlsSha256:await fingerprint(await service.restorationIndexControlSnapshot(env.DB)),
    jobsSha256:await fingerprint(all('SELECT * FROM beedle_restore_jobs ORDER BY document_id')),
    receiptsSha256:await fingerprint(all('SELECT * FROM beedle_restore_source_results ORDER BY document_id')),
    corpusRevision:one('SELECT revision FROM vector_lifecycle_epoch').revision,catalogVersion:one('SELECT active_version FROM legal_catalog_state').active_version,
    documents:one('SELECT COUNT(*) n FROM documents').n,eligibleDocuments:one("SELECT COUNT(*) n FROM beedle_restore_jobs WHERE phase='processed'").n,
    notBefore:new Date(now-1000).toISOString(),notAfter:new Date(now+20*60000).toISOString(),completionDeadline:new Date(now+60*60000).toISOString(),
    maximumSteps:Math.floor(one('SELECT COUNT(*) n FROM documents').n/25)+3};
  return{...x,sourcePlan:x.plan,plan,env:{DB:x.env.DB,PROOFS:x.proofs,OPERATION_ENABLED:'true',OPERATION_JSON:JSON.stringify(plan)}};
}

export async function provisionalRestorationReviewerFixture(mode='seeded'){
  const x=await restorationReviewerFixture(mode),service=loadTs('src/services/restoration-reviewer-workflow.ts'),publication=loadTs('src/services/restoration-provisional-publication.ts');
  const docs=x.sqlite.prepare("SELECT d.* FROM documents d JOIN beedle_restore_jobs j ON j.document_id=d.id WHERE j.phase='processed' ORDER BY d.id").all();
  for(let offset=0;offset<docs.length;offset+=25){const records=await Promise.all(docs.slice(offset,offset+25).map(async doc=>{const job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(doc.id);return{documentId:doc.id,documentRevision:doc.document_revision,generationId:doc.active_text_generation,metadataSha256:await fingerprint(doc),sourceKey:doc.source_r2_key,sourceSha256:job.source_sha256,sourceBytes:job.source_bytes};}));await publication.publishRestorationProvisionalBatch(x.env.DB,x.identity,'unreviewed-provisional-v1',records);}
  const facets=loadTs('src/services/search-facet-build.ts');for(let count=0;(await facets.facetBuildStatus({DB:x.env.DB})).phase!=='ready';count++){if(count>100)throw Error('Synthetic provisional facet bound');await facets.advanceFacetBuild({DB:x.env.DB});}
  x.plan={...x.plan,version:2,publicationPolicy:'unreviewed-provisional-v1',publishedDocuments:docs.length,corpusRevision:x.sqlite.prepare('SELECT revision FROM vector_lifecycle_epoch').get().revision,indexControlsSha256:await fingerprint(await service.restorationIndexControlSnapshot(x.env.DB))};x.env.OPERATION_JSON=JSON.stringify(x.plan);return x;
}
