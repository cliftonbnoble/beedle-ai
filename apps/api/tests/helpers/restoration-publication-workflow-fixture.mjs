import {restorationReviewerFixture} from './restoration-reviewer-workflow-fixture.mjs';
import {loadTs} from './load-ts.mjs';
import {fingerprint} from '../../scripts/lib/release-contract.mjs';

export async function restorationPublicationFixture(mode='seeded') {
  const x=await restorationReviewerFixture(mode), service=loadTs('src/services/restoration-publication-workflow.ts');
  const documents=x.sqlite.prepare('SELECT * FROM documents ORDER BY id').all(),records=[];
  for(const doc of documents) {
    const job=x.sqlite.prepare('SELECT * FROM beedle_restore_jobs WHERE document_id=?').get(doc.id);
    if(job.phase==='processed'&&job.extraction_policy!=='held-unreadable')records.push({documentId:doc.id,documentRevision:doc.document_revision,
      generationId:doc.active_text_generation,metadataSha256:await fingerprint(doc),sourceKey:doc.source_r2_key,sourceSha256:job.source_sha256,sourceBytes:job.source_bytes});
  }
  const {eligibleDocuments,corpusRevision,version,...base}=x.plan;
  const plan={...base,version:1,publicationPolicy:'unreviewed-provisional-v1',records,initialCorpusRevision:corpusRevision,
    documentsSha256:await fingerprint(service.restorationPublicationStableDocuments(documents)),maximumSteps:Math.ceil(records.length/25)+2};
  const env={DB:x.env.DB,PROOFS:x.proofs,OPERATION_ENABLED:'true',OPERATION_JSON:JSON.stringify({version:1,operationId:plan.operationId,operationPlanSha256:await fingerprint(plan)})};
  return{...x,plan,env};
}
