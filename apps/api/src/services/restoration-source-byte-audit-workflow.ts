import type {WorkflowEvent, WorkflowStep} from 'cloudflare:workers';
import {z} from 'zod';
import {canonical, fingerprint} from '../../scripts/lib/release-contract.mjs';
import {sha256Bytes} from '../lib/digests';
import {auditRestorationSourceBytes, restorationByteAuditRecordSchema,
  restorationByteAuditSourceSchema} from './restoration-source-byte-audit';

const hash=z.string().regex(/^[a-f0-9]{64}$/), uuid=z.string().uuid();
const rootSchema=z.object({version:z.literal(1),auditRootId:uuid,
  partitions:z.array(z.object({source:restorationByteAuditSourceSchema,partitionPlanSha256:hash}).strict()).length(32),
  notBefore:z.string().datetime(),completionDeadline:z.string().datetime()}).strict();
const planSchema=z.object({version:z.literal(1),operationId:uuid,auditRootId:uuid,
  partitionIndex:z.number().int().min(0).max(31),databaseBookendSha256:hash,
  records:z.array(restorationByteAuditRecordSchema).min(1).max(1000),
  notBefore:z.string().datetime(),notAfter:z.string().datetime(),completionDeadline:z.string().datetime()}).strict();
// Component capabilities, deliberately excluding every database and source-write method.
export interface RestorationByteAuditCapabilities {
  originals:Pick<R2Bucket,'get'>; target:Pick<R2Bucket,'get'>;
  proofs:Pick<R2Bucket,'get'|'put'>; operationEnabled:string; operationJson:string;
}
export type RestorationByteAuditParams={operationPlanJson:string}|{rootPlanJson:string};
export function parseRestorationByteAuditRoot(text:string) {
  if(new TextEncoder().encode(text).length>131072)throw Error('byte_audit_root_size');
  const root=rootSchema.parse(JSON.parse(text));
  if(canonical(root)!==text||Date.parse(root.completionDeadline)<=Date.parse(root.notBefore)||
    Date.parse(root.completionDeadline)-Date.parse(root.notBefore)>4*60*60000||
    root.partitions.some((row,index)=>row.source.partitionIndex!==index)||
    new Set(root.partitions.map(row=>row.source.resourceId)).size!==32||
    root.partitions.some(row=>row.source.ownerId!==root.partitions[0]!.source.ownerId||
      row.source.planSha256!==root.partitions[0]!.source.planSha256||
      row.source.resourcePlanSha256!==root.partitions[0]!.source.resourcePlanSha256||
      row.source.sourceManifestSha256!==root.partitions[0]!.source.sourceManifestSha256||
      row.source.catalogSha256!==root.partitions[0]!.source.catalogSha256))throw Error('byte_audit_root_bounds');
  return root;
}
export function parseRestorationByteAuditPlan(text:string) {
  if(new TextEncoder().encode(text).length>768*1024)throw Error('byte_audit_plan_size');
  const plan=planSchema.parse(JSON.parse(text)),begin=Date.parse(plan.notBefore),end=Date.parse(plan.notAfter);
  if(end<=begin||end-begin>30*60000||Date.parse(plan.completionDeadline)<=end||
    Date.parse(plan.completionDeadline)-begin>4*60*60000||
    plan.records.some((row,index)=>index>0&&plan.records[index-1]!.documentId>=row.documentId))
    throw Error('byte_audit_plan_bounds');
  return plan;
}
async function readText(bucket:Pick<R2Bucket,'get'>,key:string,maximum:number) {
  const value=await bucket.get(key);
  if(!value||!value.version||!value.etag||!Number.isSafeInteger(value.size)||value.size<1||value.size>maximum)
    throw Error('byte_audit_proof_identity');
  const bytes=new Uint8Array(await value.arrayBuffer());
  if(bytes.length!==value.size)throw Error('byte_audit_proof_size');
  return{value,bytes,text:new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(bytes)};
}
async function putProof(bucket:Pick<R2Bucket,'get'|'put'>,key:string,value:unknown,metadata:Record<string,string>) {
  try {
    const text=canonical(value),written=await bucket.put(key,text,
      {onlyIf:{etagDoesNotMatch:'*'},customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('byte_audit_proof_conflict');
    const stored=await readText(bucket,key,768*1024);
    if(stored.value.version!==written.version||stored.value.etag!==written.etag||stored.text!==text||
      Object.entries(metadata).some(([key,value])=>stored.value.customMetadata?.[key]!==value))
      throw Error('byte_audit_proof_unacknowledged');
    return{version:written.version,etag:written.etag};
  }catch{throw Error('byte_audit_proof_unacknowledged');}
}
/** Private, zero-retry byte readback. Durable outcomes still require independent
 * matching D1 bookends; this component has no database or publication capability.
 */
export async function runRestorationByteAuditWorkflow(env:RestorationByteAuditCapabilities,
  event:Pick<WorkflowEvent<RestorationByteAuditParams>,'instanceId'|'payload'>,step:Pick<WorkflowStep,'do'>) {
  if(env.operationEnabled!=='true'||new TextEncoder().encode(env.operationJson).length>5000||
    env.originals===env.target||env.originals===env.proofs||env.target===env.proofs)
    throw Error('byte_audit_disabled_or_binding_alias');
  const header=z.object({version:z.literal(1),auditRootId:uuid,rootPlanSha256:hash}).strict().parse(JSON.parse(env.operationJson));
  const payload=event.payload;
  if(!payload||typeof payload!=='object')throw Error('byte_audit_payload');
  if('rootPlanJson' in payload) {
    if(Object.keys(payload).join('|')!=='rootPlanJson'||event.instanceId!==header.auditRootId)
      throw Error('byte_audit_root_instance');
    const root=parseRestorationByteAuditRoot(payload.rootPlanJson);
    if(root.auditRootId!==header.auditRootId||await fingerprint(root)!==header.rootPlanSha256)
      throw Error('byte_audit_root_identity');
    const prefix=`source-byte-audit/${root.auditRootId}/`,metadata={auditrootid:root.auditRootId,rootsha256:header.rootPlanSha256};
    return step.do('source-byte-audit-root-upload-once',
      {retries:{limit:0,delay:'1 second' as const,backoff:'constant' as const},timeout:'10 minutes' as const},async()=>{
        if(await env.proofs.get(prefix+'root-bootstrap-claim.json'))throw Error('byte_audit_root_already_claimed');
        await putProof(env.proofs,prefix+'root-bootstrap-claim.json',header,metadata);
        const now=Date.now();
        if(now<Date.parse(root.notBefore)||now>=Math.min(Date.parse(root.notBefore)+30*60000,Date.parse(root.completionDeadline)))
          throw Error('byte_audit_root_upload_window');
        const stored=await putProof(env.proofs,prefix+'root.json',root,metadata),result={state:'source_byte_audit_root_ready' as const,
          auditRootId:root.auditRootId,rootPlanSha256:header.rootPlanSha256,rootVersion:stored.version,rootEtag:stored.etag,
          noDatabaseSourceMutationOrIntegrityCredit:true as const};
        await putProof(env.proofs,prefix+'root-bootstrap-outcome.json',result,metadata);
        return result;
      });
  }
  if(!('operationPlanJson' in payload)||Object.keys(payload).join('|')!=='operationPlanJson')throw Error('byte_audit_payload');
  const plan=parseRestorationByteAuditPlan(payload.operationPlanJson),planSha256=await fingerprint(plan);
  if(event.instanceId!==plan.operationId||plan.operationId===header.auditRootId||plan.auditRootId!==header.auditRootId)
    throw Error('byte_audit_instance');
  const rootKey=`source-byte-audit/${header.auditRootId}/root.json`,rootStored=await readText(env.proofs,rootKey,131072);
  if(await sha256Bytes(rootStored.bytes)!==header.rootPlanSha256||rootStored.value.customMetadata?.auditrootid!==header.auditRootId||
    rootStored.value.customMetadata.rootsha256!==header.rootPlanSha256)throw Error('byte_audit_root_identity');
  const root=parseRestorationByteAuditRoot(rootStored.text);
  if(root.auditRootId!==header.auditRootId||
    Date.parse(plan.notBefore)<Date.parse(root.notBefore)||Date.parse(plan.completionDeadline)>Date.parse(root.completionDeadline))
    throw Error('byte_audit_root_bounds');
  const rootMetadata={auditrootid:root.auditRootId,rootsha256:header.rootPlanSha256},rootPrefix=`source-byte-audit/${root.auditRootId}/`,
    rootClaim=await readText(env.proofs,rootPrefix+'root-bootstrap-claim.json',32768),
    rootOutcome=await readText(env.proofs,rootPrefix+'root-bootstrap-outcome.json',32768);
  const ready=z.object({state:z.literal('source_byte_audit_root_ready'),auditRootId:uuid,rootPlanSha256:hash,
    rootVersion:z.string().min(1).max(1024),rootEtag:z.string().min(1).max(1024),
    noDatabaseSourceMutationOrIntegrityCredit:z.literal(true)}).strict().parse(JSON.parse(rootOutcome.text));
  if(rootClaim.text!==canonical(header)||[rootClaim,rootOutcome].some(stored=>Object.entries(rootMetadata)
    .some(([key,value])=>stored.value.customMetadata?.[key]!==value))||ready.auditRootId!==root.auditRootId||
    ready.rootPlanSha256!==header.rootPlanSha256||ready.rootVersion!==rootStored.value.version||ready.rootEtag!==rootStored.value.etag)
    throw Error('byte_audit_root_upload_receipt');
  const partition=root.partitions[plan.partitionIndex]!,prefix=`source-byte-audit/${header.auditRootId}/${plan.operationId}/`,
    metadata={auditrootid:header.auditRootId,operationid:plan.operationId,plansha256:planSha256};
  async function put(name:string,value:unknown) {
    await putProof(env.proofs,prefix+name,value,metadata);
  }
  async function checkMembership() {
    const stored=await readText(env.proofs,`plans/${partition.partitionPlanSha256}.json`,768*1024);
    if(await sha256Bytes(stored.bytes)!==partition.partitionPlanSha256)throw Error('byte_audit_partition_bytes');
    const descriptor=z.object({version:z.literal(1),ownerId:uuid,resourceId:uuid,
      partitionIndex:z.number().int().min(0).max(31),resourcePlanSha256:hash,sourceManifestSha256:hash,catalogSha256:hash,
      jobs:z.array(z.object({documentId:z.string(),originalKey:z.string(),originalVersion:z.string(),
        sourceSha256:hash,sourceBytes:z.number().int().positive()}).passthrough()).min(1).max(1000)}).strict().parse(JSON.parse(stored.text));
    const source=partition.source;
    if(descriptor.ownerId!==source.ownerId||descriptor.resourceId!==source.resourceId||descriptor.partitionIndex!==source.partitionIndex||
      descriptor.resourcePlanSha256!==source.resourcePlanSha256||descriptor.sourceManifestSha256!==source.sourceManifestSha256||
      descriptor.catalogSha256!==source.catalogSha256||new Set(descriptor.jobs.map(row=>row.documentId)).size!==descriptor.jobs.length)
      throw Error('byte_audit_partition_identity');
    for(const row of plan.records) {
      const sealed=descriptor.jobs.find(value=>value.documentId===row.documentId);
      if(!sealed||sealed.originalKey!==row.originalKey||sealed.originalVersion!==row.originalVersion||
        sealed.sourceSha256!==row.sourceSha256||sealed.sourceBytes!==row.sourceBytes)throw Error('byte_audit_partition_membership');
    }
  }
  const options={retries:{limit:0,delay:'1 second' as const,backoff:'constant' as const},timeout:'10 minutes' as const};
  async function once<T>(name:string,run:()=>Promise<T>) {
    if(await env.proofs.get(prefix+name+'-claim.json'))throw Error('byte_audit_already_claimed');
    await put(name+'-claim.json',{version:1,auditRootId:root.auditRootId,operationId:plan.operationId,planSha256,step:name});
    try {
      const now=Date.now();
      if(now<Date.parse(root.notBefore)||now>=Date.parse(root.completionDeadline)||
        (name==='initialize'?(now<Date.parse(plan.notBefore)||now>=Date.parse(plan.notAfter)):now>=Date.parse(plan.completionDeadline)))
        throw Error('byte_audit_window');
      if(name==='initialize'){await checkMembership();await put('plan.json',plan);}
      else {
        const stored=await readText(env.proofs,prefix+'plan.json',768*1024);
        if(stored.text!==canonical(plan)||Object.entries(metadata).some(([key,value])=>stored.value.customMetadata?.[key]!==value))
          throw Error('byte_audit_stored_plan');
      }
      const result=await run();
      await put(name+'-outcome.json',{version:1,auditRootId:root.auditRootId,operationId:plan.operationId,planSha256,
        databaseBookendSha256:plan.databaseBookendSha256,step:name,result,at:new Date().toISOString(),
        databaseBookendsVerified:false,fullIntegrityOrPublicationApproval:false});
      return result;
    }catch{
      // No raw engine/R2 messages: they may include a private key or case identity.
      try{await put(name+'-failure.json',{version:1,operationId:plan.operationId,planSha256,step:name,
        state:'held_or_unknown',noRetry:true,at:new Date().toISOString()});}catch{/* Preserve the immutable claim. */}
      throw Error('byte_audit_held');
    }
  }
  await step.do('source-byte-audit-initialize-once',options,()=>once('initialize',async()=>({records:plan.records.length})));
  for(let offset=0;offset<plan.records.length;offset+=8) {
    const name='bytes-'+String(offset/8).padStart(4,'0');
    await step.do('source-byte-audit-'+name,options,()=>once(name,async()=>{
      const results=[];
      for(const record of plan.records.slice(offset,offset+8))results.push(await auditRestorationSourceBytes({source:partition.source,record,
        originals:env.originals,target:env.target,proofs:env.proofs}));
      return{records:results.length,results};
    }));
  }
  const result={state:'source_byte_audit_complete' as const,auditRootId:root.auditRootId,operationId:plan.operationId,
    partitionIndex:plan.partitionIndex,records:plan.records.length,recordsSha256:await fingerprint(plan.records),
    databaseBookendSha256:plan.databaseBookendSha256,databaseBookendsVerified:false as const,
    fullIntegrityOrPublicationApproval:false as const};
  await step.do('source-byte-audit-complete-once',options,()=>once('complete',async()=>result));
  return result;
}
