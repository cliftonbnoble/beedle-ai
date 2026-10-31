import {canonical, fingerprint} from '../../scripts/lib/release-contract.mjs';
import {sha256Bytes} from '../lib/digests';
import {RestorationControlError, type RestorationIdentity} from './restoration-job-control';

const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const digest=/^[a-f0-9]{64}$/;
const knownNames=new Set(['Error','TypeError','RangeError','SyntaxError','RestorationControlError','RequestValidationError','RequestConflictError','ZodError']);
const markers=[
  ['time_limit',/time limit|timed? out|timeout/i],
  ['constraint',/SQLITE_CONSTRAINT/],
  ['corruption_reported',/SQLITE_CORRUPT|database disk image is malformed/i],
  ['permission',/SQLITE_AUTH|not authorized|permission denied/i],
  ['overloaded',/overloaded|too many requests queued/i],
  ['binding_type',/D1_TYPE_ERROR/]
] as const;

// Hash messages, never store them: provider errors can contain SQL, bound case
// values or object keys. Markers report observed text, not an inferred cause.
export async function restorationErrorEvidence(error:unknown){
  const chain:{name:string;messageSha256:string;messageTruncated:boolean;codes:string[];markers:string[]}[]=[];
  const seen=new Set<unknown>();let current=error;
  for(let depth=0;depth<3&&current!==undefined&&current!==null&&!seen.has(current);depth++){
    seen.add(current);
    const object=typeof current==='object'?current as {name?:unknown;message?:unknown;cause?:unknown}:null;
    const message=typeof object?.message==='string'?object.message:String(current);
    const bounded=message.slice(0,65536);
    chain.push({name:typeof object?.name==='string'&&knownNames.has(object.name)?object.name:'other',
      messageSha256:await fingerprint(bounded),messageTruncated:bounded.length!==message.length,
      codes:['D1_ERROR','D1_EXEC_ERROR','D1_TYPE_ERROR','D1_COLUMN_NOTFOUND','SQLITE_ERROR','SQLITE_CONSTRAINT','SQLITE_CORRUPT','SQLITE_AUTH','SQLITE_TOOBIG','SQLITE_BUSY','SQLITE_LOCKED']
        .filter(code=>new RegExp('\\b'+code+'\\b').test(bounded)),
      markers:markers.filter(([,pattern])=>pattern.test(bounded)).map(([name])=>name)});
    current=object?.cause;
  }
  return chain;
}

export async function recordRestorationFailure(input:{
  proofs:Pick<R2Bucket,'put'|'get'>;identity:RestorationIdentity;attemptId:string;
  stage:'preflight'|'claim'|'source';bindingCalls:number;error:unknown;
  bindingFailure?:{method:'prepare'|'bind'|'first'|'all'|'run'|'raw'|'batch';statements:number;sqlSha256:string[]};
}){
  const {identity,attemptId}=input;
  if(!uuid.test(attemptId)||!uuid.test(identity.ownerId)||!uuid.test(identity.resourceId)||
    ![identity.planSha256,identity.resourcePlanSha256,identity.sourceManifestSha256,identity.catalogSha256].every(x=>digest.test(x))||
    !Number.isSafeInteger(identity.partitionIndex)||identity.partitionIndex<0||identity.partitionIndex>31||
    !Number.isSafeInteger(input.bindingCalls)||input.bindingCalls<0||input.bindingCalls>900||!['preflight','claim','source'].includes(input.stage))
    throw new RestorationControlError('failure_proof_identity');
  if(input.bindingFailure&&(!['prepare','bind','first','all','run','raw','batch'].includes(input.bindingFailure.method)||
    !Number.isSafeInteger(input.bindingFailure.statements)||input.bindingFailure.statements<1||input.bindingFailure.statements>10000||
    input.bindingFailure.sqlSha256.length<1||input.bindingFailure.sqlSha256.length>100||input.bindingFailure.sqlSha256.some(x=>!digest.test(x))))
    throw new RestorationControlError('failure_proof_binding_evidence');
  const proof={version:1,identity:{ownerId:identity.ownerId,resourceId:identity.resourceId,partitionIndex:identity.partitionIndex,
    planSha256:identity.planSha256,resourcePlanSha256:identity.resourcePlanSha256,sourceManifestSha256:identity.sourceManifestSha256,catalogSha256:identity.catalogSha256},
    attemptId,stage:input.stage,bindingCalls:input.bindingCalls,
    bindingFailure:input.bindingFailure?{method:input.bindingFailure.method,statements:input.bindingFailure.statements,sqlSha256:[...input.bindingFailure.sqlSha256]}:null,errorChain:await restorationErrorEvidence(input.error),
    engineSuccessCredited:false,holdPreserved:true,at:new Date().toISOString()};
  const text=canonical(proof),bytes=new TextEncoder().encode(text),sha256=await sha256Bytes(bytes);
  const key=`failures/${identity.ownerId}/${identity.planSha256}/${identity.partitionIndex}/${attemptId}.json`;
  const result=await input.proofs.put(key,bytes,{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:'application/json'},
    customMetadata:{ownerid:identity.ownerId,plansha256:identity.planSha256,proofsha256:sha256}});
  if(!result)throw new RestorationControlError('failure_proof_conditional_conflict');
  const read=await input.proofs.get(key);
  if(!read||!read.version||!read.etag||read.size!==bytes.length||read.version!==result.version||read.etag!==result.etag||
    read.customMetadata?.ownerid!==identity.ownerId||read.customMetadata.plansha256!==identity.planSha256||read.customMetadata.proofsha256!==sha256||
    await sha256Bytes(new Uint8Array(await read.arrayBuffer()))!==sha256)
    throw new RestorationControlError('failure_proof_readback_unconfirmed');
  return {key,sha256,version:read.version};
}
