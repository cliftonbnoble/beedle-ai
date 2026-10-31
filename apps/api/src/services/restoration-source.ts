import {sha256Bytes} from '../lib/digests';

export interface RestorationSourceDescriptor {
  ownerId:string;
  planSha256:string;
  partitionIndex:number;
  originalKey:string;
  sourceSha256:string;
  sourceBytes:number;
  originalVersion?:string;
}
export class RestorationSourceError extends Error {
  constructor(readonly code:string,readonly outcomeUnknown=false,readonly causeSha256?:string,
    readonly causeKind?:'permission_denied'|'transport_timeout'|'transport_temporary'|'unclassified',readonly causeCode?:number){super(code);this.name='RestorationSourceError';}
}
export interface RestorationSourceProof {
  ownerId:string;planSha256:string;sourceSha256:string;sourceBytes:number;
  originalVersion:string;targetKey:string;targetVersion:string;targetEtag:string;
  disposition:'created_verified'|'existing_verified'|'observed_verified';
}
function validate(plan:RestorationSourceDescriptor){
  if(!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(plan.ownerId)||
    ![plan.planSha256,plan.sourceSha256].every(value=>/^[a-f0-9]{64}$/.test(value))||
    !Number.isSafeInteger(plan.partitionIndex)||plan.partitionIndex<0||plan.partitionIndex>31||
    !Number.isSafeInteger(plan.sourceBytes)||plan.sourceBytes<1||plan.sourceBytes>1_000_000||
    typeof plan.originalKey!=='string'||!plan.originalKey.length||new TextEncoder().encode(plan.originalKey).length>1024||/[\u0000-\u001f\u007f]/.test(plan.originalKey)||
    new TextDecoder().decode(new TextEncoder().encode(plan.originalKey))!==plan.originalKey||
    plan.originalVersion!==undefined&&(typeof plan.originalVersion!=='string'||!plan.originalVersion.length||plan.originalVersion.length>1024))
    throw new RestorationSourceError('invalid_source_descriptor');
}
async function sourceOperation<T>(operation:()=>Promise<T>,code:string,outcomeUnknown=false):Promise<T>{
  try{return await operation();}
  catch(error){
    const message=error instanceof Error?error.message:String(error),name=error instanceof Error?error.name:'';
    // Only documented provider codes become structured facts. Raw messages can
    // contain private keys. This classification does not retry any operation.
    // https://developers.cloudflare.com/r2/api/error-codes/
    const match=message.match(/\((\d{5,6})\)\s*$/),candidate=match?Number(match[1]):undefined;
    const known=[10001,10002,10003,10005,10006,10007,10008,10009,10011,10012,10013,10014,10018,10020,10024,10025,10031,10033,10035,10037,10039,10042,10043,10048,10054,10058,10069,10073,100100];
    const providerCode=candidate!==undefined&&known.includes(candidate)?candidate:undefined;
    const kind=providerCode!==undefined&&[10002,10003,10018,10035,10042].includes(providerCode)||/\b(?:401|403|AccessDenied|permission denied|unauthorized|forbidden)\b/i.test(message)?'permission_denied':
      name==='TimeoutError'||/\b(?:timed out|timeout)\b/i.test(message)?'transport_timeout':providerCode!==undefined&&[10001,10043,10054,10058].includes(providerCode)?'transport_temporary':'unclassified';
    throw new RestorationSourceError(code,outcomeUnknown,await sha256Bytes(new TextEncoder().encode(message)),kind,providerCode);
  }
}
export function restorationSourceObjectKey(plan:RestorationSourceDescriptor){
  validate(plan);
  return `restored/${plan.ownerId}/${String(plan.partitionIndex).padStart(2,'0')}/${plan.sourceSha256}.md`;
}
async function originalBytes(source:Pick<R2Bucket,'get'>,plan:RestorationSourceDescriptor){
  const object=await sourceOperation(()=>source.get(plan.originalKey),'original_access_failed');
  if(!object||object.size!==plan.sourceBytes||typeof object.version!=='string'||!object.version||
    plan.originalVersion!==undefined&&object.version!==plan.originalVersion)throw new RestorationSourceError('original_identity_changed');
  const bytes=new Uint8Array(await sourceOperation(()=>object.arrayBuffer(),'original_read_failed'));
  if(bytes.byteLength!==plan.sourceBytes||await sha256Bytes(bytes)!==plan.sourceSha256)throw new RestorationSourceError('original_bytes_changed');
  return{bytes,version:object.version};
}
async function targetProof(target:Pick<R2Bucket,'get'>,plan:RestorationSourceDescriptor,originalVersion:string,disposition:RestorationSourceProof['disposition'],written?:R2Object){
  const key=restorationSourceObjectKey(plan),object=await sourceOperation(()=>target.get(key),'target_access_failed');
  if(!object)return null;
  if(object.size!==plan.sourceBytes||!object.version||!object.etag||object.customMetadata?.ownerid!==plan.ownerId||
    object.customMetadata.plansha256!==plan.planSha256||object.customMetadata.sourcesha256!==plan.sourceSha256||
    written&&(written.version!==object.version||written.etag!==object.etag))throw new RestorationSourceError('target_owner_or_version_changed');
  const bytes=new Uint8Array(await sourceOperation(()=>object.arrayBuffer(),'target_read_failed'));
  if(bytes.byteLength!==plan.sourceBytes||await sha256Bytes(bytes)!==plan.sourceSha256)throw new RestorationSourceError('target_bytes_changed');
  return{ownerId:plan.ownerId,planSha256:plan.planSha256,sourceSha256:plan.sourceSha256,sourceBytes:plan.sourceBytes,
    originalVersion,targetKey:key,targetVersion:object.version,targetEtag:object.etag,disposition};
}
// The caller owns the immutable D1 job claim before calling this function.
// The original binding exposes only get; all writes use the distinct target.
// Originals are copied byte-for-byte, including held/unreadable text. No parser,
// whitespace repair, human approval or legal provenance is supplied here.
export async function copyVerifiedRestorationSource(source:Pick<R2Bucket,'get'>,target:Pick<R2Bucket,'get'|'put'>,plan:RestorationSourceDescriptor):Promise<RestorationSourceProof>{
  validate(plan);if(source===target)throw new RestorationSourceError('source_target_alias');
  const original=await originalBytes(source,plan);
  const existing=await targetProof(target,plan,original.version,'existing_verified');
  if(existing)return existing;
  const written=await sourceOperation(()=>target.put(restorationSourceObjectKey(plan),original.bytes,{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:'text/markdown'},
    customMetadata:{ownerid:plan.ownerId,plansha256:plan.planSha256,sourcesha256:plan.sourceSha256}}),'source_copy_write_unacknowledged',true);
  if(!written)throw new RestorationSourceError('source_copy_conditional_conflict');
  const result=await targetProof(target,plan,original.version,'created_verified',written);
  if(!result)throw new RestorationSourceError('source_copy_readback_missing',true);
  return result;
}
// Unknown writes are observed through this read-only capability. This does not
// reset a job, waive an access error, or authorize reprocessing automatically.
export async function observeVerifiedRestorationSource(source:Pick<R2Bucket,'get'>,target:Pick<R2Bucket,'get'>,plan:RestorationSourceDescriptor):Promise<RestorationSourceProof>{
  validate(plan);if(source===target)throw new RestorationSourceError('source_target_alias');
  const original=await originalBytes(source,plan);
  const result=await targetProof(target,plan,original.version,'observed_verified');
  if(!result)throw new RestorationSourceError('source_copy_outcome_unconfirmed',true);
  return result;
}
