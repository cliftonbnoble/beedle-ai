import {canonical,fingerprint} from '../../scripts/lib/release-contract.mjs';
import {sha256Bytes} from '../lib/digests';

interface ObjectBody {
  size:number;version:string;etag:string;customMetadata?:Record<string,string>;
  arrayBuffer():Promise<ArrayBuffer>;
}
interface Bucket {
  get(key:string):Promise<ObjectBody|null>;
  put(key:string,value:string,options:{onlyIf:{etagDoesNotMatch:string};customMetadata:Record<string,string>;httpMetadata:{contentType:string}}):Promise<{version:string;etag:string}|null>;
}
interface Root {binding:'TARGET'|'PROOFS';body:string;sha256:string}
interface PlannedObject {key:string;body:string;sha256:string}
export interface StorageSetupPlan {
  version:1;ownerId:string;attemptId:string;storagePlanSha256:string;resourcePlanSha256:string;
  sourceManifestSha256:string;catalogSha256:string;roots:Root[];objects:PlannedObject[];
}
export interface StorageSetupAdmission {
  ownerId:string;attemptId:string;planSha256:string;notBefore:string;notAfter:string;
}
const digest=/^[a-f0-9]{64}$/,uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const keyRoot='_beedle_restore_owner.json';
export async function storageSetupIdentity(plan:StorageSetupPlan):Promise<string>{
  return fingerprint({...plan,roots:plan.roots.map(({binding,sha256})=>({binding,sha256})),objects:plan.objects.map(({key,sha256})=>({key,sha256}))});
}
async function validate(plan:StorageSetupPlan,admission:StorageSetupAdmission,now:number){
  const begin=Date.parse(admission.notBefore),end=Date.parse(admission.notAfter);
  if(plan.version!==1||!uuid.test(plan.ownerId)||!uuid.test(plan.attemptId)||
    ![plan.storagePlanSha256,plan.resourcePlanSha256,plan.sourceManifestSha256,plan.catalogSha256].every(x=>digest.test(x))||
    admission.ownerId!==plan.ownerId||admission.attemptId!==plan.attemptId||admission.planSha256!==await storageSetupIdentity(plan)||
    !Number.isFinite(begin)||!Number.isFinite(end)||end<=begin||end-begin>30*60000||now<begin||now>=end)throw Error('storage_setup_admission');
  if(plan.roots.length!==2||plan.roots[0]?.binding!=='TARGET'||plan.roots[1]?.binding!=='PROOFS'||plan.objects.length!==32||new Set(plan.objects.map(x=>x.key)).size!==32)throw Error('storage_setup_manifest');
  for(const root of plan.roots){
    if(await sha256Bytes(new TextEncoder().encode(root.body))!==root.sha256)throw Error('storage_setup_root_hash');
    const value=JSON.parse(root.body) as Record<string,unknown>,role=root.binding==='TARGET'?'originals':'proofs';
    if(canonical(value)!==root.body||value.version!==1||value.ownerId!==plan.ownerId||value.storagePlanSha256!==plan.storagePlanSha256||value.resourcePlanSha256!==plan.resourcePlanSha256||value.sourceManifestSha256!==plan.sourceManifestSha256||value.catalogSha256!==plan.catalogSha256||value.role!==role||
      value.bucketName!==(role==='originals'?'beedle-restored-':'beedle-restoration-proof-')+plan.ownerId||!Number.isFinite(Date.parse(String(value.creationDate))))throw Error('storage_setup_root_owner');
  }
  const partitions=new Set<number>();
  for(const object of plan.objects){
    if(!digest.test(object.sha256)||object.key!=='plans/'+object.sha256+'.json'||new TextEncoder().encode(object.body).length>2*1024**2||await sha256Bytes(new TextEncoder().encode(object.body))!==object.sha256)throw Error('storage_setup_object_hash');
    const value=JSON.parse(object.body) as Record<string,unknown>;
    if(value.version!==1||value.ownerId!==plan.ownerId||value.resourcePlanSha256!==plan.resourcePlanSha256||value.sourceManifestSha256!==plan.sourceManifestSha256||value.catalogSha256!==plan.catalogSha256||typeof value.partitionIndex!=='number'||!Number.isSafeInteger(value.partitionIndex)||value.partitionIndex<0||value.partitionIndex>31||partitions.has(value.partitionIndex)||!Array.isArray(value.jobs)||value.jobs.length<1||value.jobs.length>1000)throw Error('storage_setup_partition');
    partitions.add(value.partitionIndex);
  }
}
export async function runRestorationStorageSetup(plan:StorageSetupPlan,admission:StorageSetupAdmission,buckets:{TARGET:Bucket;PROOFS:Bucket},now=Date.now()){
  await validate(plan,admission,now);
  const identity=await storageSetupIdentity(plan),prefix='storage-setup/'+plan.attemptId+'/',claimKey=prefix+'claim.json';
  const claimBody=canonical({version:1,ownerId:plan.ownerId,attemptId:plan.attemptId,planSha256:identity,admission});
  const metadata={ownerid:plan.ownerId,storageplansha256:plan.storagePlanSha256};
  async function read(bucket:Bucket,key:string,body:string,written?:{version:string;etag:string}){
    const object=await bucket.get(key);
    if(!object||object.size!==new TextEncoder().encode(body).length||!object.version||!object.etag||object.customMetadata?.ownerid!==plan.ownerId||object.customMetadata.storageplansha256!==plan.storagePlanSha256||written&&(object.version!==written.version||object.etag!==written.etag)||
      new TextDecoder('utf-8',{fatal:true,ignoreBOM:false}).decode(await object.arrayBuffer())!==body)throw Error('storage_setup_readback');
    return{version:object.version,etag:object.etag,bytes:object.size,bodySha256:await sha256Bytes(new TextEncoder().encode(body))};
  }
  async function put(bucket:Bucket,key:string,body:string){
    const written=await bucket.put(key,body,{onlyIf:{etagDoesNotMatch:'*'},customMetadata:metadata,httpMetadata:{contentType:'application/json'}});
    if(!written)throw Error('storage_setup_conditional_conflict');
    return read(bucket,key,body,written);
  }
  if(await buckets.PROOFS.get(claimKey)){
    await read(buckets.PROOFS,claimKey,claimBody);
    return{state:'already_claimed' as const,objects:0};
  }
  // A lost acknowledgment anywhere below retains the single immutable claim.
  // No later delivery can reissue a root or plan write.
  for(const root of plan.roots)if(await buckets[root.binding].get(keyRoot))throw Error('storage_setup_existing_root');
  for(const object of plan.objects)if(await buckets.PROOFS.get(object.key))throw Error('storage_setup_existing_plan');
  await put(buckets.PROOFS,claimKey,claimBody);
  const completed:{index:number;kind:string;version:string;etag:string;bytes:number;bodySha256:string}[]=[];
  let stage='roots',index=0;
  try{
    for(const root of plan.roots){
      await put(buckets.PROOFS,prefix+'root-'+index+'-intent.json',canonical({ownerId:plan.ownerId,planSha256:identity,index,bodySha256:root.sha256}));
      const result=await put(buckets[root.binding],keyRoot,root.body);
      completed.push({index,kind:'root',...result});
      await put(buckets.PROOFS,prefix+'root-'+index+'-verified.json',canonical(completed.at(-1)));index++;
    }
    stage='plans';index=0;
    for(const object of plan.objects){
      await put(buckets.PROOFS,prefix+'plan-'+index+'-intent.json',canonical({ownerId:plan.ownerId,planSha256:identity,index,bodySha256:object.sha256}));
      const result=await put(buckets.PROOFS,object.key,object.body);
      completed.push({index,kind:'plan',...result});
      await put(buckets.PROOFS,prefix+'plan-'+index+'-verified.json',canonical(completed.at(-1)));index++;
    }
    stage='completion';
    await put(buckets.PROOFS,prefix+'result.json',canonical({version:1,state:'completed',ownerId:plan.ownerId,attemptId:plan.attemptId,planSha256:identity,completedAt:new Date().toISOString(),completed}));
    return{state:'completed' as const,objects:32};
  }catch(error){
    try{await put(buckets.PROOFS,prefix+'held.json',canonical({version:1,state:'held',ownerId:plan.ownerId,attemptId:plan.attemptId,planSha256:identity,stage,index,completed,errorSha256:await fingerprint(error instanceof Error?error.message:String(error)),noReplay:true}));}catch{/* Preserve unknown receipts without overwrite or retry. */}
    throw Error('storage_setup_held');
  }
}
