import {fingerprint} from './release-contract.mjs';
const digest=/^[a-f0-9]{64}$/,uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
function declaration(plan,index){
 if(plan?.version!==1||plan.kind!=='owned-empty-current-index-initialization'||!uuid.test(plan.ownerId)||!digest.test(plan.resourcePlanSha256)||!digest.test(plan.testedBundleSha256)||!Array.isArray(plan.workers)||plan.workers.length!==32||!Number.isInteger(index)||index<0||index>31)throw Error('Complete owned initializer plan required');
 const row=plan.workers[index];
 if(row.index!==index||!uuid.test(row.resourceId)||row.workerName!==`beedle-empty-index-${plan.ownerId.slice(0,8)}-p${String(index).padStart(2,'0')}`||new Set(plan.workers.map(x=>x.resourceId)).size!==32||new Set(plan.workers.map(x=>x.workerName)).size!==32)throw Error('Initializer target identity differs');
 return row;
}
function fresh(plan,guard,now){const age=now-Date.parse(guard?.observedAt);
 if(!guard||guard.ownerId!==plan.ownerId||guard.passed!==true||guard.sourceFrozen!==true||guard.candidateUnchanged!==true||guard.originalAnchorMatched!==true||guard.productionPaused!==true||guard.onlyExactOwnedInitializersBound!==true||guard.localReserveBytes<20*1024**3||!Number.isFinite(age)||age<0||age>300000)throw Error('Fresh original-source and exact-owner inventory guards required');
}
export async function inspectOwnedIndexInitializerWorker({plan,index,cloud,versionId,scheduled}){
 const row=declaration(plan,index),actual=await cloud.readWorker(row.workerName);
 if(!actual||actual.workerName!==row.workerName||!uuid.test(actual.versionId)||versionId&&actual.versionId!==versionId||actual.bundleSha256!==plan.testedBundleSha256||actual.public!==false||actual.preview!==false||actual.otherAccess!==false||actual.compatibilityDate!=='2026-10-27'||actual.cpuMs!==30000)throw Error('Initializer deployed owner/source/settings differ');
 const bindings=actual.bindings;
 if(!Array.isArray(bindings)||bindings.length!==3||new Set(bindings.map(x=>x.name)).size!==3||!bindings.some(x=>x.name==='DB'&&x.type==='d1'&&x.id===row.resourceId)||!bindings.some(x=>x.name==='RESTORE_ENABLED'&&x.type==='plain_text'&&x.text==='true'))throw Error('Initializer deployed bindings differ');
 const spec=bindings.find(x=>x.name==='RESTORE_SPEC_JSON');
 const expected={ownerId:plan.ownerId,resourcePlanSha256:plan.resourcePlanSha256,resourceId:row.resourceId,partitionIndex:index,generation:plan.generation,planSha256:await fingerprint(plan),catalogSha256:plan.catalogSha256,catalogRows:plan.catalogRows,allowedSchemaSha256:plan.allowedSchemaSha256,finalSchemaSha256:plan.finalSchemaSha256};
 if(spec?.type!=='plain_text'||spec.text!==JSON.stringify(expected)||JSON.stringify(actual.crons)!==JSON.stringify(scheduled?[plan.enabledCron]:[]))throw Error('Initializer plan/schedule differs');
 return actual;
}
export async function deployOwnedIndexInitializerWorker({plan,index,store,cloud,guard,deploy,now=Date.now()}){
 const row=declaration(plan,index);fresh(plan,guard,now);const id=`database-${String(index).padStart(2,'0')}-initializer-deploy`;
 if(await store.exists(id+'-start'))throw Error('Existing deployment intent requires observation, never replay');
 if(await cloud.readWorker(row.workerName)!==null)throw Error('Worker name already exists; no overwrite authorized');
 const intent={index,ownerId:plan.ownerId,workerName:row.workerName,planSha256:await fingerprint(plan),startedAt:new Date(now).toISOString()};await store.reserve(id+'-start',intent);
 try{const result=await deploy();if(result.acknowledged!==true)throw Error('No deployment acknowledgment');}catch(error){await store.reserve(id+'-unacknowledged',{...intent,errorSha256:await fingerprint(String(error?.message||error)),noRetryAuthorized:true});throw Error('Deployment outcome unknown; inspect exact owner without replay');}
 const actual=await inspectOwnedIndexInitializerWorker({plan,index,cloud,scheduled:false});const outcome={...intent,phase:'private_initializer_deployed_verified',versionId:actual.versionId,bundleSha256:actual.bundleSha256};await store.reserve(id+'-verified',outcome);return outcome;
}
export async function observeOwnedIndexInitializerWorker({plan,index,store,cloud,scheduled=false}){
 const id=`database-${String(index).padStart(2,'0')}-initializer-deploy`,intent=await store.read(id+'-start');
 if(intent.ownerId!==plan.ownerId||intent.index!==index||intent.planSha256!==await fingerprint(plan))throw Error('Deployment observation intent differs');
 const actual=await inspectOwnedIndexInitializerWorker({plan,index,cloud,scheduled});return{...intent,phase:'private_initializer_deployment_reconciled',versionId:actual.versionId,deployReissued:false};
}
export async function setOwnedIndexInitializerSchedule({plan,index,store,cloud,guard,versionId,enable,now=Date.now()}){
 declaration(plan,index);fresh(plan,guard,now);const id=`database-${String(index).padStart(2,'0')}-initializer-${enable?'enable':'retire'}`;
 if(await store.exists(id+'-start'))throw Error('Existing schedule intent requires observation, never replay');
 await inspectOwnedIndexInitializerWorker({plan,index,cloud,versionId,scheduled:!enable});
 if(!enable&&!await cloud.completedEmptyInitialization(plan.workers[index].resourceId,await fingerprint(plan)))throw Error('Actual completed/quiescent empty initialization required');
 const intent={index,ownerId:plan.ownerId,versionId,planSha256:await fingerprint(plan),startedAt:new Date(now).toISOString()};await store.reserve(id+'-start',intent);
 try{await cloud.putSchedule(plan.workers[index].workerName,enable?[plan.enabledCron]:[]);}catch(error){await store.reserve(id+'-unacknowledged',{...intent,errorSha256:await fingerprint(String(error?.message||error)),noRetryAuthorized:true});throw Error('Schedule outcome unknown; read exact remote schedule without replay');}
 await inspectOwnedIndexInitializerWorker({plan,index,cloud,versionId,scheduled:enable});const outcome={...intent,phase:enable?'private_initializer_scheduled_verified':'completed_initializer_schedule_retired'};await store.reserve(id+'-verified',outcome);return outcome;
}
