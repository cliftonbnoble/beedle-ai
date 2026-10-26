import { z } from "zod";
import type { Env } from "../lib/types";
import { RequestConflictError } from "../lib/http";
import { withinProviderDeadline } from "../lib/provider-operation";
import { reserveComputeBudget } from "./compute-budget";
import type { VectorProfile } from "./vector-profile";
const targetsSchema=z.array(z.object({id:z.string().max(64),live:z.boolean()})).max(16);
type Lock={profileId:string;documentId:string;token:string;profile:VectorProfile};
const now=()=>new Date().toISOString();
const owned="profile_id=? AND document_id=? AND owner_token=? AND lease_expires_at>?";
export async function acquireVectorSync(env:Env,profile:VectorProfile,documentId:string,recoverUncertain=false):Promise<Lock|null> {
  await env.DB.prepare(`INSERT INTO vector_sync_locks(profile_id,document_id) VALUES(?,?) ON CONFLICT DO NOTHING`).bind(profile.id,documentId).run();
  // A crash after persisting a mutation intent may have left an unacknowledged
  // external write. It cannot safely race a new metadata promotion/retirement.
  await env.DB.prepare(`UPDATE vector_sync_locks SET uncertain=CASE WHEN pending_json IS NOT NULL THEN 1 ELSE uncertain END,owner_token=NULL,lease_expires_at=NULL
    WHERE profile_id=? AND document_id=? AND lease_expires_at<=?`).bind(profile.id,documentId,now()).run();
  if(recoverUncertain) await env.DB.prepare(`UPDATE vector_sync_locks SET uncertain=0,pending_json=NULL WHERE profile_id=? AND document_id=? AND owner_token IS NULL`).bind(profile.id,documentId).run();
  const token=crypto.randomUUID();
  const result=await env.DB.prepare(`UPDATE vector_sync_locks SET owner_token=?,lease_expires_at=? WHERE profile_id=? AND document_id=? AND owner_token IS NULL AND uncertain=0`)
    .bind(token,new Date(Date.now()+120_000).toISOString(),profile.id,documentId).run();
  return Number(result.meta.changes)===1?{profileId:profile.id,documentId,token,profile}:null;
}
export async function releaseVectorSync(env:Env,lock:Lock) {
  await env.DB.prepare("UPDATE vector_sync_locks SET owner_token=NULL,lease_expires_at=NULL WHERE profile_id=? AND document_id=? AND owner_token=?").bind(lock.profileId,lock.documentId,lock.token).run();
}
export async function observeVectorMetadata(env:Env,lock:Lock) {
  const row=await env.DB.prepare(`SELECT pending_json FROM vector_sync_locks WHERE ${owned}`).bind(lock.profileId,lock.documentId,lock.token,now()).first<{pending_json:string|null}>();
  if(!row) throw new RequestConflictError("Vector synchronization lease changed");
  if(!row.pending_json) return true;
  const targets=targetsSchema.parse(JSON.parse(row.pending_json));
  const visible=await withinProviderDeadline({},15_000,()=>env.VECTOR_INDEX!.getByIds(targets.map(target=>target.id)));
  if(!targets.every(target=>visible.some(vector=>vector.id===target.id&&vector.namespace===lock.profile.namespace&&vector.metadata?.live===target.live&&vector.metadata?.entryDigest===target.id))) return false;
  // Record observed provider state even if the source changed during the wait.
  // A following reconciliation corrects it to the latest source revision.
  await env.DB.batch([
    ...targets.map(target=>env.DB.prepare(`UPDATE vector_entries SET metadata_live=?,obsolete_at=CASE WHEN ?=0 THEN COALESCE(obsolete_at,?) ELSE NULL END WHERE vector_id=?
      AND EXISTS(SELECT 1 FROM vector_sync_locks WHERE ${owned})`).bind(Number(target.live),Number(target.live),now(),target.id,lock.profileId,lock.documentId,lock.token,now())),
    env.DB.prepare(`UPDATE vector_sync_locks SET pending_json=NULL WHERE ${owned}`).bind(lock.profileId,lock.documentId,lock.token,now())
  ]);
  return true;
}
export async function submitVectorMetadata(env:Env,lock:Lock,vectors:VectorizeVector[]) {
  await reserveComputeBudget(env,'vector_write',vectors.length);
  const targets=vectors.map(vector=>({id:vector.id,live:vector.metadata?.live===true}));
  const result=await env.DB.prepare(`UPDATE vector_sync_locks SET pending_json=?,uncertain=1 WHERE ${owned}`)
    .bind(JSON.stringify(targets),lock.profileId,lock.documentId,lock.token,now()).run();
  if(Number(result.meta.changes)!==1) throw new RequestConflictError("Vector synchronization lease changed before submission");
  await withinProviderDeadline({},15_000,()=>env.VECTOR_INDEX!.upsert(vectors));
  await env.DB.prepare(`UPDATE vector_sync_locks SET uncertain=0 WHERE ${owned}`).bind(lock.profileId,lock.documentId,lock.token,now()).run();
  return observeVectorMetadata(env,lock);
}
