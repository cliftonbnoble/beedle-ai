import { z } from "zod";
import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse } from "../lib/http";
import { reconcileVectorGenerations, synchronizeVectorLifecycle, collectObsoleteVectors } from "../services/vector-generations";
export async function handleVectorLifecycle(request:Request,env:Env,action:'inspect'|'sync'|'cleanup'|'reconcile') {
  try {
    if(action==='inspect') return json(await reconcileVectorGenerations(env));
    if(action==='reconcile') {const input=z.object({after:z.string().max(64).default('')}).strict().parse(await readJson(request));return json(await reconcileVectorGenerations(env,{...input,probe:true}));}
    if(action==='sync') {const input=z.object({documentId:z.string().min(1).max(128).optional(),recoverUncertain:z.boolean().default(false)}).strict().parse(await readJson(request));return json(await synchronizeVectorLifecycle(env,input.documentId,input.recoverUncertain));}
    const input=z.object({dryRun:z.boolean().default(true)}).strict().parse(await readJson(request));
    return json(await collectObsoleteVectors(env,input.dryRun));
  }catch(error) {return toErrorResponse(error);}
}
