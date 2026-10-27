import { z } from 'zod';
import type { Env } from '../lib/types';
import { json,readJson,toErrorResponse } from '../lib/http';
import { inspectIndexRecovery,startIndexRecovery,advanceIndexRecovery,indexCoveragePage } from '../services/search-index-recovery';
export async function handleIndexRecovery(request:Request,env:Env,action:'inspect'|'start'|'step'|'coverage'){
 try{
  if(action==='inspect')return json(await inspectIndexRecovery(env));
  if(action==='coverage'){const url=new URL(request.url);const input=z.object({kind:z.enum(['metadata','document','retrieval']).default('document'),after:z.string().max(256).default('')}).strict().parse(Object.fromEntries(url.searchParams));return json(await indexCoveragePage(env,input.kind,input.after));}
  if(action==='step'){const input=z.object({generation:z.string().uuid()}).strict().parse(await readJson(request));return json(await advanceIndexRecovery(env,input.generation));}
  const input=z.object({dryRun:z.boolean().default(true),generation:z.string().uuid().optional(),expectedGeneration:z.string().min(1).max(64).optional(),replaceArchive:z.boolean().default(false),restart:z.boolean().default(false)}).strict().parse(await readJson(request));
  return json(await startIndexRecovery(env,input));
 }catch(error){return toErrorResponse(error);}
}
