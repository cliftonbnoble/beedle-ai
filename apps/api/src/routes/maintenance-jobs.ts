import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse, RequestValidationError } from "../lib/http";
import { inspectMaintenanceJob, stepMaintenanceJob, controlMaintenanceJob } from "../services/maintenance-jobs";
import { z } from "zod";
export async function handleMaintenanceJob(request:Request,env:Env,action:'inspect'|'step'|'cancel'|'retry') {
  try {
    const encoded=(request as Request & {params?:{jobId?:string}}).params?.jobId;
    let id:string|undefined;
    try {id=encoded?decodeURIComponent(encoded):undefined;} catch {throw new RequestValidationError("Invalid job ID encoding");}
    if(!id) throw new RequestValidationError("Job ID required");
    if(action!=='inspect') z.object({}).strict().parse(await readJson(request));
    const report=action==='inspect'?await inspectMaintenanceJob(env,id):action==='step'?await stepMaintenanceJob(env,id):await controlMaintenanceJob(env,id,action);
    return json(report);
  }catch(error) {return toErrorResponse(error);}
}
