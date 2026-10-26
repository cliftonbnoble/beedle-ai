import { z } from 'zod';
import type { Env } from '../lib/types';
import { json, readJson, toErrorResponse } from '../lib/http';
import { searchIndexStatus, advanceSearchIndex } from '../services/search-index-build';
export async function handleSearchIndex(request:Request,env:Env,step:boolean) {
  try{if(step)z.object({}).strict().parse(await readJson(request));return json(await(step?advanceSearchIndex(env):searchIndexStatus(env)));}catch(error){return toErrorResponse(error);}
}
