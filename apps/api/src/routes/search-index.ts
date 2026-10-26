import { phraseIndexStatus, advancePhraseIndex } from "../services/search-phrase-build";
import { z } from 'zod';
import type { Env } from '../lib/types';
import { json, readJson, toErrorResponse } from '../lib/http';
import { searchIndexStatus, advanceSearchIndex } from '../services/search-index-build';
export async function handleSearchIndex(request:Request,env:Env,step:boolean) {
  try{
    if(step)z.object({}).strict().parse(await readJson(request));
    const state=await searchIndexStatus(env);
    if(step){if(state.phase==='ready')await advancePhraseIndex(env);else await advanceSearchIndex(env);}
    return json({...await searchIndexStatus(env),phrase:await phraseIndexStatus(env)});
  }catch(error){return toErrorResponse(error);}
}
