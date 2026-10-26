import { facetBuildStatus, advanceFacetBuild } from "../services/search-facet-build";
import { inspectSearchReadiness } from "../services/search-readiness";
import { phraseIndexStatus, advancePhraseIndex } from "../services/search-phrase-build";
import { z } from 'zod';
import type { Env } from '../lib/types';
import { json, readJson, toErrorResponse } from '../lib/http';
import { searchIndexStatus, advanceSearchIndex } from '../services/search-index-build';
export async function handleSearchIndex(request:Request,env:Env,step:boolean) {
  try{
    if(step)z.object({}).strict().parse(await readJson(request));
    const state=await searchIndexStatus(env);
    if(step){if(state.phase!=='ready')await advanceSearchIndex(env);else if((await phraseIndexStatus(env))?.phase!=='ready')await advancePhraseIndex(env);else await advanceFacetBuild(env);}
    return json({...await searchIndexStatus(env),phrase:await phraseIndexStatus(env),facets:await facetBuildStatus(env),readiness:await inspectSearchReadiness(env)});
  }catch(error){return toErrorResponse(error);}
}
