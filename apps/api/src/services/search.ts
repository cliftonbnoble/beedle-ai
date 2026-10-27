// Validated API entrypoints, readiness/budget setup and immutable search session pagination.
import { searchReadyEnvironment } from "./search-readiness";
import { RequestValidationError } from "../lib/http";
import { searchSessionPolicy, searchCorpusRevision, createSearchSession, readSearchSession, searchSnapshotPage } from "./search-sessions";
import { searchBudgetEnvironment } from "./search-work-budget";
import { searchDebugRequestSchema, searchDebugResponseSchema, searchRequestSchema, searchResponseSchema, type SearchRequest } from "@beedle/shared";
import type { Env } from "../lib/types";
import type { SearchContext } from "./search-types";
import { runSearchInternal } from './search-pipeline';



export async function search(env: Env, input: unknown, options:{session?:boolean}={}) {
  const parsed=searchRequestSchema.parse(input);
  const result=await searchPage(env,parsed,"search",options.session!==false||Boolean(parsed.cursor));env.TELEMETRY?.search(result.results.length,"keyword",result.corpusRevision);return result;
}

export async function searchDebug(env: Env, input: unknown, options:{session?:boolean}={}) {
  const parsed=searchDebugRequestSchema.parse(input);
  const result=await searchPage(env,parsed,"debug",options.session!==false||Boolean(parsed.cursor));env.TELEMETRY?.search(result.results.length,parsed.queryType,result.corpusRevision);return result;
}

async function searchPage(original:Env,parsed:SearchRequest&{queryType?:SearchContext["queryType"]},kind:"search"|"debug",session:boolean) {
  if(parsed.offset!==0)throw new RequestValidationError("Offset pagination has been replaced. Start a search and use nextCursor.");
  const env=await searchReadyEnvironment(searchBudgetEnvironment(original));
  const schema=kind==="debug"?searchDebugResponseSchema:searchResponseSchema;
  if(parsed.cursor){const {row,snapshot,position}=await readSearchSession(env,parsed,kind);return schema.parse(await searchSnapshotPage(env,parsed,snapshot,position,row.corpus_revision,row));}
  const revision=await searchCorpusRevision(env);
  const normalized={...parsed,offset:0,limit:searchSessionPolicy.candidatePageSize};
  const snapshot=await runSearchInternal(env,normalized,parsed.queryType||"keyword",kind==="debug");
  const row=session && snapshot.results.length>parsed.limit?await createSearchSession(env,parsed,kind,snapshot,revision):undefined;
  return schema.parse(await searchSnapshotPage(env,parsed,snapshot,0,revision,row));
}
