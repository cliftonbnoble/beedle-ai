import { createBackfillJob } from "../services/maintenance-jobs";
import { listLegalCatalogVersions, restoreLegalCatalog } from "../services/legal-catalog";
import type { Env } from "../lib/types";
import { json, readJson, toErrorResponse } from "../lib/http";
import { inspectLegalReferences, listRulesCitationInventory, rebuildLegalReferences, verifyCitations } from "../services/legal-references";
import {corpusPartitions,assertCorpusPartitionRevisions} from '../services/corpus-partitions';
import {legalReferenceInspectResponseSchema} from '@beedle/shared';

export async function handleInspectReferences(_request: Request, env: Env): Promise<Response> {
  try {
    const layout=corpusPartitions(env);
    if(layout)await assertCorpusPartitionRevisions(env);
    const result = await inspectLegalReferences(env);
    if(layout){
      let count=0;
      const issues:Array<{document_id:string;reference_type:string;raw_value:string;normalized_value:string;message:string;created_at:string}>=[];
      for(let at=0;at<layout.partitions.length;at+=4){
        const batch=await Promise.all(layout.partitions.slice(at,at+4).map(async part=>{
          const total=await part.database.prepare('SELECT COUNT(*) AS count FROM document_reference_issues').first<number>('count');
          const rows=await part.database.prepare(`SELECT document_id,reference_type,raw_value,normalized_value,message,created_at
            FROM document_reference_issues ORDER BY created_at DESC,document_id,reference_type,normalized_value LIMIT 60`)
            .all<typeof issues[number]>();
          if(total===null||!Number.isSafeInteger(total)||total<0)throw new Error('Reference issue count is unavailable');
          return{total:total!,rows:rows.results};
        }));
        for(const item of batch){count+=item.total;issues.push(...item.rows);}
      }
      await assertCorpusPartitionRevisions(env);
      return json(legalReferenceInspectResponseSchema.parse({...result,
        summary:{...result.summary,unmatched_reference_issue_count:count},
        unmatched_reference_issues:issues.sort((a,b)=>b.created_at.localeCompare(a.created_at)||a.document_id.localeCompare(b.document_id)||
          a.reference_type.localeCompare(b.reference_type)||a.normalized_value.localeCompare(b.normalized_value)).slice(0,60)}));
    }
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleRebuildReferences(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await rebuildLegalReferences(env, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleBackfillReferenceValidation(request: Request, env: Env): Promise<Response> {
  try {
    const result = await createBackfillJob(env, "reference_backfill", await readJson(request));
    return json(result, {status:202});
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleVerifyReferenceCitations(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await verifyCitations(env, payload);
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleRulesCitationInventory(request: Request, env: Env): Promise<Response> {
  try {
    const url = new URL(request.url);
    const result = await listRulesCitationInventory(env, {
      citation: url.searchParams.get("citation") ?? undefined,
      normalized: url.searchParams.get("normalized") ?? undefined,
      bare: url.searchParams.get("bare") ?? undefined,
      prefix: url.searchParams.get("prefix") ?? undefined,
      limit: Number.isFinite(Number(url.searchParams.get("limit") ?? "100")) ? Number(url.searchParams.get("limit") ?? "100") : 100
    });
    return json(result);
  } catch (error) {
    return toErrorResponse(error);
  }
}

export async function handleListCatalogVersions(_request: Request, env: Env): Promise<Response> {
  try {return json(await listLegalCatalogVersions(env));} catch(error){return toErrorResponse(error);}
}
export async function handleRestoreCatalog(request: Request, env: Env): Promise<Response> {
  try {return json(await restoreLegalCatalog(env,await readJson(request)));} catch(error){return toErrorResponse(error);}
}
