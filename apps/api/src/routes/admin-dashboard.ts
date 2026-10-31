import { json, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";
import { corpusPartitions, assertCorpusPartitionRevisions } from '../services/corpus-partitions';
import { RequestConflictError } from '../lib/http';

type CountRow = {
  count: number | string | null;
};

function coerceCount(value: number | string | null | undefined): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") return Number(value) || 0;
  return 0;
}

export async function handleDashboardSummary(_request: Request, env: Env): Promise<Response> {
  try {
    const layout=corpusPartitions(env);
    if(layout){
      await assertCorpusPartitionRevisions(env);
      const counts=[];
      for(let at=0;at<layout.partitions.length;at+=4){
        counts.push(...await Promise.all(layout.partitions.slice(at,at+4).map(async part=>{
          const row=await part.database.prepare("SELECT COUNT(*) AS count FROM documents WHERE file_type='decision_docx' AND searchable_at IS NOT NULL").first<CountRow>();
          const count=coerceCount(row?.count);
          if(count!==part.publishedDocuments)throw new RequestConflictError('Restored corpus publication counts changed. Reconcile the release before serving the dashboard.');
          return count;
        })));
      }
      await assertCorpusPartitionRevisions(env);
      return json({searchableDecisionCount:counts.reduce((sum,count)=>sum+count,0)});
    }
    const searchableDecisions = await env.DB.prepare(
      `
        SELECT COUNT(*) AS count
        FROM documents
        WHERE file_type = 'decision_docx'
          AND searchable_at IS NOT NULL
      `
    ).first<CountRow>();

    return json({
      searchableDecisionCount: coerceCount(searchableDecisions?.count)
    });
  } catch (error) {
    return toErrorResponse(error);
  }
}
