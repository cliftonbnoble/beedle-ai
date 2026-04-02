import { json, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";

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
