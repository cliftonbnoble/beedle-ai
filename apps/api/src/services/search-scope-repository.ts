// Bounded corpus coverage reads used to choose strict or compatible index-code scope.
import { normalizeFilterValue } from "./legal-references";
import { directIndexCodeMatchValuesForRequestedCode, isRetryableSearchError, requestedIndexCodeFilters } from "./search-query-analysis";
import { SearchRequest } from "@beedle/shared";
import type { Env } from "../lib/types";



export async function hasAnyExactIndexCodeCoverage(env: Env, filters: SearchRequest["filters"]): Promise<boolean> {
  const requestedCodes = requestedIndexCodeFilters(filters);
  if (!requestedCodes.length) return false;

  for (const code of requestedCodes) {
    const directValues = directIndexCodeMatchValuesForRequestedCode(code);
    // The FACET table (document_index_codes, FACET-01) is the authoritative population — checking
    // only document_reference_links here (the reference-EXTRACTION table, ~1% index-code coverage)
    // sent well-covered codes down the SOFT scope, which boosts instead of constraining: measured,
    // K12 has 121 faceted documents but zero reference-link rows, so a K12-filtered search returned
    // documents that are not K12 at all. Facet coverage decides; reference links remain a fallback
    // for codes the facet backfill missed.
    const facetClause = directValues.map(() => "(dic.normalized_code = ? OR lower(dic.code) = lower(?))").join(" OR ");
    const facetBindings = directValues.flatMap((value) => [normalizeFilterValue("index_code", value), value]);
    try {
      const facetRows = await env.DB.prepare(
        `SELECT 1
         FROM document_index_codes dic
         JOIN documents d ON d.id = dic.document_id
         WHERE d.rejected_at IS NULL
           AND d.file_type = 'decision_docx'
           AND (${facetClause})
         LIMIT 1`
      )
        .bind(...facetBindings)
        .all<{ "1": number }>();
      if ((facetRows.results || []).length > 0) continue;
    } catch (error) {
      if (!isRetryableSearchError(error) && !/no such table/i.test(error instanceof Error ? error.message : "")) throw error;
    }

    const codeClause = directValues.map(() => "(l.normalized_value = ? OR lower(l.canonical_value) = lower(?))").join(" OR ");
    const bindings = directValues.flatMap((value) => [normalizeFilterValue("index_code", value), value]);
    try {
      const rows = await env.DB.prepare(
        `SELECT 1
         FROM document_reference_links l
         JOIN documents d ON d.id = l.document_id
         WHERE d.rejected_at IS NULL
           AND d.file_type = 'decision_docx'
           AND l.reference_type = 'index_code'
           AND l.is_valid = 1
           AND (${codeClause})
         LIMIT 1`
      )
        .bind(...bindings)
        .all<{ "1": number }>();
      if ((rows.results || []).length === 0) return false;
    } catch (error) {
      if (isRetryableSearchError(error)) return false;
      throw error;
    }
  }

  return true;
}
