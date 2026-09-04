type IndexCodeMatch = { code: string; normalizedCode: string };

// Bind selections as JSON, including each code's aliases. This keeps large family
// selections within the SQL parameter budget used by every retrieval path.
export function buildIndexCodeSelectionClause(
  codeGroups: IndexCodeMatch[][],
  operator: "and" | "or",
  params: Array<string | number>
): string {
  if (operator === "or") {
    const values = codeGroups.flat();
    const normalized = JSON.stringify([...new Set(values.map((value) => value.normalizedCode))]);
    const raw = JSON.stringify([...new Set(values.map((value) => value.code.toLowerCase()))]);
    params.push(normalized, raw, normalized, raw);
    // These uncorrelated IN lists can be evaluated once, instead of expanding the
    // entire selected family separately for every document.
    return `(
      EXISTS (
        SELECT 1 FROM document_index_codes dic
        WHERE dic.document_id = d.id
          AND (dic.normalized_code IN (SELECT value FROM json_each(?))
            OR lower(dic.code) IN (SELECT value FROM json_each(?)))
      )
      OR EXISTS (
        SELECT 1 FROM document_reference_links l
        WHERE l.document_id = d.id
          AND l.reference_type = 'index_code'
          AND l.is_valid = 1
          AND (l.normalized_value IN (SELECT value FROM json_each(?))
            OR lower(l.canonical_value) IN (SELECT value FROM json_each(?)))
      )
    )`;
  }
  params.push(JSON.stringify(codeGroups));
  const matchesCode = `(
    EXISTS (
      SELECT 1 FROM document_index_codes dic
      JOIN json_each(selected_code.value) candidate
        ON dic.normalized_code = json_extract(candidate.value, '$.normalizedCode')
          OR lower(dic.code) = lower(json_extract(candidate.value, '$.code'))
      WHERE dic.document_id = d.id
    )
    OR EXISTS (
      SELECT 1 FROM document_reference_links l
      JOIN json_each(selected_code.value) candidate
        ON l.normalized_value = json_extract(candidate.value, '$.normalizedCode')
          OR lower(l.canonical_value) = lower(json_extract(candidate.value, '$.code'))
      WHERE l.document_id = d.id
        AND l.reference_type = 'index_code'
        AND l.is_valid = 1
    )
  )`;
  return `NOT EXISTS (SELECT 1 FROM json_each(?) selected_code WHERE NOT ${matchesCode})`;
}
