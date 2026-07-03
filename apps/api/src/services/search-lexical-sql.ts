// Lexical SQL clause/expression builders extracted from search.ts (SEARCH-02c module split, step 1).
//
// These are pure, leaf helpers: given column expressions and a list of terms they return a parameterised
// SQL fragment (a substring-match WHERE clause or a weighted rank expression) plus its bound params. They
// have no dependency on the rest of the search service, which is why they are the safe first extraction.
// Callers cap the term count to stay under D1's bound-parameter limit (see boundLexicalTermsForD1).

export function buildLexicalMatchClause(
  chunkExpr: string,
  citationExpr: string,
  titleExpr: string,
  authorExpr: string,
  terms: string[]
): { clause: string; params: string[] } {
  const safeTerms = terms.length ? terms : [""];
  const chunks = safeTerms.map(
    () =>
      `(instr(lower(${chunkExpr}), lower(?)) > 0 OR instr(lower(${citationExpr}), lower(?)) > 0 OR instr(lower(${titleExpr}), lower(?)) > 0 OR instr(lower(coalesce(${authorExpr}, '')), lower(?)) > 0)`
  );
  return {
    clause: `(${chunks.join(" OR ")})`,
    params: safeTerms.flatMap((term) => [term, term, term, term])
  };
}

export function buildLexicalRankExpr(
  chunkExpr: string,
  citationExpr: string,
  titleExpr: string,
  authorExpr: string,
  sectionExpr: string,
  terms: string[]
): { expr: string; params: string[] } {
  const safeTerms = terms.length ? terms : [""];
  return {
    expr: safeTerms
      .map(
        () =>
          `(
            CASE WHEN instr(lower(${titleExpr}), lower(?)) > 0 THEN 2.4 ELSE 0 END +
            CASE WHEN instr(lower(${citationExpr}), lower(?)) > 0 THEN 2.0 ELSE 0 END +
            CASE WHEN instr(lower(coalesce(${authorExpr}, '')), lower(?)) > 0 THEN 1.9 ELSE 0 END +
            CASE WHEN instr(lower(${sectionExpr}), lower(?)) > 0 THEN 1.4 ELSE 0 END +
            CASE WHEN instr(lower(${chunkExpr}), lower(?)) > 0 THEN 1.0 ELSE 0 END
          )`
      )
      .join(" + "),
    params: safeTerms.flatMap((term) => [term, term, term, term, term])
  };
}

function normalizedWholeWordExpr(expr: string): string {
  return `(' ' || lower(
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(coalesce(${expr}, ''), char(10), ' '), char(13), ' '), '.', ' '), ',', ' '), ';', ' '), ':', ' '), '(', ' '), ')', ' '), '-', ' '), '/', ' ')
  ) || ' ')`;
}

export function buildWholeWordLexicalMatchClause(
  chunkExpr: string,
  citationExpr: string,
  titleExpr: string,
  authorExpr: string,
  terms: string[]
): { clause: string; params: string[] } {
  const safeTerms = terms.length ? terms : [""];
  const chunks = safeTerms.map(
    () =>
      `(instr(${normalizedWholeWordExpr(chunkExpr)}, ' ' || lower(?) || ' ') > 0 OR instr(lower(${citationExpr}), lower(?)) > 0 OR instr(${normalizedWholeWordExpr(titleExpr)}, ' ' || lower(?) || ' ') > 0 OR instr(${normalizedWholeWordExpr(authorExpr)}, ' ' || lower(?) || ' ') > 0)`
  );
  return {
    clause: `(${chunks.join(" OR ")})`,
    params: safeTerms.flatMap((term) => [term, term, term, term])
  };
}

export function buildWholeWordLexicalRankExpr(
  chunkExpr: string,
  citationExpr: string,
  titleExpr: string,
  authorExpr: string,
  sectionExpr: string,
  terms: string[]
): { expr: string; params: string[] } {
  const safeTerms = terms.length ? terms : [""];
  return {
    expr: safeTerms
      .map(
        () =>
          `(
            CASE WHEN instr(${normalizedWholeWordExpr(titleExpr)}, ' ' || lower(?) || ' ') > 0 THEN 2.4 ELSE 0 END +
            CASE WHEN instr(lower(${citationExpr}), lower(?)) > 0 THEN 2.0 ELSE 0 END +
            CASE WHEN instr(${normalizedWholeWordExpr(authorExpr)}, ' ' || lower(?) || ' ') > 0 THEN 1.9 ELSE 0 END +
            CASE WHEN instr(lower(${sectionExpr}), lower(?)) > 0 THEN 1.4 ELSE 0 END +
            CASE WHEN instr(${normalizedWholeWordExpr(chunkExpr)}, ' ' || lower(?) || ' ') > 0 THEN 1.0 ELSE 0 END
          )`
      )
      .join(" + "),
    params: safeTerms.flatMap((term) => [term, term, term, term, term])
  };
}
