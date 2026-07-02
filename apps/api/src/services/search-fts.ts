// DB fetch / FTS / scope-execution layer extracted from search.ts (SEARCH-02c module split, step 5c).
//
// The env.DB side of retrieval: FTS + lexical + vector search execution, candidate-document fetching,
// chunk hydration, and the document-facet / runtime-index bootstrap. Builds on the pure query-analysis
// layer (search-query-analysis) plus the text / concept / classification / lexical-SQL leaves. It owns
// the retrieval memoization flags; searchFtsAvailable is an `export let` because runSearchInternal (which
// stays in search.ts) reads it as a live binding. No back-references into scoring / orchestration.
import { embed } from "./embeddings";
import { normalizeFilterValue } from "./legal-references";
import { phraseSearchFtsQuery } from "./search-concepts";
import { buildLexicalMatchClause, buildLexicalRankExpr, buildWholeWordLexicalMatchClause, buildWholeWordLexicalRankExpr } from "./search-lexical-sql";
import {
  bindIndexCodeMatchValues,
  bindReferenceSectionMatchValues,
  boundLexicalTermsForD1,
  buildDirectIndexCodeCompatibilityClause,
  buildIndexCodeFilterContext,
  buildReferenceSectionCompatibilityClause,
  cachedNormalizedChunkText,
  decisionLayerSectionLabelClause,
  getQueryDerivedContext,
  habitabilityScopePhraseHints,
  isConclusionsLikeSectionLabel,
  isFindingsLikeSectionLabel,
  isMissingDocumentFacetTableError,
  isRetryableSearchError,
  isSupportingFactSectionLabel,
  issueQueryIndexCodeHints,
  issueQueryPhraseHints,
  issueQueryReferenceHints,
  keywordBoundaryGuardTerms,
  keywordCandidateTerms,
  lexicalTerms,
  lockoutScopePhraseHints,
  textContainsIssueSignal,
  wholeWordLexicalTerms
} from "./search-query-analysis";
import { isOwnerMoveInIssueSearch, isVectorFirstIssueSearch } from "./search-query-classification";
import { normalize, uniq } from "./search-text";
import { SearchRequest } from "@beedle/shared";
import type { Env } from "../lib/types";
import type { ChunkRow, SearchContext } from "./search-types";

let searchRuntimeIndexesEnsured = false;
let searchRuntimeIndexesPromise: Promise<void> | null = null;
export let searchFtsAvailable = false;
let documentFacetTablesEnsured = false;

export const maxSqliteIdBatchSize = 30;

export const maxScopedLexicalDocumentBatchSize = 4;

export const maxKeywordCandidateDocumentBatchSize = 12;

export const documentFacetTableDdlStatements = [
  `CREATE TABLE IF NOT EXISTS document_index_codes (
    document_id TEXT NOT NULL,
    code TEXT NOT NULL,
    normalized_code TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (document_id, normalized_code),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
  )`,
  `CREATE INDEX IF NOT EXISTS idx_document_index_codes_normalized
    ON document_index_codes (normalized_code, document_id)`,
  `CREATE TABLE IF NOT EXISTS document_rules_sections (
    document_id TEXT NOT NULL,
    section TEXT NOT NULL,
    normalized_section TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (document_id, normalized_section),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
  )`,
  `CREATE INDEX IF NOT EXISTS idx_document_rules_sections_normalized
    ON document_rules_sections (normalized_section, document_id)`,
  `CREATE TABLE IF NOT EXISTS document_ordinance_sections (
    document_id TEXT NOT NULL,
    section TEXT NOT NULL,
    normalized_section TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (document_id, normalized_section),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
  )`,
  `CREATE INDEX IF NOT EXISTS idx_document_ordinance_sections_normalized
    ON document_ordinance_sections (normalized_section, document_id)`
];

export const documentFacetIndexCodeBackfillSql = `INSERT OR IGNORE INTO document_index_codes (document_id, code, normalized_code)
SELECT document_id, code, normalized_code
FROM (
  SELECT
    d.id AS document_id,
    trim(CAST(je.value AS TEXT)) AS code,
    CASE
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ic-%'
        OR lower(trim(CAST(je.value AS TEXT))) LIKE 'ic %'
        THEN substr(lower(trim(CAST(je.value AS TEXT))), 4)
      ELSE lower(trim(CAST(je.value AS TEXT)))
    END AS normalized_code
  FROM documents d
  JOIN json_each(
    CASE
      WHEN json_valid(COALESCE(d.index_codes_json, '[]')) THEN COALESCE(d.index_codes_json, '[]')
      ELSE '[]'
    END
  ) je
)
WHERE code != '' AND normalized_code != ''`;

export const documentFacetRulesBackfillSql = `INSERT OR IGNORE INTO document_rules_sections (document_id, section, normalized_section)
SELECT document_id, section, normalized_section
FROM (
  SELECT
    d.id AS document_id,
    trim(CAST(je.value AS TEXT)) AS section,
    CASE
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule %'
        THEN substr(lower(trim(CAST(je.value AS TEXT))), 6)
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule.%'
        THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 7))
      ELSE lower(trim(CAST(je.value AS TEXT)))
    END AS normalized_section
  FROM documents d
  JOIN json_each(
    CASE
      WHEN json_valid(COALESCE(d.rules_sections_json, '[]')) THEN COALESCE(d.rules_sections_json, '[]')
      ELSE '[]'
    END
  ) je
)
WHERE section != '' AND normalized_section != ''`;

export const documentFacetOrdinanceBackfillSql = `INSERT OR IGNORE INTO document_ordinance_sections (document_id, section, normalized_section)
SELECT document_id, section, normalized_section
FROM (
  SELECT
    d.id AS document_id,
    trim(CAST(je.value AS TEXT)) AS section,
    CASE
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance %'
        THEN substr(lower(trim(CAST(je.value AS TEXT))), 11)
      WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance.%'
        THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 12))
      ELSE lower(trim(CAST(je.value AS TEXT)))
    END AS normalized_section
  FROM documents d
  JOIN json_each(
    CASE
      WHEN json_valid(COALESCE(d.ordinance_sections_json, '[]')) THEN COALESCE(d.ordinance_sections_json, '[]')
      ELSE '[]'
    END
  ) je
)
WHERE section != '' AND normalized_section != ''`;

export const documentFacetSyncTriggerStatements = [
  `CREATE TRIGGER IF NOT EXISTS documents_ai_document_facets
AFTER INSERT ON documents
BEGIN
  INSERT OR IGNORE INTO document_index_codes (document_id, code, normalized_code)
  SELECT new.id, code, normalized_code
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS code,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ic-%'
          OR lower(trim(CAST(je.value AS TEXT))) LIKE 'ic %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 4)
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_code
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.index_codes_json, '[]')) THEN COALESCE(new.index_codes_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE code != '' AND normalized_code != '';

  INSERT OR IGNORE INTO document_rules_sections (document_id, section, normalized_section)
  SELECT new.id, section, normalized_section
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS section,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 6)
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule.%'
          THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 7))
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_section
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.rules_sections_json, '[]')) THEN COALESCE(new.rules_sections_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE section != '' AND normalized_section != '';

  INSERT OR IGNORE INTO document_ordinance_sections (document_id, section, normalized_section)
  SELECT new.id, section, normalized_section
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS section,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 11)
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance.%'
          THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 12))
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_section
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.ordinance_sections_json, '[]')) THEN COALESCE(new.ordinance_sections_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE section != '' AND normalized_section != '';
END`,
  `CREATE TRIGGER IF NOT EXISTS documents_au_document_facets
AFTER UPDATE OF index_codes_json, rules_sections_json, ordinance_sections_json ON documents
BEGIN
  DELETE FROM document_index_codes WHERE document_id = new.id;
  DELETE FROM document_rules_sections WHERE document_id = new.id;
  DELETE FROM document_ordinance_sections WHERE document_id = new.id;

  INSERT OR IGNORE INTO document_index_codes (document_id, code, normalized_code)
  SELECT new.id, code, normalized_code
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS code,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ic-%'
          OR lower(trim(CAST(je.value AS TEXT))) LIKE 'ic %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 4)
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_code
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.index_codes_json, '[]')) THEN COALESCE(new.index_codes_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE code != '' AND normalized_code != '';

  INSERT OR IGNORE INTO document_rules_sections (document_id, section, normalized_section)
  SELECT new.id, section, normalized_section
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS section,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 6)
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'rule.%'
          THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 7))
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_section
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.rules_sections_json, '[]')) THEN COALESCE(new.rules_sections_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE section != '' AND normalized_section != '';

  INSERT OR IGNORE INTO document_ordinance_sections (document_id, section, normalized_section)
  SELECT new.id, section, normalized_section
  FROM (
    SELECT
      trim(CAST(je.value AS TEXT)) AS section,
      CASE
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance %'
          THEN substr(lower(trim(CAST(je.value AS TEXT))), 11)
        WHEN lower(trim(CAST(je.value AS TEXT))) LIKE 'ordinance.%'
          THEN ltrim(substr(lower(trim(CAST(je.value AS TEXT))), 12))
        ELSE lower(trim(CAST(je.value AS TEXT)))
      END AS normalized_section
    FROM json_each(
      CASE
        WHEN json_valid(COALESCE(new.ordinance_sections_json, '[]')) THEN COALESCE(new.ordinance_sections_json, '[]')
        ELSE '[]'
      END
    ) je
  )
  WHERE section != '' AND normalized_section != '';
END`,
  `CREATE TRIGGER IF NOT EXISTS documents_ad_document_facets
AFTER DELETE ON documents
BEGIN
  DELETE FROM document_index_codes WHERE document_id = old.id;
  DELETE FROM document_rules_sections WHERE document_id = old.id;
  DELETE FROM document_ordinance_sections WHERE document_id = old.id;
END`
];

export async function ensureDocumentFacetTables(env: Env): Promise<void> {
  if (documentFacetTablesEnsured) return;

  for (const sql of [...documentFacetTableDdlStatements, ...documentFacetSyncTriggerStatements]) {
    try {
      await env.DB.prepare(sql).run();
    } catch (error) {
      if (!isRetryableSearchError(error)) throw error;
    }
  }

  // Backfill only when the facet tables are empty so a corpus already populated by migration 0009 (or a
  // prior worker) skips the json_each scan. The EXISTS probe short-circuits and is cheap.
  try {
    const populated = await env.DB.prepare(
      `SELECT (EXISTS(SELECT 1 FROM document_index_codes)
            OR EXISTS(SELECT 1 FROM document_rules_sections)
            OR EXISTS(SELECT 1 FROM document_ordinance_sections)) AS populated`
    ).first<{ populated: number }>();
    if (!populated?.populated) {
      for (const sql of [
        documentFacetIndexCodeBackfillSql,
        documentFacetRulesBackfillSql,
        documentFacetOrdinanceBackfillSql
      ]) {
        try {
          await env.DB.prepare(sql).run();
        } catch (error) {
          if (!isRetryableSearchError(error)) throw error;
        }
      }
    }
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
  }

  documentFacetTablesEnsured = true;
}

export async function ensureSearchRuntimeIndexes(env: Env) {
  if (searchRuntimeIndexesEnsured) return;
  if (searchRuntimeIndexesPromise) return searchRuntimeIndexesPromise;

  searchRuntimeIndexesPromise = (async () => {
    const statements = [
      `CREATE INDEX IF NOT EXISTS idx_documents_author_name_lookup
        ON documents (lower(coalesce(author_name, '')), file_type, rejected_at, approved_at, decision_date, searchable_at)`,
      `CREATE INDEX IF NOT EXISTS idx_documents_search_runtime
        ON documents (file_type, rejected_at, approved_at, searchable_at, decision_date)`,
      `CREATE INDEX IF NOT EXISTS idx_retrieval_search_chunks_doc
        ON retrieval_search_chunks (document_id, active, batch_id)`
    ];

    for (const sql of statements) {
      try {
        await env.DB.prepare(sql).run();
      } catch (error) {
        if (!isRetryableSearchError(error)) throw error;
      }
    }
    searchFtsAvailable = await ensureSearchFts(env);
    await ensureDocumentFacetTables(env);
    searchRuntimeIndexesEnsured = true;
  })();

  try {
    await searchRuntimeIndexesPromise;
  } finally {
    searchRuntimeIndexesPromise = null;
  }
}

export async function ensureSearchFts(env: Env): Promise<boolean> {
  const statements = [
    `CREATE VIRTUAL TABLE IF NOT EXISTS search_chunks_fts USING fts5(
      source_kind UNINDEXED,
      chunk_id UNINDEXED,
      document_id UNINDEXED,
      active UNINDEXED,
      section_label,
      paragraph_anchor UNINDEXED,
      citation_anchor UNINDEXED,
      chunk_text,
      created_at UNINDEXED,
      order_rank UNINDEXED,
      tokenize = 'unicode61'
    )`,
    `CREATE TRIGGER IF NOT EXISTS document_chunks_ai_search_fts
      AFTER INSERT ON document_chunks
      BEGIN
        INSERT INTO search_chunks_fts (
          source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
          citation_anchor, chunk_text, created_at, order_rank
        )
        VALUES (
          'document', new.id, new.document_id, 1, new.section_label, new.paragraph_anchor,
          new.citation_anchor, new.chunk_text, new.created_at, new.chunk_order
        );
      END`,
    `CREATE TRIGGER IF NOT EXISTS document_chunks_ad_search_fts
      AFTER DELETE ON document_chunks
      BEGIN
        DELETE FROM search_chunks_fts
        WHERE source_kind = 'document' AND chunk_id = old.id;
      END`,
    `CREATE TRIGGER IF NOT EXISTS document_chunks_au_search_fts
      AFTER UPDATE ON document_chunks
      BEGIN
        DELETE FROM search_chunks_fts
        WHERE source_kind = 'document' AND chunk_id = old.id;

        INSERT INTO search_chunks_fts (
          source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
          citation_anchor, chunk_text, created_at, order_rank
        )
        VALUES (
          'document', new.id, new.document_id, 1, new.section_label, new.paragraph_anchor,
          new.citation_anchor, new.chunk_text, new.created_at, new.chunk_order
        );
      END`,
    `CREATE TRIGGER IF NOT EXISTS retrieval_search_chunks_ai_search_fts
      AFTER INSERT ON retrieval_search_chunks
      BEGIN
        INSERT INTO search_chunks_fts (
          source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
          citation_anchor, chunk_text, created_at, order_rank
        )
        VALUES (
          'retrieval', new.chunk_id, new.document_id, new.active, new.section_label, new.paragraph_anchor,
          new.citation_anchor, new.chunk_text, new.created_at, 999999
        );
      END`,
    `CREATE TRIGGER IF NOT EXISTS retrieval_search_chunks_ad_search_fts
      AFTER DELETE ON retrieval_search_chunks
      BEGIN
        DELETE FROM search_chunks_fts
        WHERE source_kind = 'retrieval' AND chunk_id = old.chunk_id;
      END`,
    `CREATE TRIGGER IF NOT EXISTS retrieval_search_chunks_au_search_fts
      AFTER UPDATE ON retrieval_search_chunks
      BEGIN
        DELETE FROM search_chunks_fts
        WHERE source_kind = 'retrieval' AND chunk_id = old.chunk_id;

        INSERT INTO search_chunks_fts (
          source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
          citation_anchor, chunk_text, created_at, order_rank
        )
        VALUES (
          'retrieval', new.chunk_id, new.document_id, new.active, new.section_label, new.paragraph_anchor,
          new.citation_anchor, new.chunk_text, new.created_at, 999999
        );
      END`
  ];

  try {
    for (const sql of statements) {
      await env.DB.prepare(sql).run();
    }

    // Emptiness check only — use an existence probe, not COUNT(*). FTS5 COUNT(*) scans the
    // entire index (~3s on a ~1M-row table) and runs once per cold isolate, dominating the
    // first search after a deploy/recycle. SELECT 1 ... LIMIT 1 returns in ~0ms.
    const existingRow = await env.DB.prepare(`SELECT 1 as present FROM search_chunks_fts LIMIT 1`).first<{ present: number }>();
    if (!existingRow) {
      await env.DB.prepare(
        `INSERT INTO search_chunks_fts (
          source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
          citation_anchor, chunk_text, created_at, order_rank
        )
        SELECT
          'document',
          c.id,
          c.document_id,
          1,
          c.section_label,
          c.paragraph_anchor,
          c.citation_anchor,
          c.chunk_text,
          c.created_at,
          c.chunk_order
        FROM document_chunks c`
      ).run();

      await env.DB.prepare(
        `INSERT INTO search_chunks_fts (
          source_kind, chunk_id, document_id, active, section_label, paragraph_anchor,
          citation_anchor, chunk_text, created_at, order_rank
        )
        SELECT
          'retrieval',
          rs.chunk_id,
          rs.document_id,
          rs.active,
          rs.section_label,
          rs.paragraph_anchor,
          rs.citation_anchor,
          rs.chunk_text,
          rs.created_at,
          999999
        FROM retrieval_search_chunks rs`
      ).run();
    }

    const probe = await env.DB.prepare(`SELECT rowid FROM search_chunks_fts WHERE search_chunks_fts MATCH ? LIMIT 1`)
      .bind("tenant")
      .all<{ rowid: number }>();
    return Array.isArray(probe.results);
  } catch (error) {
    console.warn("[search-fts] disabled", error instanceof Error ? error.message : String(error));
    return false;
  }
}

export async function fetchHabitabilityCandidateDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number
): Promise<string[]> {
  if (!limit || limit <= 0) return [];
  const normalizedQuery = normalize(query || "");
  const { conditionSignals, reportingHints, repairHints } = habitabilityScopePhraseHints(query, { normalizedQuery });
  if (conditionSignals.length === 0) return [];

  const conditionClause = conditionSignals.map(() => "lower(s.chunk_text) LIKE ?").join(" OR ");
  const reportingClause = reportingHints.length > 0 ? reportingHints.map(() => "lower(s.chunk_text) LIKE ?").join(" OR ") : "0";
  const repairClause = repairHints.length > 0 ? repairHints.map(() => "lower(s.chunk_text) LIKE ?").join(" OR ") : "0";
  const conditionScoreExpr = conditionSignals.map(() => "CASE WHEN lower(s.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ");
  const reportingScoreExpr = reportingHints.length > 0 ? reportingHints.map(() => "CASE WHEN lower(s.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ") : "0";
  const repairScoreExpr = repairHints.length > 0 ? repairHints.map(() => "CASE WHEN lower(s.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ") : "0";
  const sectionBoostExpr = `CASE
    WHEN lower(s.section_label) LIKE '%finding%' THEN 0.45
    WHEN lower(s.section_label) LIKE '%background%' OR lower(s.section_label) LIKE '%evidence%' OR lower(s.section_label) LIKE '%testimony%' THEN 0.35
    WHEN lower(s.section_label) LIKE '%conclusion%' THEN 0.18
    ELSE 0
  END`;

  const conditionBindings = conditionSignals.map((signal) => `%${normalize(signal)}%`);
  const reportingBindings = reportingHints.map((signal) => `%${normalize(signal)}%`);
  const repairBindings = repairHints.map((signal) => `%${normalize(signal)}%`);

  try {
    const rows = await env.DB.prepare(
      `SELECT
         scope.documentId
       FROM (
         SELECT
           d.id as documentId,
           d.searchable_at as searchableAt,
           d.decision_date as decisionDate,
           s.section_label,
           s.chunk_text,
           (${conditionScoreExpr}) * 6 + (${reportingScoreExpr}) * 2.5 + (${repairScoreExpr}) * 3 + ${sectionBoostExpr} as scopeScore
         FROM (
           SELECT rs.document_id, rs.section_label, rs.chunk_text
           FROM retrieval_search_chunks rs
           WHERE rs.active = 1
           UNION ALL
           SELECT c.document_id, c.section_label, c.chunk_text
           FROM document_chunks c
         ) s
         JOIN documents d ON d.id = s.document_id
         ${where}
           AND (${conditionClause})
           AND ((${reportingClause}) OR (${repairClause}))
       ) scope
       GROUP BY scope.documentId
       ORDER BY
         MAX(scope.scopeScore) DESC,
         MAX(COALESCE(scope.searchableAt, '')) DESC,
         MAX(COALESCE(scope.decisionDate, '')) DESC,
         scope.documentId ASC
       LIMIT ?`
    )
      .bind(
        ...conditionBindings,
        ...reportingBindings,
        ...repairBindings,
        ...params,
        ...conditionBindings,
        ...reportingBindings,
        ...repairBindings,
        limit
      )
      .all<{ documentId: string }>();
    return uniq((rows.results || []).map((row) => row.documentId).filter(Boolean));
  } catch (error) {
    if (isRetryableSearchError(error)) return [];
    throw error;
  }
}

export async function fetchLockoutCandidateDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number
): Promise<string[]> {
  if (!limit || limit <= 0) return [];
  const normalizedQuery = normalize(query || "");
  const normalizedQueryContext = { normalizedQuery };
  const phraseHints = lockoutScopePhraseHints(query, normalizedQueryContext);
  if (phraseHints.length === 0) return [];

  const likeBindings = phraseHints.map((phrase) => `%${normalize(phrase)}%`);
  const retrievalTextHitClause = phraseHints.map(() => "lower(rs.chunk_text) LIKE ?").join(" OR ");
  const retrievalTextHitScoreExpr = phraseHints.map(() => "CASE WHEN lower(rs.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ");
  const retrievalSectionBoostExpr = `CASE
    WHEN lower(rs.section_label) LIKE '%conclusion%' THEN 0.55
    WHEN lower(rs.section_label) LIKE '%finding%' THEN 0.4
    WHEN lower(rs.section_label) LIKE '%background%' OR lower(rs.section_label) LIKE '%evidence%' OR lower(rs.section_label) LIKE '%testimony%' THEN 0.3
    ELSE 0
  END`;
  const documentTextHitClause = phraseHints.map(() => "lower(c.chunk_text) LIKE ?").join(" OR ");
  const documentTextHitScoreExpr = phraseHints.map(() => "CASE WHEN lower(c.chunk_text) LIKE ? THEN 1 ELSE 0 END").join(" + ");
  const documentSectionBoostExpr = `CASE
    WHEN lower(c.section_label) LIKE '%conclusion%' THEN 0.55
    WHEN lower(c.section_label) LIKE '%finding%' THEN 0.4
    WHEN lower(c.section_label) LIKE '%background%' OR lower(c.section_label) LIKE '%evidence%' OR lower(c.section_label) LIKE '%testimony%' THEN 0.3
    ELSE 0
  END`;

  try {
    const rows = await env.DB.prepare(
      `SELECT
         scope.documentId
       FROM (
         SELECT
           d.id as documentId,
           d.searchable_at as searchableAt,
           d.decision_date as decisionDate,
           rs.section_label as section_label,
           rs.chunk_text as chunk_text,
           ${retrievalTextHitScoreExpr} + ${retrievalSectionBoostExpr} + 0.2 as scopeScore
         FROM retrieval_search_chunks rs
         JOIN documents d ON d.id = rs.document_id
         ${where}
           AND rs.active = 1
           AND (${retrievalTextHitClause})

         UNION ALL

         SELECT
           d.id as documentId,
           d.searchable_at as searchableAt,
           d.decision_date as decisionDate,
           c.section_label as section_label,
           c.chunk_text as chunk_text,
           ${documentTextHitScoreExpr} + ${documentSectionBoostExpr} as scopeScore
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${where}
           AND (${documentTextHitClause})
       ) scope
       GROUP BY scope.documentId
       ORDER BY
         MAX(scope.scopeScore) DESC,
         MAX(COALESCE(scope.searchableAt, '')) DESC,
         MAX(COALESCE(scope.decisionDate, '')) DESC,
         scope.documentId ASC
       LIMIT ?`
    )
      .bind(
        ...likeBindings,
        ...params,
        ...likeBindings,
        ...likeBindings,
        ...params,
        ...likeBindings,
        limit
      )
      .all<{ documentId: string }>();
    return uniq((rows.results || []).map((row) => row.documentId).filter(Boolean));
  } catch (error) {
    if (isRetryableSearchError(error)) return [];
    throw error;
  }
}

export async function fetchIssueCandidateDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number
): Promise<string[]> {
  if (!limit || limit <= 0) return [];
  const normalizedQuery = normalize(query || "");
  const normalizedQueryContext = { normalizedQuery };

  const documentIds: string[] = [];
  const pushIds = (ids: string[]) => {
    for (const id of ids) {
      if (!id || documentIds.includes(id)) continue;
      documentIds.push(id);
      if (documentIds.length >= limit) break;
    }
  };

  const codeHints = issueQueryIndexCodeHints(query, normalizedQueryContext);
  const phraseHints = issueQueryPhraseHints(query, normalizedQueryContext);
  const manualReferenceHints = issueQueryReferenceHints(query, normalizedQueryContext);
  const ownerMoveInSearch = isOwnerMoveInIssueSearch(query, normalizedQueryContext);

  const hasReferenceDrivenHints =
    codeHints.length > 0 || manualReferenceHints.rulesSections.length > 0 || manualReferenceHints.ordinanceSections.length > 0;

  if (hasReferenceDrivenHints) {
    const filterContext = buildIndexCodeFilterContext(
      { approvedOnly: false, indexCodes: codeHints } as SearchRequest["filters"],
      { includeGenericDhsFamilyAlias: false }
    );
    const hintedRulesSections = uniq([...filterContext.relatedRulesSections, ...manualReferenceHints.rulesSections]).filter(Boolean);
    const hintedOrdinanceSections = uniq([...filterContext.relatedOrdinanceSections, ...manualReferenceHints.ordinanceSections]).filter(Boolean);

    const clauses: string[] = [];
    const bindings: Array<string | number> = [];
    const directCodes = uniq([...codeHints, ...filterContext.legacyCodeAliases]).filter(Boolean);

    if (directCodes.length > 0) {
      clauses.push(buildDirectIndexCodeCompatibilityClause(directCodes));
      bindIndexCodeMatchValues(bindings, directCodes);
    }

    if (hintedRulesSections.length > 0) {
      clauses.push(buildReferenceSectionCompatibilityClause("rules_section", hintedRulesSections, { includePrefixMatch: true }));
      bindReferenceSectionMatchValues(bindings, "rules_section", hintedRulesSections, { includePrefixMatch: true });
    }

    if (hintedOrdinanceSections.length > 0) {
      clauses.push(buildReferenceSectionCompatibilityClause("ordinance_section", hintedOrdinanceSections, { includePrefixMatch: true }));
      bindReferenceSectionMatchValues(bindings, "ordinance_section", hintedOrdinanceSections, { includePrefixMatch: true });
    }

    if (clauses.length > 0) {
      try {
        const rows = await env.DB.prepare(
          `SELECT DISTINCT d.id as documentId
           FROM documents d
           ${where}
             AND (${clauses.join(" OR ")})
           ORDER BY COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
           LIMIT ?`
        )
          .bind(...params, ...bindings, limit)
          .all<{ documentId: string }>();
        pushIds((rows.results || []).map((row) => row.documentId));
      } catch (error) {
        if (!isRetryableSearchError(error)) throw error;
      }
    }
  }

  if (documentIds.length > 0 && hasReferenceDrivenHints) return documentIds.slice(0, limit);

  if (documentIds.length === 0 && ownerMoveInSearch) {
    pushIds(await fetchOwnerMoveInOrdinanceFallbackDocumentIds(env, where, params, Math.max(limit - documentIds.length, 1)));
  }

  if (documentIds.length === 0 && isVectorFirstIssueSearch(query)) return [];
  if (documentIds.length >= limit || phraseHints.length === 0) return documentIds.slice(0, limit);

  const phraseQuery = uniq([query, ...phraseHints]).filter(Boolean).join(" ");
  // This statement binds 9 params per term (one match + one rank builder) + the filter params + a LIMIT.
  const phraseLexicalTerms = boundLexicalTermsForD1(lexicalTerms(phraseQuery), 9, params.length + 1);
  const match = buildLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", phraseLexicalTerms);
  const rank = buildLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", phraseLexicalTerms);

  try {
    const rows = await env.DB.prepare(
      `SELECT
         d.id as documentId,
         MAX(${rank.expr}) as lexicalRank
       FROM retrieval_search_chunks rs
       JOIN documents d ON d.id = rs.document_id
       ${where}
         AND rs.active = 1
         AND ${match.clause}
       GROUP BY d.id
       ORDER BY lexicalRank DESC, COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
       LIMIT ?`
    )
      .bind(...rank.params, ...params, ...match.params, Math.max(limit - documentIds.length, 1))
      .all<{ documentId: string }>();
    pushIds((rows.results || []).map((row) => row.documentId));
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
  }

  return documentIds.slice(0, limit);
}

export async function fetchOwnerMoveInOrdinanceFallbackDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  limit: number
): Promise<string[]> {
  if (limit <= 0) return [];

  try {
    const rows = await env.DB.prepare(
      `SELECT DISTINCT d.id as documentId
       FROM document_ordinance_sections dos
       JOIN documents d ON d.id = dos.document_id
       ${where}
         AND (dos.normalized_section = ? OR dos.normalized_section LIKE ? OR lower(dos.section) LIKE lower(?))
       ORDER BY COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
       LIMIT ?`
    )
      .bind(...params, normalizeFilterValue("ordinance_section", "37.9"), `${normalizeFilterValue("ordinance_section", "37.9")}%`, "37.9%", limit)
      .all<{ documentId: string }>();
    return (rows.results || []).map((row) => row.documentId);
  } catch (error) {
    if (!isMissingDocumentFacetTableError(error)) {
      if (!isRetryableSearchError(error)) throw error;
      return [];
    }
  }

  try {
    const rows = await env.DB.prepare(
      `SELECT d.id as documentId
       FROM documents d
       ${where}
         AND lower(coalesce(d.ordinance_sections_json, '')) LIKE lower(?)
       ORDER BY COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
       LIMIT ?`
    )
      .bind(...params, "%37.9%", limit)
      .all<{ documentId: string }>();
    return (rows.results || []).map((row) => row.documentId);
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
    return [];
  }
}

export async function fetchKeywordCandidateDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number,
  scopedDocumentIds: string[] = []
): Promise<string[]> {
  if (!limit || limit <= 0) return [];
  const normalizedQuery = normalize(query || "");
  const normalizedQueryContext = { normalizedQuery };
  if (scopedDocumentIds.length > maxKeywordCandidateDocumentBatchSize) {
    const out: string[] = [];
    for (let index = 0; index < scopedDocumentIds.length; index += maxKeywordCandidateDocumentBatchSize) {
      const batch = scopedDocumentIds.slice(index, index + maxKeywordCandidateDocumentBatchSize);
      const ids = await fetchKeywordCandidateDocumentIds(env, where, params, query, limit, batch);
      for (const id of ids) {
        if (!id || out.includes(id)) continue;
        out.push(id);
        if (out.length >= limit) return out;
      }
    }
    return out;
  }

  const wholeWordGuarded = keywordBoundaryGuardTerms(query, normalizedQueryContext).length > 0;
  const useScopedDocumentIds = scopedDocumentIds.length > 0;
  const documentScopeClause = useScopedDocumentIds ? `WHERE d.id IN (${scopedDocumentIds.map(() => "?").join(",")})` : where;
  const documentScopeParams = useScopedDocumentIds ? scopedDocumentIds : params;
  // D1 rejects queries with more than ~100 bound parameters. This query binds 9 params per term (a
  // 4-column match clause + a 5-column rank expression) plus the scope params + a LIMIT, so an unbounded
  // curated-family expansion (e.g. "infestation" → 12 terms → 121 params) overflows and fails the
  // request. Cap the term expansion to keep every statement under D1's limit. (SEARCH-05 root fix; the
  // degrade-on-overflow path remains a backstop.)
  const terms = boundLexicalTermsForD1(keywordCandidateTerms(query, normalizedQueryContext), 9, documentScopeParams.length + 1);
  if (terms.length === 0) return [];

  const documentIds: string[] = [];
  const pushIds = (ids: string[]) => {
    for (const id of ids) {
      if (!id || documentIds.includes(id)) continue;
      documentIds.push(id);
      if (documentIds.length >= limit) break;
    }
  };

  const retrievalMatch = wholeWordGuarded
    ? buildWholeWordLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", terms)
    : buildLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", terms);
  const retrievalRank = wholeWordGuarded
    ? buildWholeWordLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", terms)
    : buildLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", terms);
  try {
    const rows = await env.DB.prepare(
      `SELECT
         d.id as documentId,
         MAX(${retrievalRank.expr}) as lexicalRank
       FROM retrieval_search_chunks rs
       JOIN documents d ON d.id = rs.document_id
       ${documentScopeClause}
         AND rs.active = 1
         AND ${retrievalMatch.clause}
       GROUP BY d.id
       ORDER BY lexicalRank DESC, COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
       LIMIT ?`
    )
      .bind(...retrievalRank.params, ...documentScopeParams, ...retrievalMatch.params, limit)
      .all<{ documentId: string }>();
    pushIds((rows.results || []).map((row) => row.documentId));
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
  }

  if (documentIds.length >= limit) return documentIds.slice(0, limit);

  const fallbackMatch = wholeWordGuarded
    ? buildWholeWordLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", terms)
    : buildLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", terms);
  const fallbackRank = wholeWordGuarded
    ? buildWholeWordLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", terms)
    : buildLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", terms);
  try {
    const rows = await env.DB.prepare(
      `SELECT
         d.id as documentId,
         MAX(${fallbackRank.expr}) as lexicalRank
       FROM document_chunks c
       JOIN documents d ON d.id = c.document_id
       ${documentScopeClause}
         AND ${fallbackMatch.clause}
       GROUP BY d.id
       ORDER BY lexicalRank DESC, COALESCE(d.searchable_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.id ASC
       LIMIT ?`
    )
      .bind(...fallbackRank.params, ...documentScopeParams, ...fallbackMatch.params, Math.max(limit - documentIds.length, 1))
      .all<{ documentId: string }>();
    pushIds((rows.results || []).map((row) => row.documentId));
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
  }

  return documentIds.slice(0, limit);
}

export async function lexicalSearch(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number,
  scopedDocumentIds: string[] = [],
  options?: { allowActiveDocumentChunkSearch?: boolean; termsOverride?: string[] }
): Promise<ChunkRow[]> {
  const terms = options?.termsOverride?.length ? options.termsOverride : lexicalTerms(query);
  if (!terms.length) return [];
  if (scopedDocumentIds.length > maxScopedLexicalDocumentBatchSize) {
    const out: ChunkRow[] = [];
    for (let index = 0; index < scopedDocumentIds.length; index += maxScopedLexicalDocumentBatchSize) {
      const batch = scopedDocumentIds.slice(index, index + maxScopedLexicalDocumentBatchSize);
      out.push(...(await lexicalSearch(env, where, params, query, limit, batch, options)));
    }
    return out
      .sort((a, b) => {
        const rankDiff = Number(b.lexicalRank || 0) - Number(a.lexicalRank || 0);
        if (rankDiff !== 0) return rankDiff;
        const searchableDiff = String(b.searchableAt || "").localeCompare(String(a.searchableAt || ""));
        if (searchableDiff !== 0) return searchableDiff;
        return Number(a.orderRank || 0) - Number(b.orderRank || 0);
      })
      .slice(0, limit);
  }
  const noActiveRetrievalChunksClause =
    "NOT EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1)";
  const primaryActiveClause = options?.allowActiveDocumentChunkSearch ? "1 = 1" : noActiveRetrievalChunksClause;
  const useScopedDocumentIds = scopedDocumentIds.length > 0;
  const documentScopeClause = useScopedDocumentIds ? `WHERE d.id IN (${scopedDocumentIds.map(() => "?").join(",")})` : where;
  const documentScopeParams = useScopedDocumentIds ? scopedDocumentIds : params;
  // This statement binds 18 params per term (4 match/rank builders) + the scope params twice + a LIMIT.
  const boundedTerms = boundLexicalTermsForD1(terms, 18, documentScopeParams.length * 2 + 1);
  const primaryMatch = buildLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", boundedTerms);
  const activatedMatch = buildLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", boundedTerms);
  const primaryRank = buildLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", boundedTerms);
  const activatedRank = buildLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", boundedTerms);
  try {
    const rows = await env.DB.prepare(
    `SELECT * FROM (
       SELECT
         c.id as chunkId,
         d.id as documentId,
         d.title,
         d.citation,
         d.author_name as authorName,
         d.decision_date as decisionDate,
         d.file_type as fileType,
         d.source_r2_key as sourceFileRef,
         d.source_link as sourceLink,
         d.index_codes_json as indexCodesJson,
         d.rules_sections_json as rulesSectionsJson,
         d.ordinance_sections_json as ordinanceSectionsJson,
         c.section_label as sectionLabel,
         c.paragraph_anchor as paragraphAnchor,
         c.citation_anchor as citationAnchor,
         c.chunk_text as chunkText,
         c.created_at as createdAt,
         CASE WHEN EXISTS (
           SELECT 1 FROM retrieval_search_chunks rs_active
           WHERE rs_active.document_id = d.id AND rs_active.active = 1
         ) THEN 1 ELSE 0 END as isTrustedTier,
         d.searchable_at as searchableAt,
         c.chunk_order as orderRank,
         ${primaryRank.expr} as lexicalRank
       FROM document_chunks c
       JOIN documents d ON d.id = c.document_id
       ${documentScopeClause}
       AND ${primaryActiveClause}
       AND ${primaryMatch.clause}

       UNION ALL

       SELECT
         rs.chunk_id as chunkId,
         d.id as documentId,
         d.title,
         d.citation,
         d.author_name as authorName,
         d.decision_date as decisionDate,
         d.file_type as fileType,
         d.source_r2_key as sourceFileRef,
         d.source_link as sourceLink,
         d.index_codes_json as indexCodesJson,
         d.rules_sections_json as rulesSectionsJson,
         d.ordinance_sections_json as ordinanceSectionsJson,
         rs.section_label as sectionLabel,
         rs.paragraph_anchor as paragraphAnchor,
         rs.citation_anchor as citationAnchor,
         rs.chunk_text as chunkText,
         rs.created_at as createdAt,
         1 as isTrustedTier,
         d.searchable_at as searchableAt,
         999999 as orderRank,
         ${activatedRank.expr} as lexicalRank
       FROM retrieval_search_chunks rs
       JOIN documents d ON d.id = rs.document_id
       ${documentScopeClause}
       AND rs.active = 1
       AND ${activatedMatch.clause}
     )
     ORDER BY lexicalRank DESC, searchableAt DESC, orderRank ASC
     LIMIT ?`
    )
      .bind(
        ...primaryRank.params,
        ...documentScopeParams,
        ...primaryMatch.params,
        ...activatedRank.params,
        ...documentScopeParams,
        ...activatedMatch.params,
        limit
      )
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch {
    const fallbackMatch = buildLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", boundedTerms);
    const fallbackRank = buildLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", boundedTerms);
    try {
      const rows = await env.DB.prepare(
        `SELECT
          c.id as chunkId,
          d.id as documentId,
          d.title,
          d.citation,
          d.author_name as authorName,
         d.decision_date as decisionDate,
          d.file_type as fileType,
          d.source_r2_key as sourceFileRef,
          d.source_link as sourceLink,
          d.index_codes_json as indexCodesJson,
          d.rules_sections_json as rulesSectionsJson,
          d.ordinance_sections_json as ordinanceSectionsJson,
          c.section_label as sectionLabel,
          c.paragraph_anchor as paragraphAnchor,
          c.citation_anchor as citationAnchor,
          c.chunk_text as chunkText,
          c.created_at as createdAt,
          CASE WHEN EXISTS (
            SELECT 1 FROM retrieval_search_chunks rs_active
            WHERE rs_active.document_id = d.id AND rs_active.active = 1
          ) THEN 1 ELSE 0 END as isTrustedTier
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${documentScopeClause}
         AND ${primaryActiveClause}
         AND ${fallbackMatch.clause}
         ORDER BY ${fallbackRank.expr} DESC, d.searchable_at DESC, c.chunk_order ASC
         LIMIT ?`
      )
        .bind(...documentScopeParams, ...fallbackMatch.params, ...fallbackRank.params, limit)
        .all<ChunkRow>();
      return rows.results ?? [];
    } catch (error) {
      if (isRetryableSearchError(error)) return [];
      throw error;
    }
  }
}

export async function ftsSearch(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number,
  scopedDocumentIds: string[] = [],
  options?: { allowActiveDocumentChunkSearch?: boolean; ftsQuery?: string }
): Promise<ChunkRow[]> {
  if (!searchFtsAvailable) return [];
  const ftsQuery = options?.ftsQuery ?? phraseSearchFtsQuery(query);
  if (!ftsQuery) return [];
  if (scopedDocumentIds.length > maxScopedLexicalDocumentBatchSize) {
    const out: ChunkRow[] = [];
    for (let index = 0; index < scopedDocumentIds.length; index += maxScopedLexicalDocumentBatchSize) {
      const batch = scopedDocumentIds.slice(index, index + maxScopedLexicalDocumentBatchSize);
      out.push(...(await ftsSearch(env, where, params, query, limit, batch, options)));
    }
    return out
      .sort((a, b) => {
        const rankDiff = Number(b.lexicalRank || 0) - Number(a.lexicalRank || 0);
        if (rankDiff !== 0) return rankDiff;
        const searchableDiff = String(b.searchableAt || "").localeCompare(String(a.searchableAt || ""));
        if (searchableDiff !== 0) return searchableDiff;
        return Number(a.orderRank || 0) - Number(b.orderRank || 0);
      })
      .slice(0, limit);
  }

  const noActiveRetrievalChunksClause =
    "NOT EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1)";
  const primaryActiveClause = options?.allowActiveDocumentChunkSearch ? "1 = 1" : noActiveRetrievalChunksClause;
  const useScopedDocumentIds = scopedDocumentIds.length > 0;
  const documentScopeClause = useScopedDocumentIds ? `WHERE d.id IN (${scopedDocumentIds.map(() => "?").join(",")})` : where;
  const documentScopeParams = useScopedDocumentIds ? scopedDocumentIds : params;

  try {
    const rows = await env.DB.prepare(
      `SELECT
         search_chunks_fts.chunk_id as chunkId,
         d.id as documentId,
         d.title,
         d.citation,
         d.author_name as authorName,
         d.decision_date as decisionDate,
         d.file_type as fileType,
         d.source_r2_key as sourceFileRef,
         d.source_link as sourceLink,
         d.index_codes_json as indexCodesJson,
         d.rules_sections_json as rulesSectionsJson,
         d.ordinance_sections_json as ordinanceSectionsJson,
         search_chunks_fts.section_label as sectionLabel,
         search_chunks_fts.paragraph_anchor as paragraphAnchor,
         search_chunks_fts.citation_anchor as citationAnchor,
         search_chunks_fts.chunk_text as chunkText,
         search_chunks_fts.created_at as createdAt,
         CASE WHEN search_chunks_fts.source_kind = 'retrieval' OR EXISTS (
           SELECT 1 FROM retrieval_search_chunks rs_active
           WHERE rs_active.document_id = d.id AND rs_active.active = 1
         ) THEN 1 ELSE 0 END as isTrustedTier,
         d.searchable_at as searchableAt,
         CAST(search_chunks_fts.order_rank AS INTEGER) as orderRank,
         (0 - bm25(search_chunks_fts)) as lexicalRank
       FROM search_chunks_fts
       JOIN documents d ON d.id = search_chunks_fts.document_id
       ${documentScopeClause}
         AND search_chunks_fts MATCH ?
         AND (
           (search_chunks_fts.source_kind = 'document' AND ${primaryActiveClause})
           OR (search_chunks_fts.source_kind = 'retrieval' AND CAST(search_chunks_fts.active AS INTEGER) = 1)
         )
       ORDER BY bm25(search_chunks_fts), searchableAt DESC, orderRank ASC
       LIMIT ?`
    )
      .bind(...documentScopeParams, ftsQuery, limit)
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch (error) {
    console.warn("[search-fts] query failed", error instanceof Error ? error.message : String(error));
    searchFtsAvailable = false;
    return [];
  }
}

export async function lexicalSearchWholeWord(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number,
  scopedDocumentIds: string[] = [],
  options?: { allowActiveDocumentChunkSearch?: boolean; termsOverride?: string[] }
): Promise<ChunkRow[]> {
  const terms = options?.termsOverride?.length ? options.termsOverride : wholeWordLexicalTerms(query);
  if (!terms.length) return [];
  const noActiveRetrievalChunksClause =
    "NOT EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1)";
  const primaryActiveClause = options?.allowActiveDocumentChunkSearch ? "1 = 1" : noActiveRetrievalChunksClause;
  const useScopedDocumentIds = scopedDocumentIds.length > 0;
  const documentScopeClause = useScopedDocumentIds ? `WHERE d.id IN (${scopedDocumentIds.map(() => "?").join(",")})` : where;
  const documentScopeParams = useScopedDocumentIds ? scopedDocumentIds : params;
  // Same shape as lexicalSearch: 18 params per term (4 whole-word builders) + the scope params twice + a LIMIT.
  const boundedTerms = boundLexicalTermsForD1(terms, 18, documentScopeParams.length * 2 + 1);
  const primaryMatch = buildWholeWordLexicalMatchClause("c.chunk_text", "d.citation", "d.title", "d.author_name", boundedTerms);
  const activatedMatch = buildWholeWordLexicalMatchClause("rs.chunk_text", "d.citation", "d.title", "d.author_name", boundedTerms);
  const primaryRank = buildWholeWordLexicalRankExpr("c.chunk_text", "d.citation", "d.title", "d.author_name", "c.section_label", boundedTerms);
  const activatedRank = buildWholeWordLexicalRankExpr("rs.chunk_text", "d.citation", "d.title", "d.author_name", "rs.section_label", boundedTerms);
  try {
    const rows = await env.DB.prepare(
      `SELECT * FROM (
         SELECT
           c.id as chunkId,
           d.id as documentId,
           d.title,
           d.citation,
           d.author_name as authorName,
         d.decision_date as decisionDate,
           d.file_type as fileType,
           d.source_r2_key as sourceFileRef,
           d.source_link as sourceLink,
           d.index_codes_json as indexCodesJson,
           d.rules_sections_json as rulesSectionsJson,
           d.ordinance_sections_json as ordinanceSectionsJson,
           c.section_label as sectionLabel,
           c.paragraph_anchor as paragraphAnchor,
           c.citation_anchor as citationAnchor,
           c.chunk_text as chunkText,
           c.created_at as createdAt,
           CASE WHEN EXISTS (
             SELECT 1 FROM retrieval_search_chunks rs_active
             WHERE rs_active.document_id = d.id AND rs_active.active = 1
           ) THEN 1 ELSE 0 END as isTrustedTier,
           d.searchable_at as searchableAt,
           c.chunk_order as orderRank,
           ${primaryRank.expr} as lexicalRank
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${documentScopeClause}
         AND ${primaryActiveClause}
         AND ${primaryMatch.clause}

         UNION ALL

         SELECT
           rs.chunk_id as chunkId,
           d.id as documentId,
           d.title,
           d.citation,
           d.author_name as authorName,
         d.decision_date as decisionDate,
           d.file_type as fileType,
           d.source_r2_key as sourceFileRef,
           d.source_link as sourceLink,
           d.index_codes_json as indexCodesJson,
           d.rules_sections_json as rulesSectionsJson,
           d.ordinance_sections_json as ordinanceSectionsJson,
           rs.section_label as sectionLabel,
           rs.paragraph_anchor as paragraphAnchor,
           rs.citation_anchor as citationAnchor,
           rs.chunk_text as chunkText,
           rs.created_at as createdAt,
           1 as isTrustedTier,
           d.searchable_at as searchableAt,
           999999 as orderRank,
           ${activatedRank.expr} as lexicalRank
         FROM retrieval_search_chunks rs
         JOIN documents d ON d.id = rs.document_id
         ${documentScopeClause}
         AND rs.active = 1
         AND ${activatedMatch.clause}
       )
       ORDER BY lexicalRank DESC, searchableAt DESC, orderRank ASC
       LIMIT ?`
    )
      .bind(
        ...primaryRank.params,
        ...documentScopeParams,
        ...primaryMatch.params,
        ...activatedRank.params,
        ...documentScopeParams,
        ...activatedMatch.params,
        limit
      )
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch (error) {
    if (isRetryableSearchError(error)) return [];
    throw error;
  }
}

export async function vectorSearch(env: Env, queries: string[], limit: number): Promise<Map<string, number>> {
  const out = new Map<string, number>();
  if (!env.AI) {
    return out;
  }
  const queryList = uniq(
    queries
      .map((item) => String(item || "").trim())
      .filter(Boolean)
  );
  if (!queryList.length) return out;

  const topK = Math.min(25, Math.max(limit * 2, 10));

  // SEARCH-01: run each query variant's embed + Vectorize query concurrently instead of
  // sequentially. In production every variant is its own Workers-AI embedding round-trip plus a
  // Vectorize query, so N sequential variants cost N round-trips; running them in parallel collapses
  // that to ~one round-trip. The merge below keeps the max score per chunk id, which is
  // order-independent, so the resulting map is identical to the sequential version.
  const perVariantMatches = await Promise.all(
    queryList.map(async (query): Promise<VectorizeMatch[]> => {
      let vector: number[] | null = null;
      try {
        vector = await embed(env, query);
      } catch (error) {
        if (isRetryableSearchError(error)) return [];
        throw error;
      }
      if (!vector) return [];

      try {
        const matches = await env.VECTOR_INDEX.query(vector, {
          topK,
          namespace: env.VECTOR_NAMESPACE,
          returnMetadata: true
        });
        return matches.matches;
      } catch {
        try {
          const matches = await env.VECTOR_INDEX.query(vector, {
            topK,
            returnMetadata: true
          });
          return matches.matches;
        } catch {
          // Vectorize query contract can vary across local/remote proxy modes.
          return [];
        }
      }
    })
  );

  for (const matches of perVariantMatches) {
    for (const match of matches) {
      const score = typeof match.score === "number" ? Math.max(0, Math.min(1, match.score)) : 0;
      const prior = out.get(match.id) ?? 0;
      if (score > prior) out.set(match.id, score);
    }
  }

  return out;
}

export async function vectorSearchWithDiagnostics(env: Env, queries: string[], limit: number): Promise<{
  scores: Map<string, number>;
  aiAvailable: boolean;
  vectorQueryAttempted: boolean;
  vectorMatchCount: number;
}> {
  const aiAvailable = Boolean(env.AI);
  if (!aiAvailable) {
    return {
      scores: new Map(),
      aiAvailable,
      vectorQueryAttempted: false,
      vectorMatchCount: 0
    };
  }

  let scores = new Map<string, number>();
  try {
    scores = await vectorSearch(env, queries, limit);
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
  }
  return {
    scores,
    aiAvailable,
    vectorQueryAttempted: true,
    vectorMatchCount: scores.size
  };
}

export async function fetchScopedDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  limit: number
): Promise<string[]> {
  if (!limit || limit <= 0) return [];
  try {
    const rows = await env.DB.prepare(
      `SELECT
        d.id as documentId
       FROM documents d
       ${where}
       ORDER BY
         CASE WHEN EXISTS (
           SELECT 1 FROM retrieval_search_chunks rs_active
           WHERE rs_active.document_id = d.id AND rs_active.active = 1
         ) THEN 1 ELSE 0 END DESC,
         COALESCE(d.decision_date, '') DESC,
         COALESCE(d.searchable_at, '') DESC,
         d.id ASC
       LIMIT ?`
    )
      .bind(...params, limit)
      .all<{ documentId: string }>();
    return uniq((rows.results || []).map((row) => row.documentId).filter(Boolean));
  } catch (error) {
    if (isRetryableSearchError(error)) return [];
    throw error;
  }
}

export async function fetchChunksByIds(
  env: Env,
  chunkIds: string[],
  where: string,
  params: Array<string | number>
): Promise<ChunkRow[]> {
  if (chunkIds.length === 0) return [];
  if (chunkIds.length > maxSqliteIdBatchSize) {
    const out: ChunkRow[] = [];
    for (let index = 0; index < chunkIds.length; index += maxSqliteIdBatchSize) {
      const batch = chunkIds.slice(index, index + maxSqliteIdBatchSize);
      out.push(...(await fetchChunksByIds(env, batch, where, params)));
    }
    return out;
  }
  const placeholders = chunkIds.map(() => "?").join(",");
  try {
    const rows = await env.DB.prepare(
    `SELECT
      c.id as chunkId,
      d.id as documentId,
      d.title,
      d.citation,
      d.author_name as authorName,
         d.decision_date as decisionDate,
      d.file_type as fileType,
      d.source_r2_key as sourceFileRef,
      d.source_link as sourceLink,
      d.index_codes_json as indexCodesJson,
      d.rules_sections_json as rulesSectionsJson,
      d.ordinance_sections_json as ordinanceSectionsJson,
      c.section_label as sectionLabel,
      c.paragraph_anchor as paragraphAnchor,
      c.citation_anchor as citationAnchor,
      c.chunk_text as chunkText,
      c.created_at as createdAt,
      CASE WHEN EXISTS (
        SELECT 1 FROM retrieval_search_chunks rs_active
        WHERE rs_active.document_id = d.id AND rs_active.active = 1
      ) THEN 1 ELSE 0 END as isTrustedTier
     FROM document_chunks c
     JOIN documents d ON d.id = c.document_id
     ${where}
     AND c.id IN (${placeholders})

     UNION ALL

     SELECT
      rs.chunk_id as chunkId,
      d.id as documentId,
      d.title,
      d.citation,
      d.author_name as authorName,
         d.decision_date as decisionDate,
      d.file_type as fileType,
      d.source_r2_key as sourceFileRef,
      d.source_link as sourceLink,
      d.index_codes_json as indexCodesJson,
      d.rules_sections_json as rulesSectionsJson,
      d.ordinance_sections_json as ordinanceSectionsJson,
      rs.section_label as sectionLabel,
      rs.paragraph_anchor as paragraphAnchor,
      rs.citation_anchor as citationAnchor,
      rs.chunk_text as chunkText,
      rs.created_at as createdAt,
      1 as isTrustedTier
     FROM retrieval_search_chunks rs
     JOIN documents d ON d.id = rs.document_id
     ${where}
     AND rs.active = 1
     AND rs.chunk_id IN (${placeholders})`
    )
      .bind(...params, ...chunkIds, ...params, ...chunkIds)
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch {
    try {
      const rows = await env.DB.prepare(
        `SELECT
          c.id as chunkId,
          d.id as documentId,
          d.title,
          d.citation,
          d.author_name as authorName,
         d.decision_date as decisionDate,
          d.file_type as fileType,
          d.source_r2_key as sourceFileRef,
          d.source_link as sourceLink,
          d.index_codes_json as indexCodesJson,
          d.rules_sections_json as rulesSectionsJson,
          d.ordinance_sections_json as ordinanceSectionsJson,
          c.section_label as sectionLabel,
          c.paragraph_anchor as paragraphAnchor,
          c.citation_anchor as citationAnchor,
          c.chunk_text as chunkText,
          c.created_at as createdAt,
          CASE WHEN EXISTS (
            SELECT 1 FROM retrieval_search_chunks rs_active
            WHERE rs_active.document_id = d.id AND rs_active.active = 1
          ) THEN 1 ELSE 0 END as isTrustedTier
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${where}
         AND c.id IN (${placeholders})`
      )
        .bind(...params, ...chunkIds)
        .all<ChunkRow>();
      return rows.results ?? [];
    } catch (error) {
      if (isRetryableSearchError(error)) return [];
      throw error;
    }
  }
}

export async function fetchChunksByDocumentIds(
  env: Env,
  documentIds: string[],
  where: string,
  params: Array<string | number>,
  decisionLayerSectionsOnly = false
): Promise<ChunkRow[]> {
  if (!documentIds.length) return [];
  if (documentIds.length > maxSqliteIdBatchSize) {
    const out: ChunkRow[] = [];
    for (let index = 0; index < documentIds.length; index += maxSqliteIdBatchSize) {
      const batch = documentIds.slice(index, index + maxSqliteIdBatchSize);
      out.push(...(await fetchChunksByDocumentIds(env, batch, where, params, decisionLayerSectionsOnly)));
    }
    return out;
  }
  const placeholders = documentIds.map(() => "?").join(",");
  const documentSectionClause = decisionLayerSectionsOnly ? decisionLayerSectionLabelClause("c.section_label") : "";
  const retrievalSectionClause = decisionLayerSectionsOnly ? decisionLayerSectionLabelClause("rs.section_label") : "";
  try {
    const rows = await env.DB.prepare(
      `SELECT
        c.id as chunkId,
        d.id as documentId,
        d.title,
        d.citation,
        d.author_name as authorName,
         d.decision_date as decisionDate,
        d.file_type as fileType,
        d.source_r2_key as sourceFileRef,
        d.source_link as sourceLink,
        d.index_codes_json as indexCodesJson,
        d.rules_sections_json as rulesSectionsJson,
        d.ordinance_sections_json as ordinanceSectionsJson,
        c.section_label as sectionLabel,
        c.paragraph_anchor as paragraphAnchor,
        c.citation_anchor as citationAnchor,
        c.chunk_text as chunkText,
        c.created_at as createdAt,
        CASE WHEN EXISTS (
          SELECT 1 FROM retrieval_search_chunks rs_active
          WHERE rs_active.document_id = d.id AND rs_active.active = 1
        ) THEN 1 ELSE 0 END as isTrustedTier
       FROM document_chunks c
       JOIN documents d ON d.id = c.document_id
       ${where}
       AND d.id IN (${placeholders})${documentSectionClause}

       UNION ALL

       SELECT
        rs.chunk_id as chunkId,
        d.id as documentId,
        d.title,
        d.citation,
        d.author_name as authorName,
         d.decision_date as decisionDate,
        d.file_type as fileType,
        d.source_r2_key as sourceFileRef,
        d.source_link as sourceLink,
        d.index_codes_json as indexCodesJson,
        d.rules_sections_json as rulesSectionsJson,
        d.ordinance_sections_json as ordinanceSectionsJson,
        rs.section_label as sectionLabel,
        rs.paragraph_anchor as paragraphAnchor,
        rs.citation_anchor as citationAnchor,
        rs.chunk_text as chunkText,
        rs.created_at as createdAt,
        1 as isTrustedTier
       FROM retrieval_search_chunks rs
       JOIN documents d ON d.id = rs.document_id
       ${where}
       AND rs.active = 1
       AND d.id IN (${placeholders})${retrievalSectionClause}`
    )
      .bind(...params, ...documentIds, ...params, ...documentIds)
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch {
    try {
      const rows = await env.DB.prepare(
        `SELECT
          c.id as chunkId,
          d.id as documentId,
          d.title,
          d.citation,
          d.author_name as authorName,
         d.decision_date as decisionDate,
          d.file_type as fileType,
          d.source_r2_key as sourceFileRef,
          d.source_link as sourceLink,
          d.index_codes_json as indexCodesJson,
          d.rules_sections_json as rulesSectionsJson,
          d.ordinance_sections_json as ordinanceSectionsJson,
          c.section_label as sectionLabel,
          c.paragraph_anchor as paragraphAnchor,
          c.citation_anchor as citationAnchor,
          c.chunk_text as chunkText,
          c.created_at as createdAt,
          CASE WHEN EXISTS (
            SELECT 1 FROM retrieval_search_chunks rs_active
            WHERE rs_active.document_id = d.id AND rs_active.active = 1
          ) THEN 1 ELSE 0 END as isTrustedTier
         FROM document_chunks c
         JOIN documents d ON d.id = c.document_id
         ${where}
         AND d.id IN (${placeholders})${documentSectionClause}`
      )
        .bind(...params, ...documentIds)
        .all<ChunkRow>();
      return rows.results ?? [];
    } catch (error) {
      if (isRetryableSearchError(error)) return [];
      throw error;
    }
  }
}

export async function fetchDecisionLayerChunksCached(
  env: Env,
  documentIds: string[],
  where: string,
  params: Array<string | number>,
  cache: Map<string, ChunkRow[]>
): Promise<ChunkRow[]> {
  if (!documentIds.length) return [];
  const missing = documentIds.filter((documentId) => !cache.has(documentId));
  if (missing.length > 0) {
    const fetched = await fetchChunksByDocumentIds(env, missing, where, params, true);
    const grouped = new Map<string, ChunkRow[]>();
    for (const row of fetched) {
      const current = grouped.get(row.documentId) || [];
      current.push(row);
      grouped.set(row.documentId, current);
    }
    for (const documentId of missing) {
      cache.set(documentId, grouped.get(documentId) ?? []);
    }
  }
  const out: ChunkRow[] = [];
  for (const documentId of documentIds) {
    const rows = cache.get(documentId);
    if (rows && rows.length > 0) out.push(...rows);
  }
  return out;
}

export async function fetchSupportingFactChunksByDocumentIds(
  env: Env,
  documentIds: string[],
  where: string,
  params: Array<string | number>,
  context: SearchContext,
  cache?: Map<string, ChunkRow[]>
): Promise<ChunkRow[]> {
  if (!documentIds.length) return [];
  const allRows = cache
    ? await fetchDecisionLayerChunksCached(env, documentIds, where, params, cache)
    : await fetchChunksByDocumentIds(env, documentIds, where, params, true);
  const supportRows = allRows.filter((row) => isSupportingFactSectionLabel(row.sectionLabel || ""));
  const queryDerived = getQueryDerivedContext(context);
  if (!queryDerived.habitabilityServiceQuery) {
    return supportRows;
  }

  const requiredConditionSignals = queryDerived.requiredHabitabilitySignals;
  const wantsReportingSignals = /\breport(?:ed|ing)?|complain(?:ed|ing)?|notified|notice\b/.test(queryDerived.normalizedQuery);
  const wantsRepairFailureSignals = /\brepair|repairs|restore|restored|service|services\b/.test(queryDerived.normalizedQuery);
  const reportingPatterns = [
    /\breport(?:ed|ing)?\b/g,
    /\bcomplain(?:ed|ing)?\b/g,
    /\bnotified\b/g,
    /\bnotice\b/g,
    /\brepair request\b/g,
    /\bwork order\b/g
  ];
  const repairFailurePatterns = [
    /\bfailed to repair\b/g,
    /\bdid not repair\b/g,
    /\brefused to repair\b/g,
    /\bnot repaired\b/g,
    /\bfailed to restore\b/g,
    /\bdid not restore\b/g,
    /\brestore service\b/g,
    /\bservice restoration\b/g,
    /\brestored service\b/g
  ];

  const grouped = new Map<string, ChunkRow[]>();
  for (const row of supportRows) {
    const current = grouped.get(row.documentId) || [];
    current.push(row);
    grouped.set(row.documentId, current);
  }

  const prioritized: ChunkRow[] = [];
  for (const rows of grouped.values()) {
    const scored = rows
      .map((row) => {
        const normalizedText = cachedNormalizedChunkText(row, context);
        const conditionHits = requiredConditionSignals.filter((signal) => textContainsIssueSignal(normalizedText, signal)).length;
        const reportingHits = reportingPatterns.reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
        const repairFailureHits = repairFailurePatterns.reduce((sum, pattern) => sum + (normalizedText.match(pattern)?.length || 0), 0);
        let priorityScore = conditionHits * 6;
        if (wantsReportingSignals) priorityScore += Math.min(4, reportingHits * 2);
        if (wantsRepairFailureSignals) priorityScore += Math.min(6, repairFailureHits * 2);
        if (isFindingsLikeSectionLabel(row.sectionLabel || "")) priorityScore += 1.5;
        return { row, priorityScore };
      })
      .sort((a, b) => b.priorityScore - a.priorityScore);

    const targeted = scored.filter((item) => item.priorityScore > 0).map((item) => item.row);
    prioritized.push(...(targeted.length > 0 ? targeted : rows));
  }

  return prioritized;
}

export async function fetchAuthorityChunksByDocumentIds(
  env: Env,
  documentIds: string[],
  where: string,
  params: Array<string | number>,
  cache?: Map<string, ChunkRow[]>
): Promise<ChunkRow[]> {
  if (!documentIds.length) return [];
  const allRows = cache
    ? await fetchDecisionLayerChunksCached(env, documentIds, where, params, cache)
    : await fetchChunksByDocumentIds(env, documentIds, where, params, true);
  return allRows.filter((row) => isConclusionsLikeSectionLabel(row.sectionLabel || ""));
}
