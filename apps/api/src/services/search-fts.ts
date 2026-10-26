import { indexedLexicalSearch } from "./search-indexed-lexical";
import { withinProviderDeadline } from "../lib/provider-operation";
import { planVectorFilters, type VectorSearchScope } from "./vector-filter-metadata";
import { vectorProfile } from "./vector-profile";
import { vectorLifecycleSnapshot, resolveCurrentVectorMatches } from "./vector-generations";
import { trustLabelSql } from "./publication-policy";
// DB fetch / FTS / scope-execution layer extracted from search.ts (SEARCH-02c module split, step 5c).
//
// The env.DB side of retrieval: FTS + lexical + vector search execution, candidate-document fetching,
// chunk hydration, and the document-facet / runtime-index bootstrap. Builds on the pure query-analysis
// layer (search-query-analysis) plus the text / concept / classification / lexical-SQL leaves. It owns
// the retrieval memoization flags; searchFtsAvailable is an `export let` because runSearchInternal (which
// stays in search.ts) reads it as a live binding. No back-references into scoring / orchestration.
import { embedWithOutcome } from "./embeddings";
import { normalizeFilterValue } from "./legal-references";
import { phraseSearchFtsQuery } from "./search-concepts";
import { buildLexicalMatchClause, buildLexicalRankExpr } from "./search-lexical-sql";
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

// Identity sentinel for "empty because the FTS query ERRORED", as opposed to a genuine zero-match.
// Callers that treat FTS emptiness as PROOF (the NS-29 futility skip) must distinguish the two —
// otherwise one transient D1 hiccup would be read as "the corpus provably contains nothing" and a
// query that should fall back to the scan would return a wrong empty result.
export const FTS_SEARCH_ERROR_RESULT: ChunkRow[] = [];

// NS-37: real document frequencies from the FTS index via an fts5vocab reader (a virtual table over
// the existing index — no data copy, so the lazy CREATE is safe pre-migration). Used to rank
// relaxation groups by rarity: the token-LENGTH selectivity proxy prefers corpus-ubiquitous words
// ("decision" appears in 221k docs; "katayama" in 870), which made relaxed tiers pick guard-hostile
// groups. Unavailable vocab (older SQLite build) degrades to an empty map and callers keep the
// length proxy.
let ftsVocabAvailable = true;
let ftsVocabEnsured = false;

export async function fetchFtsTokenDocFrequencies(env: Env, tokens: string[]): Promise<Map<string, number>> {
  const out = new Map<string, number>();
  const wanted = uniq(tokens.map((token) => String(token || "").trim().toLowerCase()).filter(Boolean)).slice(0, 20);
  if (!ftsVocabAvailable || !searchFtsAvailable || wanted.length === 0) return out;
  try {
    if (!ftsVocabEnsured) {
      await env.DB.prepare(
        "CREATE VIRTUAL TABLE IF NOT EXISTS search_chunks_fts_vocab USING fts5vocab(search_chunks_fts, 'row')"
      ).run();
      ftsVocabEnsured = true;
    }
    const placeholders = wanted.map(() => "?").join(",");
    const rows = await env.DB.prepare(
      `SELECT term, doc FROM search_chunks_fts_vocab WHERE term IN (${placeholders})`
    )
      .bind(...wanted)
      .all<{ term: string; doc: number }>();
    for (const row of rows.results || []) out.set(row.term, Number(row.doc) || 0);
  } catch (error) {
    ftsVocabAvailable = false;
    console.warn("[search-fts] fts5vocab unavailable", error instanceof Error ? error.message : String(error || ""));
  }
  return out;
}

// D1 can hit bind-variable limits sooner than stock SQLite in these UNION-heavy queries.
// Keep batches small so paged retrieval does not fail on broader searches.
const maxSqliteIdBatchSize = 30;




const documentFacetTableDdlStatements = [
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

const documentFacetIndexCodeBackfillSql = `INSERT OR IGNORE INTO document_index_codes (document_id, code, normalized_code)
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

const documentFacetRulesBackfillSql = `INSERT OR IGNORE INTO document_rules_sections (document_id, section, normalized_section)
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

const documentFacetOrdinanceBackfillSql = `INSERT OR IGNORE INTO document_ordinance_sections (document_id, section, normalized_section)
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

// Keep the facet tables in sync on document writes during the pre-migration bridge window. Copied
// verbatim from migrations/0009_document_facets.sql (triggers section). On a migrated DB these are
// no-ops (CREATE TRIGGER IF NOT EXISTS).
const documentFacetSyncTriggerStatements = [
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

// FACET-01 safety net. Index/rules/ordinance filters query the document_* facet tables (created by
// migration 0009). Because production migrations are decoupled from code deploys (REL-02), the facet
// code can ship before 0009 is applied, and `no such table` is not a retryable error — so filtered
// searches would throw. This lazily provisions the same tables/indexes/triggers as 0009 (idempotent),
// mirroring ensureSearchFts, and backfills once when empty. The DDL is copied verbatim from
// migrations/0009_document_facets.sql, which is immutable once applied (a future change would be 0010),
// so the two cannot drift. A populated corpus skips the backfill scan entirely.
async function ensureDocumentFacetTables(env: Env): Promise<void> {
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
        ON retrieval_search_chunks (document_id, active, batch_id)`,
      // PROD-PERF-01: the corpus-scope WHERE embeds EXISTS probes correlated on
      // document_chunks.document_id, which had NO index — every evaluated document row scanned up
      // to 467k chunk rows. Invisible locally (file + page cache), catastrophic on production D1:
      // measured 24.5s in the vector chunk fetch alone (4 batches x 30 rows x ~200k-row scans).
      `CREATE INDEX IF NOT EXISTS idx_document_chunks_doc
        ON document_chunks (document_id)`
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

async function ensureSearchFts(env: Env): Promise<boolean> {
  try {
    // FTS schema and the one-time corpus backfill belong to D1 migrations. Performing either
    // operation in a request races across cold isolates and can duplicate every FTS row.
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
           d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
           d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
           d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
  const candidates=await indexedLexicalSearch(env,where,params,lexicalTerms(phraseQuery),1600);
  pushIds(candidates.map(row=>row.documentId));

  return documentIds.slice(0, limit);
}

async function fetchOwnerMoveInOrdinanceFallbackDocumentIds(
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
  const normalizedQueryContext={normalizedQuery:normalize(query||"")};
  const terms=keywordCandidateTerms(query,normalizedQueryContext);
  if(!terms.length)return [];
  const rows=await indexedLexicalSearch(env,where,params,terms,2000,scopedDocumentIds,{wholeWord:keywordBoundaryGuardTerms(query,normalizedQueryContext).length>0,allowActiveDocumentChunkSearch:true});
  return [...new Set(rows.map(row=>row.documentId))].slice(0,limit);
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
  if(!terms.length)return [];
  return indexedLexicalSearch(env,where,params,terms,limit,scopedDocumentIds,options);
}

export async function ftsSearch(
  env: Env,
  where: string,
  params: Array<string | number>,
  query: string,
  limit: number,
  scopedDocumentIds: string[] = [],
  options?: { allowActiveDocumentChunkSearch?: boolean; ftsQuery?: string; scanParityRankTerms?: string[] }
): Promise<ChunkRow[]> {
  if (!searchFtsAvailable) return [];
  const ftsQuery = options?.ftsQuery ?? phraseSearchFtsQuery(query);
  if (!ftsQuery) return [];

  const noActiveRetrievalChunksClause =
    "NOT EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1)";
  const primaryActiveClause = options?.allowActiveDocumentChunkSearch ? "1 = 1" : noActiveRetrievalChunksClause;
  const useScopedDocumentIds = scopedDocumentIds.length > 0;
  const documentScopeClause = where+(useScopedDocumentIds?" AND d.id IN (SELECT value FROM json_each(?))":"");
  const documentScopeParams = [...params,...(useScopedDocumentIds?[JSON.stringify(scopedDocumentIds)]:[])];

  // Scan-parity mode (NS-30): callers replacing the substring fallback scan pass its execution terms,
  // and this query reproduces the scan over FTS-recalled rows only — the scan's match clause as a
  // filter (so rows only the index would match, e.g. section-label-only hits, are excluded exactly as
  // the scan excludes them), the scan's weighted instr() rank expression, and the scan's tiebreak
  // order (retrieval rows carry the scan's sentinel 999999 orderRank). bm25 order stays for the
  // phrase path.
  const parityTerms=options?.scanParityRankTerms?.length?boundLexicalTermsForD1(options.scanParityRankTerms,9,documentScopeParams.length+2):[];
  const parityRank = parityTerms.length
    ? buildLexicalRankExpr(
        "search_chunks_fts.chunk_text",
        "d.citation",
        "d.title",
        "d.author_name",
        "search_chunks_fts.section_label",
        parityTerms
      )
    : null;
  const parityMatch = parityTerms.length
    ? buildLexicalMatchClause(
        "search_chunks_fts.chunk_text",
        "d.citation",
        "d.title",
        "d.author_name",
        parityTerms
      )
    : null;
  const lexicalRankExpr = parityRank ? `(${parityRank.expr})` : "(0 - bm25(search_chunks_fts))";
  // Candidate-slate ordering must be a TOTAL order (PROD-SEARCH-01): searchable_at is an activation
  // batch timestamp shared by many documents, so rank ties at the LIMIT boundary were left to the
  // engine's row order — stable on a local SQLite file, unstable across production D1 replicas
  // (measured: "mold" returned a different top-5 on runs 40 minutes apart, with judged documents
  // falling out of the slate entirely). chunkId terminates every chain uniquely; the existing
  // preference keys are deliberately unchanged — substituting a decision-date preference here was
  // tried and STARVED older strong documents out of the bounded slate (mold lost a judged doc
  // locally). Deterministic slates make local gate runs predictive of production.
  const parityOrderRankExpr =
    "(CASE WHEN search_chunks_fts.source_kind = 'retrieval' THEN 999999 ELSE CAST(search_chunks_fts.order_rank AS INTEGER) END)";
  const orderByExpr = parityRank
    ? `lexicalRank DESC, searchableAt DESC, ${parityOrderRankExpr} ASC, chunkId ASC`
    : "bm25(search_chunks_fts), searchableAt DESC, orderRank ASC, chunkId ASC";

  try {
    const rows = await env.DB.prepare(
      `SELECT
         search_chunks_fts.chunk_id as chunkId,
         d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
         ${trustLabelSql()} as isTrustedTier,
         d.searchable_at as searchableAt,
         CAST(search_chunks_fts.order_rank AS INTEGER) as orderRank,
         ${lexicalRankExpr} as lexicalRank
       FROM search_chunks_fts
       JOIN documents d ON d.id = search_chunks_fts.document_id
       ${documentScopeClause}
         AND search_chunks_fts MATCH ?
         AND (
           (search_chunks_fts.source_kind = 'document' AND ${primaryActiveClause})
           OR (search_chunks_fts.source_kind = 'retrieval' AND CAST(search_chunks_fts.active AS INTEGER) = 1)
         )
         ${parityMatch ? `AND ${parityMatch.clause}` : ""}
       ORDER BY ${orderByExpr}
       LIMIT ?`
    )
      .bind(...(parityRank?.params ?? []), ...documentScopeParams, ftsQuery, ...(parityMatch?.params ?? []), limit)
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error || "");
    console.warn("[search-fts] query failed", message);
    // Only a STRUCTURAL absence of the FTS table/module disables the index, and only for this
    // isolate. Everything else — transient D1 contention, a bind overflow, one malformed MATCH —
    // affects this query alone: return the error sentinel so fallbacks answer it, and the next
    // request still gets the index. (Previously ANY error flipped the flag permanently, silently
    // degrading every subsequent query in the isolate to the 25-50s substring-scan path with a
    // different candidate slate — observed as golden rank flapping under load.)
    if (/no such table|no such module|no such column/i.test(message)) searchFtsAvailable = false;
    return FTS_SEARCH_ERROR_RESULT;
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
  if(!terms.length)return [];
  return indexedLexicalSearch(env,where,params,terms,limit,scopedDocumentIds,{...options,wholeWord:true});
}

export async function vectorSearch(
  env: Env,
  queries: string[],
  limit: number,
  scope?:VectorSearchScope
): Promise<{ scores: Map<string, number>; errored: boolean; firstErrorMessage: string; attempted:boolean }> {
  const out = new Map<string, number>();
  if (!env.AI || !env.VECTOR_INDEX) {
    return { scores: out, errored: false, firstErrorMessage: "", attempted:false };
  }
  const profile=await vectorProfile(env);
  let lifecycle:{ready:boolean;epoch:number};
  try {
    lifecycle=await vectorLifecycleSnapshot(env);
    if(!lifecycle.ready) return {scores:out,errored:true,firstErrorMessage:"vector_generation_sync_pending",attempted:false};
  }catch { return {scores:out,errored:true,firstErrorMessage:"vector_generation_schema_unavailable",attempted:false}; }
  const filterPlan=await planVectorFilters(env,scope);
  if(!filterPlan.filters.length) return {scores:out,errored:Boolean(filterPlan.reason),firstErrorMessage:filterPlan.reason||'',attempted:false};
  let attempted=false;
  const queryList = uniq(
    queries
      .map((item) => String(item || "").trim())
      .filter(Boolean)
  );
  if (!queryList.length) return { scores: out, errored: false, firstErrorMessage: "", attempted:false };

  // NS-10/NS-21: 25 was an afterthought ceiling over a 667k-chunk index. The merge below reads
  // only match.id + match.score, so metadata is not requested (Vectorize caps metadata-returning
  // queries at a much lower topK) and the ceiling rises to Vectorize's 100.
  const topK = Math.min(100, Math.max(limit * 2, 40));
  let errored = false;
  let firstErrorMessage = "";
  const recordError = (error: unknown) => {
    errored = true;
    if (!firstErrorMessage) firstErrorMessage = error instanceof Error ? error.message : String(error || "unknown");
  };

  // SEARCH-01: run each query variant's embed + Vectorize query concurrently instead of
  // sequentially. In production every variant is its own Workers-AI embedding round-trip plus a
  // Vectorize query, so N sequential variants cost N round-trips; running them in parallel collapses
  // that to ~one round-trip. The merge below keeps the max score per chunk id, which is
  // order-independent, so the resulting map is identical to the sequential version.
  const perVariantMatches = await Promise.all(
    queryList.map(async (query): Promise<VectorizeMatch[]> => {
      let vector: number[] | null = null;
      try {
        env.SEARCH_WORK_BUDGET?.consume('embeddingTexts');
        const outcome = await embedWithOutcome(env, query, { isQuery: true, signal:env.SEARCH_WORK_BUDGET?.signal, deadlineAt:env.SEARCH_WORK_BUDGET?.deadlineAt });
        vector = outcome.vector;
        if (outcome.reason && outcome.reason !== "unavailable") recordError(`embedding_${outcome.reason}`);
      } catch (error) {
        recordError(error);
        if (isRetryableSearchError(error)) return [];
        throw error;
      }
      if (!vector) return [];

      try {
        attempted=true;
        const partitions=await Promise.allSettled(filterPlan.filters.map(async filter=>{env.SEARCH_WORK_BUDGET?.consume('vectorQueries');return withinProviderDeadline({signal:env.SEARCH_WORK_BUDGET?.signal,deadlineAt:env.SEARCH_WORK_BUDGET?.deadlineAt},15_000,()=>env.VECTOR_INDEX!.query(vector,{topK,namespace:profile.namespace,filter}));}));
        for(const partition of partitions) if(partition.status==='rejected') throw partition.reason;
        return partitions.flatMap(partition=>partition.status==='fulfilled'?partition.value.matches:[]);
      }catch(error) {recordError(error);return [];}

    })
  );

  // A publication change while the provider was searching invalidates this
  // recall set. It must be synchronized before a later semantic attempt.
  const after=await vectorLifecycleSnapshot(env);
  if(!after.ready||after.epoch!==lifecycle.epoch) return {scores:out,errored:true,firstErrorMessage:"vector_generation_changed_during_query",attempted};
  const identities=await resolveCurrentVectorMatches(env,perVariantMatches.flat().map(match=>match.id),profile.id);
  for (const matches of perVariantMatches) {
    for (const match of matches) {
      const chunkId=identities.get(match.id);
      if(!chunkId) continue;
      const score = typeof match.score === "number" ? Math.max(0, Math.min(1, match.score)) : 0;
      const prior = out.get(chunkId) ?? 0;
      if (score > prior) out.set(chunkId, score);
    }
  }

  return { scores: out, errored, firstErrorMessage, attempted };
}

export async function vectorSearchWithDiagnostics(env: Env, queries: string[], limit: number, scope?:VectorSearchScope): Promise<{
  scores: Map<string, number>;
  aiAvailable: boolean;
  vectorQueryAttempted: boolean;
  vectorMatchCount: number;
  vectorErrored: boolean;
  vectorErrorMessage: string;
}> {
  const aiAvailable = Boolean(env.AI);
  if (!aiAvailable) {
    return {
      scores: new Map(),
      aiAvailable,
      vectorQueryAttempted: false,
      vectorMatchCount: 0,
      vectorErrored: false,
      vectorErrorMessage: ""
    };
  }

  let scores = new Map<string, number>();
  let vectorErrored = false;
  let vectorErrorMessage = "";
  let vectorQueryAttempted=false;
  try {
    const result = await vectorSearch(env, queries, limit, scope);
    scores = result.scores;
    vectorQueryAttempted=result.attempted;
    vectorErrored = result.errored;
    vectorErrorMessage = result.firstErrorMessage;
  } catch (error) {
    if (!isRetryableSearchError(error)) throw error;
    vectorErrored = true;
    vectorErrorMessage = error instanceof Error ? error.message : String(error || "unknown");
  }
  // NS-27: a dead vector channel must be distinguishable from "no semantic matches" — both in the
  // debug response (vectorErrored below) and in logs for production visibility.
  if (vectorErrored) {
    console.warn("[search-vector] query-channel error", vectorErrorMessage);
  }
  return {
    scores,
    aiAvailable,
    vectorQueryAttempted,
    vectorMatchCount: scores.size,
    vectorErrored,
    vectorErrorMessage
  };
}

// NS-11: match-aware variant of fetchScopedDocumentIds for the keyword-family + judge universe.
// Same tiebreak order (trusted tier, decision date, activation recency), but only documents with at
// least one FTS match for the query's execution terms qualify — so the 200-doc cap trims the
// LEAST-RECENT MATCHES instead of silently making older matching decisions unreachable behind
// non-matching recent ones ("recency masquerading as relevance").
export async function fetchFtsMatchingDocumentIds(
  env: Env,
  where: string,
  params: Array<string | number>,
  ftsQuery: string,
  limit: number
): Promise<string[]> {
  if (!limit || limit <= 0 || !searchFtsAvailable || !ftsQuery) return [];
  try {
    const rows = await env.DB.prepare(
      `SELECT
        d.id as documentId
       FROM documents d
       ${where}
       AND EXISTS (
         SELECT 1 FROM search_chunks_fts
         WHERE search_chunks_fts.document_id = d.id
           AND search_chunks_fts MATCH ?
           AND (
             search_chunks_fts.source_kind = 'retrieval' AND CAST(search_chunks_fts.active AS INTEGER) = 1
             OR search_chunks_fts.source_kind = 'document'
           )
       )
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
      .bind(...params, ftsQuery, limit)
      .all<{ documentId: string }>();
    return (rows.results || []).map((row) => row.documentId);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error || "");
    console.warn("[search-fts] matching-universe query failed", message);
    if (/no such table|no such module|no such column/i.test(message)) searchFtsAvailable = false;
    return [];
  }
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
  // CROSS JOIN is SQLite's join-order pin: the chunk table (entered via the selective id
  // IN-list, a per-id PK lookup) must drive, with documents probed per matched row. Left as a
  // plain JOIN, the planner drove from documents (~13k decision rows) and walked EVERY chunk of
  // every document per batch — measured 24.5s on production D1 and reproduced at 29.9s locally;
  // pinned, the same batch answers in milliseconds.
  const placeholders = chunkIds.map(() => "?").join(",");
  try {
    const rows = await env.DB.prepare(
    `SELECT
      c.id as chunkId,
      d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
      ${trustLabelSql()} as isTrustedTier
     FROM document_chunks c
     CROSS JOIN documents d ON d.id = c.document_id
     ${where}
     AND c.id IN (${placeholders})

     UNION ALL

     SELECT
      rs.chunk_id as chunkId,
      d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
      ${trustLabelSql()} as isTrustedTier
     FROM retrieval_search_chunks rs
     CROSS JOIN documents d ON d.id = rs.document_id
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
          d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
          ${trustLabelSql()} as isTrustedTier
         FROM document_chunks c
         CROSS JOIN documents d ON d.id = c.document_id
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
  // Planner pin, stronger form than fetchChunksByIds needs: here the id list constrains DOCUMENTS,
  // and even with a CROSS JOIN order pin the planner kept accessing d via
  // idx_documents_search_runtime (a scan of every decision row filtered by the IN-list — ~700ms
  // locally per batch, seconds on production D1). Driving from json_each over the id array leaves
  // it no choice: SCAN ids (a handful of rows) -> d by primary key -> chunks per document via the
  // document_id indexes. Measured 763ms -> 7ms per batch. One JSON bind also replaces N id binds.
  const documentSectionClause = decisionLayerSectionsOnly ? decisionLayerSectionLabelClause("c.section_label") : "";
  const retrievalSectionClause = decisionLayerSectionsOnly ? decisionLayerSectionLabelClause("rs.section_label") : "";
  try {
    const rows = await env.DB.prepare(
      `SELECT
        c.id as chunkId,
        d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
        ${trustLabelSql()} as isTrustedTier
       FROM json_each(?) AS ids
       CROSS JOIN documents d
       CROSS JOIN document_chunks c
       ${where}
       AND d.id = ids.value
       AND c.document_id = d.id${documentSectionClause}

       UNION ALL

       SELECT
        rs.chunk_id as chunkId,
        d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
        ${trustLabelSql()} as isTrustedTier
       FROM json_each(?) AS ids
       CROSS JOIN documents d
       CROSS JOIN retrieval_search_chunks rs
       ${where}
       AND d.id = ids.value
       AND rs.document_id = d.id
       AND rs.active = 1${retrievalSectionClause}`
    )
      .bind(JSON.stringify(documentIds), ...params, JSON.stringify(documentIds), ...params)
      .all<ChunkRow>();
    return rows.results ?? [];
  } catch {
    try {
      const rows = await env.DB.prepare(
        `SELECT
          c.id as chunkId,
          d.id as documentId, d.document_revision as documentRevision, d.retrieval_revision as retrievalRevision,
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
          ${trustLabelSql()} as isTrustedTier
         FROM json_each(?) AS ids
         CROSS JOIN documents d
         CROSS JOIN document_chunks c
         ${where}
         AND d.id = ids.value
         AND c.document_id = d.id${documentSectionClause}`
      )
        .bind(JSON.stringify(documentIds), ...params)
        .all<ChunkRow>();
      return rows.results ?? [];
    } catch (error) {
      if (isRetryableSearchError(error)) return [];
      throw error;
    }
  }
}

// Request-scoped per-document cache for the decision-layer fallbacks. The authority and
// supporting-fact fallbacks request overlapping document sets with identical where/params
// and the same section prefilter, so without caching the second pass re-runs the expensive
// all-chunks fetch for documents the first pass already loaded. Caching the prefiltered rows
// per documentId returns identical rows while skipping the redundant round-trip; each
// fallback still applies its own section filtering afterward.
async function fetchDecisionLayerChunksCached(
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
  const wantsRepairFailureSignals = /\b(?:repair|repairs|restore|restored|service|services\b)/.test(queryDerived.normalizedQuery);
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
