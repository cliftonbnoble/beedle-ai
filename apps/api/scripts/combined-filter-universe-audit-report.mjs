import fs from "node:fs/promises";
import path from "node:path";
import { execFileSync } from "node:child_process";

const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.COMBINED_FILTER_UNIVERSE_AUDIT_JSON_NAME || "combined-filter-universe-audit-report.json";
const markdownName = process.env.COMBINED_FILTER_UNIVERSE_AUDIT_MARKDOWN_NAME || "combined-filter-universe-audit-report.md";
const csvName = process.env.COMBINED_FILTER_UNIVERSE_AUDIT_CSV_NAME || "combined-filter-universe-audit-report.csv";
const tasksFile = process.env.COMBINED_FILTER_UNIVERSE_TASKS_FILE
  ? path.resolve(process.cwd(), process.env.COMBINED_FILTER_UNIVERSE_TASKS_FILE)
  : process.env.COMBINED_FILTER_QA_TASKS_FILE
    ? path.resolve(process.cwd(), process.env.COMBINED_FILTER_QA_TASKS_FILE)
    : "";
const qaReportPath = path.resolve(process.cwd(), "reports/combined-filter-search-qa-report.json");
const dbDir = path.resolve(process.cwd(), ".wrangler/state/v3/d1/miniflare-D1DatabaseObject");
const indexCatalogPath = path.resolve(process.cwd(), "../../packages/shared/src/index-codes.ts");

const MANUAL_INDEX_CODE_ALIAS_MAP = {
  g27: {
    searchPhrases: ["substantial decrease in housing services", "code violation substantial decrease in housing services"]
  },
  g28: {
    searchPhrases: [
      "not substantial decrease in housing services",
      "decrease in service not substantial",
      "not a substantial decrease in housing services",
      "does not constitute a substantial decrease in housing services",
      "did not constitute a substantial decrease in housing services"
    ]
  },
  g93: {
    searchPhrases: [
      "uniform hotel visitor policy",
      "visitor policy for residential hotel",
      "uniform visitor policy",
      "rent reduction for noncompliance with uniform policy",
      "supplemental visitor policy",
      "chapter 41d"
    ]
  }
};

const defaultTasks = [
  {
    id: "rent_reduction_katayama_g27",
    label: "Rent reduction + Katayama + G27",
    query: "rent reduction",
    filters: {
      approvedOnly: false,
      judgeNames: ["Erin E. Katayama"],
      indexCodes: ["G27"]
    }
  },
  {
    id: "decrease_services_katayama_g27_g28",
    label: "Decrease in services + Katayama + G27/G28",
    query: "decrease in services",
    filters: {
      approvedOnly: false,
      judgeNames: ["Erin E. Katayama"],
      indexCodes: ["G27", "G28"]
    }
  },
  {
    id: "visitor_policy_yick_g93",
    label: "Visitor policy + Andrew Yick + G93",
    query: "uniform visitor policy",
    filters: {
      approvedOnly: false,
      judgeNames: ["Andrew Yick"],
      indexCodes: ["G93"]
    }
  },
  {
    id: "owner_move_in_juarez_date",
    label: "Owner move-in + René Juárez + date range",
    query: "owner move in",
    filters: {
      approvedOnly: false,
      judgeNames: ["René Juárez"],
      fromDate: "2022-01-01",
      toDate: "2025-12-31"
    }
  },
  {
    id: "capital_improvement_lim_rules_index",
    label: "Capital improvements + Deborah K. Lim + R&R + index",
    query: "capital improvements",
    filters: {
      approvedOnly: false,
      judgeNames: ["Deborah K. Lim"],
      rulesSection: "6.15C",
      indexCodes: ["A1"]
    }
  }
];

function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function sqlQuote(value) {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function normalizeFilterValue(value) {
  return normalize(value).replace(/\s+/g, "");
}

function extractCatalogReferenceCitations(raw) {
  const text = String(raw || "");
  const matches = new Set();
  for (const match of text.matchAll(/\b\d+\.\d+[a-z]?(?:\([a-z0-9]+\))*[a-z]?\b/gi)) {
    matches.add(match[0]);
  }
  for (const match of text.matchAll(/§\s*([0-9]+(?:\.[0-9]+)?(?:\([a-z0-9]+\))*)/gi)) {
    if (match[1]) matches.add(match[1]);
  }
  return Array.from(matches);
}

function extractBaseCitation(value) {
  const match = String(value || "").match(/^(\d+\.\d+[a-z]?)/i);
  return match?.[1] || null;
}

function buildCitationVariants(values) {
  const variants = new Set();
  for (const value of values) {
    const trimmed = String(value || "").trim();
    if (!trimmed) continue;
    variants.add(trimmed);
    const base = extractBaseCitation(trimmed);
    if (base) variants.add(base);
  }
  return Array.from(variants);
}

async function resolveDbPath() {
  const entries = await fs.readdir(dbDir, { withFileTypes: true });
  const sqliteNames = entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".sqlite") && !entry.name.includes(".bak-"))
    .map((entry) => entry.name)
    .sort();
  if (!sqliteNames.length) {
    throw new Error(`No live sqlite DB found under ${dbDir}`);
  }
  return path.join(dbDir, sqliteNames[0]);
}

async function loadTasks() {
  if (!tasksFile) return defaultTasks;
  const raw = await fs.readFile(tasksFile, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error(`Expected an array of tasks in ${tasksFile}`);
  }
  return parsed;
}

async function loadIndexCatalog() {
  const raw = await fs.readFile(indexCatalogPath, "utf8");
  const start = raw.indexOf("[");
  const end = raw.lastIndexOf("] as const;");
  if (start < 0 || end < 0) {
    throw new Error(`Could not parse canonical index code catalog from ${indexCatalogPath}`);
  }
  return JSON.parse(raw.slice(start, end + 1));
}

async function loadLatestQaRows() {
  try {
    const raw = await fs.readFile(qaReportPath, "utf8");
    const parsed = JSON.parse(raw);
    return new Map((parsed.rows || []).map((row) => [row.id, row]));
  } catch {
    return new Map();
  }
}

function buildIndexCodeFilterContext(filters, catalogByCode) {
  const requestedCodes = unique([...(filters.indexCodes || []), filters.indexCode || ""].filter(Boolean).map((value) => String(value).trim()));
  const normalizedCodes = unique(requestedCodes.map((value) => normalizeFilterValue(value)));
  const legacyCodeAliases = new Set();
  const relatedRulesSections = new Set();
  const relatedOrdinanceSections = new Set();

  for (const normalizedCode of normalizedCodes) {
    const option = catalogByCode.get(normalizedCode);
    const manualAliases = MANUAL_INDEX_CODE_ALIAS_MAP[normalizedCode];
    for (const legacyCode of manualAliases?.legacyCodes || []) legacyCodeAliases.add(String(legacyCode).trim());
    if (!option) continue;

    for (const ruleCitation of buildCitationVariants(extractCatalogReferenceCitations(option.rules || ""))) {
      relatedRulesSections.add(ruleCitation);
    }
    for (const ordinanceCitation of buildCitationVariants(extractCatalogReferenceCitations(option.ordinance || ""))) {
      relatedOrdinanceSections.add(ordinanceCitation);
    }
  }

  return {
    requestedCodes,
    normalizedCodes,
    legacyCodeAliases: Array.from(legacyCodeAliases).filter(Boolean),
    relatedRulesSections: Array.from(relatedRulesSections),
    relatedOrdinanceSections: Array.from(relatedOrdinanceSections)
  };
}

function hasActiveRetrievalChunkClause() {
  return "EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1)";
}

function hasBasicChunkedDecisionClause() {
  return "EXISTS (SELECT 1 FROM document_chunks c_basic WHERE c_basic.document_id = d.id) AND d.source_r2_key IS NOT NULL AND COALESCE(d.title, '') != ''";
}

function buildSearchScopeSql(filters, corpusMode, catalogByCode) {
  const clauses = [
    corpusMode === "trusted_plus_provisional"
      ? `(d.file_type != 'decision_docx' OR ${hasBasicChunkedDecisionClause()} OR ${hasActiveRetrievalChunkClause()})`
      : `(d.file_type != 'decision_docx' OR ${hasActiveRetrievalChunkClause()})`,
    "d.rejected_at IS NULL",
    "d.file_type = 'decision_docx'"
  ];

  const indexCodeFilterContext = buildIndexCodeFilterContext(filters, catalogByCode);
  if (indexCodeFilterContext.requestedCodes.length > 0) {
    const compatibilityClauses = [];
    const directIndexCodeValues = unique([...indexCodeFilterContext.requestedCodes, ...indexCodeFilterContext.legacyCodeAliases]).filter(Boolean);

    if (directIndexCodeValues.length > 0) {
      compatibilityClauses.push(
        `EXISTS (
          SELECT 1 FROM document_reference_links l
          WHERE l.document_id = d.id
            AND l.reference_type = 'index_code'
            AND l.is_valid = 1
            AND (${directIndexCodeValues
              .map((code) => `(l.normalized_value = ${sqlQuote(normalizeFilterValue(code))} OR lower(l.canonical_value) = lower(${sqlQuote(code)}))`)
              .join(" OR ")})
        )`
      );
    }

    if (indexCodeFilterContext.relatedRulesSections.length > 0) {
      compatibilityClauses.push(
        `EXISTS (
          SELECT 1 FROM document_reference_links l
          WHERE l.document_id = d.id
            AND l.reference_type = 'rules_section'
            AND l.is_valid = 1
            AND (${indexCodeFilterContext.relatedRulesSections
              .map((citation) => `(l.normalized_value = ${sqlQuote(normalizeFilterValue(citation))} OR lower(l.canonical_value) = lower(${sqlQuote(citation)}))`)
              .join(" OR ")})
        )`
      );
    }

    if (indexCodeFilterContext.relatedOrdinanceSections.length > 0) {
      compatibilityClauses.push(
        `EXISTS (
          SELECT 1 FROM document_reference_links l
          WHERE l.document_id = d.id
            AND l.reference_type = 'ordinance_section'
            AND l.is_valid = 1
            AND (${indexCodeFilterContext.relatedOrdinanceSections
              .map((citation) => `(l.normalized_value = ${sqlQuote(normalizeFilterValue(citation))} OR lower(l.canonical_value) = lower(${sqlQuote(citation)}))`)
              .join(" OR ")})
        )`
      );
    }

    if (compatibilityClauses.length > 0) {
      clauses.push(`(${compatibilityClauses.join(" OR ")})`);
    }
  }

  if (filters.rulesSection) {
    clauses.push(
      `EXISTS (
        SELECT 1 FROM document_reference_links l
        WHERE l.document_id = d.id
          AND l.reference_type = 'rules_section'
          AND l.is_valid = 1
          AND (l.normalized_value = ${sqlQuote(normalizeFilterValue(filters.rulesSection))} OR lower(l.canonical_value) = lower(${sqlQuote(filters.rulesSection)}))
      )`
    );
  }

  if (filters.ordinanceSection) {
    clauses.push(
      `EXISTS (
        SELECT 1 FROM document_reference_links l
        WHERE l.document_id = d.id
          AND l.reference_type = 'ordinance_section'
          AND l.is_valid = 1
          AND (l.normalized_value = ${sqlQuote(normalizeFilterValue(filters.ordinanceSection))} OR lower(l.canonical_value) = lower(${sqlQuote(filters.ordinanceSection)}))
      )`
    );
  }

  if (filters.partyName) {
    clauses.push(`instr(lower(d.title), lower(${sqlQuote(filters.partyName)})) > 0`);
  }

  const judgeNames = unique([...(filters.judgeNames || []), filters.judgeName || ""].filter(Boolean));
  if (judgeNames.length > 0) {
    clauses.push(`(${judgeNames.map((judge) => `lower(coalesce(d.author_name, '')) = lower(${sqlQuote(judge)})`).join(" OR ")})`);
  }

  if (filters.fromDate) {
    clauses.push(`(d.decision_date IS NOT NULL AND d.decision_date >= ${sqlQuote(filters.fromDate)})`);
  }
  if (filters.toDate) {
    clauses.push(`(d.decision_date IS NOT NULL AND d.decision_date <= ${sqlQuote(filters.toDate)})`);
  }
  if (filters.approvedOnly) {
    clauses.push(`(d.file_type != 'decision_docx' OR d.approved_at IS NOT NULL OR ${hasActiveRetrievalChunkClause()})`);
  }

  return `WHERE ${clauses.join(" AND ")}`;
}

function conclusionsExistsClause(trustedOnly = false) {
  if (trustedOnly) {
    return `EXISTS (
      SELECT 1 FROM retrieval_search_chunks rs
      WHERE rs.document_id = d.id
        AND rs.active = 1
        AND replace(lower(coalesce(rs.section_label, '')), ' ', '') LIKE '%conclusionsoflaw%'
    )`;
  }
  return `(
    EXISTS (
      SELECT 1 FROM document_chunks c
      WHERE c.document_id = d.id
        AND replace(lower(coalesce(c.section_label, '')), ' ', '') LIKE '%conclusionsoflaw%'
    )
    OR EXISTS (
      SELECT 1 FROM retrieval_search_chunks rs
      WHERE rs.document_id = d.id
        AND rs.active = 1
        AND replace(lower(coalesce(rs.section_label, '')), ' ', '') LIKE '%conclusionsoflaw%'
    )
  )`;
}

function findingsExistsClause(trustedOnly = false) {
  if (trustedOnly) {
    return `EXISTS (
      SELECT 1 FROM retrieval_search_chunks rs
      WHERE rs.document_id = d.id
        AND rs.active = 1
        AND replace(lower(coalesce(rs.section_label, '')), ' ', '') LIKE '%findingsoffact%'
    )`;
  }
  return `(
    EXISTS (
      SELECT 1 FROM document_chunks c
      WHERE c.document_id = d.id
        AND replace(lower(coalesce(c.section_label, '')), ' ', '') LIKE '%findingsoffact%'
    )
    OR EXISTS (
      SELECT 1 FROM retrieval_search_chunks rs
      WHERE rs.document_id = d.id
        AND rs.active = 1
        AND replace(lower(coalesce(rs.section_label, '')), ' ', '') LIKE '%findingsoffact%'
    )
  )`;
}

function runSqliteJson(dbPath, sql) {
  const raw = execFileSync("sqlite3", ["-json", "-cmd", ".timeout 5000", dbPath, sql], { encoding: "utf8" });
  return JSON.parse(raw || "[]");
}

function countByWhere(dbPath, where, extraClause = "") {
  const sql = `SELECT count(*) as count FROM documents d ${where}${extraClause ? ` AND ${extraClause}` : ""};`;
  return Number(runSqliteJson(dbPath, sql)[0]?.count || 0);
}

function sampleDocsByWhere(dbPath, where, limit = 8) {
  const sql = `
    SELECT
      d.id as documentId,
      d.citation as citation,
      d.title as title,
      d.author_name as authorName,
      d.decision_date as decisionDate,
      CASE WHEN ${hasActiveRetrievalChunkClause()} THEN 1 ELSE 0 END as trusted,
      CASE WHEN ${conclusionsExistsClause(false)} THEN 1 ELSE 0 END as hasConclusions,
      CASE WHEN ${findingsExistsClause(false)} THEN 1 ELSE 0 END as hasFindings
    FROM documents d
    ${where}
    ORDER BY trusted DESC, COALESCE(d.decision_date, '') DESC, COALESCE(d.citation, d.id) ASC
    LIMIT ${Number(limit)};
  `;
  return runSqliteJson(dbPath, sql);
}

function buildFilterSlices(filters) {
  const slices = [];
  if ((filters.judgeNames || []).length || filters.judgeName) {
    slices.push({
      key: "judge",
      label: "Judge",
      filters: {
        judgeNames: unique([...(filters.judgeNames || []), filters.judgeName || ""].filter(Boolean))
      }
    });
  }
  if ((filters.indexCodes || []).length || filters.indexCode) {
    slices.push({
      key: "index_code",
      label: "Index code",
      filters: {
        indexCodes: unique([...(filters.indexCodes || []), filters.indexCode || ""].filter(Boolean))
      }
    });
  }
  if (filters.rulesSection) {
    slices.push({
      key: "rules_section",
      label: "R&R Section",
      filters: { rulesSection: filters.rulesSection }
    });
  }
  if (filters.ordinanceSection) {
    slices.push({
      key: "ordinance_section",
      label: "Ordinance",
      filters: { ordinanceSection: filters.ordinanceSection }
    });
  }
  if (filters.partyName) {
    slices.push({
      key: "party_name",
      label: "Party",
      filters: { partyName: filters.partyName }
    });
  }
  if (filters.fromDate || filters.toDate) {
    slices.push({
      key: "date_range",
      label: "Date range",
      filters: { fromDate: filters.fromDate, toDate: filters.toDate }
    });
  }
  return slices;
}

function chooseDiagnosis(task, qaRow, counts) {
  if (qaRow?.status === "aborted") {
    return {
      code: "runtime_stability_lane",
      summary: "Search aborted before relevance could be measured. Keep this query in the runtime lane."
    };
  }
  if (counts.allFiltersDocCount === 0) {
    const indexOnly = counts.singleFilterCounts.index_code?.docCount || 0;
    const judgeOnly = counts.singleFilterCounts.judge?.docCount || 0;
    const rulesOnly = counts.singleFilterCounts.rules_section?.docCount || 0;
    if (judgeOnly > 0 && indexOnly > 0) {
      return {
        code: "empty_intersection_judge_index_gap",
        summary: "The judge slice and index-code slice exist separately, but their intersection is empty under current metadata."
      };
    }
    if (rulesOnly > 0 && indexOnly > 0) {
      return {
        code: "empty_intersection_rules_index_gap",
        summary: "The R&R slice and index-code slice exist separately, but the combined intersection is empty."
      };
    }
    if (indexOnly === 0 && ((task.filters.indexCodes || []).length || task.filters.indexCode)) {
      return {
        code: "index_metadata_gap",
        summary: "No documents match the selected index-code filter on metadata alone."
      };
    }
    return {
      code: "empty_filtered_universe",
      summary: "No documents match the structured filters before text search starts."
    };
  }
  if (counts.allFiltersTrustedDocCount === 0) {
    return {
      code: "matching_docs_without_trusted_chunks",
      summary: "Documents match the filters, but none currently have active trusted search chunks."
    };
  }
  if (counts.prioritySectionDocCount === 0) {
    return {
      code: "matching_docs_missing_priority_sections",
      summary: "Documents match the filters, but none currently show Conclusions of Law or Findings of Fact sections."
    };
  }
  if (counts.trustedPrioritySectionDocCount === 0) {
    return {
      code: "priority_sections_not_activated",
      summary: "Documents match the filters, but their priority sections are not in the active trusted search layer."
    };
  }
  if (qaRow?.status === "zero_results") {
    return {
      code: "retrieval_gap_with_nonempty_universe",
      summary: "Documents and priority sections exist inside the filtered universe, so this looks like a remaining retrieval/ranking gap."
    };
  }
  return {
    code: "healthy_or_not_yet_classified",
    summary: "The filtered universe looks healthy for this task."
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Combined-Filter Universe Audit");
  lines.push("");
  lines.push(`- taskCount: ${report.taskCount}`);
  lines.push(`- zeroResultTaskCount: ${report.zeroResultTaskCount}`);
  lines.push(`- abortedTaskCount: ${report.abortedTaskCount}`);
  lines.push(`- zeroResultEmptyUniverseCount: ${report.zeroResultEmptyUniverseCount}`);
  lines.push(`- zeroResultNonEmptyUniverseCount: ${report.zeroResultNonEmptyUniverseCount}`);
  lines.push("");
  lines.push("## Tasks");
  lines.push("");
  for (const row of report.rows) {
    lines.push(
      `- ${row.id} | qaStatus=${row.qaStatus || "unknown"} | allFiltersDocs=${row.allFiltersDocCount} | trusted=${row.allFiltersTrustedDocCount} | conclusions=${row.allFiltersConclusionsDocCount} | findings=${row.allFiltersFindingsDocCount} | diagnosis=${row.diagnosisCode}`
    );
  }
  lines.push("");
  lines.push("## Targeted Metadata Diagnosis");
  lines.push("");
  for (const row of report.rows.filter((item) => item.qaStatus === "zero_results")) {
    lines.push(`- ${row.id}`);
    lines.push(`  diagnosis: ${row.diagnosisSummary}`);
    lines.push(`  docUniverse: ${row.allFiltersDocCount} docs`);
    lines.push(`  trustedDocs: ${row.allFiltersTrustedDocCount}`);
    lines.push(`  prioritySections: conclusions=${row.allFiltersConclusionsDocCount}, findings=${row.allFiltersFindingsDocCount}, trustedPriority=${row.trustedPrioritySectionDocCount}`);
    lines.push(`  singles: ${row.singleFilterSummary || "<none>"}`);
    lines.push(`  pairs: ${row.pairFilterSummary || "<none>"}`);
    lines.push(`  samples: ${row.sampleCitations.length ? row.sampleCitations.join(" | ") : "<none>"}`);
  }
  return `${lines.join("\n")}\n`;
}

function toCsv(report) {
  const rows = [
    [
      "id",
      "label",
      "qaStatus",
      "allFiltersDocCount",
      "allFiltersTrustedDocCount",
      "allFiltersConclusionsDocCount",
      "allFiltersFindingsDocCount",
      "trustedPrioritySectionDocCount",
      "diagnosisCode",
      "diagnosisSummary",
      "singleFilterSummary",
      "pairFilterSummary",
      "sampleCitations"
    ],
    ...report.rows.map((row) => [
      row.id,
      row.label,
      row.qaStatus || "",
      row.allFiltersDocCount,
      row.allFiltersTrustedDocCount,
      row.allFiltersConclusionsDocCount,
      row.allFiltersFindingsDocCount,
      row.trustedPrioritySectionDocCount,
      row.diagnosisCode,
      row.diagnosisSummary,
      row.singleFilterSummary,
      row.pairFilterSummary,
      row.sampleCitations.join(" | ")
    ])
  ];
  return rows.map((row) => row.map(csvEscape).join(",")).join("\n");
}

async function writeReports(report) {
  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  const csvPath = path.join(reportsDir, csvName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  await fs.writeFile(markdownPath, toMarkdown(report));
  await fs.writeFile(csvPath, `${toCsv(report)}\n`);
  return { jsonPath, markdownPath, csvPath };
}

async function main() {
  const [tasks, dbPath, qaRows, catalog] = await Promise.all([
    loadTasks(),
    resolveDbPath(),
    loadLatestQaRows(),
    loadIndexCatalog()
  ]);
  const catalogByCode = new Map(catalog.map((item) => [normalizeFilterValue(item.code), item]));
  const rows = [];

  for (const task of tasks) {
    const filters = { approvedOnly: false, ...(task.filters || {}) };
    const corpusMode = task.corpusMode || "trusted_plus_provisional";
    const where = buildSearchScopeSql(filters, corpusMode, catalogByCode);

    const allFiltersDocCount = countByWhere(dbPath, where);
    const allFiltersTrustedDocCount = countByWhere(dbPath, where, hasActiveRetrievalChunkClause());
    const allFiltersConclusionsDocCount = countByWhere(dbPath, where, conclusionsExistsClause(false));
    const allFiltersFindingsDocCount = countByWhere(dbPath, where, findingsExistsClause(false));
    const allFiltersTrustedConclusionsDocCount = countByWhere(dbPath, where, conclusionsExistsClause(true));
    const allFiltersTrustedFindingsDocCount = countByWhere(dbPath, where, findingsExistsClause(true));
    const prioritySectionDocCount = countByWhere(
      dbPath,
      where,
      `(${conclusionsExistsClause(false)} OR ${findingsExistsClause(false)})`
    );
    const trustedPrioritySectionDocCount = countByWhere(
      dbPath,
      where,
      `(${conclusionsExistsClause(true)} OR ${findingsExistsClause(true)})`
    );

    const filterSlices = buildFilterSlices(filters);
    const singleFilterCounts = {};
    for (const slice of filterSlices) {
      const sliceWhere = buildSearchScopeSql({ approvedOnly: false, ...slice.filters }, corpusMode, catalogByCode);
      singleFilterCounts[slice.key] = {
        label: slice.label,
        docCount: countByWhere(dbPath, sliceWhere)
      };
    }

    const pairFilterCounts = {};
    for (let i = 0; i < filterSlices.length; i += 1) {
      for (let j = i + 1; j < filterSlices.length; j += 1) {
        const pairKey = `${filterSlices[i].key}+${filterSlices[j].key}`;
        const pairWhere = buildSearchScopeSql(
          { approvedOnly: false, ...filterSlices[i].filters, ...filterSlices[j].filters },
          corpusMode,
          catalogByCode
        );
        pairFilterCounts[pairKey] = {
          label: `${filterSlices[i].label} + ${filterSlices[j].label}`,
          docCount: countByWhere(dbPath, pairWhere)
        };
      }
    }

    const sampleDocs = sampleDocsByWhere(dbPath, where, 8);
    const qaRow = qaRows.get(task.id) || null;
    const diagnosis = chooseDiagnosis(task, qaRow, {
      allFiltersDocCount,
      allFiltersTrustedDocCount,
      prioritySectionDocCount,
      trustedPrioritySectionDocCount,
      singleFilterCounts
    });

    rows.push({
      id: task.id,
      label: task.label || task.id,
      qaStatus: qaRow?.status || null,
      filters,
      allFiltersDocCount,
      allFiltersTrustedDocCount,
      allFiltersConclusionsDocCount,
      allFiltersFindingsDocCount,
      allFiltersTrustedConclusionsDocCount,
      allFiltersTrustedFindingsDocCount,
      prioritySectionDocCount,
      trustedPrioritySectionDocCount,
      diagnosisCode: diagnosis.code,
      diagnosisSummary: diagnosis.summary,
      singleFilterCounts,
      pairFilterCounts,
      singleFilterSummary: Object.values(singleFilterCounts)
        .map((item) => `${item.label}=${item.docCount}`)
        .join(" ; "),
      pairFilterSummary: Object.values(pairFilterCounts)
        .map((item) => `${item.label}=${item.docCount}`)
        .join(" ; "),
      sampleDocs,
      sampleCitations: sampleDocs.map((doc) => String(doc.citation || doc.documentId))
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    dbPath,
    tasksFile: tasksFile || null,
    qaReportPath,
    taskCount: rows.length,
    zeroResultTaskCount: rows.filter((row) => row.qaStatus === "zero_results").length,
    abortedTaskCount: rows.filter((row) => row.qaStatus === "aborted").length,
    zeroResultEmptyUniverseCount: rows.filter((row) => row.qaStatus === "zero_results" && row.allFiltersDocCount === 0).length,
    zeroResultNonEmptyUniverseCount: rows.filter((row) => row.qaStatus === "zero_results" && row.allFiltersDocCount > 0).length,
    rows
  };

  const paths = await writeReports(report);
  console.log(
    JSON.stringify(
      {
        taskCount: report.taskCount,
        zeroResultTaskCount: report.zeroResultTaskCount,
        abortedTaskCount: report.abortedTaskCount,
        zeroResultEmptyUniverseCount: report.zeroResultEmptyUniverseCount,
        zeroResultNonEmptyUniverseCount: report.zeroResultNonEmptyUniverseCount
      },
      null,
      2
    )
  );
  console.log(`Combined-filter universe audit JSON report written to ${paths.jsonPath}`);
  console.log(`Combined-filter universe audit Markdown report written to ${paths.markdownPath}`);
  console.log(`Combined-filter universe audit CSV report written to ${paths.csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
