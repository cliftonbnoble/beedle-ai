import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.FACET_STORAGE_AUDIT_JSON_NAME || "facet-storage-audit-report.json";
const markdownName = process.env.FACET_STORAGE_AUDIT_MARKDOWN_NAME || "facet-storage-audit-report.md";
const busyTimeoutMs = Number(process.env.FACET_STORAGE_AUDIT_BUSY_TIMEOUT_MS || "5000");
const dbDir = path.resolve(process.cwd(), ".wrangler/state/v3/d1/miniflare-D1DatabaseObject");

function parseJsonArray(value) {
  if (!value) return { values: [], malformed: false };
  try {
    const parsed = JSON.parse(value);
    return {
      values: Array.isArray(parsed) ? parsed.map((item) => String(item).trim()).filter(Boolean) : [],
      malformed: !Array.isArray(parsed)
    };
  } catch {
    return { values: [], malformed: true };
  }
}

function normalizeFacetValue(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/^ordinance\s*/i, "")
    .replace(/^rule\s*/i, "")
    .replace(/^ic[-\s]*/i, "")
    .trim();
}

function unique(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function pushCounts(map, values) {
  for (const value of values) {
    map.set(value, (map.get(value) || 0) + 1);
  }
}

function topCounts(map, limit = 20) {
  return Array.from(map.entries())
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value))
    .slice(0, limit);
}

export function buildFacetStorageReport(rows, options = {}) {
  const indexCounts = new Map();
  const rulesCounts = new Map();
  const ordinanceCounts = new Map();
  let docsWithIndexCodes = 0;
  let docsWithRulesSections = 0;
  let docsWithOrdinanceSections = 0;
  let estimatedDocumentIndexCodeRows = 0;
  let estimatedDocumentRulesRows = 0;
  let estimatedDocumentOrdinanceRows = 0;
  let malformedIndexJsonRows = 0;
  let malformedRulesJsonRows = 0;
  let malformedOrdinanceJsonRows = 0;

  for (const row of rows) {
    const index = parseJsonArray(row.indexCodesJson);
    const rules = parseJsonArray(row.rulesSectionsJson);
    const ordinance = parseJsonArray(row.ordinanceSectionsJson);
    const indexValues = unique(index.values.map(normalizeFacetValue));
    const rulesValues = unique(rules.values.map(normalizeFacetValue));
    const ordinanceValues = unique(ordinance.values.map(normalizeFacetValue));

    if (index.malformed) malformedIndexJsonRows += 1;
    if (rules.malformed) malformedRulesJsonRows += 1;
    if (ordinance.malformed) malformedOrdinanceJsonRows += 1;

    if (indexValues.length > 0) docsWithIndexCodes += 1;
    if (rulesValues.length > 0) docsWithRulesSections += 1;
    if (ordinanceValues.length > 0) docsWithOrdinanceSections += 1;

    estimatedDocumentIndexCodeRows += indexValues.length;
    estimatedDocumentRulesRows += rulesValues.length;
    estimatedDocumentOrdinanceRows += ordinanceValues.length;

    pushCounts(indexCounts, indexValues);
    pushCounts(rulesCounts, rulesValues);
    pushCounts(ordinanceCounts, ordinanceValues);
  }

  return {
    generatedAt: new Date().toISOString(),
    dbPath: options.dbPath || "",
    summary: {
      decisionRows: rows.length,
      docsWithIndexCodes,
      docsWithRulesSections,
      docsWithOrdinanceSections,
      estimatedDocumentIndexCodeRows,
      estimatedDocumentRulesRows,
      estimatedDocumentOrdinanceRows,
      distinctIndexCodes: indexCounts.size,
      distinctRulesSections: rulesCounts.size,
      distinctOrdinanceSections: ordinanceCounts.size,
      malformedIndexJsonRows,
      malformedRulesJsonRows,
      malformedOrdinanceJsonRows
    },
    likeBackedColumns: ["documents.index_codes_json", "documents.rules_sections_json", "documents.ordinance_sections_json"],
    recommendedTables: [
      { name: "document_index_codes", columns: ["document_id", "code"] },
      { name: "document_rules_sections", columns: ["document_id", "section"] },
      { name: "document_ordinance_sections", columns: ["document_id", "section"] }
    ],
    topIndexCodes: topCounts(indexCounts),
    topRulesSections: topCounts(rulesCounts),
    topOrdinanceSections: topCounts(ordinanceCounts)
  };
}

function formatMarkdown(report) {
  const lines = [
    "# Facet Storage Audit",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Database: \`${report.dbPath}\``,
    `- Decision rows: \`${report.summary.decisionRows}\``,
    `- Docs with index codes: \`${report.summary.docsWithIndexCodes}\``,
    `- Docs with rules sections: \`${report.summary.docsWithRulesSections}\``,
    `- Docs with ordinance sections: \`${report.summary.docsWithOrdinanceSections}\``,
    `- Estimated document-index rows: \`${report.summary.estimatedDocumentIndexCodeRows}\``,
    `- Estimated document-rules rows: \`${report.summary.estimatedDocumentRulesRows}\``,
    `- Estimated document-ordinance rows: \`${report.summary.estimatedDocumentOrdinanceRows}\``,
    `- Distinct index codes: \`${report.summary.distinctIndexCodes}\``,
    `- Distinct rules sections: \`${report.summary.distinctRulesSections}\``,
    `- Distinct ordinance sections: \`${report.summary.distinctOrdinanceSections}\``,
    `- Malformed JSON rows: index=\`${report.summary.malformedIndexJsonRows}\`, rules=\`${report.summary.malformedRulesJsonRows}\`, ordinance=\`${report.summary.malformedOrdinanceJsonRows}\``,
    "",
    "## LIKE-Backed Columns",
    "",
    ...report.likeBackedColumns.map((column) => `- \`${column}\``),
    "",
    "## Recommended Join Tables",
    "",
    ...report.recommendedTables.map((table) => `- \`${table.name}(${table.columns.join(", ")})\``),
    "",
    "## Top Index Codes",
    "",
    ...report.topIndexCodes.map((row) => `- \`${row.value}\`: ${row.count}`),
    "",
    "## Top Rules Sections",
    "",
    ...report.topRulesSections.map((row) => `- \`${row.value}\`: ${row.count}`),
    "",
    "## Top Ordinance Sections",
    "",
    ...report.topOrdinanceSections.map((row) => `- \`${row.value}\`: ${row.count}`),
    ""
  ];
  return `${lines.join("\n")}\n`;
}

async function resolveDbPath() {
  if (process.env.D1_DB_PATH) return process.env.D1_DB_PATH;
  const entries = await fs.readdir(dbDir, { withFileTypes: true });
  const sqliteEntries = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".sqlite") || entry.name.startsWith("backup-")) continue;
    const entryPath = path.join(dbDir, entry.name);
    const stat = await fs.stat(entryPath);
    sqliteEntries.push({ path: entryPath, mtimeMs: stat.mtimeMs });
  }
  sqliteEntries.sort((a, b) => b.mtimeMs - a.mtimeMs || a.path.localeCompare(b.path));
  if (!sqliteEntries.length) throw new Error(`No live sqlite DB found under ${dbDir}`);
  return sqliteEntries[0].path;
}

async function runSqlJson(dbPath, sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 200 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const dbPath = await resolveDbPath();
  const rows = await runSqlJson(
    dbPath,
    `
      SELECT
        id,
        citation,
        index_codes_json AS indexCodesJson,
        rules_sections_json AS rulesSectionsJson,
        ordinance_sections_json AS ordinanceSectionsJson
      FROM documents
      WHERE file_type = 'decision_docx'
      ORDER BY created_at DESC;
    `
  );

  const report = buildFacetStorageReport(rows, { dbPath });
  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Facet storage audit JSON report written to ${jsonPath}`);
  console.log(`Facet storage audit Markdown report written to ${markdownPath}`);
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
