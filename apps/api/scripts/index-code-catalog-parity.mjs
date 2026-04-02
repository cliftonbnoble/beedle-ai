import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.INDEX_CODE_CATALOG_PARITY_JSON_NAME || "index-code-catalog-parity-report.json";
const markdownName = process.env.INDEX_CODE_CATALOG_PARITY_MARKDOWN_NAME || "index-code-catalog-parity-report.md";
const csvName = process.env.INDEX_CODE_CATALOG_PARITY_CSV_NAME || "index-code-catalog-parity-report.csv";
const apply = (process.env.INDEX_CODE_CATALOG_PARITY_APPLY || "0") === "1";
const busyTimeoutMs = Number(process.env.INDEX_CODE_CATALOG_PARITY_BUSY_TIMEOUT_MS || "5000");
const dbDir = path.resolve(process.cwd(), ".wrangler/state/v3/d1/miniflare-D1DatabaseObject");
const indexCatalogPath = path.resolve(process.cwd(), "../../packages/shared/src/index-codes.ts");

function id(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeToken(input) {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

function normalizeIndexCode(input) {
  return normalizeToken(input).replace(/^ic/, "").replace(/^[-]+/, "");
}

function sqlQuote(value) {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
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

function deriveFamily(description) {
  const text = normalizeWhitespace(description);
  if (!text) return null;
  const parts = text.split("--").map((part) => normalizeWhitespace(part)).filter(Boolean);
  return parts.length > 1 ? parts[0] : null;
}

function deriveLabel(description) {
  const text = normalizeWhitespace(description);
  if (!text) return null;
  const parts = text.split("--").map((part) => normalizeWhitespace(part)).filter(Boolean);
  return parts.length > 1 ? parts.slice(1).join(" -- ") : text;
}

function isReserved(description) {
  return /\[reserved\]|\breserved\b/i.test(String(description || ""));
}

function isLegacyPre1002(description) {
  return /\bpre-?10\/?02\b|\blegacy\b/i.test(String(description || ""));
}

function buildDesiredRow(option) {
  const code = normalizeWhitespace(option.code).toUpperCase();
  const description = normalizeWhitespace(option.description || "");
  const linkedOrdinance = unique(extractCatalogReferenceCitations(option.ordinance || ""));
  const linkedRules = unique(extractCatalogReferenceCitations(option.rules || "").map((item) => `Rule ${item}`));
  return {
    codeIdentifier: code,
    normalizedCode: normalizeIndexCode(code),
    family: deriveFamily(description),
    label: deriveLabel(description),
    description,
    isReserved: isReserved(description) ? 1 : 0,
    isLegacyPre1002: isLegacyPre1002(description) ? 1 : 0,
    linkedOrdinanceSectionsJson: JSON.stringify(linkedOrdinance),
    linkedRulesSectionsJson: JSON.stringify(linkedRules),
    sourcePageAnchor: `shared-index-codes:${code}`,
    active: 1
  };
}

async function resolveDbPath() {
  const entries = await fs.readdir(dbDir, { withFileTypes: true });
  const sqliteNames = entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".sqlite") && !entry.name.includes(".bak-"))
    .map((entry) => entry.name)
    .sort();
  if (!sqliteNames.length) throw new Error(`No live sqlite DB found under ${dbDir}`);
  return path.join(dbDir, sqliteNames[0]);
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

async function runSqlJson(dbPath, sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

async function runSql(dbPath, sql) {
  await execFileAsync("sqlite3", ["-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
}

function rowDiff(existing, desired) {
  const diffs = [];
  const fields = [
    ["family", existing.family, desired.family],
    ["label", existing.label, desired.label],
    ["description", existing.description, desired.description],
    ["is_reserved", Number(existing.is_reserved || 0), Number(desired.isReserved || 0)],
    ["is_legacy_pre_1002", Number(existing.is_legacy_pre_1002 || 0), Number(desired.isLegacyPre1002 || 0)],
    ["linked_ordinance_sections_json", existing.linked_ordinance_sections_json || "[]", desired.linkedOrdinanceSectionsJson],
    ["linked_rules_sections_json", existing.linked_rules_sections_json || "[]", desired.linkedRulesSectionsJson],
    ["source_page_anchor", existing.source_page_anchor, desired.sourcePageAnchor],
    ["active", Number(existing.active || 0), Number(desired.active || 0)]
  ];
  for (const [field, current, next] of fields) {
    if (String(current ?? "") !== String(next ?? "")) diffs.push(field);
  }
  return diffs;
}

function buildCsv(report) {
  const header = ["action", "code", "normalized_code", "family", "label", "description", "diff_fields"];
  const lines = [header.join(",")];
  for (const row of [...report.insertCandidates, ...report.updateCandidates, ...report.conflicts]) {
    lines.push(
      [
        row.action,
        row.codeIdentifier,
        row.normalizedCode,
        row.family || "",
        row.label || "",
        row.description || "",
        (row.diffFields || []).join("; ")
      ]
        .map(csvEscape)
        .join(",")
    );
  }
  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Index Code Catalog Parity Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- Database: \`${report.dbPath}\``,
    "",
    "## Summary",
    "",
    `- shared catalog count: \`${report.summary.sharedCatalogCount}\``,
    `- local catalog count before: \`${report.summary.localCatalogCountBefore}\``,
    `- local catalog matched shared codes: \`${report.summary.matchedSharedCount}\``,
    `- missing shared codes to insert: \`${report.summary.insertCount}\``,
    `- shared codes with drift to update: \`${report.summary.updateCount}\``,
    `- normalized-code conflicts: \`${report.summary.conflictCount}\``,
    `- applied inserts: \`${report.summary.appliedInsertCount}\``,
    `- applied updates: \`${report.summary.appliedUpdateCount}\``,
    "",
    "## Notes",
    "",
    "- This sync is additive. It preserves existing local legacy rows such as numeric family codes like `13`.",
    "- The goal is backend catalog parity with the shared canonical catalog, not replacement of legacy corpus metadata.",
    "- Once the catalog is in parity, document reference revalidation can use these canonical rows instead of treating them as unknown.",
    ""
  ];

  const sections = [
    ["Insert Candidates", report.insertCandidates],
    ["Update Candidates", report.updateCandidates],
    ["Conflicts", report.conflicts]
  ];

  for (const [title, rows] of sections) {
    lines.push(`## ${title}`);
    lines.push("");
    if (!rows.length) {
      lines.push("- None");
      lines.push("");
      continue;
    }
    lines.push("| Code | Family | Label | Diff Fields |");
    lines.push("| --- | --- | --- | --- |");
    for (const row of rows.slice(0, 40)) {
      lines.push(`| ${row.codeIdentifier} | ${row.family || ""} | ${row.label || ""} | ${(row.diffFields || []).join(", ")} |`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const dbPath = await resolveDbPath();
  const sharedCatalog = await loadIndexCatalog();
  const existingRows = await runSqlJson(
    dbPath,
    `SELECT id, code_identifier, normalized_code, family, label, description, is_reserved, is_legacy_pre_1002,
            linked_ordinance_sections_json, linked_rules_sections_json, source_page_anchor, active
     FROM legal_index_codes`
  );

  const existingByNormalized = new Map(existingRows.map((row) => [String(row.normalized_code), row]));
  const insertCandidates = [];
  const updateCandidates = [];
  const conflicts = [];

  for (const option of sharedCatalog) {
    const desired = buildDesiredRow(option);
    const existing = existingByNormalized.get(desired.normalizedCode);
    if (!existing) {
      insertCandidates.push({ action: "insert", ...desired });
      continue;
    }
    const existingCode = String(existing.code_identifier || "").toUpperCase();
    if (existingCode !== desired.codeIdentifier) {
      conflicts.push({
        action: "conflict",
        ...desired,
        existingCodeIdentifier: existingCode,
        diffFields: ["code_identifier"]
      });
      continue;
    }
    const diffFields = rowDiff(existing, desired);
    if (diffFields.length > 0) {
      updateCandidates.push({ action: "update", ...desired, id: existing.id, diffFields });
    }
  }

  let appliedInsertCount = 0;
  let appliedUpdateCount = 0;

  if (apply) {
    const now = new Date().toISOString();
    for (const row of insertCandidates) {
      await runSql(
        dbPath,
        `INSERT INTO legal_index_codes (
           id, code_identifier, normalized_code, family, label, description, is_reserved, is_legacy_pre_1002,
           linked_ordinance_sections_json, linked_rules_sections_json, source_page_anchor, active, created_at, updated_at
         ) VALUES (
           ${sqlQuote(id("idx"))},
           ${sqlQuote(row.codeIdentifier)},
           ${sqlQuote(row.normalizedCode)},
           ${row.family ? sqlQuote(row.family) : "NULL"},
           ${row.label ? sqlQuote(row.label) : "NULL"},
           ${row.description ? sqlQuote(row.description) : "NULL"},
           ${row.isReserved},
           ${row.isLegacyPre1002},
           ${sqlQuote(row.linkedOrdinanceSectionsJson)},
           ${sqlQuote(row.linkedRulesSectionsJson)},
           ${sqlQuote(row.sourcePageAnchor)},
           ${row.active},
           ${sqlQuote(now)},
           ${sqlQuote(now)}
         )`
      );
      appliedInsertCount += 1;
    }

    for (const row of updateCandidates) {
      await runSql(
        dbPath,
        `UPDATE legal_index_codes
         SET family = ${row.family ? sqlQuote(row.family) : "NULL"},
             label = ${row.label ? sqlQuote(row.label) : "NULL"},
             description = ${row.description ? sqlQuote(row.description) : "NULL"},
             is_reserved = ${row.isReserved},
             is_legacy_pre_1002 = ${row.isLegacyPre1002},
             linked_ordinance_sections_json = ${sqlQuote(row.linkedOrdinanceSectionsJson)},
             linked_rules_sections_json = ${sqlQuote(row.linkedRulesSectionsJson)},
             source_page_anchor = ${sqlQuote(row.sourcePageAnchor)},
             active = ${row.active},
             updated_at = ${sqlQuote(now)}
         WHERE id = ${sqlQuote(row.id)}`
      );
      appliedUpdateCount += 1;
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    dbPath,
    summary: {
      sharedCatalogCount: sharedCatalog.length,
      localCatalogCountBefore: existingRows.length,
      matchedSharedCount: sharedCatalog.length - insertCandidates.length - conflicts.length,
      insertCount: insertCandidates.length,
      updateCount: updateCandidates.length,
      conflictCount: conflicts.length,
      appliedInsertCount,
      appliedUpdateCount
    },
    insertCandidates,
    updateCandidates,
    conflicts
  };

  await Promise.all([
    fs.writeFile(path.join(reportsDir, jsonName), JSON.stringify(report, null, 2)),
    fs.writeFile(path.join(reportsDir, markdownName), formatMarkdown(report)),
    fs.writeFile(path.join(reportsDir, csvName), buildCsv(report))
  ]);

  console.log(
    JSON.stringify(
      {
        sharedCatalogCount: report.summary.sharedCatalogCount,
        localCatalogCountBefore: report.summary.localCatalogCountBefore,
        insertCount: report.summary.insertCount,
        updateCount: report.summary.updateCount,
        conflictCount: report.summary.conflictCount,
        appliedInsertCount: report.summary.appliedInsertCount,
        appliedUpdateCount: report.summary.appliedUpdateCount
      },
      null,
      2
    )
  );
  console.log(`Index code catalog parity JSON report written to ${path.join(reportsDir, jsonName)}`);
  console.log(`Index code catalog parity Markdown report written to ${path.join(reportsDir, markdownName)}`);
  console.log(`Index code catalog parity CSV report written to ${path.join(reportsDir, csvName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
