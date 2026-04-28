import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const databaseName = process.env.D1_REMOTE_DATABASE || "beedle";
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.D1_REMOTE_POSTMIGRATION_JSON_NAME || "d1-remote-postmigration-repair-report.json";
const markdownName = process.env.D1_REMOTE_POSTMIGRATION_MARKDOWN_NAME || "d1-remote-postmigration-repair-report.md";
const apply = (process.env.D1_REMOTE_POSTMIGRATION_APPLY || "0") === "1";
const catalogBatchSize = Math.max(1, Number(process.env.D1_REMOTE_POSTMIGRATION_CATALOG_BATCH_SIZE || "25"));
const indexCatalogPath = path.resolve(process.cwd(), "../../packages/shared/src/index-codes.ts");

const CANONICAL_JUDGE_NAMES = [
  "René Juárez",
  "Andrew Yick",
  "Connie Brandon",
  "Deborah K. Lim",
  "Dorothy Chou Proudfoot",
  "Erin E. Katayama",
  "Harrison Nam",
  "Jeffrey Eckber",
  "Jill Figg Dayal",
  "Joseph Koomas",
  "Michael J. Berg",
  "Peter Kearns"
];

const JUDGE_TEXT_ALIASES = new Map([
  ["René Juárez", ["René Juárez", "Rene Juarez"]],
  ["Andrew Yick", ["Andrew Yick"]],
  ["Connie Brandon", ["Connie Brandon"]],
  ["Deborah K. Lim", ["Deborah K. Lim", "Deborah Lim"]],
  ["Dorothy Chou Proudfoot", ["Dorothy Chou Proudfoot", "Dorothy Proudfoot"]],
  ["Erin E. Katayama", ["Erin E. Katayama", "Erin Katayama"]],
  ["Harrison Nam", ["Harrison Nam"]],
  ["Jeffrey Eckber", ["Jeffrey Eckber"]],
  ["Jill Figg Dayal", ["Jill Figg Dayal", "Jill Dayal"]],
  ["Joseph Koomas", ["Joseph Koomas"]],
  ["Michael J. Berg", ["Michael J. Berg", "Michael Berg"]],
  ["Peter Kearns", ["Peter Kearns"]]
]);

const JUDGE_FOOTER_INITIALS = new Map([
  ["dkl", "Deborah K. Lim"],
  ["jfd", "Jill Figg Dayal"],
  ["ay", "Andrew Yick"]
]);

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
  if (value === null || value === undefined) return "NULL";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function id(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function extractCatalogReferenceCitations(raw) {
  const text = String(raw || "");
  const matches = new Set();
  for (const match of text.matchAll(/\b\d+\.\d+[a-z]?(?:\([a-z0-9]+\))*[a-z]?\b/gi)) matches.add(match[0]);
  for (const match of text.matchAll(/§\s*([0-9]+(?:\.[0-9]+)?(?:\([a-z0-9]+\))*)/gi)) {
    if (match[1]) matches.add(match[1]);
  }
  return Array.from(matches);
}

function deriveFamily(description) {
  const parts = normalizeWhitespace(description).split("--").map(normalizeWhitespace).filter(Boolean);
  return parts.length > 1 ? parts[0] : null;
}

function deriveLabel(description) {
  const parts = normalizeWhitespace(description).split("--").map(normalizeWhitespace).filter(Boolean);
  return parts.length > 1 ? parts.slice(1).join(" -- ") : normalizeWhitespace(description) || null;
}

function isReserved(description) {
  return /\[reserved\]|\breserved\b/i.test(String(description || ""));
}

function isLegacyPre1002(description) {
  return /\bpre-?10\/?02\b|\blegacy\b/i.test(String(description || ""));
}

function buildDesiredCatalogRow(option) {
  const code = normalizeWhitespace(option.code).toUpperCase();
  const description = normalizeWhitespace(option.description || "");
  const linkedOrdinance = unique(extractCatalogReferenceCitations(option.ordinance || ""));
  const linkedRules = unique(extractCatalogReferenceCitations(option.rules || "").map((item) => `Rule ${item}`));
  return {
    id: id("idx"),
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

async function loadIndexCatalog() {
  const raw = await fs.readFile(indexCatalogPath, "utf8");
  const start = raw.indexOf("[");
  const end = raw.lastIndexOf("] as const;");
  if (start < 0 || end < 0) throw new Error(`Could not parse canonical index code catalog from ${indexCatalogPath}`);
  return JSON.parse(raw.slice(start, end + 1));
}

async function wranglerSql(sql) {
  const { stdout, stderr } = await execFileAsync("npx", ["wrangler", "d1", "execute", databaseName, "--remote", "--json", "--command", sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  const parsed = JSON.parse(stdout || "[]");
  const failed = parsed.find((item) => item && item.success === false);
  if (failed) throw new Error(`Remote D1 SQL failed: ${JSON.stringify(failed)}\n${stderr || ""}`);
  return parsed.flatMap((item) => item.results || []);
}

function catalogUpsertSql(rows, now) {
  const values = rows.map((row) => `(
    ${sqlQuote(row.id)}, ${sqlQuote(row.codeIdentifier)}, ${sqlQuote(row.normalizedCode)}, ${sqlQuote(row.family)},
    ${sqlQuote(row.label)}, ${sqlQuote(row.description)}, ${row.isReserved}, ${row.isLegacyPre1002},
    ${sqlQuote(row.linkedOrdinanceSectionsJson)}, ${sqlQuote(row.linkedRulesSectionsJson)}, ${sqlQuote(row.sourcePageAnchor)},
    ${row.active}, ${sqlQuote(now)}, ${sqlQuote(now)}
  )`).join(",\n");

  return `INSERT INTO legal_index_codes (
    id, code_identifier, normalized_code, family, label, description, is_reserved, is_legacy_pre_1002,
    linked_ordinance_sections_json, linked_rules_sections_json, source_page_anchor, active, created_at, updated_at
  ) VALUES ${values}
  ON CONFLICT(normalized_code) DO UPDATE SET
    code_identifier = excluded.code_identifier,
    family = excluded.family,
    label = excluded.label,
    description = excluded.description,
    is_reserved = excluded.is_reserved,
    is_legacy_pre_1002 = excluded.is_legacy_pre_1002,
    linked_ordinance_sections_json = excluded.linked_ordinance_sections_json,
    linked_rules_sections_json = excluded.linked_rules_sections_json,
    source_page_anchor = excluded.source_page_anchor,
    active = excluded.active,
    updated_at = excluded.updated_at;`;
}

function invalidAuthorWhere() {
  return `(
    d.author_name IS NULL OR trim(d.author_name) = ''
    OR lower(d.author_name) LIKE '%tenant%'
    OR lower(d.author_name) LIKE '%landlord%'
    OR lower(d.author_name) LIKE '%substantial%'
    OR lower(d.author_name) LIKE '%mold%'
    OR lower(d.author_name) LIKE '%storage%'
    OR lower(d.author_name) LIKE '%service%'
    OR lower(d.author_name) LIKE '%petition%'
    OR lower(d.author_name) LIKE '%denied%'
    OR lower(d.author_name) LIKE '%granted%'
    OR lower(d.author_name) LIKE '%attorney%'
    OR lower(d.author_name) LIKE '%trustee%'
    OR lower(d.author_name) LIKE '%emails%'
  )`;
}

function inferJudgeFromSections(sections) {
  const fullText = sections.map((row) => `${row.heading || ""}\n${row.section_text || ""}`).join("\n\n");
  const signatureText = fullText.slice(Math.max(0, fullText.length - 6000));
  const matches = new Set();
  for (const [judge, aliases] of JUDGE_TEXT_ALIASES.entries()) {
    const escapedAliases = aliases.map((alias) => alias.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|");
    const escaped = `(?:${escapedAliases})`;
    const nearAlj = new RegExp(`${escaped}[\\s\\S]{0,160}(Administrative Law Judge|ALJ)|(?:Administrative Law Judge|ALJ)[\\s\\S]{0,160}${escaped}`, "i");
    if (nearAlj.test(signatureText) || nearAlj.test(fullText)) matches.add(judge);
  }
  if (matches.size === 1) return Array.from(matches)[0];

  const footerMatches = new Set();
  for (const [initials, judge] of JUDGE_FOOTER_INITIALS.entries()) {
    const footerPattern = new RegExp(`\\b${initials}\\s*/\\s*[A-Z0-9]`, "i");
    if (footerPattern.test(signatureText)) footerMatches.add(judge);
  }
  if (footerMatches.size === 1) return Array.from(footerMatches)[0];

  return matches.size === 1 ? Array.from(matches)[0] : null;
}

async function repairJudges(now) {
  const candidates = await wranglerSql(`SELECT d.id, d.citation, d.title, d.author_name
    FROM documents d
    WHERE ${invalidAuthorWhere()}
    ORDER BY d.citation
    LIMIT 500;`);
  const repaired = [];
  const unresolved = [];

  const sectionsByDocument = new Map();
  for (let index = 0; index < candidates.length; index += 20) {
    const batch = candidates.slice(index, index + 20);
    if (!batch.length) continue;
    const sectionRows = await wranglerSql(`SELECT document_id, heading, section_text
      FROM document_sections
      WHERE document_id IN (${batch.map((row) => sqlQuote(row.id)).join(", ")})
      ORDER BY document_id, section_order;`);
    for (const section of sectionRows) {
      const current = sectionsByDocument.get(section.document_id) || [];
      current.push(section);
      sectionsByDocument.set(section.document_id, current);
    }
  }

  for (const row of candidates) {
    const sections = sectionsByDocument.get(row.id) || [];
    const nextAuthorName = inferJudgeFromSections(sections);
    if (!nextAuthorName) {
      unresolved.push(row);
      continue;
    }
    repaired.push({ ...row, nextAuthorName });
    if (apply) {
      await wranglerSql(`UPDATE documents SET author_name = ${sqlQuote(nextAuthorName)}, updated_at = ${sqlQuote(now)} WHERE id = ${sqlQuote(row.id)};`);
    }
  }

  return { candidateCount: candidates.length, repaired, unresolved };
}

function markdown(report) {
  const lines = [
    "# D1 Remote Post-Migration Repair Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- Remote database: \`${report.databaseName}\``,
    "",
    "## Summary",
    "",
    `- shared index-code catalog rows: \`${report.summary.sharedCatalogCount}\``,
    `- catalog rows before: \`${report.before.legalIndexCodes}\``,
    `- catalog rows after: \`${report.after.legalIndexCodes}\``,
    `- index links missing canonical before: \`${report.before.indexLinksMissingCanonical}\``,
    `- index links missing canonical after: \`${report.after.indexLinksMissingCanonical}\``,
    `- valid index links before: \`${report.before.validIndexLinks}\``,
    `- valid index links after: \`${report.after.validIndexLinks}\``,
    `- judge candidates scanned: \`${report.judges.candidateCount}\``,
    `- judge repairs ${report.apply ? "applied" : "identified"}: \`${report.judges.repaired.length}\``,
    `- unresolved judge candidates: \`${report.judges.unresolved.length}\``,
    "",
    "## Judge Repairs",
    ""
  ];
  for (const row of report.judges.repaired.slice(0, 80)) {
    lines.push(`- \`${row.citation}\`: \`${row.author_name || "<missing>"}\` -> \`${row.nextAuthorName}\``);
  }
  if (!report.judges.repaired.length) lines.push("- None");
  return `${lines.join("\n")}\n`;
}

async function counts() {
  const [row] = await wranglerSql(`SELECT
    (SELECT COUNT(*) FROM legal_index_codes) AS legalIndexCodes,
    (SELECT COUNT(*) FROM document_reference_links WHERE reference_type='index_code') AS indexLinks,
    (SELECT COUNT(*) FROM document_reference_links WHERE reference_type='index_code' AND canonical_value IS NULL) AS indexLinksMissingCanonical,
    (SELECT COUNT(*) FROM document_reference_links WHERE reference_type='index_code' AND is_valid=1) AS validIndexLinks,
    (SELECT COUNT(*) FROM documents WHERE author_name IS NULL OR trim(author_name)='') AS missingAuthorNames,
    (SELECT COUNT(*) FROM documents WHERE author_name IS NOT NULL AND trim(author_name) != '' AND author_name NOT IN (${CANONICAL_JUDGE_NAMES.map(sqlQuote).join(", ")})) AS nonCanonicalAuthorNames;`);
  return row;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const generatedAt = new Date().toISOString();
  const now = generatedAt;
  const before = await counts();
  const sharedCatalog = await loadIndexCatalog();
  const desiredRows = sharedCatalog.map(buildDesiredCatalogRow);

  if (apply) {
    for (let index = 0; index < desiredRows.length; index += catalogBatchSize) {
      const batch = desiredRows.slice(index, index + catalogBatchSize);
      await wranglerSql(catalogUpsertSql(batch, now));
    }
    await wranglerSql(`UPDATE document_reference_links
      SET canonical_value = (
            SELECT code_identifier FROM legal_index_codes c
            WHERE c.normalized_code = document_reference_links.normalized_value AND c.active = 1
            LIMIT 1
          ),
          is_valid = 1
      WHERE reference_type = 'index_code'
        AND EXISTS (
          SELECT 1 FROM legal_index_codes c
          WHERE c.normalized_code = document_reference_links.normalized_value AND c.active = 1
        );`);
  }

  const judges = await repairJudges(now);
  const after = await counts();

  const report = {
    generatedAt,
    apply,
    databaseName,
    before,
    after,
    summary: {
      sharedCatalogCount: sharedCatalog.length,
      catalogBatchSize
    },
    judges
  };

  await Promise.all([
    fs.writeFile(path.join(reportsDir, jsonName), JSON.stringify(report, null, 2)),
    fs.writeFile(path.join(reportsDir, markdownName), markdown(report))
  ]);

  console.log(JSON.stringify({ before, after, judgeRepairs: judges.repaired.length, unresolvedJudges: judges.unresolved.length }, null, 2));
  console.log(`Remote post-migration repair JSON report written to ${path.join(reportsDir, jsonName)}`);
  console.log(`Remote post-migration repair Markdown report written to ${path.join(reportsDir, markdownName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
