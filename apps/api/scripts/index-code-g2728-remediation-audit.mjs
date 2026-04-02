import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.G2728_AUDIT_REPORT_NAME || "index-code-g2728-remediation-audit-report.json";
const markdownName = process.env.G2728_AUDIT_MARKDOWN_NAME || "index-code-g2728-remediation-audit-report.md";
const csvName = process.env.G2728_AUDIT_CSV_NAME || "index-code-g2728-remediation-audit-report.csv";
const busyTimeoutMs = Number(process.env.G2728_AUDIT_BUSY_TIMEOUT_MS || "5000");

const G27_PRIMARY_PHRASES = [
  "resulting in a code violation constitutes a substantial decrease in housing services",
  "notice of violation constitutes a substantial decrease in housing services",
  "nov constitutes a substantial decrease in housing services",
  "code violation constitutes a substantial decrease in housing services"
];

const G28_PRIMARY_PHRASES = [
  "does not constitute a substantial decrease in housing services",
  "did not constitute a substantial decrease in housing services",
  "not a substantial decrease in housing services"
];

const CODE_SIGNAL_PHRASES = ["code violation", "notice of violation", "nov", "dbi"];

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeIndexCode(input) {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/[^a-z0-9.()\-]/g, "")
    .replace(/^ic/, "")
    .replace(/^[-]+/, "");
}

function parseJsonArray(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
  } catch {
    return [];
  }
}

function uniqueSorted(values) {
  return Array.from(new Set(values.filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

function countBy(values) {
  const counts = new Map();
  for (const value of values) {
    const key = normalizeWhitespace(value || "<unknown>");
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || a.key.localeCompare(b.key));
}

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

function buildAnyPhraseClause(column, phrases) {
  return phrases.map((phrase) => `instr(lower(${column}), lower('${phrase.replace(/'/g, "''")}')) > 0`).join(" OR ");
}

function buildCsv(report) {
  const header = [
    "citation",
    "title",
    "author_name",
    "decision_date",
    "current_codes",
    "current_link_codes",
    "detected_codes",
    "next_codes",
    "g27_hit",
    "g28_hit",
    "g27_evidence",
    "g28_evidence"
  ];
  const lines = [header.join(",")];

  for (const row of report.candidates) {
    lines.push(
      [
        row.citation,
        row.title,
        row.authorName || "",
        row.decisionDate || "",
        row.currentCodes.join("; "),
        row.currentLinkCodes.join("; "),
        row.detectedCodes.join("; "),
        row.nextCodes.join("; "),
        row.g27Hit,
        row.g28Hit,
        row.g27Evidence || "",
        row.g28Evidence || ""
      ]
        .map((value) => {
          const text = String(value ?? "");
          return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
        })
        .join(",")
    );
  }

  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# G27/G28 Remediation Audit",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Database: \`${report.dbPath}\``,
    "",
    "## Summary",
    "",
    `- inspected docs: \`${report.summary.inspectedDocCount}\``,
    `- candidate docs: \`${report.summary.candidateCount}\``,
    `- G27 candidates: \`${report.summary.g27CandidateCount}\``,
    `- G28 candidates: \`${report.summary.g28CandidateCount}\``,
    `- dual-code candidates: \`${report.summary.bothCandidateCount}\``,
    "",
    "## Candidate Judges",
    ""
  ];

  for (const row of report.summary.byJudge.slice(0, 12)) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }

  lines.push("");
  lines.push("## Candidate Slice");
  lines.push("");

  for (const row of report.candidates.slice(0, 40)) {
    lines.push(
      `- \`${row.citation}\` | judge=\`${row.authorName || "<unknown>"}\` | current=\`${row.currentCodes.join(", ") || "<none>"}\` | detected=\`${row.detectedCodes.join(", ")}\` | next=\`${row.nextCodes.join(", ")}\``
    );
    if (row.g27Evidence) lines.push(`  - G27 evidence: ${row.g27Evidence}`);
    if (row.g28Evidence) lines.push(`  - G28 evidence: ${row.g28Evidence}`);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const g27PrimaryClause = buildAnyPhraseClause("c.chunk_text", G27_PRIMARY_PHRASES);
  const g28PrimaryClause = buildAnyPhraseClause("c.chunk_text", G28_PRIMARY_PHRASES);
  const codeSignalClause = buildAnyPhraseClause("c.chunk_text", CODE_SIGNAL_PHRASES);

  const docs = await runSqlJson(`
    SELECT
      d.id,
      d.citation,
      d.title,
      d.author_name AS authorName,
      d.decision_date AS decisionDate,
      d.index_codes_json AS indexCodesJson,
      EXISTS (
        SELECT 1 FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${g27PrimaryClause})
      ) AS g27PrimaryHit,
      EXISTS (
        SELECT 1 FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${g28PrimaryClause})
      ) AS g28PrimaryHit,
      EXISTS (
        SELECT 1 FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${codeSignalClause})
      ) AS codeSignalHit,
      (
        SELECT substr(c.chunk_text, 1, 500)
        FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${g27PrimaryClause})
        ORDER BY c.chunk_order ASC
        LIMIT 1
      ) AS g27Evidence,
      (
        SELECT substr(c.chunk_text, 1, 500)
        FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${g28PrimaryClause})
        ORDER BY c.chunk_order ASC
        LIMIT 1
      ) AS g28Evidence
    FROM documents d
    WHERE d.file_type = 'decision_docx'
      AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
      AND (
        EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id AND (${g27PrimaryClause}))
        OR EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id AND (${g28PrimaryClause}))
      )
    ORDER BY d.decision_date DESC, d.citation ASC;
  `);

  const links = await runSqlJson(`
    SELECT
      document_id AS documentId,
      canonical_value AS canonicalValue,
      raw_value AS rawValue,
      is_valid AS isValid
    FROM document_reference_links
    WHERE reference_type = 'index_code';
  `);

  const linkCodesByDocumentId = new Map();
  for (const row of links) {
    if (!row.documentId || Number(row.isValid || 0) !== 1) continue;
    const current = linkCodesByDocumentId.get(row.documentId) || [];
    current.push(normalizeWhitespace(row.canonicalValue || row.rawValue));
    linkCodesByDocumentId.set(row.documentId, current);
  }

  const inspected = docs.map((row) => {
    const currentCodes = uniqueSorted(parseJsonArray(row.indexCodesJson).map(normalizeWhitespace));
    const currentLinkCodes = uniqueSorted((linkCodesByDocumentId.get(row.id) || []).map(normalizeWhitespace));
    const hasCanonicalG27 =
      currentCodes.some((item) => normalizeIndexCode(item) === "g27") ||
      currentLinkCodes.some((item) => normalizeIndexCode(item) === "g27");
    const hasCanonicalG28 =
      currentCodes.some((item) => normalizeIndexCode(item) === "g28") ||
      currentLinkCodes.some((item) => normalizeIndexCode(item) === "g28");

    const g27Hit = Boolean(row.g27PrimaryHit) && Boolean(row.codeSignalHit);
    const g28Hit = Boolean(row.g28PrimaryHit) && Boolean(row.codeSignalHit);
    const detectedCodes = uniqueSorted([g27Hit ? "G27" : "", g28Hit ? "G28" : ""]);
    const nextCodes = uniqueSorted([
      ...currentCodes,
      ...currentLinkCodes,
      ...(g27Hit && !hasCanonicalG27 ? ["G27"] : []),
      ...(g28Hit && !hasCanonicalG28 ? ["G28"] : [])
    ]);

    return {
      id: row.id,
      citation: row.citation,
      title: row.title,
      authorName: row.authorName,
      decisionDate: row.decisionDate,
      currentCodes,
      currentLinkCodes,
      g27Hit,
      g28Hit,
      detectedCodes,
      nextCodes,
      g27Evidence: normalizeWhitespace(row.g27Evidence),
      g28Evidence: normalizeWhitespace(row.g28Evidence),
      hasCanonicalG27,
      hasCanonicalG28
    };
  });

  const candidates = inspected.filter(
    (row) => (row.g27Hit && !row.hasCanonicalG27) || (row.g28Hit && !row.hasCanonicalG28)
  );

  const report = {
    generatedAt: new Date().toISOString(),
    dbPath,
    candidates,
    summary: {
      inspectedDocCount: inspected.length,
      candidateCount: candidates.length,
      g27CandidateCount: candidates.filter((row) => row.g27Hit).length,
      g28CandidateCount: candidates.filter((row) => row.g28Hit).length,
      bothCandidateCount: candidates.filter((row) => row.g27Hit && row.g28Hit).length,
      byJudge: countBy(candidates.map((row) => row.authorName || "<unknown>"))
    }
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`G27/G28 remediation audit JSON report written to ${jsonPath}`);
  console.log(`G27/G28 remediation audit Markdown report written to ${markdownPath}`);
  console.log(`G27/G28 remediation audit CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
