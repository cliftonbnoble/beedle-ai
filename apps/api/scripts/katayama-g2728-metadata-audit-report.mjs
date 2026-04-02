import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.KATAYAMA_G2728_AUDIT_JSON_NAME || "katayama-g2728-metadata-audit-report.json";
const markdownName = process.env.KATAYAMA_G2728_AUDIT_MARKDOWN_NAME || "katayama-g2728-metadata-audit-report.md";
const csvName = process.env.KATAYAMA_G2728_AUDIT_CSV_NAME || "katayama-g2728-metadata-audit-report.csv";
const busyTimeoutMs = Number(process.env.KATAYAMA_G2728_AUDIT_BUSY_TIMEOUT_MS || "5000");

const G27_PRIMARY_PHRASES = [
  "resulting in a code violation constitutes a substantial decrease in housing services",
  "notice of violation constitutes a substantial decrease in housing services",
  "nov constitutes a substantial decrease in housing services",
  "code violation constitutes a substantial decrease in housing services",
  "substantial decrease in housing services"
];

const G28_PRIMARY_PHRASES = [
  "does not constitute a substantial decrease in housing services",
  "did not constitute a substantial decrease in housing services",
  "not a substantial decrease in housing services",
  "not substantial decrease in housing services"
];

const RENT_REDUCTION_CONTEXT_PHRASES = [
  "rent reduction",
  "decrease in services",
  "housing services",
  "code violation",
  "notice of violation",
  "nov"
];

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => a.localeCompare(b));
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

function buildAnyPhraseClause(column, phrases) {
  return phrases.map((phrase) => `instr(lower(${column}), lower('${phrase.replace(/'/g, "''")}')) > 0`).join(" OR ");
}

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

function buildCsv(report) {
  const header = [
    "citation",
    "title",
    "decision_date",
    "current_codes",
    "current_link_codes",
    "suggested_codes",
    "g27_hit",
    "g28_hit",
    "context_hit",
    "g27_evidence",
    "g28_evidence"
  ];
  const lines = [header.join(",")];
  for (const row of report.candidates) {
    lines.push(
      [
        row.citation,
        row.title,
        row.decisionDate || "",
        row.currentCodes.join("; "),
        row.currentLinkCodes.join("; "),
        row.suggestedCodes.join("; "),
        row.g27Hit,
        row.g28Hit,
        row.contextHit,
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
    "# Katayama G27/G28 Metadata Audit",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Database: \`${report.dbPath}\``,
    "",
    "## Summary",
    "",
    `- inspected docs: \`${report.summary.inspectedDocCount}\``,
    `- candidate docs: \`${report.summary.candidateCount}\``,
    `- suggested G27 docs: \`${report.summary.g27CandidateCount}\``,
    `- suggested G28 docs: \`${report.summary.g28CandidateCount}\``,
    `- docs already carrying G27/G28: \`${report.summary.alreadyTaggedCount}\``,
    "",
    "## Candidate Slice",
    ""
  ];

  for (const row of report.candidates.slice(0, 40)) {
    lines.push(
      `- \`${row.citation}\` | current=\`${row.currentCodes.join(", ") || "<none>"}\` | links=\`${row.currentLinkCodes.join(", ") || "<none>"}\` | suggested=\`${row.suggestedCodes.join(", ") || "<none>"}\``
    );
    if (row.g27Evidence) lines.push(`  - G27 evidence: ${row.g27Evidence}`);
    if (row.g28Evidence) lines.push(`  - G28 evidence: ${row.g28Evidence}`);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const g27Clause = buildAnyPhraseClause("c.chunk_text", G27_PRIMARY_PHRASES);
  const g28Clause = buildAnyPhraseClause("c.chunk_text", G28_PRIMARY_PHRASES);
  const contextClause = buildAnyPhraseClause("c.chunk_text", RENT_REDUCTION_CONTEXT_PHRASES);

  const docs = await runSqlJson(`
    SELECT
      d.id,
      d.citation,
      d.title,
      d.decision_date as decisionDate,
      d.index_codes_json as indexCodesJson,
      EXISTS (
        SELECT 1 FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${g27Clause})
      ) AS g27Hit,
      EXISTS (
        SELECT 1 FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${g28Clause})
      ) AS g28Hit,
      EXISTS (
        SELECT 1 FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${contextClause})
      ) AS contextHit,
      (
        SELECT substr(c.chunk_text, 1, 500)
        FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${g27Clause})
        ORDER BY c.chunk_order ASC
        LIMIT 1
      ) AS g27Evidence,
      (
        SELECT substr(c.chunk_text, 1, 500)
        FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${g28Clause})
        ORDER BY c.chunk_order ASC
        LIMIT 1
      ) AS g28Evidence
    FROM documents d
    WHERE d.file_type = 'decision_docx'
      AND lower(coalesce(d.author_name, '')) = lower('Erin E. Katayama')
      AND (
        EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id AND (${contextClause}))
        OR EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id AND (${g27Clause}))
        OR EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id AND (${g28Clause}))
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
    const existingCodes = new Set([...currentCodes, ...currentLinkCodes].map((value) => value.toUpperCase()));
    const suggestedCodes = [];
    if (Number(row.g27Hit || 0) === 1 && !existingCodes.has("G27")) suggestedCodes.push("G27");
    if (Number(row.g28Hit || 0) === 1 && !existingCodes.has("G28")) suggestedCodes.push("G28");
    return {
      documentId: row.id,
      citation: row.citation,
      title: row.title,
      decisionDate: row.decisionDate,
      currentCodes,
      currentLinkCodes,
      g27Hit: Number(row.g27Hit || 0) === 1,
      g28Hit: Number(row.g28Hit || 0) === 1,
      contextHit: Number(row.contextHit || 0) === 1,
      g27Evidence: normalizeWhitespace(row.g27Evidence || ""),
      g28Evidence: normalizeWhitespace(row.g28Evidence || ""),
      suggestedCodes
    };
  });

  const candidates = inspected.filter((row) => row.suggestedCodes.length > 0);
  const alreadyTaggedCount = inspected.filter((row) => row.g27Hit || row.g28Hit).length - candidates.length;

  const report = {
    generatedAt: new Date().toISOString(),
    dbPath,
    summary: {
      inspectedDocCount: inspected.length,
      candidateCount: candidates.length,
      g27CandidateCount: candidates.filter((row) => row.suggestedCodes.includes("G27")).length,
      g28CandidateCount: candidates.filter((row) => row.suggestedCodes.includes("G28")).length,
      alreadyTaggedCount
    },
    candidates
  };

  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  const csvPath = path.join(reportsDir, csvName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(report));

  console.log(
    JSON.stringify(
      {
        inspectedDocCount: report.summary.inspectedDocCount,
        candidateCount: report.summary.candidateCount,
        g27CandidateCount: report.summary.g27CandidateCount,
        g28CandidateCount: report.summary.g28CandidateCount,
        alreadyTaggedCount: report.summary.alreadyTaggedCount
      },
      null,
      2
    )
  );
  console.log(`Katayama G27/G28 audit JSON report written to ${jsonPath}`);
  console.log(`Katayama G27/G28 audit Markdown report written to ${markdownPath}`);
  console.log(`Katayama G27/G28 audit CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
