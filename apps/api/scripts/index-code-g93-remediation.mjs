import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.G93_REMEDIATION_REPORT_NAME || "index-code-g93-remediation-report.json";
const markdownName = process.env.G93_REMEDIATION_MARKDOWN_NAME || "index-code-g93-remediation-report.md";
const csvName = process.env.G93_REMEDIATION_CSV_NAME || "index-code-g93-remediation-report.csv";
const apply = (process.env.G93_REMEDIATION_APPLY || "0") === "1";
const busyTimeoutMs = Number(process.env.G93_REMEDIATION_BUSY_TIMEOUT_MS || "5000");

const PRIMARY_PHRASES = [
  "uniform hotel visitor policy",
  "uniform visitor policy",
  "supplemental visitor policy",
  "visitor policy for sro hotels"
];

const CORROBORATING_PHRASES = [
  "chapter 41d",
  "ordinance section 37.14",
  "37.14a",
  "diminution in housing services",
  "corresponding reduction in rent",
  "residential hotel"
];

function id(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function sqlQuote(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

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

async function runSql(sql) {
  await execFileAsync("sqlite3", ["-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
}

function buildAnyPhraseClause(column, phrases) {
  return phrases.map((phrase) => `instr(lower(${column}), lower(${sqlQuote(phrase)})) > 0`).join(" OR ");
}

function buildCsv(report) {
  const header = [
    "citation",
    "title",
    "author_name",
    "decision_date",
    "current_codes",
    "current_link_codes",
    "next_codes",
    "primary_phrase_hit",
    "corroborating_phrase_hit",
    "ordinance_37_14_hit",
    "evidence_snippet"
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
        row.nextCodes.join("; "),
        row.primaryPhraseHit,
        row.corroboratingPhraseHit,
        row.ordinance3714Hit,
        row.evidenceSnippet || ""
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
    "# G93 Remediation Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- Database: \`${report.dbPath}\``,
    "",
    "## Summary",
    "",
    `- inspected docs: \`${report.summary.inspectedDocCount}\``,
    `- primary phrase docs: \`${report.summary.primaryPhraseDocCount}\``,
    `- corroborated docs: \`${report.summary.corroboratedDocCount}\``,
    `- ordinance 37.14 docs: \`${report.summary.ordinance3714DocCount}\``,
    `- candidate docs: \`${report.summary.candidateCount}\``,
    `- already canonical G93: \`${report.summary.alreadyCanonicalCount}\``,
    `- applied updates: \`${report.summary.appliedUpdateCount}\``,
    "",
    "## Candidate Judges",
    ""
  ];

  for (const row of report.summary.byJudge.slice(0, 12)) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }

  lines.push("");
  lines.push("## Candidate Decisions");
  lines.push("");

  for (const row of report.candidates.slice(0, 50)) {
    lines.push(
      `- \`${row.citation}\` | judge=\`${row.authorName || "<unknown>"}\` | current=\`${row.currentCodes.join(", ") || "<none>"}\` | next=\`${row.nextCodes.join(", ")}\``
    );
    lines.push(`  - evidence: ${row.evidenceSnippet || "<none>"}`);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const primaryClause = buildAnyPhraseClause("c.chunk_text", PRIMARY_PHRASES);
  const corroboratingClause = buildAnyPhraseClause("c.chunk_text", CORROBORATING_PHRASES);

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
          AND (${primaryClause})
      ) AS primaryPhraseHit,
      EXISTS (
        SELECT 1 FROM document_chunks c
        WHERE c.document_id = d.id
          AND (${corroboratingClause})
      ) AS corroboratingPhraseHit,
      EXISTS (
        SELECT 1 FROM document_chunks c
        WHERE c.document_id = d.id
          AND (
            instr(lower(c.chunk_text), '37.14a') > 0
            OR instr(lower(c.chunk_text), '37.14(a)') > 0
            OR instr(lower(c.chunk_text), '37.14(b)') > 0
            OR instr(lower(c.chunk_text), 'ordinance section 37.14') > 0
          )
      ) AS ordinance3714Hit,
      (
        SELECT substr(c.chunk_text, 1, 500)
        FROM document_chunks c
        WHERE c.document_id = d.id
          AND ((${primaryClause}) OR (${corroboratingClause}))
        ORDER BY c.chunk_order ASC
        LIMIT 1
      ) AS evidenceSnippet
    FROM documents d
    WHERE d.file_type = 'decision_docx'
      AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
      AND (
        EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id AND (${primaryClause}))
        OR EXISTS (
          SELECT 1 FROM document_chunks c
          WHERE c.document_id = d.id
            AND (
              instr(lower(c.chunk_text), '37.14a') > 0
              OR instr(lower(c.chunk_text), '37.14(a)') > 0
              OR instr(lower(c.chunk_text), '37.14(b)') > 0
              OR instr(lower(c.chunk_text), 'ordinance section 37.14') > 0
            )
        )
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
    const hasCanonicalG93 =
      currentCodes.some((item) => normalizeIndexCode(item) === "g93") ||
      currentLinkCodes.some((item) => normalizeIndexCode(item) === "g93");
    const candidate = Boolean(row.primaryPhraseHit) && (Boolean(row.corroboratingPhraseHit) || Boolean(row.ordinance3714Hit));
    const nextCodes = candidate ? uniqueSorted([...currentCodes, ...currentLinkCodes, "G93"]) : uniqueSorted([...currentCodes, ...currentLinkCodes]);

    return {
      id: row.id,
      citation: row.citation,
      title: row.title,
      authorName: row.authorName,
      decisionDate: row.decisionDate,
      currentCodes,
      currentLinkCodes,
      hasCanonicalG93,
      primaryPhraseHit: Boolean(row.primaryPhraseHit),
      corroboratingPhraseHit: Boolean(row.corroboratingPhraseHit),
      ordinance3714Hit: Boolean(row.ordinance3714Hit),
      evidenceSnippet: normalizeWhitespace(row.evidenceSnippet),
      candidate,
      nextCodes
    };
  });

  const candidates = inspected.filter((row) => row.candidate && !row.hasCanonicalG93);

  if (apply && candidates.length > 0) {
    const statements = ["BEGIN IMMEDIATE;"];
    for (const row of candidates) {
      const nextIndexCodesJson = JSON.stringify(uniqueSorted(row.nextCodes));
      statements.push(
        `UPDATE documents
         SET index_codes_json = ${sqlQuote(nextIndexCodesJson)},
             qc_has_index_codes = 1,
             updated_at = datetime('now')
         WHERE id = ${sqlQuote(row.id)};`
      );
      statements.push(
        `DELETE FROM document_reference_links
         WHERE document_id = ${sqlQuote(row.id)}
           AND reference_type = 'index_code';`
      );
      statements.push(
        `DELETE FROM document_reference_issues
         WHERE document_id = ${sqlQuote(row.id)}
           AND reference_type = 'index_code';`
      );
      for (const code of uniqueSorted(row.nextCodes)) {
        statements.push(
          `INSERT INTO document_reference_links (
             id, document_id, reference_type, raw_value, normalized_value, canonical_value, is_valid, created_at
           ) VALUES (
             ${sqlQuote(id("drl"))},
             ${sqlQuote(row.id)},
             'index_code',
             ${sqlQuote(code)},
             ${sqlQuote(normalizeIndexCode(code))},
             ${sqlQuote(code)},
             1,
             datetime('now')
           );`
        );
      }
    }
    statements.push("COMMIT;");
    await runSql(statements.join("\n"));
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    dbPath,
    candidates,
    summary: {
      inspectedDocCount: inspected.length,
      primaryPhraseDocCount: inspected.filter((row) => row.primaryPhraseHit).length,
      corroboratedDocCount: inspected.filter((row) => row.primaryPhraseHit && row.corroboratingPhraseHit).length,
      ordinance3714DocCount: inspected.filter((row) => row.ordinance3714Hit).length,
      candidateCount: candidates.length,
      alreadyCanonicalCount: inspected.filter((row) => row.hasCanonicalG93).length,
      appliedUpdateCount: apply ? candidates.length : 0,
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
  console.log(`G93 remediation JSON report written to ${jsonPath}`);
  console.log(`G93 remediation Markdown report written to ${markdownPath}`);
  console.log(`G93 remediation CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
