import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const apply = (process.env.G2728_REMEDIATION_APPLY || "0") === "1";
const busyTimeoutMs = Number(process.env.G2728_REMEDIATION_BUSY_TIMEOUT_MS || "5000");
const allowlistPath = path.resolve(
  process.cwd(),
  process.env.G2728_REMEDIATION_ALLOWLIST || "./scripts/index-code-g2728-remediation-allowlist.json"
);
const auditReportPath = path.resolve(
  process.cwd(),
  process.env.G2728_REMEDIATION_AUDIT_REPORT || "./reports/index-code-g2728-remediation-audit-report.json"
);
const jsonName = process.env.G2728_REMEDIATION_REPORT_NAME || "index-code-g2728-remediation-write-report.json";
const markdownName = process.env.G2728_REMEDIATION_MARKDOWN_NAME || "index-code-g2728-remediation-write-report.md";
const csvName = process.env.G2728_REMEDIATION_CSV_NAME || "index-code-g2728-remediation-write-report.csv";

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

async function readJson(filePath, fallback = null) {
  try {
    const content = await fs.readFile(filePath, "utf8");
    return JSON.parse(content);
  } catch {
    return fallback;
  }
}

function validateApprovedEntries(entries) {
  const allowedCodes = new Set(["G27", "G28"]);
  return entries.map((entry) => {
    const approvedCodes = uniqueSorted((entry.approvedCodes || []).map(normalizeWhitespace)).filter((code) => allowedCodes.has(code));
    return {
      citation: normalizeWhitespace(entry.citation),
      approvedCodes,
      reviewReason: normalizeWhitespace(entry.reviewReason),
      reviewer: normalizeWhitespace(entry.reviewer)
    };
  });
}

function buildCsv(report) {
  const header = [
    "citation",
    "title",
    "author_name",
    "decision_date",
    "approved_codes",
    "current_codes",
    "current_link_codes",
    "next_codes",
    "already_satisfied",
    "review_reason",
    "audit_evidence"
  ];
  const lines = [header.join(",")];

  for (const row of report.rows) {
    lines.push(
      [
        row.citation,
        row.title || "",
        row.authorName || "",
        row.decisionDate || "",
        row.approvedCodes.join("; "),
        row.currentCodes.join("; "),
        row.currentLinkCodes.join("; "),
        row.nextCodes.join("; "),
        row.alreadySatisfied,
        row.reviewReason || "",
        row.auditEvidence || ""
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
    "# G27/G28 Remediation Write Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- Database: \`${report.dbPath}\``,
    `- Allowlist: \`${report.allowlistPath}\``,
    "",
    "## Summary",
    "",
    `- allowlist approved entries: \`${report.summary.allowlistApprovedCount}\``,
    `- matched docs: \`${report.summary.matchedDocCount}\``,
    `- unmatched citations: \`${report.summary.unmatchedCitationCount}\``,
    `- already satisfied: \`${report.summary.alreadySatisfiedCount}\``,
    `- pending updates: \`${report.summary.pendingUpdateCount}\``,
    `- applied updates: \`${report.summary.appliedUpdateCount}\``,
    "",
    "## By Code",
    ""
  ];

  for (const row of report.summary.byCode) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }

  if (report.summary.unmatchedCitations.length > 0) {
    lines.push("");
    lines.push("## Unmatched Citations");
    lines.push("");
    for (const citation of report.summary.unmatchedCitations) {
      lines.push(`- \`${citation}\``);
    }
  }

  lines.push("");
  lines.push("## Reviewed Rows");
  lines.push("");

  for (const row of report.rows) {
    lines.push(
      `- \`${row.citation}\` | approved=\`${row.approvedCodes.join(", ")}\` | current=\`${row.currentCodes.join(", ") || "<none>"}\` | next=\`${row.nextCodes.join(", ")}\` | alreadySatisfied=\`${row.alreadySatisfied}\``
    );
    if (row.reviewReason) lines.push(`  - review: ${row.reviewReason}`);
    if (row.auditEvidence) lines.push(`  - evidence: ${row.auditEvidence}`);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const allowlist = (await readJson(allowlistPath, { approved: [] })) || { approved: [] };
  const auditReport = (await readJson(auditReportPath, { candidates: [] })) || { candidates: [] };
  const auditByCitation = new Map(
    (auditReport.candidates || []).map((row) => [normalizeWhitespace(row.citation), normalizeWhitespace(row.g27Evidence || row.g28Evidence || "")])
  );

  const approvedEntries = validateApprovedEntries(allowlist.approved || []).filter(
    (entry) => entry.citation && entry.approvedCodes.length > 0
  );

  const citations = uniqueSorted(approvedEntries.map((entry) => entry.citation));
  const unmatchedCitations = [];
  let docs = [];

  if (citations.length > 0) {
    const citationList = citations.map(sqlQuote).join(", ");
    docs = await runSqlJson(`
      SELECT
        d.id,
        d.citation,
        d.title,
        d.author_name AS authorName,
        d.decision_date AS decisionDate,
        d.index_codes_json AS indexCodesJson
      FROM documents d
      WHERE d.file_type = 'decision_docx'
        AND d.citation IN (${citationList})
      ORDER BY d.decision_date DESC, d.citation ASC;
    `);
  }

  const docIds = docs.map((row) => row.id).filter(Boolean);
  let links = [];
  if (docIds.length > 0) {
    const idList = docIds.map(sqlQuote).join(", ");
    links = await runSqlJson(`
      SELECT
        document_id AS documentId,
        canonical_value AS canonicalValue,
        raw_value AS rawValue,
        is_valid AS isValid
      FROM document_reference_links
      WHERE reference_type = 'index_code'
        AND document_id IN (${idList});
    `);
  }

  const linkCodesByDocumentId = new Map();
  for (const row of links) {
    if (!row.documentId || Number(row.isValid || 0) !== 1) continue;
    const current = linkCodesByDocumentId.get(row.documentId) || [];
    current.push(normalizeWhitespace(row.canonicalValue || row.rawValue));
    linkCodesByDocumentId.set(row.documentId, current);
  }

  const docsByCitation = new Map(docs.map((row) => [normalizeWhitespace(row.citation), row]));

  const rows = approvedEntries
    .map((entry) => {
      const doc = docsByCitation.get(entry.citation);
      if (!doc) {
        unmatchedCitations.push(entry.citation);
        return null;
      }

      const currentCodes = uniqueSorted(parseJsonArray(doc.indexCodesJson).map(normalizeWhitespace));
      const currentLinkCodes = uniqueSorted((linkCodesByDocumentId.get(doc.id) || []).map(normalizeWhitespace));
      const nextCodes = uniqueSorted([...currentCodes, ...currentLinkCodes, ...entry.approvedCodes]);
      const currentNormalized = new Set([...currentCodes, ...currentLinkCodes].map(normalizeIndexCode));
      const alreadySatisfied = entry.approvedCodes.every((code) => currentNormalized.has(normalizeIndexCode(code)));

      return {
        id: doc.id,
        citation: doc.citation,
        title: doc.title,
        authorName: doc.authorName,
        decisionDate: doc.decisionDate,
        approvedCodes: entry.approvedCodes,
        currentCodes,
        currentLinkCodes,
        nextCodes,
        alreadySatisfied,
        reviewReason: entry.reviewReason,
        auditEvidence: auditByCitation.get(entry.citation) || ""
      };
    })
    .filter(Boolean);

  const pendingUpdates = rows.filter((row) => !row.alreadySatisfied);

  if (apply && pendingUpdates.length > 0) {
    const statements = ["BEGIN IMMEDIATE;"];
    for (const row of pendingUpdates) {
      const nextIndexCodesJson = JSON.stringify(row.nextCodes);
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
      for (const code of row.nextCodes) {
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
    allowlistPath,
    rows,
    summary: {
      allowlistApprovedCount: approvedEntries.length,
      matchedDocCount: rows.length,
      unmatchedCitationCount: unmatchedCitations.length,
      unmatchedCitations: uniqueSorted(unmatchedCitations),
      alreadySatisfiedCount: rows.filter((row) => row.alreadySatisfied).length,
      pendingUpdateCount: pendingUpdates.length,
      appliedUpdateCount: apply ? pendingUpdates.length : 0,
      byCode: countBy(rows.flatMap((row) => row.approvedCodes))
    }
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`G27/G28 remediation write JSON report written to ${jsonPath}`);
  console.log(`G27/G28 remediation write Markdown report written to ${markdownPath}`);
  console.log(`G27/G28 remediation write CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
