import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.METADATA_ANOMALY_AUDIT_REPORT_NAME || "document-metadata-anomaly-audit-report.json";
const markdownName = process.env.METADATA_ANOMALY_AUDIT_MARKDOWN_NAME || "document-metadata-anomaly-audit-report.md";

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 20 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

function formatMarkdown(report) {
  const lines = [
    "# Document Metadata Anomaly Audit",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Database: \`${report.dbPath}\``,
    `- Total decision rows: \`${report.summary.totalDecisionRows}\``,
    `- Future-date rows: \`${report.summary.futureDateCount}\``,
    `- Pre-1980 rows: \`${report.summary.pre1980DateCount}\``,
    `- Missing decision-date rows: \`${report.summary.missingDecisionDateCount}\``,
    `- Suspicious title spacing rows: \`${report.summary.suspiciousTitleSpacingCount}\``,
    "",
    "## Future-Dated Rows",
    ""
  ];

  for (const row of report.futureDateRows) {
    lines.push(`- \`${row.citation}\` | \`${row.title}\` | decisionDate=\`${row.decisionDate}\` | createdAt=\`${row.createdAt}\``);
  }

  lines.push("");
  lines.push("## Pre-1980 Rows");
  lines.push("");
  for (const row of report.pre1980DateRows) {
    lines.push(`- \`${row.citation}\` | \`${row.title}\` | decisionDate=\`${row.decisionDate}\` | createdAt=\`${row.createdAt}\``);
  }

  lines.push("");
  lines.push("## Suspicious Title Spacing");
  lines.push("");
  for (const row of report.suspiciousTitleSpacingRows) {
    lines.push(`- \`${row.citation}\` | \`${row.title}\``);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [summaryRow] = await runSqlJson(`
    SELECT
      COUNT(*) AS totalDecisionRows,
      SUM(CASE WHEN decision_date > date('now', '+1 year') THEN 1 ELSE 0 END) AS futureDateCount,
      SUM(CASE WHEN decision_date < '1980-01-01' THEN 1 ELSE 0 END) AS pre1980DateCount,
      SUM(CASE WHEN decision_date IS NULL OR decision_date = '' THEN 1 ELSE 0 END) AS missingDecisionDateCount,
      SUM(CASE WHEN title LIKE '%  %' OR title LIKE '%.docx%' OR title LIKE '% .docx%' THEN 1 ELSE 0 END) AS suspiciousTitleSpacingCount
    FROM documents
    WHERE file_type = 'decision_docx';
  `);

  const futureDateRows = await runSqlJson(`
    SELECT id, title, citation, decision_date AS decisionDate, created_at AS createdAt
    FROM documents
    WHERE file_type = 'decision_docx'
      AND decision_date > date('now', '+1 year')
    ORDER BY decision_date ASC, created_at DESC
    LIMIT 25;
  `);

  const pre1980DateRows = await runSqlJson(`
    SELECT id, title, citation, decision_date AS decisionDate, created_at AS createdAt
    FROM documents
    WHERE file_type = 'decision_docx'
      AND decision_date < '1980-01-01'
    ORDER BY decision_date ASC, created_at DESC
    LIMIT 25;
  `);

  const suspiciousTitleSpacingRows = await runSqlJson(`
    SELECT id, title, citation, decision_date AS decisionDate, created_at AS createdAt
    FROM documents
    WHERE file_type = 'decision_docx'
      AND (title LIKE '%  %' OR title LIKE '%.docx%' OR title LIKE '% .docx%')
    ORDER BY created_at DESC
    LIMIT 25;
  `);

  const report = {
    generatedAt: new Date().toISOString(),
    dbPath,
    summary: {
      totalDecisionRows: Number(summaryRow?.totalDecisionRows || 0),
      futureDateCount: Number(summaryRow?.futureDateCount || 0),
      pre1980DateCount: Number(summaryRow?.pre1980DateCount || 0),
      missingDecisionDateCount: Number(summaryRow?.missingDecisionDateCount || 0),
      suspiciousTitleSpacingCount: Number(summaryRow?.suspiciousTitleSpacingCount || 0)
    },
    futureDateRows,
    pre1980DateRows,
    suspiciousTitleSpacingRows
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Document metadata anomaly audit JSON report written to ${jsonPath}`);
  console.log(`Document metadata anomaly audit Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
