import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.METADATA_REPAIR_REPORT_NAME || "document-metadata-repair-report.json";
const markdownName = process.env.METADATA_REPAIR_MARKDOWN_NAME || "document-metadata-repair-report.md";
const apply = (process.env.METADATA_REPAIR_APPLY || "0") === "1";

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 50 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

async function runSql(sql) {
  await execFileAsync("sqlite3", [dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 50 * 1024 * 1024
  });
}

function sqlQuote(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function normalizeTitle(title) {
  return String(title || "")
    .replace(/\s+/g, " ")
    .replace(/\s+-\s+/g, " - ")
    .trim();
}

function formatMarkdown(report) {
  const lines = [
    "# Document Metadata Repair Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- Database: \`${report.dbPath}\``,
    `- Implausible dates cleared: \`${report.summary.implausibleDatesCleared}\``,
    `- Titles normalized: \`${report.summary.titlesNormalized}\``,
    "",
    "## Planned Date Clears",
    ""
  ];

  for (const row of report.implausibleDateRepairs) {
    lines.push(`- \`${row.citation}\` | \`${row.title}\` | oldDate=\`${row.oldDecisionDate}\``);
  }

  lines.push("");
  lines.push("## Planned Title Normalizations");
  lines.push("");
  for (const row of report.titleRepairs.slice(0, 25)) {
    lines.push(`- \`${row.citation}\` | before=\`${row.oldTitle}\` | after=\`${row.newTitle}\``);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const implausibleDates = await runSqlJson(`
    SELECT id, citation, title, decision_date AS oldDecisionDate
    FROM documents
    WHERE file_type = 'decision_docx'
      AND (decision_date > date('now', '+1 year') OR decision_date < '1980-01-01')
    ORDER BY decision_date ASC, created_at DESC;
  `);

  const suspiciousTitles = await runSqlJson(`
    SELECT id, citation, title AS oldTitle
    FROM documents
    WHERE file_type = 'decision_docx'
      AND (title LIKE '%  %' OR title LIKE '% -  %' OR title LIKE '%  - %')
    ORDER BY created_at DESC;
  `);

  const titleRepairs = suspiciousTitles
    .map((row) => ({
      id: row.id,
      citation: row.citation,
      oldTitle: row.oldTitle,
      newTitle: normalizeTitle(row.oldTitle)
    }))
    .filter((row) => row.oldTitle !== row.newTitle);

  if (apply) {
    const statements = ["BEGIN IMMEDIATE;"];

    for (const row of implausibleDates) {
      statements.push(
        `UPDATE documents SET decision_date = NULL, updated_at = datetime('now') WHERE id = ${sqlQuote(row.id)};`
      );
    }

    for (const row of titleRepairs) {
      statements.push(
        `UPDATE documents SET title = ${sqlQuote(row.newTitle)}, updated_at = datetime('now') WHERE id = ${sqlQuote(row.id)};`
      );
    }

    statements.push("COMMIT;");
    await runSql(statements.join("\n"));
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    dbPath,
    summary: {
      implausibleDatesCleared: apply ? implausibleDates.length : 0,
      titlesNormalized: apply ? titleRepairs.length : 0,
      proposedImplausibleDateClears: implausibleDates.length,
      proposedTitleNormalizations: titleRepairs.length
    },
    implausibleDateRepairs: implausibleDates,
    titleRepairs
  };

  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Document metadata repair JSON report written to ${jsonPath}`);
  console.log(`Document metadata repair Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
