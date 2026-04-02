import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.JUDGE_VERIFICATION_JSON_NAME || "judge-verification-report.json";
const markdownName = process.env.JUDGE_VERIFICATION_MARKDOWN_NAME || "judge-verification-report.md";
const csvName = process.env.JUDGE_VERIFICATION_CSV_NAME || "judge-verification-report.csv";
const limit = Number.parseInt(process.env.JUDGE_VERIFICATION_LIMIT || "5", 10);
const retryCount = Number.parseInt(process.env.JUDGE_VERIFICATION_RETRY_COUNT || "3", 10);
const retryDelayMs = Number.parseInt(process.env.JUDGE_VERIFICATION_RETRY_DELAY_MS || "1200", 10);
const broadQuery = process.env.JUDGE_VERIFICATION_BROAD_QUERY || "housing services";
const issueQuery = process.env.JUDGE_VERIFICATION_ISSUE_QUERY || "rent reduction";
const corpusMode = process.env.JUDGE_VERIFICATION_CORPUS_MODE || "trusted_plus_provisional";

const dbPath =
  process.env.D1_DB_PATH ||
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";

const JUDGES = [
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

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON response from ${url}; received non-JSON.`);
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function fetchJsonWithRetry(url, payload, label) {
  let lastError = null;
  for (let attempt = 1; attempt <= retryCount; attempt += 1) {
    try {
      return await fetchJson(url, payload);
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`[judge-verify] ${label} attempt ${attempt}/${retryCount} failed: ${message}`);
      if (attempt < retryCount) await sleep(retryDelayMs * attempt);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", ".timeout 5000", dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

function formatMarkdown(report) {
  const lines = [
    "# Judge Verification Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Broad query: \`${report.summary.broadQuery}\``,
    `- Issue query: \`${report.summary.issueQuery}\``,
    `- Blank real author count: \`${report.summary.blankRealAuthorCount}\``,
    `- Invalid nonblank author count: \`${report.summary.invalidNonblankAuthorCount}\``,
    "",
    "## Metadata By Judge",
    ""
  ];

  for (const row of report.summary.metadataByJudge) {
    lines.push(`- \`${row.judgeName}\`: \`${row.count}\``);
  }

  lines.push("");
  lines.push("## Top Invalid Author Values");
  lines.push("");
  for (const row of report.summary.topInvalidAuthorValues.slice(0, 25)) {
    lines.push(`- \`${row.authorName}\`: \`${row.count}\``);
  }

  lines.push("");
  lines.push("## Search Coverage");
  lines.push("");
  for (const row of report.summary.searchByJudge) {
    lines.push(
      `- \`${row.judgeName}\` | metadata=\`${row.metadataCount}\` | broadReturned=${row.broadReturned ? "yes" : "no"} | issueReturned=${row.issueReturned ? "yes" : "no"}`
    );
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

function formatCsv(report) {
  const header = ["judge_name", "metadata_count", "broad_returned", "broad_total", "issue_returned", "issue_total"];
  const lines = [header.join(",")];
  for (const row of report.summary.searchByJudge) {
    lines.push(
      [
        row.judgeName,
        row.metadataCount,
        row.broadReturned,
        row.broadTotal,
        row.issueReturned,
        row.issueTotal
      ]
        .map(csvEscape)
        .join(",")
    );
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [blankCounts, invalidValues, metadataByJudge] = await Promise.all([
    runSqlJson(`
      SELECT count(*) AS blankRealAuthorCount
      FROM documents
      WHERE file_type = 'decision_docx'
        AND (author_name IS NULL OR trim(author_name) = '')
        AND (citation IS NULL OR citation NOT LIKE 'BEE-%')
        AND (citation IS NULL OR citation NOT LIKE 'KNOWN-REF-%')
        AND (citation IS NULL OR citation NOT LIKE 'PILOT-%')
        AND (citation IS NULL OR citation NOT LIKE 'HISTORICAL-%');
    `),
    runSqlJson(`
      SELECT author_name AS authorName, count(*) AS count
      FROM documents
      WHERE file_type = 'decision_docx'
        AND author_name IS NOT NULL
        AND trim(author_name) != ''
        AND author_name NOT IN (${JUDGES.map((judge) => `'${judge.replace(/'/g, "''")}'`).join(", ")})
      GROUP BY author_name
      ORDER BY count DESC, author_name ASC
      LIMIT 100;
    `),
    runSqlJson(`
      SELECT author_name AS judgeName, count(*) AS count
      FROM documents
      WHERE file_type = 'decision_docx'
        AND author_name IN (${JUDGES.map((judge) => `'${judge.replace(/'/g, "''")}'`).join(", ")})
      GROUP BY author_name
      ORDER BY count DESC, author_name ASC;
    `)
  ]);

  const invalidNonblankAuthorCount = invalidValues.reduce((sum, row) => sum + Number(row.count || 0), 0);

  const searchByJudge = [];
  for (const judgeName of JUDGES) {
    const broadPayload = {
      query: broadQuery,
      queryType: "keyword",
      limit,
      corpusMode,
      filters: {
        approvedOnly: false,
        judgeName
      }
    };
    const issuePayload = {
      query: issueQuery,
      queryType: "keyword",
      limit,
      corpusMode,
      filters: {
        approvedOnly: false,
        judgeName
      }
    };

    const [broadResponse, issueResponse] = await Promise.all([
      fetchJsonWithRetry(`${apiBase}/search`, broadPayload, `broad:${judgeName}`),
      fetchJsonWithRetry(`${apiBase}/search`, issuePayload, `issue:${judgeName}`)
    ]);

    const metadataCount = Number(metadataByJudge.find((row) => row.judgeName === judgeName)?.count || 0);
    const broadResults = Array.isArray(broadResponse?.results) ? broadResponse.results : [];
    const issueResults = Array.isArray(issueResponse?.results) ? issueResponse.results : [];
    searchByJudge.push({
      judgeName,
      metadataCount,
      broadReturned: broadResults.length > 0,
      broadTotal: Number(broadResponse?.total || broadResults.length || 0),
      issueReturned: issueResults.length > 0,
      issueTotal: Number(issueResponse?.total || issueResults.length || 0)
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    summary: {
      broadQuery,
      issueQuery,
      blankRealAuthorCount: Number(blankCounts[0]?.blankRealAuthorCount || 0),
      invalidNonblankAuthorCount,
      topInvalidAuthorValues: invalidValues.map((row) => ({
        authorName: row.authorName,
        count: Number(row.count || 0)
      })),
      metadataByJudge: metadataByJudge.map((row) => ({
        judgeName: row.judgeName,
        count: Number(row.count || 0)
      })),
      searchByJudge
    }
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, formatCsv(report));

  console.log(
    JSON.stringify(
      {
        blankRealAuthorCount: report.summary.blankRealAuthorCount,
        invalidNonblankAuthorCount: report.summary.invalidNonblankAuthorCount,
        broadReturnedJudges: report.summary.searchByJudge.filter((row) => row.broadReturned).length,
        issueReturnedJudges: report.summary.searchByJudge.filter((row) => row.issueReturned).length
      },
      null,
      2
    )
  );
  console.log(`Judge verification JSON report written to ${jsonPath}`);
  console.log(`Judge verification Markdown report written to ${markdownPath}`);
  console.log(`Judge verification CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
