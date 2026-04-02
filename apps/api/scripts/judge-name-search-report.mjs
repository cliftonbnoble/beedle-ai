import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.JUDGE_NAME_SEARCH_JSON_NAME || "judge-name-search-report.json";
const markdownName = process.env.JUDGE_NAME_SEARCH_MARKDOWN_NAME || "judge-name-search-report.md";
const csvName = process.env.JUDGE_NAME_SEARCH_CSV_NAME || "judge-name-search-report.csv";
const limit = Number.parseInt(process.env.JUDGE_NAME_SEARCH_LIMIT || "5", 10);
const corpusMode = process.env.JUDGE_NAME_SEARCH_CORPUS_MODE || "trusted_plus_provisional";
const retryCount = Number.parseInt(process.env.JUDGE_NAME_SEARCH_RETRY_COUNT || "3", 10);
const retryDelayMs = Number.parseInt(process.env.JUDGE_NAME_SEARCH_RETRY_DELAY_MS || "1200", 10);
const issueQuery = process.env.JUDGE_NAME_SEARCH_ISSUE || "rent reduction";

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

function avg(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(4));
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJsonWithRetry(url, payload, label) {
  let lastError = null;
  for (let attempt = 1; attempt <= retryCount; attempt += 1) {
    try {
      return await fetchJson(url, payload);
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`[judge-name-search] ${label} attempt ${attempt}/${retryCount} failed: ${message}`);
      if (attempt < retryCount) await sleep(retryDelayMs * attempt);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function buildSpecs() {
  return JUDGES.flatMap((judge) => [
    {
      id: `judge_only:${judge}`,
      lane: "judge_only",
      judgeName: judge,
      query: "decision",
      payload: {
        query: "decision",
        queryType: "keyword",
        limit,
        corpusMode,
        filters: {
          approvedOnly: true,
          judgeName: judge
        }
      }
    },
    {
      id: `judge_issue:${judge}`,
      lane: "judge_plus_issue",
      judgeName: judge,
      query: issueQuery,
      payload: {
        query: issueQuery,
        queryType: "keyword",
        limit,
        corpusMode,
        filters: {
          approvedOnly: true,
          judgeName: judge
        }
      }
    }
  ]);
}

function summarizeResult(spec, response) {
  const results = Array.isArray(response?.results) ? response.results : [];
  return {
    ...spec,
    totalResults: Number(response?.total || results.length || 0),
    returnedAny: results.length > 0,
    uniqueDecisionCount: unique(results.map((row) => row.documentId)).length,
    avgLexicalScore: avg(results.map((row) => Number(row.lexicalScore || 0))),
    avgVectorScore: avg(results.map((row) => Number(row.vectorScore || 0))),
    topResults: results.slice(0, limit).map((row, index) => ({
      rank: index + 1,
      documentId: row.documentId,
      title: row.title,
      authorName: row.authorName || null,
      citation: row.citation,
      sectionLabel: row.sectionLabel,
      score: Number(row.score || 0),
      lexicalScore: Number(row.lexicalScore || 0),
      vectorScore: Number(row.vectorScore || 0),
      snippet: row.snippet || ""
    })),
    error: null
  };
}

function formatMarkdown(report) {
  const lines = [
    "# Judge Name Search Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Query count: \`${report.summary.queryCount}\``,
    `- Returned query count: \`${report.summary.returnedQueryCount}\``,
    `- Failed query count: \`${report.summary.failedQueryCount}\``,
    `- Issue query: \`${report.summary.issueQuery}\``,
    "",
    "## Judge Coverage",
    ""
  ];

  for (const row of report.summary.judgeStats) {
    lines.push(
      `- \`${row.judgeName}\` | judgeOnly=${row.judgeOnlyReturned ? "yes" : "no"} | issueFiltered=${row.issueReturned ? "yes" : "no"} | issueUniqueDecisions=\`${row.issueUniqueDecisionCount}\``
    );
  }

  lines.push("");
  lines.push("## Query Detail");
  lines.push("");

  for (const query of report.queries) {
    lines.push(`### ${query.id}`);
    lines.push("");
    lines.push(`- judge: \`${query.judgeName}\``);
    lines.push(`- lane: \`${query.lane}\``);
    lines.push(`- query: \`${query.query}\``);
    lines.push(`- totalResults: \`${query.totalResults}\``);
    if (query.error) lines.push(`- error: ${query.error}`);
    lines.push("");
    for (const row of query.topResults || []) {
      lines.push(
        `- #${row.rank} \`${row.title}\` | author=\`${row.authorName || "<none>"}\` | score=\`${row.score}\` | chunk=\`${row.sectionLabel}\``
      );
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function formatCsv(report) {
  const header = [
    "query_id",
    "lane",
    "judge_name",
    "query",
    "total_results",
    "unique_decision_count",
    "avg_lexical_score",
    "avg_vector_score",
    "rank",
    "document_id",
    "title",
    "author_name",
    "citation",
    "section_label",
    "score",
    "lexical_score",
    "vector_score",
    "snippet",
    "error"
  ];
  const lines = [header.join(",")];

  for (const query of report.queries) {
    if (!query.topResults.length) {
      lines.push(
        [
          query.id,
          query.lane,
          query.judgeName,
          query.query,
          query.totalResults,
          query.uniqueDecisionCount,
          query.avgLexicalScore,
          query.avgVectorScore,
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          query.error || ""
        ]
          .map(csvEscape)
          .join(",")
      );
      continue;
    }

    for (const row of query.topResults) {
      lines.push(
        [
          query.id,
          query.lane,
          query.judgeName,
          query.query,
          query.totalResults,
          query.uniqueDecisionCount,
          query.avgLexicalScore,
          query.avgVectorScore,
          row.rank,
          row.documentId,
          row.title,
          row.authorName || "",
          row.citation,
          row.sectionLabel,
          row.score,
          row.lexicalScore,
          row.vectorScore,
          row.snippet,
          query.error || ""
        ]
          .map(csvEscape)
          .join(",")
      );
    }
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const specs = buildSpecs();
  const queries = [];

  for (const spec of specs) {
    try {
      const response = await fetchJsonWithRetry(`${apiBase}/admin/retrieval/debug`, spec.payload, spec.id);
      queries.push(summarizeResult(spec, response));
    } catch (error) {
      queries.push({
        ...spec,
        totalResults: 0,
        returnedAny: false,
        uniqueDecisionCount: 0,
        avgLexicalScore: 0,
        avgVectorScore: 0,
        topResults: [],
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  const judgeStats = JUDGES.map((judgeName) => {
    const judgeOnly = queries.find((row) => row.id === `judge_only:${judgeName}`);
    const judgeIssue = queries.find((row) => row.id === `judge_issue:${judgeName}`);
    return {
      judgeName,
      judgeOnlyReturned: Boolean(judgeOnly?.returnedAny),
      issueReturned: Boolean(judgeIssue?.returnedAny),
      issueUniqueDecisionCount: Number(judgeIssue?.uniqueDecisionCount || 0)
    };
  });

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    summary: {
      queryCount: queries.length,
      returnedQueryCount: queries.filter((row) => row.returnedAny).length,
      failedQueryCount: queries.filter((row) => row.error).length,
      issueQuery,
      judgeStats
    },
    queries
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, formatCsv(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Judge name search JSON report written to ${jsonPath}`);
  console.log(`Judge name search Markdown report written to ${markdownPath}`);
  console.log(`Judge name search CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
