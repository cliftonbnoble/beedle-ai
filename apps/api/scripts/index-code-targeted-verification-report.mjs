import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.INDEX_CODE_TARGETED_VERIFY_JSON_NAME || "index-code-targeted-verification-report.json";
const markdownName = process.env.INDEX_CODE_TARGETED_VERIFY_MARKDOWN_NAME || "index-code-targeted-verification-report.md";
const csvName = process.env.INDEX_CODE_TARGETED_VERIFY_CSV_NAME || "index-code-targeted-verification-report.csv";
const limit = Number.parseInt(process.env.INDEX_CODE_TARGETED_VERIFY_LIMIT || "6", 10);
const corpusMode = process.env.INDEX_CODE_TARGETED_VERIFY_CORPUS_MODE || "trusted_plus_provisional";
const retryCount = Number.parseInt(process.env.INDEX_CODE_TARGETED_VERIFY_RETRY_COUNT || "3", 10);
const retryDelayMs = Number.parseInt(process.env.INDEX_CODE_TARGETED_VERIFY_RETRY_DELAY_MS || "1000", 10);
const requestTimeoutMs = Number.parseInt(process.env.INDEX_CODE_TARGETED_VERIFY_REQUEST_TIMEOUT_MS || "15000", 10);
const g93JudgeName = process.env.INDEX_CODE_TARGETED_VERIFY_G93_JUDGE || "Andrew Yick";

const PROBES = [
  {
    id: "g27_issue",
    lane: "g27_issue",
    code: "G27",
    label: "DHS substantial decrease",
    query: "decrease in services",
    expectation: "Should surface substantial DHS decisions for G27 without relying on a numeric alias.",
    filters: {
      approvedOnly: false,
      indexCodes: ["G27"]
    }
  },
  {
    id: "g28_filter_only",
    lane: "g28_filter_only",
    code: "G28",
    label: "DHS not substantial filter-only",
    query: "decision",
    expectation: "Should surface not-substantial DHS decisions when filtering only by G28.",
    filters: {
      approvedOnly: false,
      indexCodes: ["G28"]
    }
  },
  {
    id: "g28_issue",
    lane: "g28_issue",
    code: "G28",
    label: "DHS not substantial issue",
    query: "decrease in services",
    expectation: "Should keep not-substantial DHS decisions in play on a short issue search.",
    filters: {
      approvedOnly: false,
      indexCodes: ["G28"]
    }
  },
  {
    id: "g93_filter_only",
    lane: "g93_filter_only",
    code: "G93",
    label: "Uniform visitor policy filter-only",
    query: "decision",
    expectation: "Should surface uniform-visitor-policy decisions when filtering only by G93.",
    filters: {
      approvedOnly: false,
      indexCodes: ["G93"]
    }
  },
  {
    id: "g93_issue",
    lane: "g93_issue",
    code: "G93",
    label: "Uniform visitor policy rent reduction",
    query: "rent reduction",
    expectation: "Should keep G93 decisions in play on a rent-reduction query.",
    filters: {
      approvedOnly: false,
      indexCodes: ["G93"]
    }
  },
  {
    id: "g93_judge_issue",
    lane: "g93_judge_issue",
    code: "G93",
    label: "Judge + G93 sanity check",
    query: "rent reduction",
    expectation: `Should preserve G93 decisions for a known judge-specific query (${g93JudgeName} + G93).`,
    filters: {
      approvedOnly: false,
      judgeNames: [g93JudgeName],
      indexCodes: ["G93"]
    }
  }
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
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(requestTimeoutMs)
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
      console.warn(`[index-code-targeted-verify] ${label} attempt ${attempt}/${retryCount} failed: ${message}`);
      if (attempt < retryCount) await sleep(retryDelayMs * attempt);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function summarizeQuery(spec, response) {
  const results = Array.isArray(response?.results) ? response.results : [];
  return {
    id: spec.id,
    lane: spec.lane,
    code: spec.code,
    label: spec.label,
    query: spec.query,
    expectation: spec.expectation,
    totalResults: Number(response?.total || results.length || 0),
    returnedAny: results.length > 0,
    uniqueDecisionCount: unique(results.map((row) => row.documentId)).length,
    avgScore: avg(results.map((row) => Number(row.score || 0))),
    avgVectorScore: avg(results.map((row) => Number(row.vectorScore || 0))),
    avgLexicalScore: avg(results.map((row) => Number(row.lexicalScore || 0))),
    hasMore: Boolean(response?.hasMore),
    topResults: results.slice(0, limit).map((row, index) => ({
      rank: index + 1,
      documentId: row.documentId,
      title: row.title,
      authorName: row.authorName || null,
      chunkType: row.chunkType || row.sectionLabel || "<none>",
      citationAnchor: row.citationAnchor || null,
      score: Number(row.score || 0),
      snippet: row.snippet || null
    })),
    error: null
  };
}

function buildCsv(report) {
  const header = [
    "probe_id",
    "lane",
    "index_code",
    "label",
    "query",
    "expectation",
    "total_results",
    "unique_decision_count",
    "avg_score",
    "avg_vector_score",
    "avg_lexical_score",
    "has_more",
    "rank",
    "document_id",
    "title",
    "author_name",
    "chunk_type",
    "citation_anchor",
    "score",
    "snippet",
    "error"
  ];
  const lines = [header.join(",")];

  for (const probe of report.queries) {
    if (!probe.topResults.length) {
      lines.push(
        [
          probe.id,
          probe.lane,
          probe.code,
          probe.label,
          probe.query,
          probe.expectation,
          probe.totalResults,
          probe.uniqueDecisionCount,
          probe.avgScore,
          probe.avgVectorScore,
          probe.avgLexicalScore,
          probe.hasMore,
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          "",
          probe.error || ""
        ]
          .map(csvEscape)
          .join(",")
      );
      continue;
    }

    for (const row of probe.topResults) {
      lines.push(
        [
          probe.id,
          probe.lane,
          probe.code,
          probe.label,
          probe.query,
          probe.expectation,
          probe.totalResults,
          probe.uniqueDecisionCount,
          probe.avgScore,
          probe.avgVectorScore,
          probe.avgLexicalScore,
          probe.hasMore,
          row.rank,
          row.documentId,
          row.title,
          row.authorName || "",
          row.chunkType,
          row.citationAnchor || "",
          row.score,
          row.snippet || "",
          probe.error || ""
        ]
          .map(csvEscape)
          .join(",")
      );
    }
  }

  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Index Code Targeted Verification Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Corpus mode: \`${report.corpusMode}\``,
    `- Query count: \`${report.summary.queryCount}\``,
    `- Returned query count: \`${report.summary.returnedQueryCount}\``,
    `- Failed query count: \`${report.summary.failedQueryCount}\``,
    `- Overall hit rate: \`${report.summary.overallHitRate}\``,
    "",
    "## Probe Coverage",
    ""
  ];

  for (const row of report.queries) {
    lines.push(
      `- \`${row.id}\` | returned=${row.returnedAny ? "yes" : "no"} | total=${row.totalResults} | unique=${row.uniqueDecisionCount} | avgScore=${row.avgScore}`
    );
  }

  lines.push("");
  lines.push("## Probe Detail");
  lines.push("");

  for (const row of report.queries) {
    lines.push(`### ${row.id}`);
    lines.push("");
    lines.push(`- lane: \`${row.lane}\``);
    lines.push(`- indexCode: \`${row.code}\``);
    lines.push(`- query: \`${row.query}\``);
    lines.push(`- expectation: ${row.expectation}`);
    lines.push(`- totalResults: \`${row.totalResults}\``);
    lines.push(`- uniqueDecisionCount: \`${row.uniqueDecisionCount}\``);
    lines.push(`- avgScore: \`${row.avgScore}\` | avgVectorScore: \`${row.avgVectorScore}\` | avgLexicalScore: \`${row.avgLexicalScore}\``);
    if (row.error) lines.push(`- error: ${row.error}`);
    lines.push("");

    for (const result of row.topResults) {
      lines.push(`- #${result.rank} \`${result.title || result.documentId}\` | author=\`${result.authorName || "<none>"}\` | score=\`${result.score}\``);
      lines.push(`  - citation: \`${result.citationAnchor || "<none>"}\``);
      lines.push(`  - chunkType: \`${result.chunkType}\``);
      lines.push(`  - snippet: ${result.snippet || "<none>"}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const queryRows = [];
  for (const spec of PROBES) {
    try {
      const response = await fetchJsonWithRetry(
        `${apiBase}/search`,
        {
          query: spec.query,
          limit,
          corpusMode,
          filters: spec.filters
        },
        spec.id
      );
      queryRows.push(summarizeQuery(spec, response));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      queryRows.push({
        id: spec.id,
        lane: spec.lane,
        code: spec.code,
        label: spec.label,
        query: spec.query,
        expectation: spec.expectation,
        totalResults: 0,
        returnedAny: false,
        uniqueDecisionCount: 0,
        avgScore: 0,
        avgVectorScore: 0,
        avgLexicalScore: 0,
        hasMore: false,
        topResults: [],
        error: message
      });
    }
  }

  const returned = queryRows.filter((row) => row.returnedAny);
  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    corpusMode,
    limit,
    g93JudgeName,
    queries: queryRows,
    summary: {
      queryCount: queryRows.length,
      returnedQueryCount: returned.length,
      failedQueryCount: queryRows.filter((row) => row.error).length,
      overallHitRate: queryRows.length > 0 ? Number((returned.length / queryRows.length).toFixed(4)) : 0
    }
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Index code targeted verification JSON report written to ${jsonPath}`);
  console.log(`Index code targeted verification Markdown report written to ${markdownPath}`);
  console.log(`Index code targeted verification CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
