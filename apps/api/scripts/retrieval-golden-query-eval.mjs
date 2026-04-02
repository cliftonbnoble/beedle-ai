import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.RETRIEVAL_GOLDEN_QUERY_REPORT_NAME || "retrieval-golden-query-eval-report.json";
const markdownName = process.env.RETRIEVAL_GOLDEN_QUERY_MARKDOWN_NAME || "retrieval-golden-query-eval-report.md";
const csvName = process.env.RETRIEVAL_GOLDEN_QUERY_CSV_NAME || "retrieval-golden-query-eval-report.csv";
const limit = Number.parseInt(process.env.RETRIEVAL_GOLDEN_QUERY_LIMIT || "5", 10);
const corpusMode = process.env.RETRIEVAL_GOLDEN_QUERY_CORPUS_MODE || "trusted_plus_provisional";
const retryCount = Number.parseInt(process.env.RETRIEVAL_GOLDEN_QUERY_RETRY_COUNT || "3", 10);
const retryDelayMs = Number.parseInt(process.env.RETRIEVAL_GOLDEN_QUERY_RETRY_DELAY_MS || "1200", 10);

const GOLDEN_QUERIES = [
  { id: "mold", family: "housing_conditions", query: "mold", queryType: "keyword", expected: "Habitability / housing-conditions decisions mentioning mold or remediation." },
  { id: "ventilation", family: "housing_conditions", query: "ventilation", queryType: "keyword", expected: "Housing-condition decisions discussing ventilation, air flow, or related defects." },
  { id: "cooling", family: "housing_conditions", query: "cooling", queryType: "keyword", expected: "Housing-condition decisions about cooling, overheating, or temperature control." },
  { id: "repair_notice", family: "procedure_notice", query: "repair notice", queryType: "keyword", expected: "Decisions discussing notice, repair requests, or landlord response obligations." },
  { id: "buyout", family: "tenant_landlord_relief", query: "buyout", queryType: "keyword", expected: "Buyout agreement disputes, disclosures, rescission, or settlement issues." },
  { id: "owner_move_in", family: "eviction", query: "owner move-in", queryType: "keyword", expected: "Owner move-in eviction decisions or related procedural analysis." },
  { id: "nuisance", family: "eviction", query: "nuisance", queryType: "keyword", expected: "Nuisance-based eviction or tenant conduct disputes." },
  { id: "capital_improvement", family: "passthroughs", query: "capital improvement", queryType: "keyword", expected: "Capital improvement passthrough decisions and hardship analysis." },
  { id: "habitability", family: "housing_conditions", query: "habitability", queryType: "keyword", expected: "Habitability standards, defects, and remedy analysis." },
  { id: "wrongful_eviction", family: "eviction", query: "wrongful eviction", queryType: "keyword", expected: "Wrongful eviction claims or factual/legal analysis tied to eviction defects." },
  { id: "rent_reduction", family: "rent_adjustment", query: "rent reduction", queryType: "keyword", expected: "Rent reduction decisions and service/habitability-driven rent relief." },
  { id: "harassment", family: "tenant_protection", query: "harassment", queryType: "keyword", expected: "Tenant harassment decisions and related landlord conduct findings." }
];

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
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

async function fetchJsonWithRetry(url, payload, queryId) {
  let lastError = null;
  for (let attempt = 1; attempt <= retryCount; attempt += 1) {
    try {
      return await fetchJson(url, payload);
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`[golden-query] ${queryId} attempt ${attempt}/${retryCount} failed: ${message}`);
      if (attempt < retryCount) {
        await sleep(retryDelayMs * attempt);
      }
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function buildCsv(rows) {
  const header = [
    "query_id",
    "family",
    "query",
    "expected",
    "rank",
    "document_id",
    "title",
    "chunk_type",
    "score",
    "vector_score",
    "lexical_score",
    "citation_anchor",
    "snippet",
    "error",
    "review_label",
    "review_notes"
  ];
  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(
      [
        row.queryId,
        row.family,
        row.query,
        row.expected,
        row.rank,
        row.documentId,
        row.title,
        row.chunkType,
        row.score,
        row.vectorScore,
        row.lexicalScore,
        row.citationAnchor,
        row.snippet,
        row.error || "",
        "",
        ""
      ]
        .map(csvEscape)
        .join(",")
    );
  }
  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Retrieval Golden Query Evaluation",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Corpus mode: \`${report.corpusMode}\``,
    `- Query count: \`${report.summary.queryCount}\``,
    `- Returned query count: \`${report.summary.returnedQueryCount}\``,
    `- Failed query count: \`${report.summary.failedQueryCount}\``,
    `- Unique decisions surfaced: \`${report.summary.uniqueDecisionCount}\``,
    ""
  ];

  for (const query of report.queries) {
    lines.push(`## ${query.query}`);
    lines.push("");
    lines.push(`- family: \`${query.family}\``);
    lines.push(`- expected: ${query.expected}`);
    lines.push(`- totalResults: \`${query.totalResults}\``);
    lines.push(`- uniqueDecisionCount: \`${query.uniqueDecisionCount}\``);
    if (query.error) {
      lines.push(`- error: ${query.error}`);
    }
    lines.push("");
    for (const row of query.topResults) {
      lines.push(
        `- #${row.rank} \`${row.title || row.documentId}\` | chunkType=\`${row.chunkType}\` | score=\`${row.score}\` | citationAnchor=\`${row.citationAnchor || "<none>"}\``
      );
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const queries = [];
  const csvRows = [];

  for (const spec of GOLDEN_QUERIES) {
    try {
      const response = await fetchJsonWithRetry(`${apiBase}/admin/retrieval/debug`, {
        query: spec.query,
        queryType: spec.queryType,
        limit,
        corpusMode,
        filters: { approvedOnly: false }
      }, spec.id);

      const results = Array.isArray(response?.results) ? response.results : [];
      const topResults = results.slice(0, limit).map((row, index) => ({
        rank: index + 1,
        documentId: row.documentId,
        title: row.title,
        chunkId: row.chunkId,
        chunkType: row.sectionLabel || "<none>",
        score: Number(row.score || 0),
        vectorScore: Number(row.vectorScore || 0),
        lexicalScore: Number(row.lexicalScore || 0),
        citationAnchor: row.citationAnchor || null,
        snippet: row.snippet || null,
        error: null
      }));

      const queryRow = {
        ...spec,
        totalResults: Number(response?.total || results.length || 0),
        uniqueDecisionCount: unique(results.map((row) => row.documentId)).length,
        topResults,
        error: null
      };
      queries.push(queryRow);

      for (const row of topResults) {
        csvRows.push({
          queryId: spec.id,
          family: spec.family,
          query: spec.query,
          expected: spec.expected,
          ...row
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      queries.push({
        ...spec,
        totalResults: 0,
        uniqueDecisionCount: 0,
        topResults: [],
        error: message
      });
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    corpusMode,
    limit,
    summary: {
      queryCount: queries.length,
      returnedQueryCount: queries.filter((row) => row.totalResults > 0).length,
      failedQueryCount: queries.filter((row) => row.error).length,
      uniqueDecisionCount: unique(queries.flatMap((row) => row.topResults.map((result) => result.documentId))).length
    },
    queries
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(csvRows));

  if (report.summary.failedQueryCount === report.summary.queryCount) {
    throw new Error("All golden queries failed; report not trustworthy.");
  }

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval golden-query JSON report written to ${jsonPath}`);
  console.log(`Retrieval golden-query Markdown report written to ${markdownPath}`);
  console.log(`Retrieval golden-query CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
