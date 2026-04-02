import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.RETRIEVAL_ISSUE_QUALITY_REPORT_NAME || "retrieval-issue-quality-audit-report.json";
const markdownName = process.env.RETRIEVAL_ISSUE_QUALITY_MARKDOWN_NAME || "retrieval-issue-quality-audit-report.md";
const limit = Number.parseInt(process.env.RETRIEVAL_ISSUE_QUALITY_LIMIT || "8", 10);
const corpusMode = process.env.RETRIEVAL_ISSUE_QUALITY_CORPUS_MODE || "trusted_plus_provisional";
const retryCount = Number.parseInt(process.env.RETRIEVAL_ISSUE_QUALITY_RETRY_COUNT || "3", 10);
const retryDelayMs = Number.parseInt(process.env.RETRIEVAL_ISSUE_QUALITY_RETRY_DELAY_MS || "1200", 10);

const ISSUE_QUERIES = [
  { id: "mold", family: "housing_conditions", query: "mold", queryType: "keyword" },
  { id: "ventilation", family: "housing_conditions", query: "ventilation", queryType: "keyword" },
  { id: "cooling", family: "housing_conditions", query: "cooling", queryType: "keyword" },
  { id: "repair_notice", family: "procedure_notice", query: "repair notice", queryType: "keyword" },
  { id: "buyout", family: "tenant_landlord_relief", query: "buyout", queryType: "keyword" },
  { id: "owner_move_in", family: "eviction", query: "owner move-in", queryType: "keyword" },
  { id: "nuisance", family: "eviction", query: "nuisance", queryType: "keyword" },
  { id: "capital_improvement", family: "passthroughs", query: "capital improvement", queryType: "keyword" },
  { id: "habitability", family: "housing_conditions", query: "habitability", queryType: "keyword" },
  { id: "wrongful_eviction", family: "eviction", query: "wrongful eviction", queryType: "keyword" },
  { id: "rent_reduction", family: "rent_adjustment", query: "rent reduction", queryType: "keyword" },
  { id: "harassment", family: "tenant_protection", query: "harassment", queryType: "keyword" }
];

function avg(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(4));
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
      console.warn(`[issue-quality] ${queryId} attempt ${attempt}/${retryCount} failed: ${message}`);
      if (attempt < retryCount) {
        await sleep(retryDelayMs * attempt);
      }
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function summarizeQuery(spec, response) {
  const results = Array.isArray(response?.results) ? response.results : [];
  const uniqueDecisionIds = unique(results.map((row) => row.documentId));
  const chunkTypes = unique(results.map((row) => row.sectionLabel || "<none>"));
  const topResults = results.slice(0, 5).map((row) => ({
    documentId: row.documentId,
    title: row.title,
    chunkId: row.chunkId,
    chunkType: row.sectionLabel || "<none>",
    score: Number(row.score || 0),
    vectorScore: Number(row.vectorScore || 0),
    lexicalScore: Number(row.lexicalScore || 0),
    citationAnchor: row.citationAnchor || null,
    snippet: row.snippet || null
  }));

  return {
    id: spec.id,
    family: spec.family,
    query: spec.query,
    queryType: spec.queryType,
    totalResults: Number(response?.total || results.length || 0),
    returnedAny: results.length > 0,
    uniqueDecisionCount: uniqueDecisionIds.length,
    uniqueDecisionIds: uniqueDecisionIds.slice(0, 10),
    chunkTypes,
    avgScore: avg(results.map((row) => Number(row.score || 0))),
    avgVectorScore: avg(results.map((row) => Number(row.vectorScore || 0))),
    avgLexicalScore: avg(results.map((row) => Number(row.lexicalScore || 0))),
    vectorMatchCount: Number(response?.runtimeDiagnostics?.vectorMatchCount || 0),
    topResults,
    error: null
  };
}

function summarizeAudit(queryRows) {
  const returned = queryRows.filter((row) => row.returnedAny);
  const uniqueDecisionUniverse = unique(returned.flatMap((row) => row.uniqueDecisionIds || []));

  const familyStats = Array.from(
    queryRows.reduce((map, row) => {
      const current = map.get(row.family) || {
        family: row.family,
        queryCount: 0,
        returnedCount: 0,
        totalResults: 0,
        uniqueDecisionCountSum: 0,
        vectorMatchCountSum: 0
      };
      current.queryCount += 1;
      current.returnedCount += row.returnedAny ? 1 : 0;
      current.totalResults += Number(row.totalResults || 0);
      current.uniqueDecisionCountSum += Number(row.uniqueDecisionCount || 0);
      current.vectorMatchCountSum += Number(row.vectorMatchCount || 0);
      map.set(row.family, current);
      return map;
    }, new Map()).values()
  ).map((entry) => ({
    family: entry.family,
    queryCount: entry.queryCount,
    returnedCount: entry.returnedCount,
    hitRate: entry.queryCount > 0 ? Number((entry.returnedCount / entry.queryCount).toFixed(4)) : 0,
    avgResults: entry.queryCount > 0 ? Number((entry.totalResults / entry.queryCount).toFixed(4)) : 0,
    avgDecisionDiversity: entry.queryCount > 0 ? Number((entry.uniqueDecisionCountSum / entry.queryCount).toFixed(4)) : 0,
    avgVectorMatchCount: entry.queryCount > 0 ? Number((entry.vectorMatchCountSum / entry.queryCount).toFixed(4)) : 0
  }));

  return {
    queryCount: queryRows.length,
    returnedQueryCount: returned.length,
    failedQueryCount: queryRows.filter((row) => row.error).length,
    overallHitRate: queryRows.length > 0 ? Number((returned.length / queryRows.length).toFixed(4)) : 0,
    avgResultsPerQuery: avg(queryRows.map((row) => Number(row.totalResults || 0))),
    avgDecisionDiversity: avg(queryRows.map((row) => Number(row.uniqueDecisionCount || 0))),
    avgVectorMatchCount: avg(queryRows.map((row) => Number(row.vectorMatchCount || 0))),
    uniqueDecisionUniverseCount: uniqueDecisionUniverse.length,
    uniqueDecisionUniverse,
    familyStats
  };
}

function formatMarkdown(report) {
  const lines = [
    "# Retrieval Issue Quality Audit",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Corpus mode: \`${report.corpusMode}\``,
    `- Query count: \`${report.summary.queryCount}\``,
    `- Returned query count: \`${report.summary.returnedQueryCount}\``,
    `- Failed query count: \`${report.summary.failedQueryCount}\``,
    `- Overall hit rate: \`${report.summary.overallHitRate}\``,
    `- Avg results/query: \`${report.summary.avgResultsPerQuery}\``,
    `- Avg decision diversity/query: \`${report.summary.avgDecisionDiversity}\``,
    `- Unique decision universe: \`${report.summary.uniqueDecisionUniverseCount}\``,
    "",
    "## Family Coverage",
    ""
  ];

  for (const family of report.summary.familyStats || []) {
    lines.push(
      `- \`${family.family}\` | hitRate=${family.hitRate} | avgResults=${family.avgResults} | avgDecisionDiversity=${family.avgDecisionDiversity} | avgVectorMatchCount=${family.avgVectorMatchCount}`
    );
  }

  lines.push("");
  lines.push("## Query Detail");
  lines.push("");

  for (const row of report.queries || []) {
    lines.push(
      `### ${row.query}`,
      "",
      `- family: \`${row.family}\``,
      `- totalResults: \`${row.totalResults}\``,
      `- uniqueDecisionCount: \`${row.uniqueDecisionCount}\``,
      `- chunkTypes: \`${row.chunkTypes.join(", ") || "<none>"}\``,
      `- avgScore: \`${row.avgScore}\` | avgVectorScore: \`${row.avgVectorScore}\` | avgLexicalScore: \`${row.avgLexicalScore}\``,
      ...(row.error ? [`- error: ${row.error}`] : []),
      ""
    );

    for (const result of row.topResults || []) {
      lines.push(
        `- \`${result.title || result.documentId}\` | chunkType=\`${result.chunkType}\` | score=\`${result.score}\` | citationAnchor=\`${result.citationAnchor || "<none>"}\``
      );
    }

    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const queryRows = [];
  for (const spec of ISSUE_QUERIES) {
    try {
      const response = await fetchJsonWithRetry(`${apiBase}/admin/retrieval/debug`, {
        query: spec.query,
        queryType: spec.queryType,
        limit,
        corpusMode,
        filters: { approvedOnly: false }
      }, spec.id);
      queryRows.push(summarizeQuery(spec, response));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      queryRows.push({
        id: spec.id,
        family: spec.family,
        query: spec.query,
        queryType: spec.queryType,
        totalResults: 0,
        returnedAny: false,
        uniqueDecisionCount: 0,
        uniqueDecisionIds: [],
        chunkTypes: [],
        avgScore: 0,
        avgVectorScore: 0,
        avgLexicalScore: 0,
        vectorMatchCount: 0,
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
    queries: queryRows,
    summary: summarizeAudit(queryRows)
  };

  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  if (report.summary.failedQueryCount === report.summary.queryCount) {
    throw new Error("All issue-quality queries failed; report not trustworthy.");
  }

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval issue-quality audit JSON report written to ${jsonPath}`);
  console.log(`Retrieval issue-quality audit Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
