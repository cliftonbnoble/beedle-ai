import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.JUDGE_SEARCH_SMOKE_JSON_NAME || "judge-search-smoke-test-report.json";
const markdownName = process.env.JUDGE_SEARCH_SMOKE_MARKDOWN_NAME || "judge-search-smoke-test-report.md";
const csvName = process.env.JUDGE_SEARCH_SMOKE_CSV_NAME || "judge-search-smoke-test-report.csv";
const limit = Number.parseInt(process.env.JUDGE_SEARCH_SMOKE_LIMIT || "5", 10);
const corpusMode = process.env.JUDGE_SEARCH_SMOKE_CORPUS_MODE || "trusted_plus_provisional";
const retryCount = Number.parseInt(process.env.JUDGE_SEARCH_SMOKE_RETRY_COUNT || "3", 10);
const retryDelayMs = Number.parseInt(process.env.JUDGE_SEARCH_SMOKE_RETRY_DELAY_MS || "1200", 10);

const SMOKE_QUERIES = [
  { id: "mold", lane: "habitability", query: "mold", queryType: "keyword", expectation: "Strong habitability decisions about mold or remediation." },
  { id: "ventilation", lane: "habitability", query: "ventilation", queryType: "keyword", expectation: "Housing-condition decisions discussing ventilation, air flow, or stale air." },
  { id: "cooling", lane: "habitability", query: "cooling", queryType: "keyword", expectation: "Housing-condition decisions discussing overheating, cooling, or temperature control." },
  { id: "heat", lane: "habitability", query: "heat", queryType: "keyword", expectation: "Decisions involving loss of heat or inadequate heating service." },
  { id: "leak", lane: "habitability", query: "water leak in ceiling", queryType: "keyword", expectation: "Decisions about water intrusion, roof leaks, or ceiling leaks." },
  { id: "repair_notice", lane: "procedure", query: "repair notice", queryType: "keyword", expectation: "Decisions about repair requests, notice, and landlord response obligations." },
  { id: "work_order", lane: "procedure", query: "work order for repairs", queryType: "keyword", expectation: "Decisions where work orders or repair logs matter to the analysis." },
  { id: "hearing_notice", lane: "procedure", query: "notice of hearing", queryType: "keyword", expectation: "Procedural decisions discussing hearing notice, service, or scheduling." },
  { id: "buyout", lane: "tenant_relief", query: "buyout", queryType: "keyword", expectation: "Buyout agreement, disclosure, rescission, or settlement issues." },
  { id: "rent_reduction", lane: "tenant_relief", query: "rent reduction", queryType: "keyword", expectation: "Decisions on rent reduction tied to housing-service decreases." },
  { id: "decrease_services", lane: "tenant_relief", query: "decrease in services", queryType: "keyword", expectation: "Decisions using service reduction language and corresponding rent relief." },
  { id: "owner_move_in", lane: "eviction", query: "owner move-in", queryType: "keyword", expectation: "Owner move-in eviction decisions or related procedural analysis." },
  { id: "wrongful_eviction", lane: "eviction", query: "wrongful eviction", queryType: "keyword", expectation: "Wrongful eviction claims, AWE matters, or related factual/legal analysis." },
  { id: "nuisance", lane: "eviction", query: "nuisance", queryType: "keyword", expectation: "Tenant-conduct nuisance cases, not merely nuisance abatement maintenance." },
  { id: "harassment", lane: "tenant_protection", query: "harassment", queryType: "keyword", expectation: "Tenant harassment decisions and landlord-conduct findings." },
  { id: "retaliation", lane: "tenant_protection", query: "retaliation", queryType: "keyword", expectation: "Tenant retaliation or protected-activity-related decisions." },
  { id: "capital_improvement", lane: "passthroughs", query: "capital improvement", queryType: "keyword", expectation: "Capital improvement passthrough decisions and standards." },
  { id: "hardship", lane: "passthroughs", query: "financial hardship capital improvement", queryType: "keyword", expectation: "Capital-improvement hardship or passthrough burden analysis." },
  { id: "habitability", lane: "habitability", query: "habitability", queryType: "keyword", expectation: "Habitability standards, defects, and remedy analysis." },
  { id: "section_37_10b", lane: "tenant_protection", query: "section 37.10b harassment", queryType: "keyword", expectation: "Tenant harassment decisions with ordinance-level grounding." },
  { id: "section_37_9", lane: "eviction", query: "section 37.9 owner move-in", queryType: "keyword", expectation: "Eviction decisions tied to the ordinance framework for OMI or protected evictions." },
  { id: "awe", lane: "eviction", query: "report of alleged wrongful eviction", queryType: "keyword", expectation: "AWE decisions or references to wrongful eviction complaints." },
  { id: "sentence_habitability", lane: "sentence_search", query: "tenant reported mold and the landlord only painted over it", queryType: "keyword", expectation: "Sentence-style factual search should find materially similar habitability disputes." },
  { id: "sentence_harassment", lane: "sentence_search", query: "landlord behavior constituted tenant harassment after repair complaints", queryType: "keyword", expectation: "Sentence-style search should find tenant-protection results, not generic repair disputes." }
];

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function avg(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(4));
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

async function fetchJsonWithRetry(url, payload, queryId) {
  let lastError = null;
  for (let attempt = 1; attempt <= retryCount; attempt += 1) {
    try {
      return await fetchJson(url, payload);
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message : String(error);
      console.warn(`[judge-smoke] ${queryId} attempt ${attempt}/${retryCount} failed: ${message}`);
      if (attempt < retryCount) await sleep(retryDelayMs * attempt);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function summarizeQuery(spec, response) {
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
    corpusTier: row.corpusTier || null,
    snippet: row.snippet || null,
    retrievalReason: Array.isArray(row.retrievalReason) ? row.retrievalReason : []
  }));

  return {
    ...spec,
    totalResults: Number(response?.total || results.length || 0),
    returnedAny: results.length > 0,
    uniqueDecisionCount: unique(results.map((row) => row.documentId)).length,
    avgVectorScore: avg(results.map((row) => Number(row.vectorScore || 0))),
    avgLexicalScore: avg(results.map((row) => Number(row.lexicalScore || 0))),
    vectorMatchCount: Number(response?.runtimeDiagnostics?.vectorMatchCount || 0),
    topResults,
    error: null
  };
}

function formatMarkdown(report) {
  const lines = [
    "# Judge Search Smoke Test",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Query count: \`${report.summary.queryCount}\``,
    `- Returned query count: \`${report.summary.returnedQueryCount}\``,
    `- Failed query count: \`${report.summary.failedQueryCount}\``,
    `- Overall hit rate: \`${report.summary.overallHitRate}\``,
    `- Unique decision universe: \`${report.summary.uniqueDecisionUniverseCount}\``,
    "",
    "## Lane Coverage",
    ""
  ];

  for (const lane of report.summary.laneStats || []) {
    lines.push(`- \`${lane.lane}\` | hitRate=${lane.hitRate} | avgResults=${lane.avgResults} | avgDecisionDiversity=${lane.avgDecisionDiversity}`);
  }

  lines.push("");
  lines.push("## Query Detail");
  lines.push("");

  for (const query of report.queries || []) {
    lines.push(`### ${query.query}`);
    lines.push("");
    lines.push(`- lane: \`${query.lane}\``);
    lines.push(`- expectation: ${query.expectation}`);
    lines.push(`- totalResults: \`${query.totalResults}\``);
    lines.push(`- uniqueDecisionCount: \`${query.uniqueDecisionCount}\``);
    lines.push(`- avgVectorScore: \`${query.avgVectorScore}\` | avgLexicalScore: \`${query.avgLexicalScore}\``);
    if (query.error) lines.push(`- error: ${query.error}`);
    lines.push("");
    for (const row of query.topResults || []) {
      lines.push(`- #${row.rank} \`${row.title || row.documentId}\` | tier=\`${row.corpusTier || "<none>"}\` | chunkType=\`${row.chunkType}\` | score=\`${row.score}\``);
      lines.push(`  - citation: \`${row.citationAnchor || "<none>"}\``);
      lines.push(`  - snippet: ${row.snippet || "<none>"}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

function formatCsv(report) {
  const header = [
    "query_id",
    "lane",
    "query",
    "expectation",
    "total_results",
    "unique_decision_count",
    "avg_vector_score",
    "avg_lexical_score",
    "rank",
    "document_id",
    "title",
    "chunk_id",
    "chunk_type",
    "corpus_tier",
    "score",
    "vector_score",
    "lexical_score",
    "citation_anchor",
    "snippet",
    "retrieval_reason",
    "error"
  ];
  const lines = [header.join(",")];

  for (const query of report.queries || []) {
    if (!query.topResults.length) {
      lines.push([
        query.id,
        query.lane,
        query.query,
        query.expectation,
        query.totalResults,
        query.uniqueDecisionCount,
        query.avgVectorScore,
        query.avgLexicalScore,
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
        "",
        "",
        query.error || ""
      ].map(csvEscape).join(","));
      continue;
    }

    for (const row of query.topResults) {
      lines.push([
        query.id,
        query.lane,
        query.query,
        query.expectation,
        query.totalResults,
        query.uniqueDecisionCount,
        query.avgVectorScore,
        query.avgLexicalScore,
        row.rank,
        row.documentId,
        row.title,
        row.chunkId,
        row.chunkType,
        row.corpusTier,
        row.score,
        row.vectorScore,
        row.lexicalScore,
        row.citationAnchor,
        row.snippet,
        (row.retrievalReason || []).join(";"),
        query.error || ""
      ].map(csvEscape).join(","));
    }
  }

  return `${lines.join("\n")}\n`;
}

function summarizeReport(queries) {
  const returned = queries.filter((query) => query.returnedAny);
  const laneStats = Array.from(
    queries.reduce((map, query) => {
      const current = map.get(query.lane) || { lane: query.lane, queryCount: 0, returnedCount: 0, totalResults: 0, uniqueDecisionCountSum: 0 };
      current.queryCount += 1;
      current.returnedCount += query.returnedAny ? 1 : 0;
      current.totalResults += Number(query.totalResults || 0);
      current.uniqueDecisionCountSum += Number(query.uniqueDecisionCount || 0);
      map.set(query.lane, current);
      return map;
    }, new Map()).values()
  ).map((entry) => ({
    lane: entry.lane,
    queryCount: entry.queryCount,
    returnedCount: entry.returnedCount,
    hitRate: entry.queryCount > 0 ? Number((entry.returnedCount / entry.queryCount).toFixed(4)) : 0,
    avgResults: entry.queryCount > 0 ? Number((entry.totalResults / entry.queryCount).toFixed(4)) : 0,
    avgDecisionDiversity: entry.queryCount > 0 ? Number((entry.uniqueDecisionCountSum / entry.queryCount).toFixed(4)) : 0
  }));

  return {
    queryCount: queries.length,
    returnedQueryCount: returned.length,
    failedQueryCount: queries.filter((query) => query.error).length,
    overallHitRate: queries.length > 0 ? Number((returned.length / queries.length).toFixed(4)) : 0,
    uniqueDecisionUniverseCount: unique(returned.flatMap((query) => (query.topResults || []).map((row) => row.documentId))).length,
    laneStats
  };
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const queries = [];
  for (const spec of SMOKE_QUERIES) {
    try {
      const response = await fetchJsonWithRetry(`${apiBase}/admin/retrieval/debug`, {
        query: spec.query,
        queryType: spec.queryType,
        limit,
        corpusMode,
        filters: { approvedOnly: false }
      }, spec.id);
      queries.push(summarizeQuery(spec, response));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      queries.push({
        ...spec,
        totalResults: 0,
        returnedAny: false,
        uniqueDecisionCount: 0,
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
    queries,
    summary: summarizeReport(queries)
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, formatCsv(report));

  if (report.summary.failedQueryCount === report.summary.queryCount) {
    throw new Error("All judge smoke queries failed; report not trustworthy.");
  }

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Judge search smoke JSON report written to ${jsonPath}`);
  console.log(`Judge search smoke Markdown report written to ${markdownPath}`);
  console.log(`Judge search smoke CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
