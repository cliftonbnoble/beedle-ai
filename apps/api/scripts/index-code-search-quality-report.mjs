import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.INDEX_CODE_SEARCH_QUALITY_JSON_NAME || "index-code-search-quality-report.json";
const markdownName = process.env.INDEX_CODE_SEARCH_QUALITY_MARKDOWN_NAME || "index-code-search-quality-report.md";
const csvName = process.env.INDEX_CODE_SEARCH_QUALITY_CSV_NAME || "index-code-search-quality-report.csv";
const limit = Number.parseInt(process.env.INDEX_CODE_SEARCH_QUALITY_LIMIT || "8", 10);
const corpusMode = process.env.INDEX_CODE_SEARCH_QUALITY_CORPUS_MODE || "trusted_plus_provisional";
const retryCount = Number.parseInt(process.env.INDEX_CODE_SEARCH_QUALITY_RETRY_COUNT || "3", 10);
const retryDelayMs = Number.parseInt(process.env.INDEX_CODE_SEARCH_QUALITY_RETRY_DELAY_MS || "1200", 10);
const judgeName = process.env.INDEX_CODE_SEARCH_QUALITY_JUDGE || "Erin E. Katayama";

const BASELINE_PROBES = [
  {
    id: "rent_reduction_unfiltered",
    lane: "baseline_issue",
    query: "rent reduction",
    expectation: "Should surface service-reduction and housing-service decisions without an index-code filter."
  },
  {
    id: "decrease_services_unfiltered",
    lane: "baseline_issue",
    query: "decrease in services",
    expectation: "Should surface DHS-style decisions even when the user does not know the index code."
  },
  {
    id: "housing_service_not_provided",
    lane: "baseline_issue",
    query: "housing service not provided",
    expectation: "Should find service-removal or promised-service disputes tied to rent relief."
  }
];

const INDEX_CODE_PROBES = [
  {
    code: "G22",
    label: "DHS -- Decrease in Service Not Substantial",
    issueQuery: "rent reduction"
  },
  {
    code: "G23",
    label: "DHS -- Housing Service Reasonably Expected But Not Provided",
    issueQuery: "housing service not provided"
  },
  {
    code: "G24",
    label: "DHS -- Housing Service Promised But Not Provided",
    issueQuery: "promised housing service not provided"
  },
  {
    code: "G27",
    label: "DHS -- Code Violation - Substantial Decrease in Housing Services",
    issueQuery: "decrease in services"
  },
  {
    code: "G28",
    label: "DHS -- Code Violation - Not Substantial Decrease in Housing Services",
    issueQuery: "decrease in services"
  },
  {
    code: "G93",
    label: "DHS -- Visitor Policy for Residential Hotel - Rent Reduction for Noncompliance with Uniform Policy",
    issueQuery: "rent reduction"
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
      console.warn(`[index-code-search-quality] ${label} attempt ${attempt}/${retryCount} failed: ${message}`);
      if (attempt < retryCount) await sleep(retryDelayMs * attempt);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function buildSpecs() {
  const specs = BASELINE_PROBES.map((probe) => ({
    ...probe,
    code: null,
    payload: {
      query: probe.query,
      limit,
      corpusMode,
      filters: {
        approvedOnly: false
      }
    }
  }));

  for (const probe of INDEX_CODE_PROBES) {
    specs.push({
      id: `index_filter_only:${probe.code}`,
      lane: "index_filter_only",
      code: probe.code,
      query: "decision",
      expectation: `Should return decisions tagged with ${probe.code} when filtering only by that index code.`,
      label: probe.label,
      payload: {
        query: "decision",
        limit,
        corpusMode,
        filters: {
          approvedOnly: false,
          indexCodes: [probe.code]
        }
      }
    });

    specs.push({
      id: `index_filter_issue:${probe.code}`,
      lane: "index_filter_issue",
      code: probe.code,
      query: probe.issueQuery,
      expectation: `Should keep ${probe.code} decisions in play for a rent-reduction/service-reduction style issue search.`,
      label: probe.label,
      payload: {
        query: probe.issueQuery,
        limit,
        corpusMode,
        filters: {
          approvedOnly: false,
          indexCodes: [probe.code]
        }
      }
    });
  }

  specs.push({
    id: "judge_and_index:rent_reduction",
    lane: "judge_plus_index",
    code: "G93",
    query: "rent reduction",
    expectation: `Checks that judge + index-code filters still behave on a realistic combined query (${judgeName} + G93).`,
    label: "Judge + index filter sanity check",
    payload: {
      query: "rent reduction",
      limit,
      corpusMode,
      filters: {
        approvedOnly: false,
        judgeNames: [judgeName],
        indexCodes: ["G93"]
      }
    }
  });

  return specs;
}

function summarizeQuery(spec, response) {
  const results = Array.isArray(response?.results) ? response.results : [];
  return {
    id: spec.id,
    lane: spec.lane,
    code: spec.code,
    label: spec.label || null,
    query: spec.query,
    expectation: spec.expectation,
    totalResults: Number(response?.total || results.length || 0),
    returnedAny: results.length > 0,
    uniqueDecisionCount: unique(results.map((row) => row.documentId)).length,
    avgScore: avg(results.map((row) => Number(row.score || 0))),
    avgVectorScore: avg(results.map((row) => Number(row.vectorScore || 0))),
    avgLexicalScore: avg(results.map((row) => Number(row.lexicalScore || 0))),
    tierCounts: response?.tierCounts || { trusted: 0, provisional: 0 },
    hasMore: Boolean(response?.hasMore),
    topResults: results.slice(0, limit).map((row, index) => ({
      rank: index + 1,
      documentId: row.documentId,
      title: row.title,
      authorName: row.authorName || null,
      chunkId: row.chunkId,
      chunkType: row.chunkType || row.sectionLabel || "<none>",
      sectionLabel: row.sectionLabel || "<none>",
      score: Number(row.score || 0),
      vectorScore: Number(row.vectorScore || 0),
      lexicalScore: Number(row.lexicalScore || 0),
      citationAnchor: row.citationAnchor || null,
      snippet: row.snippet || null
    })),
    error: null
  };
}

function summarizeByLane(rows) {
  return Array.from(
    rows.reduce((map, row) => {
      const current = map.get(row.lane) || {
        lane: row.lane,
        queryCount: 0,
        returnedCount: 0,
        totalResults: 0,
        uniqueDecisionCountSum: 0
      };
      current.queryCount += 1;
      current.returnedCount += row.returnedAny ? 1 : 0;
      current.totalResults += Number(row.totalResults || 0);
      current.uniqueDecisionCountSum += Number(row.uniqueDecisionCount || 0);
      map.set(row.lane, current);
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
}

function summarizeCodeStats(rows) {
  const filtered = rows.filter((row) => row.code);
  return Array.from(
    filtered.reduce((map, row) => {
      const current = map.get(row.code) || {
        code: row.code,
        label: row.label || null,
        filterOnlyReturned: false,
        filterOnlyResults: 0,
        filterOnlyUniqueDecisionCount: 0,
        issueReturned: false,
        issueResults: 0,
        issueUniqueDecisionCount: 0
      };
      if (row.lane === "index_filter_only") {
        current.filterOnlyReturned = row.returnedAny;
        current.filterOnlyResults = row.totalResults;
        current.filterOnlyUniqueDecisionCount = row.uniqueDecisionCount;
      }
      if (row.lane === "index_filter_issue") {
        current.issueReturned = row.returnedAny;
        current.issueResults = row.totalResults;
        current.issueUniqueDecisionCount = row.uniqueDecisionCount;
      }
      map.set(row.code, current);
      return map;
    }, new Map()).values()
  );
}

function buildCsv(report) {
  const header = [
    "query_id",
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
    "chunk_id",
    "chunk_type",
    "section_label",
    "score",
    "vector_score",
    "lexical_score",
    "citation_anchor",
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
          query.code || "",
          query.label || "",
          query.query,
          query.expectation,
          query.totalResults,
          query.uniqueDecisionCount,
          query.avgScore,
          query.avgVectorScore,
          query.avgLexicalScore,
          query.hasMore,
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
          query.code || "",
          query.label || "",
          query.query,
          query.expectation,
          query.totalResults,
          query.uniqueDecisionCount,
          query.avgScore,
          query.avgVectorScore,
          query.avgLexicalScore,
          query.hasMore,
          row.rank,
          row.documentId,
          row.title,
          row.authorName || "",
          row.chunkId,
          row.chunkType,
          row.sectionLabel,
          row.score,
          row.vectorScore,
          row.lexicalScore,
          row.citationAnchor || "",
          row.snippet || "",
          query.error || ""
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
    "# Index Code Search Quality Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Corpus mode: \`${report.corpusMode}\``,
    `- Query count: \`${report.summary.queryCount}\``,
    `- Returned query count: \`${report.summary.returnedQueryCount}\``,
    `- Failed query count: \`${report.summary.failedQueryCount}\``,
    `- Overall hit rate: \`${report.summary.overallHitRate}\``,
    `- Unique decision universe: \`${report.summary.uniqueDecisionUniverseCount}\``,
    "",
    "## Lane Coverage",
    ""
  ];

  for (const lane of report.summary.laneStats) {
    lines.push(
      `- \`${lane.lane}\` | hitRate=${lane.hitRate} | avgResults=${lane.avgResults} | avgDecisionDiversity=${lane.avgDecisionDiversity}`
    );
  }

  lines.push("");
  lines.push("## Index Code Coverage");
  lines.push("");

  for (const row of report.summary.codeStats) {
    lines.push(
      `- \`${row.code}\` ${row.label ? `| ${row.label}` : ""} | filterOnly=${row.filterOnlyReturned ? "yes" : "no"} (${row.filterOnlyResults} results) | issueFiltered=${row.issueReturned ? "yes" : "no"} (${row.issueResults} results)`
    );
  }

  lines.push("");
  lines.push("## Query Detail");
  lines.push("");

  for (const row of report.queries) {
    lines.push(`### ${row.id}`);
    lines.push("");
    lines.push(`- lane: \`${row.lane}\``);
    if (row.code) lines.push(`- indexCode: \`${row.code}\``);
    if (row.label) lines.push(`- label: ${row.label}`);
    lines.push(`- query: \`${row.query}\``);
    lines.push(`- expectation: ${row.expectation}`);
    lines.push(`- totalResults: \`${row.totalResults}\``);
    lines.push(`- uniqueDecisionCount: \`${row.uniqueDecisionCount}\``);
    lines.push(`- avgScore: \`${row.avgScore}\` | avgVectorScore: \`${row.avgVectorScore}\` | avgLexicalScore: \`${row.avgLexicalScore}\``);
    lines.push(`- hasMore: \`${row.hasMore}\``);
    if (row.error) lines.push(`- error: ${row.error}`);
    lines.push("");

    for (const result of row.topResults) {
      lines.push(
        `- #${result.rank} \`${result.title || result.documentId}\` | author=\`${result.authorName || "<none>"}\` | chunkType=\`${result.chunkType}\` | score=\`${result.score}\``
      );
      lines.push(`  - citation: \`${result.citationAnchor || "<none>"}\``);
      lines.push(`  - snippet: ${result.snippet || "<none>"}`);
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const specs = buildSpecs();
  const queryRows = [];

  for (const spec of specs) {
    try {
      const response = await fetchJsonWithRetry(`${apiBase}/search`, spec.payload, spec.id);
      queryRows.push(summarizeQuery(spec, response));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      queryRows.push({
        id: spec.id,
        lane: spec.lane,
        code: spec.code,
        label: spec.label || null,
        query: spec.query,
        expectation: spec.expectation,
        totalResults: 0,
        returnedAny: false,
        uniqueDecisionCount: 0,
        avgScore: 0,
        avgVectorScore: 0,
        avgLexicalScore: 0,
        tierCounts: { trusted: 0, provisional: 0 },
        hasMore: false,
        topResults: [],
        error: message
      });
    }
  }

  const returned = queryRows.filter((row) => row.returnedAny);
  const uniqueDecisionUniverse = unique(returned.flatMap((row) => row.topResults.map((result) => result.documentId)));

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    corpusMode,
    limit,
    judgeName,
    queries: queryRows,
    summary: {
      queryCount: queryRows.length,
      returnedQueryCount: returned.length,
      failedQueryCount: queryRows.filter((row) => row.error).length,
      overallHitRate: queryRows.length > 0 ? Number((returned.length / queryRows.length).toFixed(4)) : 0,
      uniqueDecisionUniverseCount: uniqueDecisionUniverse.length,
      uniqueDecisionUniverse,
      laneStats: summarizeByLane(queryRows),
      codeStats: summarizeCodeStats(queryRows)
    }
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Index code search-quality JSON report written to ${jsonPath}`);
  console.log(`Index code search-quality Markdown report written to ${markdownPath}`);
  console.log(`Index code search-quality CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
