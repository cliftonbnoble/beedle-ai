import fs from "node:fs/promises";
import path from "node:path";

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.COMBINED_FILTER_QA_JSON_NAME || "combined-filter-search-qa-report.json";
const markdownName = process.env.COMBINED_FILTER_QA_MARKDOWN_NAME || "combined-filter-search-qa-report.md";
const csvName = process.env.COMBINED_FILTER_QA_CSV_NAME || "combined-filter-search-qa-report.csv";
const requestTimeoutMs = Number(process.env.COMBINED_FILTER_QA_TIMEOUT_MS || "45000");
const retries = Math.max(1, Number(process.env.COMBINED_FILTER_QA_RETRIES || "3"));
const pauseBetweenQueriesMs = Number(process.env.COMBINED_FILTER_QA_PAUSE_MS || "800");
const resultLimit = Math.max(1, Number(process.env.COMBINED_FILTER_QA_LIMIT || "12"));
const tasksFile = process.env.COMBINED_FILTER_QA_TASKS_FILE ? path.resolve(process.cwd(), process.env.COMBINED_FILTER_QA_TASKS_FILE) : "";

const defaultTasks = [
  {
    id: "rent_reduction_katayama_g27",
    label: "Rent reduction + Katayama + G27",
    query: "rent reduction",
    expectation: "Should surface judge-specific G27 rent-reduction decisions without collapsing recall.",
    filters: {
      approvedOnly: false,
      judgeNames: ["Erin E. Katayama"],
      indexCodes: ["G27"]
    }
  },
  {
    id: "decrease_services_katayama_g27_g28",
    label: "Decrease in services + Katayama + G27/G28",
    query: "decrease in services",
    expectation: "Should preserve multiple DHS decisions when both G27 and G28 are active with a judge filter.",
    filters: {
      approvedOnly: false,
      judgeNames: ["Erin E. Katayama"],
      indexCodes: ["G27", "G28"]
    }
  },
  {
    id: "visitor_policy_yick_g93",
    label: "Visitor policy + Andrew Yick + G93",
    query: "uniform visitor policy",
    expectation: "Should keep G93 decisions retrievable when query, judge, and index code are all present.",
    filters: {
      approvedOnly: false,
      judgeNames: ["Andrew Yick"],
      indexCodes: ["G93"]
    }
  },
  {
    id: "owner_move_in_juarez_date",
    label: "Owner move-in + René Juárez + date range",
    query: "owner move in",
    expectation: "Should search a date-bounded judge slice without starving the candidate pool.",
    filters: {
      approvedOnly: false,
      judgeNames: ["René Juárez"],
      fromDate: "2022-01-01",
      toDate: "2025-12-31"
    }
  },
  {
    id: "capital_improvement_lim_vii712_code13",
    label: "Capital improvements + Deborah K. Lim + R&R VII-7.12 + code 13",
    query: "capital improvements",
    expectation: "Should keep Deborah K. Lim capital-improvement decisions in play when the query is combined with a real metadata intersection.",
    filters: {
      approvedOnly: false,
      judgeNames: ["Deborah K. Lim"],
      rulesSection: "VII-7.12",
      indexCodes: ["13"]
    }
  }
];

function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function describeFilters(filters) {
  const parts = [];
  if (Array.isArray(filters?.judgeNames) && filters.judgeNames.length) parts.push(`judges=${filters.judgeNames.join(" | ")}`);
  if (Array.isArray(filters?.indexCodes) && filters.indexCodes.length) parts.push(`index=${filters.indexCodes.join(" | ")}`);
  if (filters?.rulesSection) parts.push(`r&r=${filters.rulesSection}`);
  if (filters?.ordinanceSection) parts.push(`ordinance=${filters.ordinanceSection}`);
  if (filters?.partyName) parts.push(`party=${filters.partyName}`);
  if (filters?.fromDate || filters?.toDate) parts.push(`dates=${filters.fromDate || "any"}..${filters.toDate || "any"}`);
  return parts.join(" ; ") || "<none>";
}

function isConclusionsLikeSection(sectionLabel) {
  const raw = String(sectionLabel || "");
  const normalized = normalize(raw).replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return (
    /conclusions? of law/i.test(raw) ||
    normalized === "conclusions_of_law" ||
    normalized === "authority_discussion" ||
    normalized === "analysis_reasoning"
  );
}

function isFindingsLikeSection(sectionLabel) {
  const raw = String(sectionLabel || "");
  const normalized = normalize(raw).replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return /findings? of fact/i.test(raw) || normalized === "findings_of_fact" || normalized === "findings";
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function loadTasks() {
  if (!tasksFile) return defaultTasks;
  const raw = await fs.readFile(tasksFile, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error(`Expected an array of tasks in ${tasksFile}`);
  }
  return parsed;
}

async function fetchJson(url, payload, label) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          accept: "application/json"
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      });
      const text = await response.text();
      if (!response.ok) {
        throw new Error(`Request failed (${response.status}) ${url}: ${text.slice(0, 500)}`);
      }
      return JSON.parse(text || "{}");
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        console.warn(`[combined-filter-qa] ${label} attempt ${attempt}/${retries} failed: ${error instanceof Error ? error.message : String(error)}`);
        await sleep(400 * attempt);
      }
    } finally {
      clearTimeout(timeout);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError || `Request failed for ${label}`));
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Combined-Filter Search QA Report");
  lines.push("");
  lines.push(`- queryCount: ${report.queryCount}`);
  lines.push(`- returnedQueryCount: ${report.returnedQueryCount}`);
  lines.push(`- zeroResultQueryCount: ${report.zeroResultQueryCount}`);
  lines.push(`- abortedQueryCount: ${report.abortedQueryCount}`);
  lines.push(`- erroredQueryCount: ${report.erroredQueryCount}`);
  lines.push(`- failedQueryCount: ${report.failedQueryCount}`);
  lines.push(`- top5ExpectedHitCount: ${report.top5ExpectedHitCount}`);
  lines.push(`- top5ConclusionsLikeCount: ${report.top5ConclusionsLikeCount}`);
  lines.push(`- top5FindingsLikeCount: ${report.top5FindingsLikeCount}`);
  lines.push("");
  lines.push("## Tasks");
  lines.push("");
  for (const row of report.rows) {
    lines.push(
      `- ${row.id} | status=${row.status} | returned=${row.returnedAny} | uniqueDecisions=${row.uniqueDecisionCount} | top1=${row.top1Citation || "<none>"} | top1Section=${row.top1SectionLabel || "<none>"} | top5Expected=${row.expectedCitationFound} | filters=${row.filtersSummary}${row.error ? ` | error=${row.error}` : ""}`
    );
  }
  return `${lines.join("\n")}\n`;
}

function toCsv(report) {
  const rows = [
    [
      "id",
      "label",
      "query",
      "filters",
      "expectation",
      "status",
      "returnedAny",
      "totalResults",
      "uniqueDecisionCount",
      "top1Citation",
      "top1SectionLabel",
      "top1Judge",
      "top1Score",
      "top5ConclusionsLike",
      "top5FindingsLike",
      "expectedCitations",
      "expectedCitationFound",
      "error"
    ],
    ...report.rows.map((row) => [
      row.id,
      row.label,
      row.query,
      row.filtersSummary,
      row.expectation || "",
      row.status || "",
      row.returnedAny ? "1" : "0",
      row.totalResults,
      row.uniqueDecisionCount,
      row.top1Citation || "",
      row.top1SectionLabel || "",
      row.top1Judge || "",
      String(row.top1Score ?? ""),
      row.top5ConclusionsLike ? "1" : "0",
      row.top5FindingsLike ? "1" : "0",
      Array.isArray(row.expectedCitations) ? row.expectedCitations.join(" | ") : "",
      row.expectedCitationFound ? "1" : "0",
      row.error || ""
    ])
  ];
  return rows.map((row) => row.map(csvEscape).join(",")).join("\n");
}

function classifyErrorStatus(error) {
  const message = String(error || "");
  if (/aborted|timeout/i.test(message)) return "aborted";
  return "error";
}

async function writeReports(report) {
  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  const csvPath = path.join(reportsDir, csvName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  await fs.writeFile(markdownPath, toMarkdown(report));
  await fs.writeFile(csvPath, `${toCsv(report)}\n`);
  return { jsonPath, markdownPath, csvPath };
}

async function main() {
  const tasks = await loadTasks();
  const rows = [];

  for (const task of tasks) {
    try {
      const response = await fetchJson(
        `${apiBase}/search`,
        {
          query: task.query,
          limit: Number(task.limit || resultLimit),
          offset: 0,
          snippetMaxLength: 320,
          corpusMode: task.corpusMode || "trusted_plus_provisional",
          filters: {
            approvedOnly: false,
            ...(task.filters || {})
          }
        },
        task.id
      );

      const results = Array.isArray(response?.results) ? response.results : [];
      const top1 = results[0] || null;
      const top5 = results.slice(0, 5);
      const expectedCitations = Array.isArray(task.expectedCitations) ? task.expectedCitations : [];
      const expectedCitationFound = expectedCitations.length
        ? top5.some((row) => expectedCitations.some((citation) => normalize(row?.citation) === normalize(citation)))
        : false;

      rows.push({
        id: task.id,
        label: task.label || task.id,
        query: task.query,
        expectation: task.expectation || "",
        filtersSummary: describeFilters(task.filters || {}),
        expectedCitations,
        status: results.length > 0 ? "returned" : "zero_results",
        returnedAny: results.length > 0,
        totalResults: Number(response?.total || results.length || 0),
        uniqueDecisionCount: unique(results.map((row) => row.documentId)).length,
        top1Citation: top1?.citation || null,
        top1SectionLabel: top1?.sectionLabel || null,
        top1Judge: top1?.authorName || null,
        top1Score: typeof top1?.score === "number" ? Number(top1.score.toFixed(6)) : null,
        top5ConclusionsLike: top5.some((row) => isConclusionsLikeSection(row?.sectionLabel)),
        top5FindingsLike: top5.some((row) => isFindingsLikeSection(row?.sectionLabel)),
        expectedCitationFound,
        topResults: top5.map((row, index) => ({
          rank: index + 1,
          documentId: row.documentId,
          title: row.title,
          citation: row.citation,
          authorName: row.authorName || null,
          sectionLabel: row.sectionLabel || row.chunkType || null,
          score: Number(row.score || 0)
        })),
        error: null
      });
    } catch (error) {
      rows.push({
        id: task.id,
        label: task.label || task.id,
        query: task.query,
        expectation: task.expectation || "",
        filtersSummary: describeFilters(task.filters || {}),
        expectedCitations: Array.isArray(task.expectedCitations) ? task.expectedCitations : [],
        status: classifyErrorStatus(error instanceof Error ? error.message : String(error)),
        returnedAny: false,
        totalResults: 0,
        uniqueDecisionCount: 0,
        top1Citation: null,
        top1SectionLabel: null,
        top1Judge: null,
        top1Score: null,
        top5ConclusionsLike: false,
        top5FindingsLike: false,
        expectedCitationFound: false,
        topResults: [],
        error: error instanceof Error ? error.message : String(error)
      });
    }

    if (pauseBetweenQueriesMs > 0) {
      await sleep(pauseBetweenQueriesMs);
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    tasksFile: tasksFile || null,
    queryCount: rows.length,
    returnedQueryCount: rows.filter((row) => row.returnedAny).length,
    zeroResultQueryCount: rows.filter((row) => row.status === "zero_results").length,
    abortedQueryCount: rows.filter((row) => row.status === "aborted").length,
    erroredQueryCount: rows.filter((row) => row.status === "error").length,
    failedQueryCount: rows.filter((row) => row.error).length,
    top5ExpectedHitCount: rows.filter((row) => row.expectedCitationFound).length,
    top5ConclusionsLikeCount: rows.filter((row) => row.top5ConclusionsLike).length,
    top5FindingsLikeCount: rows.filter((row) => row.top5FindingsLike).length,
    rows
  };

  const paths = await writeReports(report);
  console.log(
    JSON.stringify(
      {
        queryCount: report.queryCount,
        returnedQueryCount: report.returnedQueryCount,
        zeroResultQueryCount: report.zeroResultQueryCount,
        abortedQueryCount: report.abortedQueryCount,
        erroredQueryCount: report.erroredQueryCount,
        failedQueryCount: report.failedQueryCount,
        top5ExpectedHitCount: report.top5ExpectedHitCount,
        top5ConclusionsLikeCount: report.top5ConclusionsLikeCount,
        top5FindingsLikeCount: report.top5FindingsLikeCount
      },
      null,
      2
    )
  );
  console.log(`Combined-filter search QA JSON report written to ${paths.jsonPath}`);
  console.log(`Combined-filter search QA Markdown report written to ${paths.markdownPath}`);
  console.log(`Combined-filter search QA CSV report written to ${paths.csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
