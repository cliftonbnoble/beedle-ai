import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.ISSUE_QUERY_QA_JSON_NAME || "issue-query-search-qa-report.json";
const markdownName = process.env.ISSUE_QUERY_QA_MARKDOWN_NAME || "issue-query-search-qa-report.md";
const csvName = process.env.ISSUE_QUERY_QA_CSV_NAME || "issue-query-search-qa-report.csv";
const requestTimeoutMs = Number(process.env.ISSUE_QUERY_QA_TIMEOUT_MS || "90000");
const sqliteTimeoutMs = Number(process.env.ISSUE_QUERY_QA_SQLITE_TIMEOUT_MS || "45000");
const retries = Math.max(1, Number(process.env.ISSUE_QUERY_QA_RETRIES || "3"));
const pauseBetweenQueriesMs = Number(process.env.ISSUE_QUERY_QA_PAUSE_MS || "1500");
const resultLimit = Math.max(1, Number(process.env.ISSUE_QUERY_QA_LIMIT || "8"));
const stopAfterTransportFailures = Math.max(1, Number(process.env.ISSUE_QUERY_QA_STOP_AFTER_TRANSPORT_FAILURES || "2"));
const healthcheckEnabled = process.env.ISSUE_QUERY_QA_HEALTHCHECK !== "0";
const healthTimeoutMs = Math.max(500, Number(process.env.ISSUE_QUERY_QA_HEALTH_TIMEOUT_MS || "3000"));
const configuredDbPath = process.env.ISSUE_QUERY_QA_DB_PATH || "";
const taskIdsFilter = new Set(
  String(process.env.ISSUE_QUERY_QA_TASK_IDS || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
);
const tasksFile = process.env.ISSUE_QUERY_QA_TASKS_FILE
  ? path.resolve(process.cwd(), process.env.ISSUE_QUERY_QA_TASKS_FILE)
  : path.resolve(process.cwd(), "scripts/issue-query-search-qa-tasks.sample.json");

function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeSqlLike(value) {
  return String(value || "").toLowerCase().replace(/\s+/g, " ").trim();
}

function normalizedWholeWordSqlExpr(expr) {
  return `(' ' || lower(
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(coalesce(${expr}, ''), char(10), ' '), char(13), ' '), '.', ' '), ',', ' '), ';', ' '), ':', ' '), '(', ' '), ')', ' '), '-', ' '), '/', ' ')
  ) || ' ')`;
}

function buildCorpusSignalMatchClause(signal) {
  const normalized = normalizeSqlLike(signal).replace(/'/g, "''");
  if (!normalized) return "0";
  return `instr(${normalizedWholeWordSqlExpr("c.chunk_text")}, ' ${normalized} ') > 0`;
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function describeFilters(filters) {
  const parts = [];
  if (Array.isArray(filters?.judgeNames) && filters.judgeNames.length) parts.push(`judges=${filters.judgeNames.join(" | ")}`);
  if (Array.isArray(filters?.indexCodes) && filters.indexCodes.length) parts.push(`index=${filters.indexCodes.join(" | ")}`);
  if (filters?.rulesSection) parts.push(`r&r=${filters.rulesSection}`);
  if (filters?.ordinanceSection) parts.push(`ordinance=${filters.ordinanceSection}`);
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

function aggregateResultText(result) {
  return normalize(
    [
      result?.title,
      result?.citation,
      result?.sectionLabel,
      result?.snippet,
      result?.matchedPassage?.sectionLabel,
      result?.matchedPassage?.snippet,
      result?.primaryAuthorityPassage?.sectionLabel,
      result?.primaryAuthorityPassage?.snippet,
      result?.supportingFactPassage?.sectionLabel,
      result?.supportingFactPassage?.snippet
    ]
      .filter(Boolean)
      .join(" ")
  );
}

function signalMatches(text, signal) {
  const normalizedText = normalize(text);
  const normalizedSignal = normalize(signal);
  if (!normalizedText || !normalizedSignal) return false;
  return normalizedText.includes(normalizedSignal);
}

function groupSatisfied(text, group) {
  return Array.isArray(group) && group.some((signal) => signalMatches(text, signal));
}

function precisionDiagnostics(task, result) {
  const text = aggregateResultText(result);
  const requiredSignalGroups = Array.isArray(task?.requiredSignalGroups) ? task.requiredSignalGroups : [];
  const forbiddenSignals = Array.isArray(task?.forbiddenSignals) ? task.forbiddenSignals : [];
  const missingGroups = requiredSignalGroups.filter((group) => !groupSatisfied(text, group));
  const forbiddenHits = forbiddenSignals.filter((signal) => signalMatches(text, signal));

  return {
    pass: missingGroups.length === 0 && forbiddenHits.length === 0,
    missingGroups: missingGroups.map((group) => group.join(" | ")),
    forbiddenHits
  };
}

function hasSupportingFact(result) {
  return Boolean(result?.supportingFactPassage?.snippet || result?.supportingFactPassage?.sectionLabel);
}

function hasStrongSupportingFact(result) {
  const debug = result?.supportingFactDebug || {};
  return (
    isFindingsLikeSection(result?.supportingFactPassage?.sectionLabel) ||
    Number(debug.anchorHits || 0) > 0 ||
    Number(debug.secondaryHits || 0) > 0 ||
    Number(debug.coverageRatio || 0) >= 0.25 ||
    Number(debug.factualAnchorScore || 0) >= 0.18
  );
}

function topSnippetLeadsWithFact(result) {
  const snippet = normalize(result?.snippet || "");
  const factSnippet = normalize(result?.supportingFactPassage?.snippet || "");
  if (!snippet || !factSnippet) return false;
  const factLead = factSnippet.slice(0, Math.min(56, factSnippet.length));
  return Boolean(factLead) && snippet.startsWith(factLead);
}

function describePrecisionBand(row) {
  if (row.corpusDocCount === 0) return "no_corpus_hits";
  if (row.status !== "returned") return row.status;
  if (row.top1PrecisionPass && row.top1FactLedSnippet && row.top3StrongSupportingFactCount > 0) return "strong";
  if (row.top1PrecisionPass && row.top3StrongSupportingFactCount > 0) return "precise_with_facts";
  if (row.top1PrecisionPass) return "top1_precise";
  if (row.top3PrecisionPass) return "top3_precise";
  if (row.top3WrongContextCount > 0) return "wrong_context_drift";
  return "weak_precision";
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function loadTasks() {
  const raw = await fs.readFile(tasksFile, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) throw new Error(`Expected an array of tasks in ${tasksFile}`);
  if (!taskIdsFilter.size) return parsed;
  return parsed.filter((task) => taskIdsFilter.has(String(task?.id || "").trim()));
}

async function sqliteQuery(dbPath, sql) {
  const immutablePath = `file:${dbPath}?mode=ro&immutable=1`;
  const { stdout } = await execFileAsync("sqlite3", [immutablePath, "-json", sql], {
    timeout: sqliteTimeoutMs,
    maxBuffer: 1024 * 1024 * 16
  });
  const trimmed = String(stdout || "").trim();
  return trimmed ? JSON.parse(trimmed) : [];
}

async function detectDatabasePath() {
  if (configuredDbPath) return configuredDbPath;
  const stateDir = path.resolve(process.cwd(), ".wrangler/state/v3/d1/miniflare-D1DatabaseObject");
  const entries = await fs.readdir(stateDir);
  const candidates = await Promise.all(
    entries
      .filter((entry) => entry.endsWith(".sqlite"))
      .map(async (entry) => {
        const fullPath = path.join(stateDir, entry);
        const stat = await fs.stat(fullPath);
        return { fullPath, size: stat.size };
      })
  );

  candidates.sort((a, b) => b.size - a.size);

  for (const candidate of candidates) {
    try {
      const rows = await sqliteQuery(
        candidate.fullPath,
        "select name from sqlite_master where type = 'table' and name in ('documents','document_chunks') order by name;"
      );
      const names = new Set(rows.map((row) => row.name));
      if (names.has("documents") && names.has("document_chunks")) return candidate.fullPath;
    } catch {
      // keep scanning
    }
  }

  throw new Error("Could not find a usable local D1 sqlite database for issue-query QA.");
}

function buildCorpusCountSql(task) {
  const clauses = [
    "d.file_type = 'decision_docx'",
    "d.searchable_at is not null",
    "d.rejected_at is null"
  ];
  const approvedOnly = task?.filters?.approvedOnly !== false;

  if (approvedOnly) {
    clauses.push(
      "(d.approved_at IS NOT NULL OR EXISTS (SELECT 1 FROM retrieval_search_chunks rs_active WHERE rs_active.document_id = d.id AND rs_active.active = 1))"
    );
  }

  const judgeNames = [
    ...(Array.isArray(task?.filters?.judgeNames) ? task.filters.judgeNames : []),
    ...(task?.filters?.judgeName ? [task.filters.judgeName] : [])
  ].filter(Boolean);

  if (judgeNames.length > 0) {
    clauses.push(
      `(${judgeNames.map((name) => `lower(coalesce(d.author_name, '')) = lower('${String(name).replace(/'/g, "''")}')`).join(" or ")})`
    );
  }

  const signalGroups = Array.isArray(task?.requiredSignalGroups) && task.requiredSignalGroups.length ? task.requiredSignalGroups : [[task?.query || ""]];
  clauses.push(
    signalGroups
      .map((group) => `(${(Array.isArray(group) ? group : [group]).map((signal) => buildCorpusSignalMatchClause(signal)).join(" or ")})`)
      .join("\n      and ")
  );

  return `
    select count(distinct d.id) as corpusDocCount
    from document_chunks c
    join documents d on d.id = c.document_id
    where ${clauses.join("\n      and ")};
  `;
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
        console.warn(`[issue-query-qa] ${label} attempt ${attempt}/${retries} failed: ${error instanceof Error ? error.message : String(error)}`);
        await sleep(400 * attempt);
      }
    } finally {
      clearTimeout(timeout);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError || `Request failed for ${label}`));
}

async function checkHealth() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), healthTimeoutMs);
  try {
    const response = await fetch(`${apiBase}/health`, { signal: controller.signal });
    return response.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timeout);
  }
}

function isTransportFailure(errorMessage) {
  const text = String(errorMessage || "").toLowerCase();
  return (
    text.includes("aborted") ||
    text.includes("fetch failed") ||
    text.includes("timed out") ||
    text.includes("readtimeout") ||
    text.includes("econnrefused") ||
    text.includes("socket hang up") ||
    text.includes("couldn't connect")
  );
}

function buildReturnedRow(task, results, corpusDocCount = null) {
  const top5 = results.slice(0, 5);
  const top3 = results.slice(0, 3);
  const top1Precision = results[0] ? precisionDiagnostics(task, results[0]) : { pass: false, missingGroups: [], forbiddenHits: [] };
  const top3PrecisionHits = top3.map((result) => precisionDiagnostics(task, result));
  const uniqueDecisionCount = new Set(results.map((item) => item.documentId || item.citation || item.id).filter(Boolean)).size;

  const row = {
    id: task.id,
    label: task.label || task.id,
    query: task.query,
    expectation: task.expectation || "",
    filtersSummary: describeFilters(task.filters || {}),
    status: results.length > 0 ? "returned" : "zero_results",
    returnedAny: results.length > 0,
    corpusDocCount: Number.isFinite(Number(corpusDocCount)) ? Number(corpusDocCount) : null,
    totalResults: results.length,
    uniqueDecisionCount,
    top1Citation: results[0]?.citation || "",
    top1SectionLabel: results[0]?.sectionLabel || "",
    top1PrecisionPass: top1Precision.pass,
    top1MissingGroups: top1Precision.missingGroups,
    top1ForbiddenHits: top1Precision.forbiddenHits,
    top3PrecisionPass: top3PrecisionHits.some((item) => item.pass),
    top3WrongContextCount: top3PrecisionHits.filter((item) => item.forbiddenHits.length > 0).length,
    top3SupportingFactCount: top3.filter((item) => hasSupportingFact(item)).length,
    top3StrongSupportingFactCount: top3.filter((item) => hasStrongSupportingFact(item)).length,
    top1FactLedSnippet: topSnippetLeadsWithFact(results[0]),
    top5ConclusionsLike: top5.some((item) => isConclusionsLikeSection(item.sectionLabel)),
    top5FindingsLike: top5.some((item) => isFindingsLikeSection(item.sectionLabel)),
    error: ""
  };
  row.precisionBand = describePrecisionBand(row);
  return row;
}

function buildStatusRow(task, status, error, corpusDocCount = null) {
  const row = {
    id: task.id,
    label: task.label || task.id,
    query: task.query,
    expectation: task.expectation || "",
    filtersSummary: describeFilters(task.filters || {}),
    status,
    returnedAny: false,
    corpusDocCount: Number.isFinite(Number(corpusDocCount)) ? Number(corpusDocCount) : null,
    totalResults: 0,
    uniqueDecisionCount: 0,
    top1Citation: "",
    top1SectionLabel: "",
    top1PrecisionPass: false,
    top1MissingGroups: [],
    top1ForbiddenHits: [],
    top3PrecisionPass: false,
    top3WrongContextCount: 0,
    top3SupportingFactCount: 0,
    top3StrongSupportingFactCount: 0,
    top1FactLedSnippet: false,
    top5ConclusionsLike: false,
    top5FindingsLike: false,
    error: error || ""
  };
  row.precisionBand = describePrecisionBand(row);
  return row;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Issue Query Search QA Report");
  lines.push("");
  lines.push(`- queryCount: ${report.queryCount}`);
  lines.push(`- returnedQueryCount: ${report.returnedQueryCount}`);
  lines.push(`- zeroResultQueryCount: ${report.zeroResultQueryCount}`);
  lines.push(`- abortedQueryCount: ${report.abortedQueryCount}`);
  lines.push(`- erroredQueryCount: ${report.erroredQueryCount}`);
  lines.push(`- noCorpusHitCount: ${report.noCorpusHitCount}`);
  lines.push(`- top1ConclusionLikeCount: ${report.top1ConclusionLikeCount}`);
  lines.push(`- top5ConclusionLikeCount: ${report.top5ConclusionLikeCount}`);
  lines.push(`- top5FindingsLikeCount: ${report.top5FindingsLikeCount}`);
  lines.push(`- top1PrecisionPassCount: ${report.top1PrecisionPassCount}`);
  lines.push(`- top3PrecisionPassCount: ${report.top3PrecisionPassCount}`);
  lines.push(`- top3SupportingFactCount: ${report.top3SupportingFactCount}`);
  lines.push(`- top3StrongSupportingFactCount: ${report.top3StrongSupportingFactCount}`);
  lines.push(`- top1FactLedSnippetCount: ${report.top1FactLedSnippetCount}`);
  lines.push("");
  lines.push("## Tasks");
  lines.push("");
  for (const row of report.rows) {
    lines.push(
      `- ${row.id} | status=${row.status} | band=${row.precisionBand} | corpus=${row.corpusDocCount ?? "<unknown>"} | returned=${row.returnedAny} | uniqueDecisions=${row.uniqueDecisionCount} | top1=${row.top1Citation || "<none>"} | top1Section=${row.top1SectionLabel || "<none>"} | top1Precision=${row.top1PrecisionPass} | top3Precision=${row.top3PrecisionPass} | strongFacts=${row.top3StrongSupportingFactCount} | factLedTop1=${row.top1FactLedSnippet} | filters=${row.filtersSummary}${row.error ? ` | error=${row.error}` : ""}${row.top1ForbiddenHits?.length ? ` | top1Forbidden=${row.top1ForbiddenHits.join(" / ")}` : ""}${row.top1MissingGroups?.length ? ` | top1Missing=${row.top1MissingGroups.join(" ; ")}` : ""}`
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
      "status",
      "precisionBand",
      "corpusDocCount",
      "returnedAny",
      "totalResults",
      "uniqueDecisionCount",
      "top1Citation",
      "top1SectionLabel",
      "top1PrecisionPass",
      "top3PrecisionPass",
      "top3WrongContextCount",
      "top3SupportingFactCount",
      "top3StrongSupportingFactCount",
      "top1FactLedSnippet",
      "top5ConclusionsLike",
      "top5FindingsLike",
      "top1MissingGroups",
      "top1ForbiddenHits",
      "error"
    ],
    ...report.rows.map((row) => [
      row.id,
      row.label,
      row.query,
      row.filtersSummary,
      row.status,
      row.precisionBand,
      row.corpusDocCount ?? "",
      row.returnedAny ? "1" : "0",
      row.totalResults,
      row.uniqueDecisionCount,
      row.top1Citation || "",
      row.top1SectionLabel || "",
      row.top1PrecisionPass ? "1" : "0",
      row.top3PrecisionPass ? "1" : "0",
      row.top3WrongContextCount,
      row.top3SupportingFactCount,
      row.top3StrongSupportingFactCount,
      row.top1FactLedSnippet ? "1" : "0",
      row.top5ConclusionsLike ? "1" : "0",
      row.top5FindingsLike ? "1" : "0",
      (row.top1MissingGroups || []).join(" ; "),
      (row.top1ForbiddenHits || []).join(" ; "),
      row.error || ""
    ])
  ];
  return `${rows.map((row) => row.map(csvEscape).join(",")).join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const tasks = await loadTasks();
  const dbPath = await detectDatabasePath().catch(() => "");
  const rows = [];
  let consecutiveTransportFailures = 0;

  for (let index = 0; index < tasks.length; index += 1) {
    const task = tasks[index];
    if (rows.length > 0 && pauseBetweenQueriesMs > 0) {
      await sleep(pauseBetweenQueriesMs);
    }

    if (healthcheckEnabled && !(await checkHealth())) {
      rows.push(buildStatusRow(task, "skipped_worker_unhealthy", "Worker health check failed before query execution."));
      for (const remaining of tasks.slice(index + 1)) {
        rows.push(buildStatusRow(remaining, "skipped_worker_unhealthy", "Skipped because the worker became unhealthy during the QA run."));
      }
      break;
    }

    let corpusDocCount = null;
    if (dbPath) {
      try {
        const corpusRows = await sqliteQuery(dbPath, buildCorpusCountSql(task));
        corpusDocCount = Number(corpusRows?.[0]?.corpusDocCount || 0);
      } catch {
        corpusDocCount = null;
      }
    }

    try {
      const response = await fetchJson(
        `${apiBase}/search`,
        {
          query: task.query,
          limit: resultLimit,
          debug: false,
          filters: {
            approvedOnly: false,
            ...(task.filters || {})
          }
        },
        task.id
      );

      const results = Array.isArray(response?.results) ? response.results : [];
      rows.push(buildReturnedRow(task, results, corpusDocCount));
      consecutiveTransportFailures = 0;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const aborted = /aborted/i.test(message);
      rows.push(buildStatusRow(task, aborted ? "aborted" : "error", message, corpusDocCount));
      if (isTransportFailure(message)) {
        consecutiveTransportFailures += 1;
        if (consecutiveTransportFailures >= stopAfterTransportFailures) {
          for (const remaining of tasks.slice(index + 1)) {
            rows.push(
              buildStatusRow(
                remaining,
                "skipped_worker_unhealthy",
                `Skipped after ${consecutiveTransportFailures} consecutive transport failures.`,
                null
              )
            );
          }
          break;
        }
      } else {
        consecutiveTransportFailures = 0;
      }
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    queryCount: rows.length,
    returnedQueryCount: rows.filter((row) => row.status === "returned").length,
    zeroResultQueryCount: rows.filter((row) => row.status === "zero_results").length,
    abortedQueryCount: rows.filter((row) => row.status === "aborted").length,
    erroredQueryCount: rows.filter((row) => row.status === "error").length,
    noCorpusHitCount: rows.filter((row) => row.corpusDocCount === 0).length,
    top1ConclusionLikeCount: rows.filter((row) => row.status === "returned" && isConclusionsLikeSection(row.top1SectionLabel)).length,
    top5ConclusionLikeCount: rows.filter((row) => row.top5ConclusionsLike).length,
    top5FindingsLikeCount: rows.filter((row) => row.top5FindingsLike).length,
    top1PrecisionPassCount: rows.filter((row) => row.top1PrecisionPass).length,
    top3PrecisionPassCount: rows.filter((row) => row.top3PrecisionPass).length,
    top3SupportingFactCount: rows.filter((row) => row.top3SupportingFactCount > 0).length,
    top3StrongSupportingFactCount: rows.filter((row) => row.top3StrongSupportingFactCount > 0).length,
    top1FactLedSnippetCount: rows.filter((row) => row.top1FactLedSnippet).length,
    rows
  };

  await Promise.all([
    fs.writeFile(path.join(reportsDir, jsonName), JSON.stringify(report, null, 2)),
    fs.writeFile(path.join(reportsDir, markdownName), toMarkdown(report)),
    fs.writeFile(path.join(reportsDir, csvName), toCsv(report))
  ]);

  console.log(
    JSON.stringify(
      {
        queryCount: report.queryCount,
        returnedQueryCount: report.returnedQueryCount,
        zeroResultQueryCount: report.zeroResultQueryCount,
        abortedQueryCount: report.abortedQueryCount,
        erroredQueryCount: report.erroredQueryCount,
        noCorpusHitCount: report.noCorpusHitCount,
        top1ConclusionLikeCount: report.top1ConclusionLikeCount,
        top5ConclusionLikeCount: report.top5ConclusionLikeCount,
        top5FindingsLikeCount: report.top5FindingsLikeCount,
        top1PrecisionPassCount: report.top1PrecisionPassCount,
        top3PrecisionPassCount: report.top3PrecisionPassCount,
        top3SupportingFactCount: report.top3SupportingFactCount,
        top3StrongSupportingFactCount: report.top3StrongSupportingFactCount,
        top1FactLedSnippetCount: report.top1FactLedSnippetCount
      },
      null,
      2
    )
  );
  console.log(`Issue query search QA JSON report written to ${path.join(reportsDir, jsonName)}`);
  console.log(`Issue query search QA Markdown report written to ${path.join(reportsDir, markdownName)}`);
  console.log(`Issue query search QA CSV report written to ${path.join(reportsDir, csvName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
