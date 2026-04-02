import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.KEYWORD_REGRESSION_AUDIT_JSON_NAME || "keyword-regression-audit-report.json";
const markdownName = process.env.KEYWORD_REGRESSION_AUDIT_MARKDOWN_NAME || "keyword-regression-audit-report.md";
const csvName = process.env.KEYWORD_REGRESSION_AUDIT_CSV_NAME || "keyword-regression-audit-report.csv";
const tasksFile = process.env.KEYWORD_REGRESSION_AUDIT_TASKS_FILE
  ? path.resolve(process.cwd(), process.env.KEYWORD_REGRESSION_AUDIT_TASKS_FILE)
  : path.resolve(process.cwd(), "scripts/keyword-regression-audit-tasks.json");
const resultLimit = Math.max(1, Number(process.env.KEYWORD_REGRESSION_AUDIT_LIMIT || "20"));
const corpusMode = process.env.KEYWORD_REGRESSION_AUDIT_CORPUS_MODE || "trusted_plus_provisional";
const timeoutMs = Number(process.env.KEYWORD_REGRESSION_AUDIT_TIMEOUT_MS || "60000");
const sqliteTimeoutMs = Number(process.env.KEYWORD_REGRESSION_AUDIT_SQLITE_TIMEOUT_MS || "60000");
const retryCount = Math.max(1, Number(process.env.KEYWORD_REGRESSION_AUDIT_RETRIES || "2"));
const pauseBetweenQueriesMs = Number(process.env.KEYWORD_REGRESSION_AUDIT_PAUSE_MS || "500");
const configuredDbPath = process.env.KEYWORD_REGRESSION_AUDIT_DB_PATH || "";
const stopAfterTransportFailures = Math.max(1, Number(process.env.KEYWORD_REGRESSION_AUDIT_STOP_AFTER_TRANSPORT_FAILURES || "2"));
const healthcheckEnabled = process.env.KEYWORD_REGRESSION_AUDIT_HEALTHCHECK !== "0";
const healthTimeoutMs = Math.max(500, Number(process.env.KEYWORD_REGRESSION_AUDIT_HEALTH_TIMEOUT_MS || "3000"));

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeSqlLike(value) {
  return String(value || "").toLowerCase().replace(/\s+/g, " ").trim();
}

function tokenizeNormalized(value) {
  return normalizeSqlLike(value)
    .split(/[^a-z0-9]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizedWholeWordSqlExpr(expr) {
  return `(' ' || lower(
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(coalesce(${expr}, ''), char(10), ' '), char(13), ' '), '.', ' '), ',', ' '), ';', ' '), ':', ' '), '(', ' '), ')', ' '), '-', ' '), '/', ' ')
  ) || ' ')`;
}

function buildCorpusTermMatchClause(term) {
  const normalized = normalizeSqlLike(term).replace(/'/g, "''");
  if (!normalized) return "0";
  return `instr(${normalizedWholeWordSqlExpr("c.chunk_text")}, ' ${normalized} ') > 0`;
}

function describeFilters(filters) {
  const parts = [];
  if (filters?.judgeName) parts.push(`judge=${filters.judgeName}`);
  if (Array.isArray(filters?.judgeNames) && filters.judgeNames.length) parts.push(`judges=${filters.judgeNames.join(" | ")}`);
  if (filters?.indexCode) parts.push(`index=${filters.indexCode}`);
  if (Array.isArray(filters?.indexCodes) && filters.indexCodes.length) parts.push(`index=${filters.indexCodes.join(" | ")}`);
  if (filters?.rulesSection) parts.push(`rules=${filters.rulesSection}`);
  if (filters?.ordinanceSection) parts.push(`ordinance=${filters.ordinanceSection}`);
  return parts.join(" ; ") || "<none>";
}

async function loadTasks() {
  const raw = await fs.readFile(tasksFile, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) throw new Error(`Expected array tasks in ${tasksFile}`);
  return parsed;
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

  throw new Error("Could not find a usable local D1 sqlite database for regression audit.");
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

  const terms = Array.isArray(task.corpusTerms) && task.corpusTerms.length ? task.corpusTerms : [task.query];
  clauses.push(
    `(${terms.map((term) => buildCorpusTermMatchClause(term)).join(" or ")})`
  );

  return `
    select count(distinct d.id) as corpusDocCount
    from document_chunks c
    join documents d on d.id = c.document_id
    where ${clauses.join("\n      and ")};
  `;
}

async function fetchJson(url, payload) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
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
  } finally {
    clearTimeout(timeout);
  }
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
    text.includes("socket hang up")
  );
}

async function fetchJsonWithRetry(url, payload, label) {
  let lastError = null;
  for (let attempt = 1; attempt <= retryCount; attempt += 1) {
    try {
      return await fetchJson(url, payload);
    } catch (error) {
      lastError = error;
      if (attempt < retryCount) await sleep(350 * attempt);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

function qualityBand(row) {
  if (row.status !== "ok") return row.status;
  if (row.corpusDocCount === 0) return "no_corpus_hits";
  if (row.pass) return "pass";
  if (row.apiTotal > 0) return "thin";
  return "fail";
}

function toMarkdown(report) {
  const lines = [
    "# Keyword Regression Audit",
    "",
    `- generatedAt: \`${report.generatedAt}\``,
    `- apiBase: \`${report.apiBase}\``,
    `- dbPath: \`${report.dbPath}\``,
    `- taskCount: \`${report.summary.taskCount}\``,
    `- passCount: \`${report.summary.passCount}\``,
    `- thinCount: \`${report.summary.thinCount}\``,
    `- failCount: \`${report.summary.failCount}\``,
    `- noCorpusHitCount: \`${report.summary.noCorpusHitCount}\``,
    "",
    "## Rows",
    ""
  ];

  for (const row of report.rows) {
    lines.push(
      `- ${row.id} | quality=${row.qualityBand} | api=${row.apiTotal} | corpus=${row.corpusDocCount} | ratio=${row.apiToCorpusRatio} | minAbs=${row.minimumApiResults} | minRatio=${row.minimumApiRatio} | top1=${row.top1Citation || "<none>"} | filters=${row.filtersSummary}${row.error ? ` | error=${row.error}` : ""}`
    );
  }

  return `${lines.join("\n")}\n`;
}

function toCsv(report) {
  const header = [
    "id",
    "label",
    "query",
    "filters",
    "status",
    "qualityBand",
    "apiTotal",
    "corpusDocCount",
    "apiToCorpusRatio",
    "minimumApiResults",
    "minimumApiRatio",
    "pass",
    "top1Citation",
    "error"
  ];

  const rows = [
    header,
    ...report.rows.map((row) => [
      row.id,
      row.label,
      row.query,
      row.filtersSummary,
      row.status,
      row.qualityBand,
      row.apiTotal,
      row.corpusDocCount,
      row.apiToCorpusRatio,
      row.minimumApiResults,
      row.minimumApiRatio,
      row.pass ? "1" : "0",
      row.top1Citation || "",
      row.error || ""
    ])
  ];

  return `${rows.map((row) => row.map(csvEscape).join(",")).join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const tasks = await loadTasks();
  const dbPath = await detectDatabasePath();
  const rows = [];
  let consecutiveTransportFailures = 0;

  for (let index = 0; index < tasks.length; index += 1) {
    const task = tasks[index];
    if (rows.length > 0 && pauseBetweenQueriesMs > 0) await sleep(pauseBetweenQueriesMs);

    if (healthcheckEnabled && !(await checkHealth())) {
      rows.push({
        id: task.id,
        label: task.label || task.id,
        query: task.query,
        filtersSummary: describeFilters(task.filters || {}),
        status: "skipped_worker_unhealthy",
        corpusDocCount: 0,
        apiTotal: 0,
        apiToCorpusRatio: 0,
        minimumApiResults: Number(task.minimumApiResults || 0),
        minimumApiRatio: Number(task.minimumApiRatio || 0),
        pass: false,
        top1Citation: "",
        qualityBand: "pending",
        error: "Worker health check failed before query execution."
      });
      for (const remaining of tasks.slice(index + 1)) {
        rows.push({
          id: remaining.id,
          label: remaining.label || remaining.id,
          query: remaining.query,
          filtersSummary: describeFilters(remaining.filters || {}),
          status: "skipped_worker_unhealthy",
          corpusDocCount: 0,
          apiTotal: 0,
          apiToCorpusRatio: 0,
          minimumApiResults: Number(remaining.minimumApiResults || 0),
          minimumApiRatio: Number(remaining.minimumApiRatio || 0),
          pass: false,
          top1Citation: "",
          qualityBand: "pending",
          error: "Skipped because the worker became unhealthy during the audit."
        });
      }
      break;
    }

    const payload = {
      query: task.query,
      queryType: "keyword",
      limit: resultLimit,
      corpusMode,
      filters: {
        approvedOnly: task?.filters?.approvedOnly !== false,
        ...(task.filters || {})
      }
    };

    try {
      const corpusRows = await sqliteQuery(dbPath, buildCorpusCountSql(task));
      const corpusDocCount = Number(corpusRows?.[0]?.corpusDocCount || 0);
      const response = await fetchJsonWithRetry(`${apiBase}/search`, payload, task.id);
      const results = Array.isArray(response?.results) ? response.results : [];
      const apiTotal = Number(response?.total || results.length || 0);
      const apiToCorpusRatio = corpusDocCount > 0 ? Number((apiTotal / corpusDocCount).toFixed(4)) : 0;
      const minimumApiResults = Number(
        task.minimumApiResults ?? (corpusDocCount >= 10 ? 5 : corpusDocCount >= 4 ? 3 : Math.max(1, corpusDocCount))
      );
      const minimumApiRatio = Number(task.minimumApiRatio ?? 0.25);
      const pass =
        corpusDocCount === 0
          ? apiTotal === 0
          : apiTotal >= Math.min(minimumApiResults, corpusDocCount) || apiToCorpusRatio >= minimumApiRatio;

      rows.push({
        id: task.id,
        label: task.label || task.id,
        query: task.query,
        filtersSummary: describeFilters(task.filters || {}),
        status: "ok",
        corpusDocCount,
        apiTotal,
        apiToCorpusRatio,
        minimumApiResults,
        minimumApiRatio,
        pass,
        top1Citation: results[0]?.citation || "",
        qualityBand: "pending",
        error: null
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      rows.push({
        id: task.id,
        label: task.label || task.id,
        query: task.query,
        filtersSummary: describeFilters(task.filters || {}),
        status: "error",
        corpusDocCount: 0,
        apiTotal: 0,
        apiToCorpusRatio: 0,
        minimumApiResults: Number(task.minimumApiResults || 0),
        minimumApiRatio: Number(task.minimumApiRatio || 0),
        pass: false,
        top1Citation: "",
        qualityBand: "pending",
        error: message
      });

      consecutiveTransportFailures = isTransportFailure(message) ? consecutiveTransportFailures + 1 : 0;
      if (consecutiveTransportFailures >= stopAfterTransportFailures) {
        for (const remaining of tasks.slice(index + 1)) {
          rows.push({
            id: remaining.id,
            label: remaining.label || remaining.id,
            query: remaining.query,
            filtersSummary: describeFilters(remaining.filters || {}),
            status: "skipped_worker_unhealthy",
            corpusDocCount: 0,
            apiTotal: 0,
            apiToCorpusRatio: 0,
            minimumApiResults: Number(remaining.minimumApiResults || 0),
            minimumApiRatio: Number(remaining.minimumApiRatio || 0),
            pass: false,
            top1Citation: "",
            qualityBand: "pending",
            error: "Skipped after consecutive transport failures suggested the worker was unhealthy."
          });
        }
        break;
      }
    }
  }

  for (const row of rows) row.qualityBand = qualityBand(row);

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    dbPath,
    summary: {
      taskCount: rows.length,
      passCount: rows.filter((row) => row.qualityBand === "pass").length,
      thinCount: rows.filter((row) => row.qualityBand === "thin").length,
      failCount: rows.filter((row) => row.qualityBand === "fail" || row.qualityBand === "error").length,
      noCorpusHitCount: rows.filter((row) => row.qualityBand === "no_corpus_hits").length,
      skippedCount: rows.filter((row) => row.status === "skipped_worker_unhealthy").length
    },
    rows
  };

  await fs.writeFile(path.join(reportsDir, jsonName), `${JSON.stringify(report, null, 2)}\n`, "utf8");
  await fs.writeFile(path.join(reportsDir, markdownName), toMarkdown(report), "utf8");
  await fs.writeFile(path.join(reportsDir, csvName), toCsv(report), "utf8");

  console.log(JSON.stringify(report.summary, null, 2));
}

await main();
