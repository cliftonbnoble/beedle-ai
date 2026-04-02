import fs from "node:fs/promises";
import path from "node:path";

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const tasksFile = process.env.ISSUE_QUERY_QA_TASKS_FILE
  ? path.resolve(process.cwd(), process.env.ISSUE_QUERY_QA_TASKS_FILE)
  : path.resolve(process.cwd(), "scripts/issue-query-search-qa-tasks.sample.json");
const taskId = String(process.env.ISSUE_QUERY_PROBE_TASK_ID || "heat_g49").trim();
const timeoutMs = Number(process.env.ISSUE_QUERY_PROBE_TIMEOUT_MS || "300000");
const limit = Math.max(1, Number(process.env.ISSUE_QUERY_PROBE_LIMIT || "8"));

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function loadTask() {
  const raw = await fs.readFile(tasksFile, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) throw new Error(`Expected task array in ${tasksFile}`);
  const task = parsed.find((item) => String(item?.id || "").trim() === taskId);
  if (!task) throw new Error(`Task ${taskId} not found in ${tasksFile}`);
  return task;
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

function describeFilters(filters) {
  const parts = [];
  if (Array.isArray(filters?.judgeNames) && filters.judgeNames.length) parts.push(`judges=${filters.judgeNames.join(" | ")}`);
  if (Array.isArray(filters?.indexCodes) && filters.indexCodes.length) parts.push(`index=${filters.indexCodes.join(" | ")}`);
  if (filters?.rulesSection) parts.push(`r&r=${filters.rulesSection}`);
  if (filters?.ordinanceSection) parts.push(`ordinance=${filters.ordinanceSection}`);
  if (filters?.fromDate || filters?.toDate) parts.push(`dates=${filters.fromDate || "any"}..${filters.toDate || "any"}`);
  return parts.join(" ; ") || "<none>";
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Issue Query Runtime Probe");
  lines.push("");
  lines.push(`- taskId: ${report.taskId}`);
  lines.push(`- query: ${report.query}`);
  lines.push(`- filters: ${report.filtersSummary}`);
  lines.push(`- status: ${report.status}`);
  lines.push(`- totalMs: ${report.stageTimingsMs?.total ?? 0}`);
  lines.push(`- lexicalScopeDocumentCount: ${report.lexicalScopeDocumentCount ?? 0}`);
  lines.push(`- lexicalRowCount: ${report.lexicalRowCount ?? 0}`);
  lines.push(`- mergedChunkCount: ${report.mergedChunkCount ?? 0}`);
  lines.push(`- decisionScopeDocumentCount: ${report.decisionScopeDocumentCount ?? 0}`);
  lines.push(`- decisionScopeChunkCount: ${report.decisionScopeChunkCount ?? 0}`);
  if (report.error) lines.push(`- error: ${report.error}`);
  lines.push("");
  lines.push("## Stage Timings (ms)");
  lines.push("");
  for (const [key, value] of Object.entries(report.stageTimingsMs || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");
  lines.push("## Top Results");
  lines.push("");
  for (const row of report.topResults || []) {
    lines.push(`- ${row.citation || "<none>"} | ${row.sectionLabel || "<none>"} | score=${row.score ?? 0}`);
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const task = await loadTask();
  await sleep(100);

  let report;
  try {
    const response = await fetchJson(`${apiBase}/admin/retrieval/debug`, {
      query: task.query,
      limit,
      offset: 0,
      queryType: "keyword",
      corpusMode: "trusted_only",
      filters: {
        approvedOnly: false,
        ...(task.filters || {})
      }
    });

    report = {
      generatedAt: new Date().toISOString(),
      apiBase,
      taskId: task.id,
      query: task.query,
      filtersSummary: describeFilters(task.filters || {}),
      status: "returned",
      lexicalScopeDocumentCount: response?.runtimeDiagnostics?.lexicalScopeDocumentCount ?? 0,
      lexicalRowCount: response?.runtimeDiagnostics?.lexicalRowCount ?? 0,
      mergedChunkCount: response?.runtimeDiagnostics?.mergedChunkCount ?? 0,
      decisionScopeDocumentCount: response?.runtimeDiagnostics?.decisionScopeDocumentCount ?? 0,
      decisionScopeChunkCount: response?.runtimeDiagnostics?.decisionScopeChunkCount ?? 0,
      stageTimingsMs: response?.runtimeDiagnostics?.stageTimingsMs || {},
      topResults: Array.isArray(response?.results)
        ? response.results.slice(0, 5).map((item) => ({
            citation: item.citation || "",
            sectionLabel: item.sectionLabel || "",
            score: item.score ?? 0
          }))
        : [],
      error: ""
    };
  } catch (error) {
    report = {
      generatedAt: new Date().toISOString(),
      apiBase,
      taskId: task.id,
      query: task.query,
      filtersSummary: describeFilters(task.filters || {}),
      status: /aborted/i.test(error instanceof Error ? error.message : String(error)) ? "aborted" : "error",
      lexicalScopeDocumentCount: 0,
      lexicalRowCount: 0,
      mergedChunkCount: 0,
      decisionScopeDocumentCount: 0,
      decisionScopeChunkCount: 0,
      stageTimingsMs: {},
      topResults: [],
      error: error instanceof Error ? error.message : String(error)
    };
  }

  const jsonPath = path.join(reportsDir, "issue-query-runtime-probe-report.json");
  const mdPath = path.join(reportsDir, "issue-query-runtime-probe-report.md");
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  await fs.writeFile(mdPath, toMarkdown(report), "utf8");
  console.log(JSON.stringify(report, null, 2));
  console.log(`Issue query runtime probe JSON report written to ${jsonPath}`);
  console.log(`Issue query runtime probe Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
