import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const runId = new Date().toISOString().replace(/[:.]/g, "-");
const reportsRoot = path.resolve(process.cwd(), "reports", "overnight-retrieval-refresh", runId);
const runMode = new Set(["full", "audit_only", "mutate_only"]).has(process.env.OVERNIGHT_RETRIEVAL_MODE || "")
  ? process.env.OVERNIGHT_RETRIEVAL_MODE
  : "full";
const maxChunkRounds = Math.max(1, Number.parseInt(process.env.OVERNIGHT_CHUNK_BACKFILL_ROUNDS || "12", 10));
const chunkBatchLimit = Math.max(1, Number.parseInt(process.env.OVERNIGHT_CHUNK_BACKFILL_LIMIT || "75", 10));
const enableLimit = Math.max(1, Number.parseInt(process.env.OVERNIGHT_ENABLE_LIMIT || "50", 10));
const enableRounds = Math.max(1, Number.parseInt(process.env.OVERNIGHT_ENABLE_MAX_ROUNDS || "80", 10));
const enableSleepMs = Math.max(0, Number.parseInt(process.env.OVERNIGHT_ENABLE_SLEEP_MS || "2500", 10));
const vectorBatchSize = Math.max(1, Number.parseInt(process.env.OVERNIGHT_VECTOR_BATCH_SIZE || "50", 10));
const keywordLimit = Math.max(1, Number.parseInt(process.env.OVERNIGHT_KEYWORD_LIMIT || "30", 10));

function rel(filePath) {
  return path.relative(process.cwd(), filePath);
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function fetchJson(url) {
  const response = await fetch(url);
  const text = await response.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { raw: text };
  }
  return { ok: response.ok, status: response.status, body };
}

async function runNodeScript(scriptFile, env = {}, timeout = 1000 * 60 * 60 * 8) {
  const startedAt = new Date().toISOString();
  try {
    const { stdout, stderr } = await execFileAsync(process.execPath, [scriptFile], {
      cwd: process.cwd(),
      env: { ...process.env, ...env },
      timeout,
      maxBuffer: 1024 * 1024 * 32
    });
    return {
      status: "ok",
      startedAt,
      finishedAt: new Date().toISOString(),
      scriptFile,
      stdout: String(stdout || ""),
      stderr: String(stderr || "")
    };
  } catch (error) {
    return {
      status: "failed",
      startedAt,
      finishedAt: new Date().toISOString(),
      scriptFile,
      error: error instanceof Error ? error.message : String(error),
      stdout: String(error?.stdout || ""),
      stderr: String(error?.stderr || "")
    };
  }
}

async function readJsonIfExists(filePath) {
  try {
    return JSON.parse(await fs.readFile(filePath, "utf8"));
  } catch {
    return null;
  }
}

async function runChunkBackfillRounds(summary) {
  const rounds = [];
  for (let round = 1; round <= maxChunkRounds; round += 1) {
    const reportName = `chunk-backfill-round-${String(round).padStart(2, "0")}.json`;
    const markdownName = `chunk-backfill-round-${String(round).padStart(2, "0")}.md`;
    const result = await runNodeScript("./scripts/searchable-chunk-backfill-batch.mjs", {
      API_BASE_URL: apiBase,
      SEARCHABLE_CHUNK_BACKFILL_LIMIT: String(chunkBatchLimit),
      SEARCHABLE_CHUNK_BACKFILL_OUTPUT_DIR: rel(reportsRoot),
      SEARCHABLE_CHUNK_BACKFILL_REPORT_NAME: reportName,
      SEARCHABLE_CHUNK_BACKFILL_MARKDOWN_NAME: markdownName
    });
    const reportPath = path.resolve(reportsRoot, reportName);
    const report = await readJsonIfExists(reportPath);
    rounds.push({
      round,
      status: result.status,
      reportPath,
      selectedDocumentCount: Number(report?.summary?.selectedDocumentCount || 0),
      rebuiltDocumentCount: Number(report?.summary?.rebuiltDocumentCount || 0),
      searchableWithoutChunkRowsAfter: Number(report?.summary?.searchableWithoutChunkRowsAfter || 0),
      error: result.status === "failed" ? result.error : null
    });
    if (result.status !== "ok") break;
    if (Number(report?.summary?.selectedDocumentCount || 0) === 0) break;
    if (Number(report?.summary?.searchableWithoutChunkRowsAfter || 0) === 0) break;
  }
  summary.chunkBackfillRounds = rounds;
}

async function runSearchabilityEnable(summary) {
  const outputDir = path.resolve(reportsRoot, "searchability-enable-loop");
  await ensureDir(outputDir);
  const result = await runNodeScript("./scripts/searchability-enable-loop.mjs", {
    API_BASE_URL: apiBase,
    SEARCHABILITY_ENABLE_LIMIT: String(enableLimit),
    SEARCHABILITY_ENABLE_MAX_ROUNDS: String(enableRounds),
    SEARCHABILITY_ENABLE_SLEEP_MS: String(enableSleepMs),
    SEARCHABILITY_ENABLE_OUTPUT_DIR: outputDir,
    SEARCHABILITY_ENABLE_REAL_ONLY: "1",
    SEARCHABILITY_ENABLE_MODE: process.env.OVERNIGHT_ENABLE_MODE || "qcPassed"
  });
  const entries = await fs.readdir(outputDir).catch(() => []);
  const candidateFiles = entries.filter((name) => name.endsWith("-candidates.json")).sort();
  const enableFiles = entries.filter((name) => name.endsWith("-enable.json")).sort();
  let finalCandidateCount = null;
  if (candidateFiles.length > 0) {
    const latest = await readJsonIfExists(path.join(outputDir, candidateFiles[candidateFiles.length - 1]));
    finalCandidateCount = Number(latest?.body?.summary?.candidateCount ?? latest?.summary?.candidateCount ?? 0);
  }
  summary.searchabilityEnable = {
    status: result.status,
    outputDir,
    roundsObserved: enableFiles.length,
    finalCandidateCount,
    error: result.status === "failed" ? result.error : null
  };
}

async function runVectorBackfill(summary) {
  const jsonName = "retrieval-vector-backfill-report.json";
  const markdownName = "retrieval-vector-backfill-report.md";
  const result = await runNodeScript("./scripts/retrieval-vector-backfill.mjs", {
    API_BASE_URL: apiBase,
    RETRIEVAL_VECTOR_BACKFILL_BATCH_SIZE: String(vectorBatchSize),
    RETRIEVAL_VECTOR_BACKFILL_REPORT_NAME: path.join("overnight-retrieval-refresh", runId, jsonName),
    RETRIEVAL_VECTOR_BACKFILL_MARKDOWN_NAME: path.join("overnight-retrieval-refresh", runId, markdownName)
  });
  const reportPath = path.resolve(process.cwd(), "reports", "overnight-retrieval-refresh", runId, jsonName);
  const report = await readJsonIfExists(reportPath);
  summary.vectorBackfill = {
    status: result.status,
    reportPath,
    processedCount: Number(report?.counts?.processedCount || 0),
    embeddedCount: Number(report?.counts?.embeddedCount || 0),
    upsertedCount: Number(report?.counts?.upsertedCount || 0),
    failedCount: Number(report?.counts?.failedCount || 0),
    error: result.status === "failed" ? result.error : null
  };
}

async function runKeywordAudit(summary, tasksFile, outputStem) {
  const jsonName = `${outputStem}.json`;
  const markdownName = `${outputStem}.md`;
  const csvName = `${outputStem}.csv`;
  const result = await runNodeScript("./scripts/keyword-regression-audit-report.mjs", {
    API_BASE_URL: apiBase,
    KEYWORD_REGRESSION_AUDIT_TASKS_FILE: tasksFile,
    KEYWORD_REGRESSION_AUDIT_LIMIT: String(keywordLimit),
    KEYWORD_REGRESSION_AUDIT_JSON_NAME: path.join("overnight-retrieval-refresh", runId, jsonName),
    KEYWORD_REGRESSION_AUDIT_MARKDOWN_NAME: path.join("overnight-retrieval-refresh", runId, markdownName),
    KEYWORD_REGRESSION_AUDIT_CSV_NAME: path.join("overnight-retrieval-refresh", runId, csvName)
  });
  const reportPath = path.resolve(process.cwd(), "reports", "overnight-retrieval-refresh", runId, jsonName);
  const report = await readJsonIfExists(reportPath);
  return {
    status: result.status,
    reportPath,
    taskCount: Number(report?.summary?.taskCount || 0),
    passCount: Number(report?.summary?.passCount || 0),
    thinCount: Number(report?.summary?.thinCount || 0),
    failCount: Number(report?.summary?.failCount || 0),
    error: result.status === "failed" ? result.error : null
  };
}

async function runIssueAudit(summary) {
  const jsonName = "retrieval-issue-quality-audit-report.json";
  const markdownName = "retrieval-issue-quality-audit-report.md";
  const result = await runNodeScript("./scripts/retrieval-issue-quality-audit.mjs", {
    API_BASE_URL: apiBase,
    RETRIEVAL_ISSUE_QUALITY_LIMIT: "8",
    RETRIEVAL_ISSUE_QUALITY_REPORT_NAME: path.join("overnight-retrieval-refresh", runId, jsonName),
    RETRIEVAL_ISSUE_QUALITY_MARKDOWN_NAME: path.join("overnight-retrieval-refresh", runId, markdownName)
  });
  const reportPath = path.resolve(process.cwd(), "reports", "overnight-retrieval-refresh", runId, jsonName);
  const report = await readJsonIfExists(reportPath);
  summary.issueQualityAudit = {
    status: result.status,
    reportPath,
    queryCount: Number(report?.summary?.queryCount || 0),
    returnedQueryCount: Number(report?.summary?.returnedQueryCount || 0),
    failedQueryCount: Number(report?.summary?.failedQueryCount || 0),
    overallHitRate: Number(report?.summary?.overallHitRate || 0),
    error: result.status === "failed" ? result.error : null
  };
}

function toMarkdown(summary) {
  const lines = [
    "# Overnight Retrieval Refresh",
    "",
    `- Generated: \`${summary.generatedAt}\``,
    `- API base: \`${summary.apiBase}\``,
    `- Run mode: \`${summary.runMode}\``,
    `- Run status: \`${summary.runStatus}\``,
    `- Health before: \`${summary.healthBefore.status}\``,
    `- Health after: \`${summary.healthAfter.status}\``,
    "",
    "## Chunk Backfill Rounds",
    ""
  ];

  for (const round of summary.chunkBackfillRounds || []) {
    lines.push(
      `- round ${round.round} | status=${round.status} | selected=${round.selectedDocumentCount} | rebuilt=${round.rebuiltDocumentCount} | remainingGap=${round.searchableWithoutChunkRowsAfter}${round.error ? ` | error=${round.error}` : ""}`
    );
  }
  if (!(summary.chunkBackfillRounds || []).length) lines.push("- none");

  lines.push("");
  lines.push("## Searchability Enable");
  lines.push("");
  lines.push(
    `- status=${summary.searchabilityEnable?.status || "unknown"} | roundsObserved=${summary.searchabilityEnable?.roundsObserved || 0} | finalCandidateCount=${summary.searchabilityEnable?.finalCandidateCount ?? "n/a"}${summary.searchabilityEnable?.error ? ` | error=${summary.searchabilityEnable.error}` : ""}`
  );
  lines.push("");
  lines.push("## Vector Backfill");
  lines.push("");
  lines.push(
    `- status=${summary.vectorBackfill?.status || "unknown"} | processed=${summary.vectorBackfill?.processedCount || 0} | upserted=${summary.vectorBackfill?.upsertedCount || 0} | failed=${summary.vectorBackfill?.failedCount || 0}${summary.vectorBackfill?.error ? ` | error=${summary.vectorBackfill.error}` : ""}`
  );
  lines.push("");
  lines.push("## Keyword Audits");
  lines.push("");
  for (const audit of summary.keywordAudits || []) {
    lines.push(
      `- ${audit.label} | status=${audit.status} | tasks=${audit.taskCount || 0} | pass=${audit.passCount || 0} | thin=${audit.thinCount || 0} | fail=${audit.failCount || 0}${audit.error ? ` | error=${audit.error}` : ""}`
    );
  }
  lines.push("");
  lines.push("## Issue Quality Audit");
  lines.push("");
  lines.push(
    `- status=${summary.issueQualityAudit?.status || "unknown"} | queryCount=${summary.issueQualityAudit?.queryCount || 0} | returned=${summary.issueQualityAudit?.returnedQueryCount || 0} | hitRate=${summary.issueQualityAudit?.overallHitRate || 0}${summary.issueQualityAudit?.error ? ` | error=${summary.issueQualityAudit.error}` : ""}`
  );
  lines.push("");

  return `${lines.join("\n")}\n`;
}

async function main() {
  await ensureDir(reportsRoot);
  const summary = {
    generatedAt: new Date().toISOString(),
    apiBase,
    runMode,
    runStatus: "running",
    reportsRoot,
    healthBefore: { status: "unknown" },
    healthAfter: { status: "unknown" },
    chunkBackfillRounds: [],
    searchabilityEnable: null,
    vectorBackfill: null,
    keywordAudits: [],
    issueQualityAudit: null
  };

  const healthBefore = await fetchJson(`${apiBase}/health`).catch((error) => ({
    ok: false,
    status: "failed",
    body: { error: error instanceof Error ? error.message : String(error) }
  }));
  summary.healthBefore = { status: healthBefore.ok ? "ok" : "failed", detail: healthBefore.body };

  const shouldRunMutations = runMode === "full" || runMode === "mutate_only";
  const shouldRunAudits = runMode === "full" || runMode === "audit_only";

  if (shouldRunMutations) {
    await runChunkBackfillRounds(summary);
    await runSearchabilityEnable(summary);
    await runVectorBackfill(summary);
  }

  const midHealth = await fetchJson(`${apiBase}/health`).catch((error) => ({
    ok: false,
    status: "failed",
    body: { error: error instanceof Error ? error.message : String(error) }
  }));
  summary.healthMid = { status: midHealth.ok ? "ok" : "failed", detail: midHealth.body };

  if (shouldRunAudits && midHealth.ok) {
    summary.keywordAudits.push({
      label: "focused-regression-pack",
      ...(await runKeywordAudit(summary, "scripts/keyword-regression-audit-tasks.json", "keyword-regression-audit-report"))
    });
    summary.keywordAudits.push({
      label: "medium-regression-pack",
      ...(await runKeywordAudit(summary, "scripts/keyword-regression-audit-tasks.medium.json", "keyword-regression-audit-medium-report"))
    });
    summary.keywordAudits.push({
      label: "common-keyword-pack",
      ...(await runKeywordAudit(
        summary,
        "scripts/keyword-regression-audit-tasks.common-keywords.json",
        "keyword-regression-audit-common-keywords-report"
      ))
    });

    await runIssueAudit(summary);
  } else if (shouldRunAudits) {
    summary.keywordAudits.push({
      label: "focused-regression-pack",
      status: "skipped_worker_unhealthy",
      reportPath: null,
      taskCount: 0,
      passCount: 0,
      thinCount: 0,
      failCount: 0,
      error: "Skipped because the API was unhealthy after mutation stages."
    });
    summary.keywordAudits.push({
      label: "medium-regression-pack",
      status: "skipped_worker_unhealthy",
      reportPath: null,
      taskCount: 0,
      passCount: 0,
      thinCount: 0,
      failCount: 0,
      error: "Skipped because the API was unhealthy after mutation stages."
    });
    summary.keywordAudits.push({
      label: "common-keyword-pack",
      status: "skipped_worker_unhealthy",
      reportPath: null,
      taskCount: 0,
      passCount: 0,
      thinCount: 0,
      failCount: 0,
      error: "Skipped because the API was unhealthy after mutation stages."
    });
    summary.issueQualityAudit = {
      status: "skipped_worker_unhealthy",
      reportPath: null,
      queryCount: 0,
      returnedQueryCount: 0,
      failedQueryCount: 0,
      overallHitRate: 0,
      error: "Skipped because the API was unhealthy after mutation stages."
    };
  }

  const healthAfter = await fetchJson(`${apiBase}/health`).catch((error) => ({
    ok: false,
    status: "failed",
    body: { error: error instanceof Error ? error.message : String(error) }
  }));
  summary.healthAfter = { status: healthAfter.ok ? "ok" : "failed", detail: healthAfter.body };
  summary.runStatus =
    summary.healthAfter.status === "ok"
      ? "completed"
      : shouldRunMutations && shouldRunAudits
        ? "completed_with_worker_failure"
        : "worker_unhealthy";

  const jsonPath = path.resolve(reportsRoot, "overnight-retrieval-refresh-report.json");
  const mdPath = path.resolve(reportsRoot, "overnight-retrieval-refresh-report.md");
  await fs.writeFile(jsonPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  await fs.writeFile(mdPath, toMarkdown(summary), "utf8");

  console.log(JSON.stringify({
    reportsRoot: rel(reportsRoot),
    healthBefore: summary.healthBefore.status,
    healthAfter: summary.healthAfter.status,
    chunkRounds: summary.chunkBackfillRounds.length,
    keywordAudits: summary.keywordAudits.map((item) => ({
      label: item.label,
      pass: item.passCount,
      thin: item.thinCount,
      fail: item.failCount
    }))
  }, null, 2));
  console.log(`Overnight retrieval refresh report written to ${jsonPath}`);
  console.log(`Overnight retrieval refresh markdown written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
