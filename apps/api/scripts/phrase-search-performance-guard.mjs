import fs from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

const apiBase = (process.env.PHRASE_SEARCH_PERF_API_BASE || process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.PHRASE_SEARCH_PERF_JSON_NAME || "phrase-search-performance-guard.json";
const markdownName = process.env.PHRASE_SEARCH_PERF_MARKDOWN_NAME || "phrase-search-performance-guard.md";
const timeoutMs = Math.max(1000, Number(process.env.PHRASE_SEARCH_PERF_TIMEOUT_MS || "45000"));
const limit = Math.max(1, Number(process.env.PHRASE_SEARCH_PERF_LIMIT || "8"));
const corpusMode = process.env.PHRASE_SEARCH_PERF_CORPUS_MODE || "trusted_only";
const warnTotalMs = Math.max(1, Number(process.env.PHRASE_SEARCH_PERF_WARN_TOTAL_MS || "7000"));
const warnLexicalMs = Math.max(1, Number(process.env.PHRASE_SEARCH_PERF_WARN_LEXICAL_MS || "1500"));

export const PHRASE_SEARCH_PERFORMANCE_TASKS = [
  { id: "ant_infestation_kitchen", query: "Ant infestation in the kitchen" },
  { id: "pipe_noise", query: "pipe noise" },
  { id: "shower_drain_backing_up", query: "shower drain backing up" },
  { id: "ceiling_leak_bedroom", query: "ceiling leak in bedroom" }
];

async function fetchDebug(task) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const started = performance.now();
  try {
    const response = await fetch(`${apiBase}/admin/retrieval/debug`, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "content-type": "application/json",
        accept: "application/json"
      },
      body: JSON.stringify({
        query: task.query,
        queryType: "keyword",
        limit,
        snippetMaxLength: 360,
        corpusMode,
        filters: { approvedOnly: true }
      })
    });
    const raw = await response.text();
    let body = {};
    try {
      body = JSON.parse(raw || "{}");
    } catch {
      body = { parseError: raw.slice(0, 500) };
    }
    return {
      ok: response.ok,
      httpStatus: response.status,
      wallMs: Math.round(performance.now() - started),
      body
    };
  } finally {
    clearTimeout(timer);
  }
}

function summarizeResult(result, index) {
  return {
    rank: index + 1,
    citation: String(result?.citation || result?.title || ""),
    title: String(result?.title || ""),
    documentId: String(result?.documentId || ""),
    sectionLabel: String(result?.sectionLabel || ""),
    score: Number(result?.score || 0)
  };
}

export function summarizeSlowestStages(stageTimingsMs, limit = 5) {
  return Object.entries(stageTimingsMs || {})
    .filter(([stage, value]) => stage !== "total" && Number.isFinite(Number(value)))
    .map(([stage, value]) => ({ stage, ms: Number(value) }))
    .sort((a, b) => b.ms - a.ms || a.stage.localeCompare(b.stage))
    .slice(0, Math.max(1, limit));
}

export function evaluatePhraseSearchPerformance(task, response, thresholds = { warnTotalMs, warnLexicalMs }) {
  const body = response.body || {};
  const timings = body?.runtimeDiagnostics?.stageTimingsMs || {};
  const lexicalMs = Number(timings.lexicalSearch || 0);
  const totalMs = Number(timings.total || response.wallMs || 0);
  const results = Array.isArray(body.results) ? body.results : [];
  const slowestStages = summarizeSlowestStages(timings);
  const warnings = [];

  if (!response.ok) warnings.push(`http_${response.httpStatus}`);
  if (totalMs > thresholds.warnTotalMs) warnings.push(`total_over_${thresholds.warnTotalMs}ms`);
  if (lexicalMs > thresholds.warnLexicalMs) warnings.push(`lexical_over_${thresholds.warnLexicalMs}ms`);

  return {
    id: task.id,
    query: task.query,
    status: response.ok ? "returned" : "http_error",
    warnings,
    totalResults: Number(body.total || results.length || 0),
    wallMs: response.wallMs,
    lexicalMs,
    totalMs,
    stageTimingsMs: timings,
    slowestStage: slowestStages[0] || null,
    slowestStages,
    topCitations: results.slice(0, 5).map((result) => String(result?.citation || result?.title || "")).filter(Boolean),
    topResults: results.slice(0, 5).map(summarizeResult),
    error: response.ok ? "" : JSON.stringify(body).slice(0, 500)
  };
}

export function formatPhraseSearchPerformanceMarkdown(report) {
  const lines = [
    "# Phrase Search Performance Guard",
    "",
    `- Generated: ${report.generatedAt}`,
    `- API base: ${report.apiBase}`,
    `- Corpus mode: ${report.corpusMode}`,
    `- Limit: ${report.limit}`,
    `- Warning thresholds: total>${report.warningThresholds.totalMs}ms, lexical>${report.warningThresholds.lexicalMs}ms`,
    `- Queries with warnings: ${report.summary.warningCount}/${report.summary.queryCount}`,
    ""
  ];

  for (const row of report.results) {
    lines.push(`## ${row.warnings.length ? "WARN" : "OK"} ${row.query}`);
    lines.push("");
    lines.push(`- status: ${row.status}`);
    lines.push(`- totalResults: ${row.totalResults}`);
    lines.push(`- wallMs: ${row.wallMs}`);
    lines.push(`- totalMs: ${row.totalMs}`);
    lines.push(`- lexicalMs: ${row.lexicalMs}`);
    lines.push(
      `- slowest stages: ${
        row.slowestStages?.length ? row.slowestStages.map((item) => `${item.stage}=${item.ms}ms`).join(", ") : "none"
      }`
    );
    lines.push(`- warnings: ${row.warnings.length ? row.warnings.join(", ") : "none"}`);
    lines.push(`- top citations: ${row.topCitations.length ? row.topCitations.join(" | ") : "none"}`);
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const results = [];

  for (const task of PHRASE_SEARCH_PERFORMANCE_TASKS) {
    try {
      const response = await fetchDebug(task);
      results.push(evaluatePhraseSearchPerformance(task, response));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      results.push({
        id: task.id,
        query: task.query,
        status: /aborted/i.test(message) ? "timeout" : "error",
        warnings: ["request_failed"],
        totalResults: 0,
        wallMs: timeoutMs,
        lexicalMs: 0,
        totalMs: timeoutMs,
        stageTimingsMs: {},
        slowestStage: null,
        slowestStages: [],
        topCitations: [],
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
    warningThresholds: {
      totalMs: warnTotalMs,
      lexicalMs: warnLexicalMs
    },
    summary: {
      queryCount: results.length,
      warningCount: results.filter((row) => row.warnings.length > 0).length
    },
    results
  };

  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  await fs.writeFile(markdownPath, formatPhraseSearchPerformanceMarkdown(report), "utf8");

  console.log(JSON.stringify(report, null, 2));
  console.log(`Phrase search performance guard JSON report written to ${jsonPath}`);
  console.log(`Phrase search performance guard Markdown report written to ${markdownPath}`);
  if (report.summary.warningCount > 0) {
    console.warn(`Phrase search performance guard completed with ${report.summary.warningCount} warning(s).`);
  }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
