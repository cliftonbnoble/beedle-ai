import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  BENCHMARK_INTENT_TO_QUERY_TYPE,
  buildBenchmarkDebugPayload,
  callBenchmarkDebug,
  normalizeSectionTypeRuntime
} from "./retrieval-benchmark-contract-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const rewrittenTasksName = process.env.RETRIEVAL_R60_10_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const normalizedName = process.env.RETRIEVAL_R60_10_NORMALIZED_NAME || "retrieval-r60_8-normalized-queries.json";
const r60_8Name = process.env.RETRIEVAL_R60_10_R60_8_NAME || "retrieval-r60_8-query-normalization-report.json";
const r60_7Name = process.env.RETRIEVAL_R60_10_R60_7_NAME || "retrieval-r60_7-failure-clustering-report.json";
const evalName = process.env.RETRIEVAL_R60_10_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const outputReportName = process.env.RETRIEVAL_R60_10_REPORT_NAME || "retrieval-r60_10-query-weighting-report.json";
const outputMdName = process.env.RETRIEVAL_R60_10_MARKDOWN_NAME || "retrieval-r60_10-query-weighting-report.md";
const outputWeightedName = process.env.RETRIEVAL_R60_10_WEIGHTED_NAME || "retrieval-r60_10-weighted-queries.json";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number.parseInt(process.env.RETRIEVAL_R60_10_LIMIT || "10", 10);

const INTENT_TO_QUERY_TYPE = BENCHMARK_INTENT_TO_QUERY_TYPE;

const PROCEDURAL_WEIGHTED_TERMS = [
  "notice",
  "service",
  "continuance",
  "hearing",
  "appearance",
  "filing",
  "deadline",
  "extension",
  "dismissal",
  "denial",
  "grant"
];

const INTENT_TEMPLATE_TERMS = {
  authority_lookup: ["authority", "ordinance", "rule", "standard", "analysis"],
  findings: ["findings", "credibility", "evidence", "fact", "witness"],
  issue_holding_disposition: ["issue", "holding", "disposition", "order", "decision"],
  analysis_reasoning: ["analysis", "reasoning", "application", "standard", "authority"],
  comparative_reasoning: ["compare", "prior", "decisions", "analysis", "reasoning"],
  citation_direct: ["rule", "ordinance", "citation", "section", "authority"]
};

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function tokenize(query) {
  return String(query || "")
    .toLowerCase()
    .replace(/[^a-z0-9.\s]/g, " ")
    .split(/\s+/)
    .filter(Boolean);
}

const normalizeSectionType = normalizeSectionTypeRuntime;

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function buildWeightedEntry(task, normalizedMap) {
  const queryId = String(task?.queryId || "");
  const intent = String(task?.intent || "");
  const originalQuery = String(task?.originalQuery || task?.adoptedQuery || "");
  const normalized = normalizedMap.get(queryId) || {};
  const normalizedQuery = String(normalized?.normalizedQuery || task?.adoptedQuery || originalQuery);

  const baseTerms = tokenize(normalizedQuery);
  const intentTerms = intent === "procedural_history" ? PROCEDURAL_WEIGHTED_TERMS : INTENT_TEMPLATE_TERMS[intent] || [];
  const weightedTerms = unique([...baseTerms, ...intentTerms]).slice(0, 20);

  const weightingTemplateUsed =
    intent === "procedural_history" ? "procedural_priority_terms_v1" : `${intent || "generic"}_weighted_terms_v1`;
  const weightedQuery = unique([
    ...intentTerms,
    ...baseTerms,
    ...(intent === "procedural_history" ? PROCEDURAL_WEIGHTED_TERMS.slice(0, 6) : [])
  ]).join(" ");

  return {
    queryId,
    originalQuery,
    normalizedQuery,
    weightedQuery: weightedQuery || normalizedQuery || originalQuery,
    intent,
    weightingTemplateUsed,
    weightedTerms
  };
}

async function fetchDebug(apiBaseUrl, query, queryType) {
  const payload = buildBenchmarkDebugPayload({ query, queryType, limit });
  const contract = await callBenchmarkDebug({ apiBaseUrl, payload });
  return {
    ok: Boolean(contract.fetchSucceeded && contract.responseOk && contract.parseSucceeded),
    results: contract.parsedResults || []
  };
}

function hitRates(taskRows) {
  const total = Math.max(1, taskRows.length);
  return {
    top1: Number((taskRows.filter((row) => row.top1Hit).length / total).toFixed(4)),
    top3: Number((taskRows.filter((row) => row.top3Hit).length / total).toFixed(4)),
    top5: Number((taskRows.filter((row) => row.top5Hit).length / total).toFixed(4)),
    sectionType: Number((taskRows.filter((row) => row.sectionTypeHit).length / total).toFixed(4))
  };
}

export async function buildR60_10QueryWeightingReport({
  rewrittenTasks,
  normalizedPackages,
  r60_8Report,
  r60_7Report,
  trustedDecisionIds,
  apiBaseUrl
}) {
  const normalizedMap = new Map((normalizedPackages || []).map((row) => [String(row?.queryId || ""), row]));
  const trustedSet = new Set((trustedDecisionIds || []).map(String));

  const weightedEntries = (rewrittenTasks || [])
    .map((task) => buildWeightedEntry(task, normalizedMap))
    .sort((a, b) => a.queryId.localeCompare(b.queryId));

  const taskRows = [];
  for (const entry of weightedEntries) {
    const task = (rewrittenTasks || []).find((t) => String(t.queryId) === String(entry.queryId)) || {};
    const runtime = await fetchDebug(apiBaseUrl, entry.weightedQuery, INTENT_TO_QUERY_TYPE[entry.intent] || "keyword");
    const trustedRows = (runtime.results || []).filter((row) => trustedSet.has(String(row?.documentId || ""))).slice(0, limit);
    const returnedDecisionIds = unique(trustedRows.map((row) => row.documentId));
    const returnedSectionTypes = unique(trustedRows.map((row) => normalizeSectionType(row.sectionLabel || row.chunkType || "")));
    const expectedDecisionIds = (task.expectedDecisionIds || []).map(String);
    const expectedSectionTypes = (task.expectedSectionTypes || []).map(String);
    const top1Hit = expectedDecisionIds.includes(String(trustedRows[0]?.documentId || ""));
    const top3Hit = trustedRows.slice(0, 3).some((row) => expectedDecisionIds.includes(String(row.documentId || "")));
    const top5Hit = trustedRows.slice(0, 5).some((row) => expectedDecisionIds.includes(String(row.documentId || "")));
    const sectionTypeHit = trustedRows
      .slice(0, 5)
      .some((row) => expectedSectionTypes.map(normalizeSectionType).includes(normalizeSectionType(row.sectionLabel || row.chunkType || "")));

    taskRows.push({
      queryId: entry.queryId,
      intent: entry.intent,
      weightedQuery: entry.weightedQuery,
      expectedDecisionIds,
      returnedDecisionIds,
      returnedSectionTypes,
      top1Hit,
      top3Hit,
      top5Hit,
      sectionTypeHit
    });
  }

  const rates = hitRates(taskRows);
  const tasksRecoveredByWeighting = taskRows.filter((row) => row.returnedDecisionIds.length > 0).map((row) => row.queryId);
  const stillEmptyTaskIds = taskRows.filter((row) => row.returnedDecisionIds.length === 0).map((row) => row.queryId);
  const wrongDecisionTaskIds = taskRows
    .filter((row) => row.returnedDecisionIds.length > 0)
    .filter((row) => !row.top5Hit)
    .map((row) => row.queryId);

  const proceduralRows = taskRows.filter((row) => row.intent === "procedural_history");
  const proceduralAfter = Number(
    (proceduralRows.filter((row) => row.top5Hit).length / Math.max(1, proceduralRows.length)).toFixed(4)
  );
  const baselineEvalById = new Map((r60_8Report?.taskEvaluations || []).map((row) => [String(row.queryId || ""), row]));
  const proceduralBeforeRows = (rewrittenTasks || [])
    .filter((row) => row.intent === "procedural_history")
    .map((row) => baselineEvalById.get(String(row.queryId || "")))
    .filter(Boolean);
  const proceduralBefore = Number(
    (
      proceduralBeforeRows.filter((row) => Boolean(row.bestDecisionHit)).length /
      Math.max(1, proceduralBeforeRows.length)
    ).toFixed(4)
  );

  const bestVariantTypeByIntent = Object.entries(
    (weightedEntries || []).reduce((acc, entry) => {
      const key = String(entry.intent || "unknown");
      if (!acc[key]) acc[key] = "weighted_query";
      return acc;
    }, {})
  )
    .map(([intent, variantType]) => ({ intent, variantType }))
    .sort((a, b) => a.intent.localeCompare(b.intent));

  const comparison = {
    versusR60_8: {
      emptyTasksBefore: toNumber(r60_8Report?.emptyTasksAfter, 0),
      emptyTasksAfter: stillEmptyTaskIds.length,
      top1Before: toNumber(r60_8Report?.top1DecisionHitRate, 0),
      top1After: rates.top1,
      top3Before: toNumber(r60_8Report?.top3DecisionHitRate, 0),
      top3After: rates.top3,
      top5Before: toNumber(r60_8Report?.top5DecisionHitRate, 0),
      top5After: rates.top5,
      sectionBefore: toNumber(r60_8Report?.sectionTypeHitRate, 0),
      sectionAfter: rates.sectionType
    },
    versusR60_7: {
      emptyClusterBefore: toNumber(r60_7Report?.clusterCounts?.empty_even_after_rewrite, 0),
      emptyAfterWeighting: stillEmptyTaskIds.length
    }
  };

  const tasksRecoveredByWeightingCount = tasksRecoveredByWeighting.length;
  let recommendedNextStep = "retain_weighted_templates_and_iterate_wrong_decision_disambiguation";
  if (tasksRecoveredByWeightingCount === 0) recommendedNextStep = "weighting_no_recovery_investigate_runtime_scope_or_indexing";
  else if (stillEmptyTaskIds.length >= toNumber(r60_8Report?.emptyTasksAfter, 0)) {
    recommendedNextStep = "weighting_partial_no_empty_reduction_refine_intent_templates_before_next_rerun";
  }

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.10",
    tasksEvaluated: taskRows.length,
    tasksRecoveredByWeightingCount,
    emptyTasksBefore: toNumber(r60_8Report?.emptyTasksAfter, 0),
    emptyTasksAfter: stillEmptyTaskIds.length,
    top1DecisionHitRate: rates.top1,
    top3DecisionHitRate: rates.top3,
    top5DecisionHitRate: rates.top5,
    sectionTypeHitRate: rates.sectionType,
    proceduralIntentHitRateBefore: proceduralBefore,
    proceduralIntentHitRateAfter: proceduralAfter,
    recoveredTaskIds: unique(tasksRecoveredByWeighting),
    stillEmptyTaskIds: unique(stillEmptyTaskIds),
    wrongDecisionTaskIds: unique(wrongDecisionTaskIds),
    bestVariantTypeByIntent,
    comparison,
    recommendedNextStep,
    weightedEntries,
    taskRows
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.10 Intent-Specific Query Weighting Templates (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- tasksEvaluated: ${report.tasksEvaluated}`);
  lines.push(`- tasksRecoveredByWeightingCount: ${report.tasksRecoveredByWeightingCount}`);
  lines.push(`- emptyTasksBefore: ${report.emptyTasksBefore}`);
  lines.push(`- emptyTasksAfter: ${report.emptyTasksAfter}`);
  lines.push(`- top1DecisionHitRate: ${report.top1DecisionHitRate}`);
  lines.push(`- top3DecisionHitRate: ${report.top3DecisionHitRate}`);
  lines.push(`- top5DecisionHitRate: ${report.top5DecisionHitRate}`);
  lines.push(`- sectionTypeHitRate: ${report.sectionTypeHitRate}`);
  lines.push(`- proceduralIntentHitRateBefore: ${report.proceduralIntentHitRateBefore}`);
  lines.push(`- proceduralIntentHitRateAfter: ${report.proceduralIntentHitRateAfter}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Comparison vs R60.8 / R60.7");
  lines.push(`- R60.8 empty before/after: ${report.comparison?.versusR60_8?.emptyTasksBefore} -> ${report.comparison?.versusR60_8?.emptyTasksAfter}`);
  lines.push(`- R60.8 top1 before/after: ${report.comparison?.versusR60_8?.top1Before} -> ${report.comparison?.versusR60_8?.top1After}`);
  lines.push(`- R60.8 top3 before/after: ${report.comparison?.versusR60_8?.top3Before} -> ${report.comparison?.versusR60_8?.top3After}`);
  lines.push(`- R60.8 top5 before/after: ${report.comparison?.versusR60_8?.top5Before} -> ${report.comparison?.versusR60_8?.top5After}`);
  lines.push(`- R60.8 section before/after: ${report.comparison?.versusR60_8?.sectionBefore} -> ${report.comparison?.versusR60_8?.sectionAfter}`);
  lines.push(`- R60.7 empty cluster before: ${report.comparison?.versusR60_7?.emptyClusterBefore}`);
  lines.push("");

  lines.push("## Best Variant Type By Intent");
  for (const row of report.bestVariantTypeByIntent || []) lines.push(`- ${row.intent}: ${row.variantType}`);
  if (!(report.bestVariantTypeByIntent || []).length) lines.push("- none");
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [tasks, normalizedPackages, r60_8Report, r60_7Report, evalReport] = await Promise.all([
    readJson(path.resolve(reportsDir, rewrittenTasksName)),
    readJson(path.resolve(reportsDir, normalizedName)),
    readJson(path.resolve(reportsDir, r60_8Name)),
    readJson(path.resolve(reportsDir, r60_7Name)),
    readJson(path.resolve(reportsDir, evalName))
  ]);

  const report = await buildR60_10QueryWeightingReport({
    rewrittenTasks: tasks,
    normalizedPackages,
    r60_8Report,
    r60_7Report,
    trustedDecisionIds: evalReport?.trustedCorpus?.trustedDocumentIds || [],
    apiBaseUrl: apiBase
  });

  const reportPath = path.resolve(reportsDir, outputReportName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  const weightedPath = path.resolve(reportsDir, outputWeightedName);
  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, toMarkdown(report)),
    fs.writeFile(weightedPath, JSON.stringify(report.weightedEntries, null, 2))
  ]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.tasksEvaluated,
        tasksRecoveredByWeightingCount: report.tasksRecoveredByWeightingCount,
        emptyTasksBefore: report.emptyTasksBefore,
        emptyTasksAfter: report.emptyTasksAfter,
        top1DecisionHitRate: report.top1DecisionHitRate,
        top3DecisionHitRate: report.top3DecisionHitRate,
        top5DecisionHitRate: report.top5DecisionHitRate,
        sectionTypeHitRate: report.sectionTypeHitRate,
        proceduralIntentHitRateBefore: report.proceduralIntentHitRateBefore,
        proceduralIntentHitRateAfter: report.proceduralIntentHitRateAfter,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R60.10 report written to ${reportPath}`);
  console.log(`R60.10 weighted query packages written to ${weightedPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
