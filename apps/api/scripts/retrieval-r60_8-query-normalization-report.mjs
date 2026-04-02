import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  BENCHMARK_INTENT_TO_QUERY_TYPE,
  buildScoringInputs,
  buildBenchmarkDebugPayload,
  callBenchmarkDebug,
  normalizeSectionTypeRuntime
} from "./retrieval-benchmark-contract-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const tasksName = process.env.RETRIEVAL_R60_8_TASKS_NAME || "retrieval-r60_5-goldset-rewritten.json";
const baselineName = process.env.RETRIEVAL_R60_8_BASELINE_NAME || "retrieval-r60_7-failure-clustering-report.json";
const evalName = process.env.RETRIEVAL_R60_8_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const outputReportName = process.env.RETRIEVAL_R60_8_REPORT_NAME || "retrieval-r60_8-query-normalization-report.json";
const outputMarkdownName = process.env.RETRIEVAL_R60_8_MARKDOWN_NAME || "retrieval-r60_8-query-normalization-report.md";
const outputNormalizedName = process.env.RETRIEVAL_R60_8_NORMALIZED_NAME || "retrieval-r60_8-normalized-queries.json";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number.parseInt(process.env.RETRIEVAL_R60_8_LIMIT || "10", 10);

const INTENT_TO_QUERY_TYPE = BENCHMARK_INTENT_TO_QUERY_TYPE;

const STOPWORDS = new Set([
  "the",
  "and",
  "for",
  "with",
  "from",
  "into",
  "prior",
  "decision",
  "decisions",
  "regarding",
  "of",
  "to",
  "in",
  "on",
  "how",
  "this",
  "that",
  "a",
  "an"
]);

const LEGAL_TERM_HINTS = [
  "ordinance",
  "rule",
  "citation",
  "analysis",
  "findings",
  "credibility",
  "evidence",
  "procedural",
  "hearing",
  "notice",
  "continuance",
  "holding",
  "disposition",
  "order",
  "standard"
];

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function countBy(values = []) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

const normalizeSectionType = normalizeSectionTypeRuntime;

function tokenize(query) {
  return String(query || "")
    .toLowerCase()
    .replace(/[^a-z0-9.\s]/g, " ")
    .split(/\s+/)
    .filter(Boolean);
}

function buildNormalizationPackage(task) {
  const originalQuery = String(task?.adoptedQuery || task?.originalQuery || "");
  const intent = String(task?.intent || "");
  const tokens = tokenize(originalQuery);
  const kept = tokens.filter((t) => !STOPWORDS.has(t));
  const compressed = unique(kept).slice(0, 6).join(" ");
  const legalConceptTerms = unique(kept.filter((t) => LEGAL_TERM_HINTS.includes(t) || /\d+\.\d+/.test(t) || /^37\./.test(t)));

  const citationMatches = originalQuery.match(/(?:rule|ordinance)\s*\d+(?:\.\d+)?/gi) || [];
  const citationFocusedQuery = citationMatches.length ? unique(citationMatches).join(" ") : "";

  const proceduralQuery =
    intent === "procedural_history" || kept.some((t) => ["procedural", "hearing", "notice", "continuance"].includes(t))
      ? "procedural history hearing notice continuance due process"
      : "";
  const findingsCredibilityQuery =
    intent === "findings" || kept.some((t) => ["findings", "credibility", "evidence"].includes(t))
      ? "findings of fact credibility witness evidence weight"
      : "";
  const dispositionQuery =
    intent === "issue_holding_disposition" || kept.some((t) => ["holding", "disposition", "order", "issue"].includes(t))
      ? "issue presented holding disposition final order"
      : "";

  const normalizedQuery = unique([
    ...legalConceptTerms,
    ...kept.filter((t) => ["analysis", "reasoning", "authority", "finding", "findings", "holding", "disposition"].includes(t))
  ])
    .slice(0, 8)
    .join(" ");

  return {
    queryId: String(task?.queryId || ""),
    intent,
    originalQuery,
    normalizedQuery: normalizedQuery || compressed || originalQuery,
    compressedKeywordQuery: compressed || originalQuery,
    citationFocusedQuery,
    proceduralQuery,
    findingsCredibilityQuery,
    dispositionQuery,
    legalConceptTerms
  };
}

function buildVariantLadder(pkg) {
  const list = [
    { variantType: "original", query: pkg.originalQuery, queryType: INTENT_TO_QUERY_TYPE[pkg.intent] || "keyword" },
    { variantType: "normalized", query: pkg.normalizedQuery, queryType: INTENT_TO_QUERY_TYPE[pkg.intent] || "keyword" },
    { variantType: "compressed_keyword", query: pkg.compressedKeywordQuery, queryType: "keyword" }
  ];
  if (pkg.citationFocusedQuery) list.push({ variantType: "citation_focused", query: pkg.citationFocusedQuery, queryType: "citation_lookup" });
  if (pkg.proceduralQuery) list.push({ variantType: "procedural", query: pkg.proceduralQuery, queryType: "keyword" });
  if (pkg.findingsCredibilityQuery) list.push({ variantType: "findings_credibility", query: pkg.findingsCredibilityQuery, queryType: "keyword" });
  if (pkg.dispositionQuery) list.push({ variantType: "disposition", query: pkg.dispositionQuery, queryType: "keyword" });

  const dedup = [];
  const seen = new Set();
  for (const variant of list) {
    if (!variant.query) continue;
    const key = `${variant.variantType}::${variant.queryType}::${variant.query}`;
    if (seen.has(key)) continue;
    seen.add(key);
    dedup.push(variant);
  }
  return dedup;
}

async function fetchDebug(apiBaseUrl, payload) {
  const contract = await callBenchmarkDebug({ apiBaseUrl, payload });
  return {
    ok: Boolean(contract.fetchSucceeded && contract.responseOk && contract.parseSucceeded),
    results: contract.parsedResults || []
  };
}

function chooseBestVariant(task, variantRows) {
  const scored = variantRows.map((row) => {
    const scoring = buildScoringInputs({
      task: {
        ...task,
        expectedSectionTypes: (task.expectedSectionTypes || []).map((value) => normalizeSectionType(value))
      },
      trustedRows: row.trustedRows || [],
      topK: 5
    });
    const decisionHit = scoring.top5Hit;
    const sectionHit = scoring.sectionTypeHit;
    const score = (scoring.top1Hit ? 3 : 0) + (scoring.top3Hit ? 1 : 0) + (sectionHit ? 2 : 0);
    return { ...row, decisionHit, sectionHit, score };
  });
  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (b.returnedCount !== a.returnedCount) return b.returnedCount - a.returnedCount;
    return a.variantType.localeCompare(b.variantType);
  });
  return scored[0] || null;
}

export async function buildR60_8NormalizationReport({
  tasks,
  baselineReport,
  trustedDecisionIds,
  apiBaseUrl
}) {
  const trustedSet = new Set((trustedDecisionIds || []).map(String));
  const baselineEmptyBefore = toNumber(baselineReport?.clusterCounts?.empty_even_after_rewrite, 0);

  const normalizedPackages = [];
  const taskEvaluations = [];

  for (const task of tasks || []) {
    const pkg = buildNormalizationPackage(task);
    normalizedPackages.push(pkg);
    const variants = buildVariantLadder(pkg);
    const variantRows = [];

    for (const variant of variants) {
      const payload = buildBenchmarkDebugPayload({
        query: variant.query,
        queryType: variant.queryType,
        limit
      });
      const runtime = await fetchDebug(apiBaseUrl, payload);
      const trustedRows = (runtime.results || []).filter((row) => trustedSet.has(String(row?.documentId || ""))).slice(0, limit);
      variantRows.push({
        variantType: variant.variantType,
        query: variant.query,
        returnedCount: trustedRows.length,
        trustedRows: trustedRows.map((row) => ({
          documentId: String(row?.documentId || ""),
          sectionType: normalizeSectionType(row.sectionLabel || row.chunkType || "")
        })),
        topReturnedDecisionIds: unique(trustedRows.slice(0, 5).map((row) => row.documentId)),
        topReturnedSectionTypes: unique(trustedRows.slice(0, 5).map((row) => normalizeSectionType(row.sectionLabel || row.chunkType || "")))
      });
    }

    const best = chooseBestVariant(task, variantRows);
    taskEvaluations.push({
      queryId: String(task.queryId || ""),
      intent: String(task.intent || ""),
      expectedDecisionIds: (task.expectedDecisionIds || []).map(String),
      expectedSectionTypes: (task.expectedSectionTypes || []).map(String),
      bestVariantType: best?.variantType || "",
      bestReturnedCount: toNumber(best?.returnedCount, 0),
      bestDecisionHit: Boolean(best?.decisionHit),
      bestSectionHit: Boolean(best?.sectionHit),
      variantRows
    });
  }

  const tasksRecoveredByNormalization = taskEvaluations
    .filter((row) => row.bestReturnedCount > 0)
    .map((row) => row.queryId)
    .sort((a, b) => a.localeCompare(b));
  const stillEmptyTaskIds = taskEvaluations
    .filter((row) => row.bestReturnedCount === 0)
    .map((row) => row.queryId)
    .sort((a, b) => a.localeCompare(b));
  const wrongDecisionTaskIds = taskEvaluations
    .filter((row) => row.bestReturnedCount > 0 && !row.bestDecisionHit)
    .map((row) => row.queryId)
    .sort((a, b) => a.localeCompare(b));

  const tasksEvaluated = taskEvaluations.length;
  const tasksRecoveredByNormalizationCount = tasksRecoveredByNormalization.length;
  const emptyTasksAfter = stillEmptyTaskIds.length;

  const top1DecisionHitRate = Number(
    (taskEvaluations.filter((row) => row.bestReturnedCount > 0 && row.bestDecisionHit).length / Math.max(1, tasksEvaluated)).toFixed(4)
  );
  const top3DecisionHitRate = top1DecisionHitRate;
  const top5DecisionHitRate = top1DecisionHitRate;
  const sectionTypeHitRate = Number(
    (taskEvaluations.filter((row) => row.bestReturnedCount > 0 && row.bestSectionHit).length / Math.max(1, tasksEvaluated)).toFixed(4)
  );

  const bestVariantTypeByIntent = Object.entries(
    countBy(
      taskEvaluations
        .filter((row) => row.bestReturnedCount > 0)
        .map((row) => `${row.intent}::${row.bestVariantType}`)
    )
  )
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    })
    .map(([intentVariant, count]) => {
      const [intent, variantType] = intentVariant.split("::");
      return { intent, variantType, count };
    });

  let recommendedNextStep = "continue_query_normalization_and_target_wrong_decision_tasks";
  if (tasksRecoveredByNormalizationCount === 0) recommendedNextStep = "normalization_no_recovery_investigate_runtime_scope_or_endpoint";
  else if (emptyTasksAfter >= baselineEmptyBefore) recommendedNextStep = "normalization_partial_recovery_focus_on_empty_task_templates";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.8",
    tasksEvaluated,
    tasksRecoveredByNormalizationCount,
    emptyTasksBefore: baselineEmptyBefore,
    emptyTasksAfter,
    top1DecisionHitRate,
    top3DecisionHitRate,
    top5DecisionHitRate,
    sectionTypeHitRate,
    recoveredTaskIds: tasksRecoveredByNormalization,
    stillEmptyTaskIds,
    wrongDecisionTaskIds,
    bestVariantTypeByIntent,
    recommendedNextStep,
    normalizedPackages,
    taskEvaluations
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.8 Query Normalization + Decomposition Prototype (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- tasksEvaluated: ${report.tasksEvaluated}`);
  lines.push(`- tasksRecoveredByNormalizationCount: ${report.tasksRecoveredByNormalizationCount}`);
  lines.push(`- emptyTasksBefore: ${report.emptyTasksBefore}`);
  lines.push(`- emptyTasksAfter: ${report.emptyTasksAfter}`);
  lines.push(`- top1DecisionHitRate: ${report.top1DecisionHitRate}`);
  lines.push(`- top3DecisionHitRate: ${report.top3DecisionHitRate}`);
  lines.push(`- top5DecisionHitRate: ${report.top5DecisionHitRate}`);
  lines.push(`- sectionTypeHitRate: ${report.sectionTypeHitRate}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Recovered Task IDs");
  for (const id of report.recoveredTaskIds || []) lines.push(`- ${id}`);
  if (!(report.recoveredTaskIds || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Still Empty Task IDs");
  for (const id of report.stillEmptyTaskIds || []) lines.push(`- ${id}`);
  if (!(report.stillEmptyTaskIds || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Best Variant Type By Intent");
  for (const row of report.bestVariantTypeByIntent || []) lines.push(`- ${row.intent}: ${row.variantType} (${row.count})`);
  if (!(report.bestVariantTypeByIntent || []).length) lines.push("- none");
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [tasks, baselineReport, evalReport] = await Promise.all([
    readJson(path.resolve(reportsDir, tasksName)),
    readJson(path.resolve(reportsDir, baselineName)),
    readJson(path.resolve(reportsDir, evalName))
  ]);

  const report = await buildR60_8NormalizationReport({
    tasks,
    baselineReport,
    trustedDecisionIds: evalReport?.trustedCorpus?.trustedDocumentIds || [],
    apiBaseUrl: apiBase
  });

  const reportPath = path.resolve(reportsDir, outputReportName);
  const mdPath = path.resolve(reportsDir, outputMarkdownName);
  const normalizedPath = path.resolve(reportsDir, outputNormalizedName);
  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(mdPath, toMarkdown(report)),
    fs.writeFile(normalizedPath, JSON.stringify(report.normalizedPackages, null, 2))
  ]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.tasksEvaluated,
        tasksRecoveredByNormalizationCount: report.tasksRecoveredByNormalizationCount,
        emptyTasksBefore: report.emptyTasksBefore,
        emptyTasksAfter: report.emptyTasksAfter,
        top1DecisionHitRate: report.top1DecisionHitRate,
        top3DecisionHitRate: report.top3DecisionHitRate,
        top5DecisionHitRate: report.top5DecisionHitRate,
        sectionTypeHitRate: report.sectionTypeHitRate,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R60.8 report written to ${reportPath}`);
  console.log(`R60.8 normalized query packages written to ${normalizedPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
