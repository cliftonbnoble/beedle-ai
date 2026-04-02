import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const repairedTasksName = process.env.RETRIEVAL_R60_4_TASKS_NAME || "retrieval-r60_2-goldset-repaired.json";
const r60EvalName = process.env.RETRIEVAL_R60_4_EVAL_NAME || "retrieval-r60-goldset-eval-report.json";
const outputJsonName = process.env.RETRIEVAL_R60_4_REPORT_NAME || "retrieval-r60_4-query-relaxation-report.json";
const outputMdName = process.env.RETRIEVAL_R60_4_MARKDOWN_NAME || "retrieval-r60_4-query-relaxation-report.md";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number.parseInt(process.env.RETRIEVAL_R60_4_LIMIT || "10", 10);

const STOPWORDS = new Set(["the", "and", "for", "with", "from", "into", "prior", "decision", "decisions", "regarding", "of", "to", "in"]);

const INTENT_TO_QUERY_TYPE = {
  authority_lookup: "rules_ordinance",
  findings: "keyword",
  procedural_history: "keyword",
  issue_holding_disposition: "keyword",
  analysis_reasoning: "keyword",
  comparative_reasoning: "keyword",
  citation_direct: "citation_lookup"
};

const INTENT_SECTION_QUERY = {
  authority_lookup: "authority discussion legal standard ordinance rule",
  findings: "findings of fact credibility evidence",
  procedural_history: "procedural history hearing notice continuance",
  issue_holding_disposition: "issue holding disposition order",
  analysis_reasoning: "analysis reasoning application legal standard",
  comparative_reasoning: "compare prior decisions analysis reasoning",
  citation_direct: "rule ordinance citation authority section"
};

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
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

function normalizeSectionType(sectionType) {
  const s = String(sectionType || "");
  const lower = s.toLowerCase();
  if (lower === "analysis" || lower === "body") return "analysis_reasoning";
  if (lower === "order") return "holding_disposition";
  return lower.replace(/\s+/g, "_");
}

function tokenize(query) {
  return String(query || "")
    .toLowerCase()
    .replace(/[^a-z0-9.\s]/g, " ")
    .split(/\s+/)
    .filter(Boolean);
}

function buildVariants(task) {
  const original = String(task?.query || "").trim();
  const intent = String(task?.intent || "");
  const tokens = tokenize(original);
  const contentTokens = tokens.filter((t) => !STOPWORDS.has(t));
  const keywordCompressed = unique(contentTokens).slice(0, 5).join(" ");
  const simplifiedLegal = unique(contentTokens.filter((t) => /ordinance|rule|standard|analysis|findings|holding|procedural|evidence|citation/.test(t))).slice(0, 6).join(" ");

  const citationMatches = original.match(/(?:rule|ordinance)\s*\d+(?:\.\d+)?/gi) || [];
  const citationFocused = unique(citationMatches).join(" ");

  const variants = [
    {
      variantType: "original_query",
      query: original,
      queryType: INTENT_TO_QUERY_TYPE[intent] || "keyword"
    },
    {
      variantType: "simplified_legal_phrase_query",
      query: simplifiedLegal || keywordCompressed || original,
      queryType: INTENT_TO_QUERY_TYPE[intent] || "keyword"
    },
    {
      variantType: "citation_focused_query",
      query: citationFocused || (intent === "citation_direct" ? keywordCompressed || original : `${original} citation`),
      queryType: "citation_lookup"
    },
    {
      variantType: "keyword_compressed_query",
      query: keywordCompressed || original,
      queryType: "keyword"
    },
    {
      variantType: "section_intent_query",
      query: INTENT_SECTION_QUERY[intent] || `${intent} analysis`,
      queryType: INTENT_TO_QUERY_TYPE[intent] || "keyword"
    }
  ];

  const dedup = [];
  const seen = new Set();
  for (const variant of variants) {
    const key = `${variant.variantType}::${variant.queryType}::${variant.query}`;
    if (seen.has(key)) continue;
    seen.add(key);
    dedup.push(variant);
  }
  return dedup;
}

async function fetchDebug({ task, variant, apiBaseUrl }) {
  const payload = {
    query: variant.query,
    queryType: variant.queryType,
    limit,
    filters: {
      approvedOnly: true,
      fileType: "decision_docx"
    }
  };
  const response = await fetch(`${apiBaseUrl}/admin/retrieval/debug`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  const raw = await response.text();
  let json;
  try {
    json = JSON.parse(raw);
  } catch {
    return { ok: false, status: response.status, results: [], error: "non_json_response" };
  }
  if (!response.ok) return { ok: false, status: response.status, results: [], error: "http_error" };
  return { ok: true, status: response.status, results: json.results || [] };
}

function inferStrictnessFailureMode(taskRow) {
  const variantCounts = Object.values(taskRow.returnedCountByVariant || {});
  const anyResults = variantCounts.some((count) => Number(count || 0) > 0);
  const originalCount = Number(taskRow.returnedCountByVariant?.original_query || 0);
  if (!anyResults) return "all_variants_empty_runtime_scope_or_index_gap";
  if (originalCount === 0 && taskRow.firstVariantThatReturnsResults && taskRow.firstVariantThatReturnsResults !== "original_query") {
    return "original_query_too_strict_recovered_by_relaxation";
  }
  if (!taskRow.expectedDecisionRecoveredByAnyVariant) return "expected_decision_not_recovered_despite_results";
  return "no_strictness_issue_detected";
}

export async function buildR60_4QueryRelaxationReport({
  repairedTasks,
  trustedDecisionIds,
  apiBaseUrl,
  fetchFn = fetchDebug
}) {
  const trustedSet = new Set((trustedDecisionIds || []).map(String));
  const perTaskRows = [];

  for (const task of repairedTasks || []) {
    const variants = buildVariants(task);
    const returnedCountByVariant = {};
    const topReturnedDecisionIdsByVariant = {};
    const topReturnedSectionTypesByVariant = {};

    for (const variant of variants) {
      let runtime;
      try {
        runtime = await fetchFn({ task, variant, apiBaseUrl });
      } catch {
        runtime = { ok: false, status: 0, results: [], error: "fetch_failed" };
      }
      const filtered = (runtime.results || []).filter((row) => trustedSet.has(String(row?.documentId || "")));
      returnedCountByVariant[variant.variantType] = filtered.length;
      topReturnedDecisionIdsByVariant[variant.variantType] = unique(filtered.slice(0, 5).map((row) => row.documentId));
      topReturnedSectionTypesByVariant[variant.variantType] = unique(
        filtered.slice(0, 5).map((row) => normalizeSectionType(row.sectionLabel || row.chunkType || ""))
      );
    }

    const firstVariant = variants.find((v) => Number(returnedCountByVariant[v.variantType] || 0) > 0);
    const expectedDecisionSet = new Set((task.expectedDecisionIds || []).map(String));
    const expectedDecisionRecoveredByAnyVariant = Object.values(topReturnedDecisionIdsByVariant).some((ids) =>
      (ids || []).some((id) => expectedDecisionSet.has(String(id)))
    );

    const row = {
      queryId: String(task.queryId || ""),
      originalQuery: String(task.query || ""),
      intent: String(task.intent || ""),
      queryVariants: variants,
      firstVariantThatReturnsResults: firstVariant ? firstVariant.variantType : "",
      returnedCountByVariant,
      topReturnedDecisionIdsByVariant,
      topReturnedSectionTypesByVariant,
      expectedDecisionIds: (task.expectedDecisionIds || []).map(String),
      expectedSectionTypes: (task.expectedSectionTypes || []).map(String),
      expectedDecisionRecoveredByAnyVariant
    };
    row.likelyStrictnessFailureMode = inferStrictnessFailureMode(row);
    perTaskRows.push(row);
  }

  const tasksRecoveredByRelaxationCount = perTaskRows.filter(
    (row) => Number(row.returnedCountByVariant?.original_query || 0) === 0 && row.firstVariantThatReturnsResults
  ).length;
  const tasksStillEmptyAfterAllVariantsCount = perTaskRows.filter((row) =>
    Object.values(row.returnedCountByVariant || {}).every((count) => Number(count || 0) === 0)
  ).length;

  const bestPerformingVariantTypes = Object.entries(
    countBy(
      perTaskRows
        .map((row) => row.firstVariantThatReturnsResults)
        .filter(Boolean)
    )
  )
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    })
    .map(([variantType, recoveredTasks]) => ({ variantType, recoveredTasks }));

  const strictnessFailureModes = Object.entries(countBy(perTaskRows.map((row) => row.likelyStrictnessFailureMode)))
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    })
    .map(([failureMode, count]) => ({ failureMode, count }));

  const candidateBenchmarkRewrites = perTaskRows
    .map((row) => {
      const variant = (row.queryVariants || []).find((v) => v.variantType === row.firstVariantThatReturnsResults);
      if (!variant || variant.variantType === "original_query") return null;
      return {
        queryId: row.queryId,
        originalQuery: row.originalQuery,
        recommendedQuery: variant.query,
        recommendedVariantType: variant.variantType,
        expectedDecisionRecoveredByAnyVariant: row.expectedDecisionRecoveredByAnyVariant
      };
    })
    .filter(Boolean)
    .sort((a, b) => String(a.queryId).localeCompare(String(b.queryId)));

  let overallRecommendation = "no_issue_detected";
  if (tasksStillEmptyAfterAllVariantsCount === perTaskRows.length) {
    overallRecommendation = "runtime_scope_or_endpoint_investigation_required_before_benchmark_use";
  } else if (tasksRecoveredByRelaxationCount > 0) {
    overallRecommendation = "adopt_relaxed_variant_rewrites_and_rerun_r60_goldset_eval";
  } else {
    overallRecommendation = "partial_recovery_review_query_design_and_expected_mapping";
  }

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.4",
    summary: {
      tasksEvaluated: perTaskRows.length,
      trustedDecisionCount: trustedSet.size
    },
    tasksRecoveredByRelaxationCount,
    tasksStillEmptyAfterAllVariantsCount,
    bestPerformingVariantTypes,
    strictnessFailureModes,
    candidateBenchmarkRewrites,
    overallRecommendation,
    perTaskRows: perTaskRows.sort((a, b) => String(a.queryId).localeCompare(String(b.queryId)))
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.4 Query Relaxation & Runtime Sanity Calibration (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push(`- tasksRecoveredByRelaxationCount: ${report.tasksRecoveredByRelaxationCount}`);
  lines.push(`- tasksStillEmptyAfterAllVariantsCount: ${report.tasksStillEmptyAfterAllVariantsCount}`);
  lines.push(`- overallRecommendation: ${report.overallRecommendation}`);
  lines.push("");

  lines.push("## Best Performing Variant Types");
  for (const row of report.bestPerformingVariantTypes || []) lines.push(`- ${row.variantType}: ${row.recoveredTasks}`);
  if (!(report.bestPerformingVariantTypes || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Strictness Failure Modes");
  for (const row of report.strictnessFailureModes || []) lines.push(`- ${row.failureMode}: ${row.count}`);
  if (!(report.strictnessFailureModes || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Candidate Benchmark Rewrites");
  for (const row of report.candidateBenchmarkRewrites || []) {
    lines.push(
      `- ${row.queryId}: ${row.recommendedVariantType} -> "${row.recommendedQuery}" | expectedRecovered=${row.expectedDecisionRecoveredByAnyVariant}`
    );
  }
  if (!(report.candidateBenchmarkRewrites || []).length) lines.push("- none");
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes or runtime mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [repairedTasks, evalReport] = await Promise.all([
    readJson(path.resolve(reportsDir, repairedTasksName)),
    readJson(path.resolve(reportsDir, r60EvalName))
  ]);

  const report = await buildR60_4QueryRelaxationReport({
    repairedTasks,
    trustedDecisionIds: evalReport?.trustedCorpus?.trustedDocumentIds || [],
    apiBaseUrl: apiBase
  });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.summary.tasksEvaluated,
        tasksRecoveredByRelaxationCount: report.tasksRecoveredByRelaxationCount,
        tasksStillEmptyAfterAllVariantsCount: report.tasksStillEmptyAfterAllVariantsCount,
        overallRecommendation: report.overallRecommendation
      },
      null,
      2
    )
  );
  console.log(`R60.4 query relaxation report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
