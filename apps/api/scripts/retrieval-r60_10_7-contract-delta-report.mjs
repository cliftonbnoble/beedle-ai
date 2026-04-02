import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { BENCHMARK_DEFAULT_FILTERS, BENCHMARK_INTENT_TO_QUERY_TYPE } from "./retrieval-benchmark-contract-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const outputReportName = process.env.RETRIEVAL_R60_10_7_REPORT_NAME || "retrieval-r60_10_7-contract-delta-report.json";
const outputMdName = process.env.RETRIEVAL_R60_10_7_MARKDOWN_NAME || "retrieval-r60_10_7-contract-delta-report.md";

const CANONICAL_CONTRACT = {
  requestShape: {
    query: "string",
    queryTypeStrategy: "intent_mapped_via_shared_mapping",
    limit: "number",
    filters: BENCHMARK_DEFAULT_FILTERS
  },
  observedEndpointShape: {
    query: "string",
    queryType: "string",
    filters: "object"
  },
  parsedResultShape: {
    source: "shared_callBenchmarkDebug.parsedResults",
    rowFields: ["documentId", "chunkId", "title", "sectionLabel", "chunkType", "score", "diagnostics", "sourceLink", "citationAnchor"]
  },
  emptyClassificationShape: {
    source: "parsedResults_filtered_to_trusted",
    basis: "rows.length === 0"
  },
  scoringShape: {
    source: "parsedResults_filtered_to_trusted",
    basis: ["topK decision hit", "section-type hit", "rank position from same rows"]
  }
};

const SCRIPT_PROFILES = [
  {
    scriptId: "R60",
    requestShape: {
      query: "task.query",
      queryTypeStrategy: "citation_direct=>citation_lookup_else_keyword",
      limit: "task limit",
      filters: BENCHMARK_DEFAULT_FILTERS
    },
    observedEndpointShape: {
      source: "shared_callBenchmarkDebug_endpointInputsObserved_via_r60-goldset-eval-report"
    },
    parsedResultShape: {
      source: "shared_callBenchmarkDebug.parsedResults",
      rowProjection: ["documentId", "title", "chunkId", "chunkType(sectionLabel||chunkType)", "rankScore", "citationAnchor", "sourceLink"]
    },
    emptyClassificationShape: {
      source: "rows derived from parsedResults filtered to trusted",
      basis: "topResults length and firstExpectedRank"
    },
    scoringShape: {
      source: "same rows",
      basis: ["top1/top3/top5", "sectionTypeHit", "noisyChunkDominated", "minimumAcceptableRank"]
    }
  },
  {
    scriptId: "R60.8",
    requestShape: {
      query: "variant query",
      queryTypeStrategy: "intent_mapped_via_shared_mapping + variant override",
      limit: "script limit",
      filters: BENCHMARK_DEFAULT_FILTERS
    },
    observedEndpointShape: {
      source: "shared_callBenchmarkDebug_endpointInputsObserved"
    },
    parsedResultShape: {
      source: "shared_callBenchmarkDebug.parsedResults",
      rowProjection: ["topReturnedDecisionIds", "topReturnedSectionTypes", "returnedCount"]
    },
    emptyClassificationShape: {
      source: "best variant from parsedResults projections",
      basis: "bestReturnedCount === 0"
    },
    scoringShape: {
      source: "variantRows",
      basis: ["decisionHit*4 + sectionHit*2 + countComponent"]
    }
  },
  {
    scriptId: "R60.10",
    requestShape: {
      query: "weightedQuery",
      queryTypeStrategy: "intent_mapped_via_shared_mapping",
      limit: "script limit",
      filters: BENCHMARK_DEFAULT_FILTERS
    },
    observedEndpointShape: {
      source: "shared_callBenchmarkDebug_endpointInputsObserved"
    },
    parsedResultShape: {
      source: "shared_callBenchmarkDebug.parsedResults",
      rowProjection: ["returnedDecisionIds", "returnedSectionTypes", "top1/top3/top5/sectionType flags"]
    },
    emptyClassificationShape: {
      source: "parsedResults filtered to trusted",
      basis: "returnedDecisionIds.length === 0"
    },
    scoringShape: {
      source: "same returned rows",
      basis: ["top1/top3/top5", "sectionTypeHit", "proceduralIntentHitRate"]
    }
  },
  {
    scriptId: "R60.10.1",
    requestShape: {
      query: "normalizedQuery and weightedQuery",
      queryTypeStrategy: "intent_mapped_via_shared_mapping",
      limit: "script limit",
      filters: BENCHMARK_DEFAULT_FILTERS
    },
    observedEndpointShape: {
      source: "shared_callBenchmarkDebug_endpointInputsObserved"
    },
    parsedResultShape: {
      source: "shared_callBenchmarkDebug.parsedResults",
      rowProjection: ["rawResultCount", "parsedResultCount", "topReturnedDecisionIds", "topReturnedSectionTypes"]
    },
    emptyClassificationShape: {
      source: "paired normalized vs weighted parsed rows",
      basis: "parsedResultCount deltas"
    },
    scoringShape: {
      source: "paired row comparison",
      basis: ["reducedRaw", "reducedParsed", "changedEndpointBehavior", "changedMatchingOnly"]
    }
  }
];

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function mismatchesForProfile(profile) {
  const out = [];
  if (profile.requestShape.queryTypeStrategy !== CANONICAL_CONTRACT.requestShape.queryTypeStrategy) {
    if (profile.requestShape.queryTypeStrategy.includes("citation_direct=>citation_lookup_else_keyword")) {
      out.push("query_type_mapping_divergence");
      out.push("request_field_divergence");
    }
  }
  if (profile.parsedResultShape?.source !== CANONICAL_CONTRACT.parsedResultShape.source) {
    out.push("parsed_result_shape_divergence");
  }
  if (!String(profile.emptyClassificationShape?.source || "").includes("parsedResults")) {
    out.push("empty_classification_divergence");
  }
  if (profile.scriptId !== "R60" && String(profile.scoringShape?.source || "").includes("variantRows")) {
    out.push("scoring_input_divergence");
  }
  if (profile.scriptId === "R60.10.1" && String(profile.scoringShape?.source || "").includes("paired row comparison")) {
    out.push("scoring_input_divergence");
  }
  return unique(out);
}

export function buildR60_10_7ContractDeltaReport() {
  const rows = SCRIPT_PROFILES.map((profile) => {
    const mismatchesDetected = mismatchesForProfile(profile);
    return {
      ...profile,
      mismatchesDetected
    };
  });

  const divergentRows = rows.filter((row) => row.mismatchesDetected.length > 0);
  const mismatchLocations = divergentRows.flatMap((row) => row.mismatchesDetected.map((mismatch) => `${row.scriptId}:${mismatch}`));
  const exactFieldsStillDivergent = unique(divergentRows.flatMap((row) => row.mismatchesDetected));

  const recommendedFixOrder = [
    "R60:align_query_type_strategy_to_shared_intent_mapping",
    "R60.8:align_empty_classification_and_scoring_inputs_to_shared_scoring_basis",
    "R60.10.1:align_delta_classification_inputs_to_shared_scoring_rows"
  ];

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R60.10.7",
    canonicalContract: CANONICAL_CONTRACT,
    scriptRows: rows,
    remainingMismatchCount: mismatchLocations.length,
    remainingMismatchLocations: unique(mismatchLocations),
    scriptsFullyAlignedCount: rows.length - divergentRows.length,
    scriptsStillDivergentCount: divergentRows.length,
    exactFieldsStillDivergent,
    recommendedFixOrder,
    contractMismatchResolved: mismatchLocations.length === 0,
    recommendedNextStep:
      mismatchLocations.length === 0
        ? "contract_fully_aligned_rerun_benchmark_suite"
        : "apply_fix_order_then_rerun_r60_10_6_and_r60_series"
  };
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60.10.7 Contract Delta Reconciliation (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  lines.push(`- remainingMismatchCount: ${report.remainingMismatchCount}`);
  lines.push(`- scriptsFullyAlignedCount: ${report.scriptsFullyAlignedCount}`);
  lines.push(`- scriptsStillDivergentCount: ${report.scriptsStillDivergentCount}`);
  lines.push(`- exactFieldsStillDivergent: ${report.exactFieldsStillDivergent.join(", ") || "<none>"}`);
  lines.push(`- contractMismatchResolved: ${report.contractMismatchResolved}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");
  lines.push("## Per Script");
  for (const row of report.scriptRows || []) {
    lines.push(`- ${row.scriptId}: mismatches=${row.mismatchesDetected.join(", ") || "<none>"}`);
  }
  lines.push("");
  lines.push("## Recommended Fix Order");
  for (const step of report.recommendedFixOrder || []) lines.push(`- ${step}`);
  lines.push("");
  lines.push("- Dry-run only. No activation/rollback writes, runtime mutation, or gate changes.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const report = buildR60_10_7ContractDeltaReport();
  const reportPath = path.resolve(reportsDir, outputReportName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(reportPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        remainingMismatchCount: report.remainingMismatchCount,
        scriptsFullyAlignedCount: report.scriptsFullyAlignedCount,
        scriptsStillDivergentCount: report.scriptsStillDivergentCount,
        exactFieldsStillDivergent: report.exactFieldsStillDivergent,
        contractMismatchResolved: report.contractMismatchResolved,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R60.10.7 report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
