import fs from "node:fs/promises";
import path from "node:path";
import {
  checkHealth,
  defaultDbPath,
  ensureDir,
  formatTimestamp,
  queryCorpusSnapshot,
  readJson,
  runNodeScript,
  writeJson,
  writeText
} from "./lib/overnight-corpus-lift-utils.mjs";

const apiBaseUrl = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.CORPUS_LIFT_LOOP_BUSY_TIMEOUT_MS || "5000", 10);
const rounds = Math.max(1, Number.parseInt(process.env.CORPUS_LIFT_LOOP_ROUNDS || "24", 10));
const sleepMs = Math.max(0, Number.parseInt(process.env.CORPUS_LIFT_LOOP_SLEEP_MS || "300000", 10));
const stopAfterNoProgressRounds = Math.max(1, Number.parseInt(process.env.CORPUS_LIFT_LOOP_STOP_AFTER_NO_PROGRESS || "3", 10));
const reprocessLimit = Math.max(1, Number.parseInt(process.env.CORPUS_LIFT_LOOP_REPROCESS_LIMIT || "100", 10));
const inferenceLimit = Math.max(1, Number.parseInt(process.env.CORPUS_LIFT_LOOP_INFERENCE_LIMIT || "100", 10));
const tailLimit = Math.max(1, Number.parseInt(process.env.CORPUS_LIFT_LOOP_TAIL_LIMIT || "100", 10));
const staleQcLimit = Math.max(1, Number.parseInt(process.env.CORPUS_LIFT_LOOP_STALE_QC_LIMIT || "100", 10));
const companionMetadataLimit = Math.max(1, Number.parseInt(process.env.CORPUS_LIFT_LOOP_COMPANION_LIMIT || "100", 10));
const enableLimit = Math.max(1, Number.parseInt(process.env.CORPUS_LIFT_LOOP_ENABLE_LIMIT || "100", 10));
const singleContextEnableLimit = Math.max(
  1,
  Number.parseInt(process.env.CORPUS_LIFT_LOOP_SINGLE_CONTEXT_ENABLE_LIMIT || String(enableLimit), 10)
);
const decisionLikeEnableLimit = Math.max(
  1,
  Number.parseInt(process.env.CORPUS_LIFT_LOOP_DECISION_LIKE_ENABLE_LIMIT || String(enableLimit), 10)
);
const retrievalLimit = Math.max(1, Number.parseInt(process.env.CORPUS_LIFT_LOOP_RETRIEVAL_LIMIT || "100", 10));
const retrievalOrder = String(
  process.env.CORPUS_LIFT_LOOP_RETRIEVAL_ORDER || "coverage_rich_decision_like_searchable_asc"
);
const outputBaseDir = path.resolve(
  process.cwd(),
  process.env.CORPUS_LIFT_LOOP_OUTPUT_DIR || "reports/corpus-lift-slow-loop"
);

function scriptPath(name) {
  return path.resolve(process.cwd(), "scripts", name);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function progressDelta(before, after) {
  return {
    searchableDelta: Number(after?.searchableDecisionDocs || 0) - Number(before?.searchableDecisionDocs || 0),
    activeRetrievalDelta: Number(after?.activeRetrievalDecisionCount || 0) - Number(before?.activeRetrievalDecisionCount || 0),
    missingIndexOnlyDelta: Number(after?.missingIndexOnlyCount || 0) - Number(before?.missingIndexOnlyCount || 0)
  };
}

function hasMeaningfulProgress(delta) {
  return delta.searchableDelta > 0 || delta.activeRetrievalDelta > 0 || delta.missingIndexOnlyDelta < 0;
}

function formatSummaryMarkdown(report) {
  const lines = [
    "# Corpus Lift Slow Loop",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Run status: \`${report.runStatus}\``,
    `- Planned rounds: \`${report.plannedRounds}\``,
    `- Completed rounds: \`${report.completedRounds}\``,
    `- Sleep ms: \`${report.sleepMs}\``,
    ""
  ];

  lines.push("## Starting Snapshot");
  for (const [key, value] of Object.entries(report.startSnapshot || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Ending Snapshot");
  for (const [key, value] of Object.entries(report.endSnapshot || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Rounds");
  for (const round of report.rounds || []) {
    lines.push(
      `- round ${round.round}: status=\`${round.runStatus}\` searchableDelta=\`${round.delta.searchableDelta}\` activeRetrievalDelta=\`${round.delta.activeRetrievalDelta}\` missingIndexOnlyDelta=\`${round.delta.missingIndexOnlyDelta}\``
    );
  }
  if (!(report.rounds || []).length) lines.push("- none");
  lines.push("");

  if (report.error) {
    lines.push("## Error");
    lines.push(`- ${report.error}`);
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function readLatestStageFile(dirPath, suffix) {
  const names = (await fs.readdir(dirPath)).filter((name) => name.endsWith(suffix)).sort();
  if (!names.length) return null;
  return readJson(path.resolve(dirPath, names[names.length - 1]));
}

async function buildSearchabilityStageSummary(stageDir, beforeSnapshot, afterSnapshot) {
  const latestCandidate = await readLatestStageFile(stageDir, "-candidates.json");
  const latestEnable = await readLatestStageFile(stageDir, "-enable.json");
  const candidateCount = Number(latestCandidate?.body?.summary?.candidateCount || 0);
  const enabledCount = Number(latestEnable?.body?.summary?.enabledCount || 0);
  const mode = String(latestCandidate?.body?.summary?.mode || latestEnable?.body?.summary?.mode || "qcPassed");

  return {
    stageStatus: enabledCount > 0 ? "written" : candidateCount > 0 ? "no_effect" : "noop",
    mode,
    candidateCount,
    enabledCount,
    searchableBefore: beforeSnapshot.searchableDecisionDocs,
    searchableAfter: afterSnapshot.searchableDecisionDocs,
    delta: enabledCount,
    reportPath: stageDir
  };
}

async function runRound(roundDir, roundNumber) {
  await ensureDir(roundDir);
  const beforeSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const shouldRunMissingIndexStages = Number(beforeSnapshot.missingIndexOnlyCount || 0) > 0;

  const searchabilityEnv = {
    API_BASE_URL: apiBaseUrl,
    SEARCHABILITY_ENABLE_LIMIT: String(enableLimit),
    SEARCHABILITY_ENABLE_MAX_ROUNDS: "1",
    SEARCHABILITY_ENABLE_REAL_ONLY: "1",
    SEARCHABILITY_ENABLE_MODE: "qcPassed",
    SEARCHABILITY_ENABLE_SLEEP_MS: "0",
    SEARCHABILITY_ENABLE_OUTPUT_DIR: path.resolve(roundDir, "searchability-enable")
  };
  const missingIndexSearchabilityEnv = {
    API_BASE_URL: apiBaseUrl,
    SEARCHABILITY_ENABLE_LIMIT: String(enableLimit),
    SEARCHABILITY_ENABLE_MAX_ROUNDS: "1",
    SEARCHABILITY_ENABLE_REAL_ONLY: "1",
    SEARCHABILITY_ENABLE_MODE: "missingIndexOnlyTextReady",
    SEARCHABILITY_ENABLE_SLEEP_MS: "0",
    SEARCHABILITY_ENABLE_OUTPUT_DIR: path.resolve(roundDir, "searchability-enable-missing-index-only")
  };
  const singleContextSearchabilityEnv = {
    API_BASE_URL: apiBaseUrl,
    SEARCHABILITY_ENABLE_LIMIT: String(singleContextEnableLimit),
    SEARCHABILITY_ENABLE_MAX_ROUNDS: "1",
    SEARCHABILITY_ENABLE_REAL_ONLY: "1",
    SEARCHABILITY_ENABLE_MODE: "singleContextTextReady",
    SEARCHABILITY_ENABLE_SLEEP_MS: "0",
    SEARCHABILITY_ENABLE_OUTPUT_DIR: path.resolve(roundDir, "searchability-enable-single-context")
  };
  const decisionLikeSearchabilityEnv = {
    API_BASE_URL: apiBaseUrl,
    SEARCHABILITY_ENABLE_LIMIT: String(decisionLikeEnableLimit),
    SEARCHABILITY_ENABLE_MAX_ROUNDS: "1",
    SEARCHABILITY_ENABLE_REAL_ONLY: "1",
    SEARCHABILITY_ENABLE_MODE: "decisionLikeTextReady",
    SEARCHABILITY_ENABLE_SLEEP_MS: "0",
    SEARCHABILITY_ENABLE_OUTPUT_DIR: path.resolve(roundDir, "searchability-enable-decision-like")
  };
  const staleQcEnv = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    STALE_QC_REMEDIATION_LIMIT: String(staleQcLimit),
    STALE_QC_REMEDIATION_OUTPUT_DIR: path.resolve(roundDir, "stale-qc-remediation")
  };
  const companionMetadataEnv = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    COMPANION_METADATA_LIMIT: String(companionMetadataLimit),
    COMPANION_METADATA_OUTPUT_DIR: path.resolve(roundDir, "companion-metadata-recovery")
  };
  if (shouldRunMissingIndexStages) {
    const missingIndexEnv = {
      API_BASE_URL: apiBaseUrl,
      D1_DB_PATH: dbPath,
      MISSING_INDEX_REPROCESS_LIMIT: String(reprocessLimit),
      MISSING_INDEX_REPROCESS_OUTPUT_DIR: path.resolve(roundDir, "missing-index-reprocess")
    };
    const inferenceEnv = {
      API_BASE_URL: apiBaseUrl,
      D1_DB_PATH: dbPath,
      MISSING_INDEX_INFERENCE_LIMIT: String(inferenceLimit),
      MISSING_INDEX_INFERENCE_OUTPUT_DIR: path.resolve(roundDir, "missing-index-inference")
    };
    const tailEnv = {
      API_BASE_URL: apiBaseUrl,
      D1_DB_PATH: dbPath,
      MISSING_INDEX_TAIL_LIMIT: String(tailLimit),
      MISSING_INDEX_TAIL_OUTPUT_DIR: path.resolve(roundDir, "missing-index-tail-recovery")
    };

    const missingIndexResult = await runNodeScript(scriptPath("missing-index-explicit-reprocess-batch.mjs"), {
      cwd: process.cwd(),
      env: missingIndexEnv
    });
    if (!missingIndexResult.ok) {
      throw new Error(`Round ${roundNumber}: missing-index reprocess failed:\n${missingIndexResult.stderr || missingIndexResult.stdout}`);
    }

    const inferenceResult = await runNodeScript(scriptPath("missing-index-inference-batch.mjs"), {
      cwd: process.cwd(),
      env: inferenceEnv
    });
    if (!inferenceResult.ok) {
      throw new Error(`Round ${roundNumber}: missing-index inference failed:\n${inferenceResult.stderr || inferenceResult.stdout}`);
    }

    const tailResult = await runNodeScript(scriptPath("missing-index-tail-recovery-batch.mjs"), {
      cwd: process.cwd(),
      env: tailEnv
    });
    if (!tailResult.ok) {
      throw new Error(`Round ${roundNumber}: missing-index tail recovery failed:\n${tailResult.stderr || tailResult.stdout}`);
    }
  }

  const staleQcResult = await runNodeScript(scriptPath("stale-qc-remediation-batch.mjs"), {
    cwd: process.cwd(),
    env: staleQcEnv
  });
  if (!staleQcResult.ok) {
    throw new Error(`Round ${roundNumber}: stale QC remediation failed:\n${staleQcResult.stderr || staleQcResult.stdout}`);
  }

  const companionMetadataResult = await runNodeScript(scriptPath("companion-metadata-recovery-batch.mjs"), {
    cwd: process.cwd(),
    env: companionMetadataEnv
  });
  if (!companionMetadataResult.ok) {
    throw new Error(
      `Round ${roundNumber}: companion metadata recovery failed:\n${companionMetadataResult.stderr || companionMetadataResult.stdout}`
    );
  }

  const searchabilityResult = await runNodeScript(scriptPath("searchability-enable-loop.mjs"), {
    cwd: process.cwd(),
    env: searchabilityEnv
  });
  if (!searchabilityResult.ok) {
    throw new Error(`Round ${roundNumber}: searchability enable failed:\n${searchabilityResult.stderr || searchabilityResult.stdout}`);
  }

  const missingIndexSearchabilityResult = await runNodeScript(scriptPath("searchability-enable-loop.mjs"), {
    cwd: process.cwd(),
    env: missingIndexSearchabilityEnv
  });
  if (!missingIndexSearchabilityResult.ok) {
    throw new Error(
      `Round ${roundNumber}: missing-index-only searchability enable failed:\n${missingIndexSearchabilityResult.stderr || missingIndexSearchabilityResult.stdout}`
    );
  }

  const singleContextSearchabilityResult = await runNodeScript(scriptPath("searchability-enable-loop.mjs"), {
    cwd: process.cwd(),
    env: singleContextSearchabilityEnv
  });
  if (!singleContextSearchabilityResult.ok) {
    throw new Error(
      `Round ${roundNumber}: single-context text-ready searchability enable failed:\n${singleContextSearchabilityResult.stderr || singleContextSearchabilityResult.stdout}`
    );
  }

  const decisionLikeSearchabilityResult = await runNodeScript(scriptPath("searchability-enable-loop.mjs"), {
    cwd: process.cwd(),
    env: decisionLikeSearchabilityEnv
  });
  if (!decisionLikeSearchabilityResult.ok) {
    throw new Error(
      `Round ${roundNumber}: decision-like text-ready searchability enable failed:\n${decisionLikeSearchabilityResult.stderr || decisionLikeSearchabilityResult.stdout}`
    );
  }

  const retrievalEnv = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    RETRIEVAL_ACTIVATION_GAP_LIMIT: String(retrievalLimit),
    RETRIEVAL_ACTIVATION_GAP_ORDER: retrievalOrder,
    RETRIEVAL_ACTIVATION_GAP_OUTPUT_DIR: path.resolve(roundDir, "retrieval-activation-gap")
  };
  const retrievalResult = await runNodeScript(scriptPath("retrieval-activation-gap-write.mjs"), {
    cwd: process.cwd(),
    env: retrievalEnv
  });
  if (!retrievalResult.ok) {
    throw new Error(`Round ${roundNumber}: retrieval activation gap failed:\n${retrievalResult.stderr || retrievalResult.stdout}`);
  }

  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const delta = progressDelta(beforeSnapshot, afterSnapshot);

  const summary = {
    round: roundNumber,
    generatedAt: new Date().toISOString(),
    runStatus: "completed",
    beforeSnapshot,
    afterSnapshot,
    delta,
    missingIndexReprocess: shouldRunMissingIndexStages
      ? await readJson(path.resolve(roundDir, "missing-index-reprocess", "missing-index-explicit-reprocess-report.json"))
      : { stageStatus: "skipped", summary: { reason: "missing_index_lane_drained" } },
    missingIndexInference: shouldRunMissingIndexStages
      ? await readJson(path.resolve(roundDir, "missing-index-inference", "missing-index-inference-report.json"))
      : { stageStatus: "skipped", summary: { reason: "missing_index_lane_drained" } },
    missingIndexTailRecovery: shouldRunMissingIndexStages
      ? await readJson(path.resolve(roundDir, "missing-index-tail-recovery", "missing-index-tail-recovery-report.json"))
      : { stageStatus: "skipped", summary: { reason: "missing_index_lane_drained" } },
    staleQcRemediation: await readJson(
      path.resolve(roundDir, "stale-qc-remediation", "stale-qc-remediation-report.json")
    ),
    companionMetadataRecovery: await readJson(
      path.resolve(roundDir, "companion-metadata-recovery", "companion-metadata-recovery-report.json")
    ),
    searchabilityEnable: await buildSearchabilityStageSummary(
      path.resolve(roundDir, "searchability-enable"),
      beforeSnapshot,
      afterSnapshot
    ),
    searchabilityEnableMissingIndexOnly: await buildSearchabilityStageSummary(
      path.resolve(roundDir, "searchability-enable-missing-index-only"),
      beforeSnapshot,
      afterSnapshot
    ),
    searchabilityEnableSingleContext: await buildSearchabilityStageSummary(
      path.resolve(roundDir, "searchability-enable-single-context"),
      beforeSnapshot,
      afterSnapshot
    ),
    searchabilityEnableDecisionLike: await buildSearchabilityStageSummary(
      path.resolve(roundDir, "searchability-enable-decision-like"),
      beforeSnapshot,
      afterSnapshot
    ),
    retrievalActivationGap: await readJson(
      path.resolve(roundDir, "retrieval-activation-gap", "retrieval-activation-gap-report.json")
    )
  };

  await Promise.all([
    writeJson(path.resolve(roundDir, "summary.json"), summary),
    writeText(
      path.resolve(roundDir, "summary.md"),
      [
        "# Corpus Lift Round",
        "",
        `- round: ${summary.round}`,
        `- generatedAt: ${summary.generatedAt}`,
        `- searchableDelta: ${summary.delta.searchableDelta}`,
        `- activeRetrievalDelta: ${summary.delta.activeRetrievalDelta}`,
        `- missingIndexOnlyDelta: ${summary.delta.missingIndexOnlyDelta}`,
        ""
      ].join("\n")
    )
  ]);

  return summary;
}

export async function main() {
  await ensureDir(outputBaseDir);

  const health = await checkHealth(apiBaseUrl);
  if (!health.ok) {
    throw new Error(`API health check failed for ${apiBaseUrl}: ${health.error}`);
  }

  const runDir = path.resolve(outputBaseDir, formatTimestamp(new Date()));
  await ensureDir(runDir);

  const startSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const roundReports = [];
  let noProgressRounds = 0;
  let runStatus = "completed";
  let error = null;

  for (let round = 1; round <= rounds; round += 1) {
    try {
      const roundReport = await runRound(path.resolve(runDir, `round-${String(round).padStart(2, "0")}`), round);
      roundReports.push(roundReport);
      if (hasMeaningfulProgress(roundReport.delta)) {
        noProgressRounds = 0;
      } else {
        noProgressRounds += 1;
      }
      if (noProgressRounds >= stopAfterNoProgressRounds) {
        runStatus = "stopped_no_progress";
        break;
      }
      if (round < rounds && sleepMs > 0) {
        await sleep(sleepMs);
      }
    } catch (stageError) {
      runStatus = "failed";
      error = stageError instanceof Error ? stageError.message : String(stageError);
      break;
    }
  }

  const endSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const report = {
    generatedAt: new Date().toISOString(),
    apiBase: apiBaseUrl,
    dbPath,
    runStatus,
    plannedRounds: rounds,
    completedRounds: roundReports.length,
    sleepMs,
    stopAfterNoProgressRounds,
    limits: {
      reprocessLimit,
      inferenceLimit,
        tailLimit,
        staleQcLimit,
        companionMetadataLimit,
        enableLimit,
        retrievalLimit,
        retrievalOrder
    },
    startSnapshot,
    endSnapshot,
    overallDelta: progressDelta(startSnapshot, endSnapshot),
    rounds: roundReports.map((round) => ({
      round: round.round,
      generatedAt: round.generatedAt,
      runStatus: round.runStatus,
      delta: round.delta
    })),
    error
  };

  await Promise.all([
    writeJson(path.resolve(runDir, "summary.json"), report),
    writeText(path.resolve(runDir, "summary.md"), formatSummaryMarkdown(report))
  ]);

  console.log(JSON.stringify({ runStatus: report.runStatus, overallDelta: report.overallDelta }, null, 2));
  console.log(`Corpus lift slow-loop summary JSON written to ${path.resolve(runDir, "summary.json")}`);
  console.log(`Corpus lift slow-loop summary Markdown written to ${path.resolve(runDir, "summary.md")}`);

  if (report.runStatus === "failed") {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
