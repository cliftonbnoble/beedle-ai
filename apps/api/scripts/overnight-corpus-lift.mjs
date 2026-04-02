import fs from "node:fs/promises";
import path from "node:path";
import {
  checkHealth,
  defaultReportsBaseDir,
  defaultDbPath,
  ensureDir,
  formatTimestamp,
  getZonedDateParts,
  isWithinOvernightWindow,
  queryCorpusSnapshot,
  readJson,
  runNodeScript,
  writeJson,
  writeText,
  computeProgressSummary
} from "./lib/overnight-corpus-lift-utils.mjs";

const apiBaseUrl = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.OVERNIGHT_CORPUS_LIFT_BUSY_TIMEOUT_MS || "5000", 10);
const outputBaseDir = path.resolve(process.cwd(), process.env.OVERNIGHT_CORPUS_LIFT_OUTPUT_BASE_DIR || defaultReportsBaseDir);
const timeZone = process.env.OVERNIGHT_CORPUS_LIFT_TIMEZONE || "America/Los_Angeles";
const overnightStartHour = Number.parseInt(process.env.OVERNIGHT_CORPUS_LIFT_START_HOUR || "22", 10);
const overnightEndHour = Number.parseInt(process.env.OVERNIGHT_CORPUS_LIFT_END_HOUR || "7", 10);
const ignoreWindow = (process.env.OVERNIGHT_CORPUS_LIFT_IGNORE_WINDOW || "0") === "1";
const targetSearchable = Number.parseInt(process.env.OVERNIGHT_CORPUS_LIFT_TARGET_SEARCHABLE || "7000", 10);
const retrievalOrder = String(
  process.env.OVERNIGHT_CORPUS_LIFT_RETRIEVAL_ORDER || "coverage_rich_decision_like_searchable_asc"
);

function scriptPath(name) {
  return path.resolve(process.cwd(), "scripts", name);
}

function formatRunMarkdown(report) {
  const lines = [];
  lines.push("# Overnight Corpus Lift Run");
  lines.push("");
  lines.push(`- Generated: \`${report.generatedAt}\``);
  lines.push(`- Run status: \`${report.runStatus}\``);
  lines.push(`- API base: \`${report.apiBase}\``);
  lines.push(`- Time zone: \`${report.timeZone}\``);
  lines.push("");

  lines.push("## Start Snapshot");
  for (const [key, value] of Object.entries(report.startSnapshot || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## End Snapshot");
  for (const [key, value] of Object.entries(report.endSnapshot || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Progress");
  for (const [key, value] of Object.entries(report.progress || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Stage Results");
  for (const [stageName, stage] of Object.entries(report.stageResults || {})) {
    lines.push(`- ${stageName}`);
    for (const [key, value] of Object.entries(stage || {})) {
      if (key === "summaryBreakdowns") continue;
      lines.push(`  - ${key}: ${typeof value === "object" ? JSON.stringify(value) : value}`);
    }
  }
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

async function runMissingIndexAuditStage(stageDir) {
  await ensureDir(stageDir);
  const env = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    MISSING_INDEX_AUDIT_OUTPUT_DIR: stageDir,
    MISSING_INDEX_AUDIT_REPORT_NAME: "missing-index-code-full-audit-report.json",
    MISSING_INDEX_AUDIT_MARKDOWN_NAME: "missing-index-code-full-audit-report.md"
  };
  const result = await runNodeScript(scriptPath("missing-index-code-full-audit.mjs"), {
    cwd: process.cwd(),
    env
  });
  if (!result.ok) {
    throw new Error(`Missing-index audit failed:\n${result.stderr || result.stdout}`);
  }

  const report = await readJson(path.resolve(stageDir, "missing-index-code-full-audit-report.json"));
  return {
    stageStatus: "completed",
    missingIndexOnlyCount: Number(report.summary?.missingIndexOnlyCount || 0),
    candidateDocCount: Number(report.summary?.candidateDocCount || 0),
    unresolvedDocCount: Number(report.summary?.unresolvedDocCount || 0),
    summaryBreakdowns: report.summaryBreakdowns || {},
    reportPath: path.resolve(stageDir, "missing-index-code-full-audit-report.json")
  };
}

async function runMissingIndexReprocessStage(stageDir) {
  await ensureDir(stageDir);
  const env = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    MISSING_INDEX_REPROCESS_OUTPUT_DIR: stageDir,
    MISSING_INDEX_REPROCESS_LIMIT: "100"
  };
  const result = await runNodeScript(scriptPath("missing-index-explicit-reprocess-batch.mjs"), {
    cwd: process.cwd(),
    env
  });
  if (!result.ok) {
    throw new Error(`Missing-index reprocess stage failed:\n${result.stderr || result.stdout}`);
  }

  const report = await readJson(path.resolve(stageDir, "missing-index-explicit-reprocess-report.json"));
  return {
    stageStatus: report.stageStatus,
    selectedDocumentCount: Number(report.summary?.selectedDocumentCount || 0),
    reprocessedDocumentCount: Number(report.summary?.reprocessedDocumentCount || 0),
    recoveredIndexCodeCount: Number(report.summary?.recoveredIndexCodeCount || 0),
    newlyQcPassedCount: Number(report.summary?.newlyQcPassedCount || 0),
    failuresCount: Number(report.summary?.failuresCount || 0),
    qcPassedNotSearchableBefore: Number(report.summary?.qcPassedNotSearchableBefore || 0),
    qcPassedNotSearchableAfter: Number(report.summary?.qcPassedNotSearchableAfter || 0),
    reportPath: path.resolve(stageDir, "missing-index-explicit-reprocess-report.json")
  };
}

async function runMissingIndexInferenceStage(stageDir) {
  await ensureDir(stageDir);
  const env = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    MISSING_INDEX_INFERENCE_OUTPUT_DIR: stageDir,
    MISSING_INDEX_INFERENCE_LIMIT: "100"
  };
  const result = await runNodeScript(scriptPath("missing-index-inference-batch.mjs"), {
    cwd: process.cwd(),
    env
  });
  if (!result.ok) {
    throw new Error(`Missing-index inference stage failed:\n${result.stderr || result.stdout}`);
  }

  const report = await readJson(path.resolve(stageDir, "missing-index-inference-report.json"));
  return {
    stageStatus: report.stageStatus,
    eligibleInferenceDocCount: Number(report.summary?.eligibleInferenceDocCount || 0),
    selectedDocumentCount: Number(report.summary?.selectedDocumentCount || 0),
    updatedDocumentCount: Number(report.summary?.updatedDocumentCount || 0),
    qcPassedCount: Number(report.summary?.qcPassedCount || 0),
    failuresCount: Number(report.summary?.failuresCount || 0),
    qcPassedNotSearchableBefore: Number(report.summary?.qcPassedNotSearchableBefore || 0),
    qcPassedNotSearchableAfter: Number(report.summary?.qcPassedNotSearchableAfter || 0),
    reportPath: path.resolve(stageDir, "missing-index-inference-report.json")
  };
}

async function runMissingIndexTailRecoveryStage(stageDir) {
  await ensureDir(stageDir);
  const env = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    MISSING_INDEX_TAIL_OUTPUT_DIR: stageDir,
    MISSING_INDEX_TAIL_LIMIT: "100"
  };
  const result = await runNodeScript(scriptPath("missing-index-tail-recovery-batch.mjs"), {
    cwd: process.cwd(),
    env
  });
  if (!result.ok) {
    throw new Error(`Missing-index tail recovery stage failed:\n${result.stderr || result.stdout}`);
  }

  const report = await readJson(path.resolve(stageDir, "missing-index-tail-recovery-report.json"));
  return {
    stageStatus: report.stageStatus,
    siblingCandidateCount: Number(report.summary?.siblingCandidateCount || 0),
    tailInferenceEligibleCount: Number(report.summary?.tailInferenceEligibleCount || 0),
    selectedDocumentCount: Number(report.summary?.selectedDocumentCount || 0),
    updatedDocumentCount: Number(report.summary?.updatedDocumentCount || 0),
    qcPassedCount: Number(report.summary?.qcPassedCount || 0),
    siblingUpdatedCount: Number(report.summary?.siblingUpdatedCount || 0),
    tailInferenceUpdatedCount: Number(report.summary?.tailInferenceUpdatedCount || 0),
    failuresCount: Number(report.summary?.failuresCount || 0),
    reportPath: path.resolve(stageDir, "missing-index-tail-recovery-report.json")
  };
}

async function runStaleQcRemediationStage(stageDir) {
  await ensureDir(stageDir);
  const env = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    STALE_QC_REMEDIATION_OUTPUT_DIR: stageDir,
    STALE_QC_REMEDIATION_LIMIT: "100"
  };
  const result = await runNodeScript(scriptPath("stale-qc-remediation-batch.mjs"), {
    cwd: process.cwd(),
    env
  });
  if (!result.ok) {
    throw new Error(`Stale QC remediation stage failed:\n${result.stderr || result.stdout}`);
  }

  const report = await readJson(path.resolve(stageDir, "stale-qc-remediation-report.json"));
  return {
    stageStatus: report.stageStatus,
    candidateDocCount: Number(report.summary?.candidateDocCount || 0),
    eligibleCandidateCount: Number(report.summary?.eligibleCandidateCount || 0),
    selectedDocumentCount: Number(report.summary?.selectedDocumentCount || 0),
    updatedDocumentCount: Number(report.summary?.updatedDocumentCount || 0),
    qcPassedCount: Number(report.summary?.qcPassedCount || 0),
    qcRequiredConfirmedCount: Number(report.summary?.qcRequiredConfirmedCount || 0),
    failuresCount: Number(report.summary?.failuresCount || 0),
    reportPath: path.resolve(stageDir, "stale-qc-remediation-report.json")
  };
}

async function runCompanionMetadataRecoveryStage(stageDir) {
  await ensureDir(stageDir);
  const env = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    COMPANION_METADATA_OUTPUT_DIR: stageDir,
    COMPANION_METADATA_LIMIT: "100"
  };
  const result = await runNodeScript(scriptPath("companion-metadata-recovery-batch.mjs"), {
    cwd: process.cwd(),
    env
  });
  if (!result.ok) {
    throw new Error(`Companion metadata recovery stage failed:\n${result.stderr || result.stdout}`);
  }

  const report = await readJson(path.resolve(stageDir, "companion-metadata-recovery-report.json"));
  return {
    stageStatus: report.stageStatus,
    targetDocCount: Number(report.summary?.targetDocCount || 0),
    eligibleCandidateCount: Number(report.summary?.eligibleCandidateCount || 0),
    selectedDocumentCount: Number(report.summary?.selectedDocumentCount || 0),
    updatedDocumentCount: Number(report.summary?.updatedDocumentCount || 0),
    qcPassedCount: Number(report.summary?.qcPassedCount || 0),
    qcRequiredConfirmedCount: Number(report.summary?.qcRequiredConfirmedCount || 0),
    failuresCount: Number(report.summary?.failuresCount || 0),
    reportPath: path.resolve(stageDir, "companion-metadata-recovery-report.json")
  };
}

async function runSearchabilityEnableStage(stageDir, mode = "qcPassed") {
  await ensureDir(stageDir);
  const beforeSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const env = {
    API_BASE_URL: apiBaseUrl,
    SEARCHABILITY_ENABLE_LIMIT: "100",
    SEARCHABILITY_ENABLE_MAX_ROUNDS: "1",
    SEARCHABILITY_ENABLE_REAL_ONLY: "1",
    SEARCHABILITY_ENABLE_MODE: mode,
    SEARCHABILITY_ENABLE_SLEEP_MS: "0",
    SEARCHABILITY_ENABLE_OUTPUT_DIR: stageDir
  };
  const result = await runNodeScript(scriptPath("searchability-enable-loop.mjs"), {
    cwd: process.cwd(),
    env
  });
  if (!result.ok) {
    throw new Error(`Searchability enable stage failed:\n${result.stderr || result.stdout}`);
  }

  const latestCandidate = await readLatestStageFile(stageDir, "-candidates.json");
  const latestEnable = await readLatestStageFile(stageDir, "-enable.json");
  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });

  const candidateCount = Number(latestCandidate?.body?.summary?.candidateCount || 0);
  const enabledCount = Number(latestEnable?.body?.summary?.enabledCount || 0);
  let stageStatus = "noop";
  if (enabledCount > 0) stageStatus = "written";
  else if (candidateCount > 0) stageStatus = "no_effect";

  const summary = {
    stageStatus,
    mode,
    candidateCount,
    enabledCount,
    searchableBefore: beforeSnapshot.searchableDecisionDocs,
    searchableAfter: afterSnapshot.searchableDecisionDocs,
    delta: afterSnapshot.searchableDecisionDocs - beforeSnapshot.searchableDecisionDocs,
    reportPath: stageDir
  };

  await Promise.all([
    writeJson(path.resolve(stageDir, "searchability-enable-stage-report.json"), summary),
    writeText(
      path.resolve(stageDir, "searchability-enable-stage-report.md"),
      [
        "# Searchability Enable Stage",
        "",
        `- stageStatus: ${summary.stageStatus}`,
        `- candidateCount: ${summary.candidateCount}`,
        `- enabledCount: ${summary.enabledCount}`,
        `- searchableBefore: ${summary.searchableBefore}`,
        `- searchableAfter: ${summary.searchableAfter}`,
        `- delta: ${summary.delta}`,
        ""
      ].join("\n")
    )
  ]);

  return summary;
}

async function runRetrievalActivationGapStage(stageDir) {
  await ensureDir(stageDir);
  const env = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    RETRIEVAL_ACTIVATION_GAP_OUTPUT_DIR: stageDir,
    RETRIEVAL_ACTIVATION_GAP_LIMIT: "100",
    RETRIEVAL_ACTIVATION_GAP_ORDER: retrievalOrder
  };
  const result = await runNodeScript(scriptPath("retrieval-activation-gap-write.mjs"), {
    cwd: process.cwd(),
    env
  });
  if (!result.ok) {
    throw new Error(`Retrieval activation gap stage failed:\n${result.stderr || result.stdout}`);
  }

  const report = await readJson(path.resolve(stageDir, "retrieval-activation-gap-report.json"));
  return {
    stageStatus: report.stageStatus,
    selectedDocumentCount: Number(report.summary?.selectedDocumentCount || 0),
    activatedDocumentCount: Number(report.summary?.activatedDocumentCount || 0),
    activatedChunkCount: Number(report.summary?.activatedChunkCount || 0),
    searchableButNotActiveBefore: Number(report.summary?.searchableButNotActiveBefore || 0),
    searchableButNotActiveAfter: Number(report.summary?.searchableButNotActiveAfter || 0),
    reportPath: path.resolve(stageDir, "retrieval-activation-gap-report.json")
  };
}

export async function main() {
  const health = await checkHealth(apiBaseUrl);
  if (!health.ok) {
    throw new Error(`API health check failed for ${apiBaseUrl}: ${health.error}`);
  }

  const zonedParts = getZonedDateParts(new Date(), timeZone);
  if (!ignoreWindow && !isWithinOvernightWindow({ timeZone, startHour: overnightStartHour, endHour: overnightEndHour })) {
    console.log(
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          runStatus: "skipped",
          reason: "outside_overnight_window",
          apiBase: apiBaseUrl,
          timeZone,
          localTime: zonedParts.isoLocal
        },
        null,
        2
      )
    );
    return;
  }

  const runDir = path.resolve(outputBaseDir, formatTimestamp(new Date()));
  await ensureDir(runDir);

  let report;
  try {
    const startSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
    const stageResults = {
      missingIndexAudit: await runMissingIndexAuditStage(path.resolve(runDir, "missing-index-audit")),
      missingIndexReprocess: await runMissingIndexReprocessStage(path.resolve(runDir, "missing-index-reprocess")),
      missingIndexInference: await runMissingIndexInferenceStage(path.resolve(runDir, "missing-index-inference")),
      missingIndexTailRecovery: await runMissingIndexTailRecoveryStage(path.resolve(runDir, "missing-index-tail-recovery")),
      staleQcRemediation: await runStaleQcRemediationStage(path.resolve(runDir, "stale-qc-remediation")),
      companionMetadataRecovery: await runCompanionMetadataRecoveryStage(path.resolve(runDir, "companion-metadata-recovery")),
      searchabilityEnable: await runSearchabilityEnableStage(path.resolve(runDir, "searchability-enable"), "qcPassed"),
      searchabilityEnableMissingIndexOnly: await runSearchabilityEnableStage(
        path.resolve(runDir, "searchability-enable-missing-index-only"),
        "missingIndexOnlyTextReady"
      ),
      searchabilityEnableSingleContext: await runSearchabilityEnableStage(
        path.resolve(runDir, "searchability-enable-single-context"),
        "singleContextTextReady"
      ),
      searchabilityEnableDecisionLike: await runSearchabilityEnableStage(
        path.resolve(runDir, "searchability-enable-decision-like"),
        "decisionLikeTextReady"
      ),
      retrievalActivationGap: await runRetrievalActivationGapStage(path.resolve(runDir, "retrieval-activation-gap"))
    };
    const endSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });

    report = {
      generatedAt: new Date().toISOString(),
      apiBase: apiBaseUrl,
      dbPath,
      timeZone,
      localTime: zonedParts.isoLocal,
      runStatus: "completed",
      startSnapshot,
      endSnapshot,
      progress: computeProgressSummary(startSnapshot, endSnapshot, targetSearchable),
      stageResults
    };
  } catch (error) {
    report = {
      generatedAt: new Date().toISOString(),
      apiBase: apiBaseUrl,
      dbPath,
      timeZone,
      localTime: zonedParts.isoLocal,
      runStatus: "failed",
      startSnapshot: await queryCorpusSnapshot({ dbPath, busyTimeoutMs }).catch(() => ({})),
      endSnapshot: await queryCorpusSnapshot({ dbPath, busyTimeoutMs }).catch(() => ({})),
      progress: computeProgressSummary({}, {}, targetSearchable),
      stageResults: {},
      error: error instanceof Error ? error.message : String(error)
    };
  }

  await Promise.all([
    writeJson(path.resolve(runDir, "summary.json"), report),
    writeText(path.resolve(runDir, "summary.md"), formatRunMarkdown(report))
  ]);

  console.log(JSON.stringify({ runStatus: report.runStatus, progress: report.progress }, null, 2));
  console.log(`Overnight corpus lift summary JSON written to ${path.resolve(runDir, "summary.json")}`);
  console.log(`Overnight corpus lift summary Markdown written to ${path.resolve(runDir, "summary.md")}`);

  if (report.runStatus === "failed") {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
