import { computeProgressSummary } from "./overnight-corpus-lift-utils.mjs";

export function aggregateRunSummaries(runSummaries = [], { targetSearchable = 7000 } = {}) {
  const runs = [...runSummaries]
    .filter(Boolean)
    .sort((a, b) => String(a.generatedAt || "").localeCompare(String(b.generatedAt || "")));

  if (!runs.length) {
    return {
      runCount: 0,
      failedRunCount: 0,
      successfulRunCount: 0,
      totalMissingIndexRecoveredOvernight: 0,
      totalEnabledOvernight: 0,
      totalRetrievalActivatedOvernight: 0,
      progress: computeProgressSummary({}, {}, targetSearchable),
      topUnresolvedMissingIndexBuckets: [],
      runs: []
    };
  }

  const first = runs[0];
  const last = runs[runs.length - 1];
  const totalEnabledOvernight = runs.reduce(
    (sum, run) =>
      sum +
      Number(run.stageResults?.searchabilityEnable?.enabledCount || 0) +
      Number(run.stageResults?.searchabilityEnableMissingIndexOnly?.enabledCount || 0) +
      Number(run.stageResults?.searchabilityEnableSingleContext?.enabledCount || 0) +
      Number(run.stageResults?.searchabilityEnableDecisionLike?.enabledCount || 0),
    0
  );
  const totalMissingIndexRecoveredOvernight = runs.reduce(
    (sum, run) =>
      sum +
      Number(run.stageResults?.missingIndexReprocess?.recoveredIndexCodeCount || 0) +
      Number(run.stageResults?.missingIndexInference?.qcPassedCount || run.stageResults?.missingIndexInference?.updatedDocumentCount || 0) +
      Number(run.stageResults?.missingIndexTailRecovery?.qcPassedCount || run.stageResults?.missingIndexTailRecovery?.updatedDocumentCount || 0),
    0
  );
  const totalRetrievalActivatedOvernight = runs.reduce(
    (sum, run) => sum + Number(run.stageResults?.retrievalActivationGap?.activatedDocumentCount || 0),
    0
  );
  const failedRuns = runs.filter((run) => run.runStatus === "failed");
  const successfulRuns = runs.filter((run) => run.runStatus !== "failed");
  const latestMissingIndexBreakdowns = last.stageResults?.missingIndexAudit?.summaryBreakdowns || {};

  return {
    runCount: runs.length,
    failedRunCount: failedRuns.length,
    successfulRunCount: successfulRuns.length,
    totalMissingIndexRecoveredOvernight,
    totalEnabledOvernight,
    totalRetrievalActivatedOvernight,
    startSnapshot: first.startSnapshot || {},
    endSnapshot: last.endSnapshot || {},
    progress: computeProgressSummary(first.startSnapshot || {}, last.endSnapshot || {}, targetSearchable),
    topUnresolvedMissingIndexBuckets: (latestMissingIndexBreakdowns.byIssueFamily || []).slice(0, 10),
    runs: runs.map((run) => ({
      generatedAt: run.generatedAt,
      runStatus: run.runStatus,
      searchableBefore: Number(run.startSnapshot?.searchableDecisionDocs || 0),
      searchableAfter: Number(run.endSnapshot?.searchableDecisionDocs || 0),
      activeRetrievalBefore: Number(run.startSnapshot?.activeRetrievalDecisionCount || 0),
      activeRetrievalAfter: Number(run.endSnapshot?.activeRetrievalDecisionCount || 0),
      missingIndexRecoveredCount:
        Number(run.stageResults?.missingIndexReprocess?.recoveredIndexCodeCount || 0) +
        Number(run.stageResults?.missingIndexInference?.qcPassedCount || run.stageResults?.missingIndexInference?.updatedDocumentCount || 0) +
        Number(run.stageResults?.missingIndexTailRecovery?.qcPassedCount || run.stageResults?.missingIndexTailRecovery?.updatedDocumentCount || 0),
      enabledCount:
        Number(run.stageResults?.searchabilityEnable?.enabledCount || 0) +
        Number(run.stageResults?.searchabilityEnableMissingIndexOnly?.enabledCount || 0) +
        Number(run.stageResults?.searchabilityEnableSingleContext?.enabledCount || 0) +
        Number(run.stageResults?.searchabilityEnableDecisionLike?.enabledCount || 0),
      retrievalActivatedCount: Number(run.stageResults?.retrievalActivationGap?.activatedDocumentCount || 0),
      missingIndexCandidateCount: Number(run.stageResults?.missingIndexAudit?.candidateDocCount || 0)
    }))
  };
}

export function formatOvernightCorpusLiftSummaryMarkdown(report) {
  const lines = [];
  lines.push("# Overnight Corpus Lift Morning Summary");
  lines.push("");
  lines.push(`- Generated: \`${report.generatedAt}\``);
  lines.push(`- Run count: ${report.summary.runCount}`);
  lines.push(`- Successful runs: ${report.summary.successfulRunCount}`);
  lines.push(`- Failed runs: ${report.summary.failedRunCount}`);
  lines.push(`- Missing-index recoveries overnight: ${report.summary.totalMissingIndexRecoveredOvernight}`);
  lines.push(`- Searchability enabled overnight: ${report.summary.totalEnabledOvernight}`);
  lines.push(`- Retrieval activated overnight: ${report.summary.totalRetrievalActivatedOvernight}`);
  lines.push("");

  lines.push("## Progress");
  for (const [key, value] of Object.entries(report.summary.progress || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Starting Snapshot");
  for (const [key, value] of Object.entries(report.summary.startSnapshot || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Ending Snapshot");
  for (const [key, value] of Object.entries(report.summary.endSnapshot || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Top Unresolved Missing-index Buckets");
  for (const row of report.summary.topUnresolvedMissingIndexBuckets || []) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }
  if (!(report.summary.topUnresolvedMissingIndexBuckets || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Runs");
  for (const run of report.summary.runs || []) {
    lines.push(
      `- \`${run.generatedAt}\` | status=\`${run.runStatus}\` | missingIndexRecovered=\`${run.missingIndexRecoveredCount}\` | enabled=\`${run.enabledCount}\` | retrievalActivated=\`${run.retrievalActivatedCount}\``
    );
  }
  if (!(report.summary.runs || []).length) lines.push("- none");
  lines.push("");

  return `${lines.join("\n")}\n`;
}
