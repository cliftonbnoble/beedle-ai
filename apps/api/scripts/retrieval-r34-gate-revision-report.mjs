import fs from "node:fs/promises";
import path from "node:path";
import {
  computeCitationTopDocumentShareAverage,
  resolveCitationTopDocumentShareCeiling
} from "./retrieval-safe-batch-activation-utils.mjs";

const reportsDir = path.resolve(process.cwd(), "reports");
const outputJsonName = process.env.RETRIEVAL_R34_REPORT_NAME || "retrieval-r34-gate-revision-report.json";
const outputMdName = process.env.RETRIEVAL_R34_MARKDOWN_NAME || "retrieval-r34-gate-revision-report.md";
const inputActivationName = process.env.RETRIEVAL_R34_INPUT_ACTIVATION_REPORT || "retrieval-r34-gate-revision-activation-report.json";
const inputLiveQaName = process.env.RETRIEVAL_R34_INPUT_LIVE_QA_REPORT || "retrieval-r34-gate-revision-live-qa-report.json";
const inputAuditName = process.env.RETRIEVAL_R34_INPUT_R33_AUDIT || "retrieval-r33-citation-threshold-audit-report.json";
const configuredCeiling = Number(process.env.RETRIEVAL_R34_CONFIGURED_CITATION_CEILING || "0.1");

async function readJson(name) {
  const raw = await fs.readFile(path.resolve(reportsDir, name), "utf8");
  return JSON.parse(raw);
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R34 Gate Revision Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Gate Formula");
  lines.push(`- ${report.gateFormula}`);
  lines.push("");
  lines.push("## Baseline Gate Behavior");
  for (const [k, v] of Object.entries(report.baselineGateBehavior || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Activation Result");
  for (const [k, v] of Object.entries(report.activationResult || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Hard Gate Thresholds");
  for (const [k, v] of Object.entries(report.hardGateThresholds || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Hard Gate Checks");
  for (const [k, v] of Object.entries(report.hardGateChecks || {})) lines.push(`- ${k}: ${v}`);
  if ((report.hardGateFailures || []).length) lines.push(`- failures: ${(report.hardGateFailures || []).join(", ")}`);
  lines.push("");
  lines.push("## Recommendation");
  lines.push(`- ${report.recommendation}`);
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [activationReport, liveQaReport, r33Audit] = await Promise.all([
    readJson(inputActivationName),
    readJson(inputLiveQaName),
    readJson(inputAuditName).catch(() => null)
  ]);

  const beforeRows = liveQaReport?.beforeQueryResults || [];
  const afterRows = liveQaReport?.afterQueryResults || [];
  const baselineCitationTopDocumentShare = computeCitationTopDocumentShareAverage(beforeRows);
  const afterCitationTopDocumentShare = computeCitationTopDocumentShareAverage(afterRows);

  const effective = resolveCitationTopDocumentShareCeiling({
    baselineCitationQueryResults: beforeRows,
    configuredGlobalCeiling: configuredCeiling,
    k: 10
  });

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: false,
    gateFormula: "effectiveCeiling = max(0.1, attainableFloorGivenUniqueDocsAtK10)",
    summary: {
      configuredGlobalCeiling: configuredCeiling,
      baselineCitationTopDocumentShare,
      baselineEffectiveCitationTopDocumentShareCeiling: effective.effectiveCeiling,
      baselinePassesOldFixedCeiling: baselineCitationTopDocumentShare <= configuredCeiling,
      baselinePassesRevisedCeiling: baselineCitationTopDocumentShare <= effective.effectiveCeiling,
      afterCitationTopDocumentShare,
      afterPassesRevisedCeiling: afterCitationTopDocumentShare <= effective.effectiveCeiling,
      keepOrRollbackDecision: activationReport?.summary?.keepOrRollbackDecision || "unknown"
    },
    baselineGateBehavior: {
      baselineCitationTopDocumentShare,
      oldFixedCeiling: configuredCeiling,
      oldFixedCeilingPass: baselineCitationTopDocumentShare <= configuredCeiling,
      attainableFloorGivenUniqueDocsAtK10: effective.attainableFloorGivenUniqueDocsAtK,
      revisedEffectiveCeiling: effective.effectiveCeiling,
      revisedEffectiveCeilingPass: baselineCitationTopDocumentShare <= effective.effectiveCeiling
    },
    activationResult: {
      activationBatchId: activationReport?.summary?.activationBatchId || "",
      docActivatedExact: activationReport?.docActivatedExact || "",
      activatedDocumentCount: Number(activationReport?.summary?.activatedDocumentCount || 0),
      activatedChunkCount: Number(activationReport?.summary?.activatedChunkCount || 0),
      keepOrRollbackDecision: activationReport?.summary?.keepOrRollbackDecision || "unknown",
      anomalyFlags: activationReport?.anomalyFlags || []
    },
    hardGateThresholds: activationReport?.hardGate?.thresholds || {},
    hardGateChecks: activationReport?.hardGate?.checks || {},
    hardGateFailures: activationReport?.hardGate?.failures || [],
    r33AuditSummary: r33Audit?.summary || null,
    recommendation:
      (activationReport?.summary?.keepOrRollbackDecision || "rollback_batch") === "keep_batch_active"
        ? "keep_batch_active"
        : "rollback_batch"
  };

  const outputJsonPath = path.resolve(reportsDir, outputJsonName);
  const outputMdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([
    fs.writeFile(outputJsonPath, JSON.stringify(report, null, 2)),
    fs.writeFile(outputMdPath, buildMarkdown(report))
  ]);

  console.log(`R34 gate revision report written to ${outputJsonPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
