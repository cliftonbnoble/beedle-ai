import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const r48Name = process.env.RETRIEVAL_R52_R48_REPORT_NAME || "retrieval-r48-frontier-quality-audit-report.json";
const r50Name = process.env.RETRIEVAL_R52_R50_REPORT_NAME || "retrieval-r50-family-probe-plan-report.json";
const r51ActivationName =
  process.env.RETRIEVAL_R52_R51_ACTIVATION_REPORT_NAME || "retrieval-r51-single-probe-activation-report.json";
const r51LiveQaName = process.env.RETRIEVAL_R52_R51_LIVE_QA_REPORT_NAME || "retrieval-r51-single-probe-live-qa-report.json";
const outputJsonName =
  process.env.RETRIEVAL_R52_REPORT_NAME || "retrieval-r52-frozen-family-postmortem-report.json";
const outputMdName = process.env.RETRIEVAL_R52_MARKDOWN_NAME || "retrieval-r52-frozen-family-postmortem-report.md";

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function avg(values) {
  if (!values.length) return 0;
  return Number((values.reduce((sum, v) => sum + Number(v || 0), 0) / values.length).toFixed(4));
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function topNCount(values = [], n = 5) {
  const counts = new Map();
  for (const value of values) {
    const key = String(value || "<none>");
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .slice(0, n)
    .map(([key, count]) => ({ key, count }));
}

function anchorFamily(anchor) {
  const raw = String(anchor || "");
  if (!raw) return "<none>";
  const hashIndex = raw.indexOf("#");
  if (hashIndex >= 0) return raw.slice(0, hashIndex);
  return raw;
}

function queryMap(rows = []) {
  const map = new Map();
  for (const row of rows || []) map.set(String(row?.queryId || ""), row);
  return map;
}

function buildQueryLevelRegressionBreakdown({ beforeRows = [], afterRows = [], targetDocId }) {
  const beforeMap = queryMap(beforeRows);
  const afterMap = queryMap(afterRows);
  const queryIds = unique([...beforeMap.keys(), ...afterMap.keys()]);

  const rows = queryIds.map((queryId) => {
    const before = beforeMap.get(queryId) || {};
    const after = afterMap.get(queryId) || {};
    const beforeQuality = Number(before?.metrics?.qualityScore || 0);
    const afterQuality = Number(after?.metrics?.qualityScore || 0);
    const qualityDelta = Number((afterQuality - beforeQuality).toFixed(2));

    const beforeTopTypes = topNCount((before?.topResults || []).slice(0, 10).map((row) => row.chunkType || row.sectionLabel || ""), 3);
    const afterTopTypes = topNCount((after?.topResults || []).slice(0, 10).map((row) => row.chunkType || row.sectionLabel || ""), 3);

    const beforeTopDocShare = Number(before?.metrics?.topDocumentShare || 0);
    const afterTopDocShare = Number(after?.metrics?.topDocumentShare || 0);

    const activatedDocHits = (after?.topResults || []).slice(0, 10).filter((row) => String(row?.documentId || "") === String(targetDocId)).length;

    return {
      queryId,
      query: String(after?.query || before?.query || ""),
      qualityBefore: beforeQuality,
      qualityAfter: afterQuality,
      qualityDelta,
      topDocumentShareBefore: beforeTopDocShare,
      topDocumentShareAfter: afterTopDocShare,
      activatedDocTop10Hits: activatedDocHits,
      topChunkTypesBefore: beforeTopTypes,
      topChunkTypesAfter: afterTopTypes,
      weaknessSignalsBefore: (before?.metrics?.weaknessSignals || []).map(String),
      weaknessSignalsAfter: (after?.metrics?.weaknessSignals || []).map(String)
    };
  });

  return rows.sort((a, b) => {
    if (a.qualityDelta !== b.qualityDelta) return a.qualityDelta - b.qualityDelta;
    return String(a.queryId).localeCompare(String(b.queryId));
  });
}

function buildCandidateRow({ candidate, actualByDocId, queryRegressions, observedAnchorFamiliesByDoc }) {
  const docId = String(candidate?.documentId || "");
  const actual = actualByDocId.get(docId) || null;

  const dominantQueryRegressions = (queryRegressions || [])
    .filter((row) => Number(row.qualityDelta || 0) < 0)
    .filter((row) => Number(row.activatedDocTop10Hits || 0) > 0)
    .map((row) => ({ queryId: row.queryId, qualityDelta: row.qualityDelta, activatedDocTop10Hits: row.activatedDocTop10Hits }))
    .slice(0, 5);

  const dominantChunkTypes = (candidate?.chunkTypeProfile || []).slice(0, 5);
  const dominantAnchorsOrFamilies = observedAnchorFamiliesByDoc.get(docId) || [];

  const simulatedDelta = Number(candidate?.simulatedQualityDelta || 0);
  const actualDelta = actual ? Number(actual.actualQualityDeltaIfKnown) : null;
  const predictionError = actualDelta === null ? null : Number((simulatedDelta - actualDelta).toFixed(2));

  const familyRiskSignals = [];
  if ((candidate?.sectionLabelProfile || [])[0]?.key === "body") familyRiskSignals.push("section_label_monoculture_body");
  if ((candidate?.chunkTypeProfile || []).length === 1) familyRiskSignals.push("chunk_type_monoculture");
  if (simulatedDelta >= 5) familyRiskSignals.push("high_simulation_gain_signal");
  if (actualDelta !== null && actualDelta < -0.5) familyRiskSignals.push("proven_live_quality_regression");

  let recommendedDisposition = "exclude_until_model_change";
  if (actualDelta === null) recommendedDisposition = "hold_pending_family_probe_model_update";
  else if (actualDelta < -0.5) recommendedDisposition = "freeze_family_pending_model_change";

  return {
    documentId: docId,
    simulatedQualityDelta: simulatedDelta,
    actualQualityDeltaIfKnown: actualDelta,
    predictionError,
    dominantQueryRegressions,
    dominantChunkTypes,
    dominantAnchorsOrFamilies,
    familyRiskSignals,
    recommendedDisposition
  };
}

export function buildR52Postmortem({ r48Report, r50Report, r51ActivationReport, r51LiveQaReport }) {
  const frozenFamilyLabel = String(r50Report?.probeFamilyLabel || "");
  const familyDocIds = unique(r50Report?.probeCandidateIds || []);

  const familyCandidates = (r48Report?.candidateRows || [])
    .filter((row) => familyDocIds.includes(String(row?.documentId || "")))
    .sort((a, b) => String(a?.documentId || "").localeCompare(String(b?.documentId || "")));

  const targetDocId = String(r51ActivationReport?.docActivatedExact || "");
  const beforeQuality = Number(r51ActivationReport?.beforeLiveMetrics?.averageQualityScore || 0);
  const afterQuality = Number(r51ActivationReport?.afterLiveMetrics?.averageQualityScore || 0);
  const actualDelta = Number((afterQuality - beforeQuality).toFixed(2));

  const actualByDocId = new Map();
  if (targetDocId) {
    actualByDocId.set(targetDocId, {
      documentId: targetDocId,
      actualQualityDeltaIfKnown: actualDelta,
      keepOrRollbackDecision: String(r51ActivationReport?.keepOrRollbackDecision || ""),
      freezeDecision: String(r51ActivationReport?.freezeDecision || "")
    });
  }

  const queryRegressions = buildQueryLevelRegressionBreakdown({
    beforeRows: r51LiveQaReport?.beforeQueryResults || [],
    afterRows: r51LiveQaReport?.afterQueryResults || [],
    targetDocId
  });

  const afterRows = r51LiveQaReport?.afterQueryResults || [];
  const observedAnchorFamiliesByDoc = new Map();
  for (const docId of familyDocIds) {
    const anchors = afterRows
      .flatMap((row) => (row?.topResults || []).slice(0, 10))
      .filter((row) => String(row?.documentId || "") === String(docId))
      .map((row) => anchorFamily(row?.citationAnchor || ""));
    observedAnchorFamiliesByDoc.set(docId, topNCount(anchors, 5));
  }

  const candidateRows = familyCandidates.map((candidate) =>
    buildCandidateRow({ candidate, actualByDocId, queryRegressions, observedAnchorFamiliesByDoc })
  );

  const simulationVsLiveDelta = {
    baselineAverageQualityScore: beforeQuality,
    liveAverageQualityScoreAfterProbe: afterQuality,
    actualQualityDelta: actualDelta,
    simulatedQualityDeltaForActivatedDoc: Number(
      (familyCandidates.find((row) => String(row.documentId) === targetDocId)?.simulatedQualityDelta || 0)
    ),
    predictionErrorForActivatedDoc: Number(
      (
        Number(familyCandidates.find((row) => String(row.documentId) === targetDocId)?.simulatedQualityDelta || 0) - actualDelta
      ).toFixed(2)
    )
  };

  const familyRiskFactors = [
    "query_level_regressions_hidden_by_average_projection",
    "section_label_monoculture_body",
    "analysis_reasoning_monoculture",
    "lexical_rerank_overestimation_for_fallback_family",
    "document_level_insertion_effect_not_captured_by_simulation"
  ];

  const recommendedFrontierPolicyChanges = [
    "add_blocker_family_label:family_probe_quality_misprediction",
    "add_predicted_risk_field:requires_real_probe_before_safe_classification",
    "add_predicted_risk_field:simulation_to_live_error_budget",
    "exclude_family_from_single_doc_safe_frontier_until_model_update"
  ];

  const recommendedFreezeDisposition = "freeze_family_pending_model_change";
  const mayUnfreezeWithoutModelChange = false;

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R52",
    summary: {
      frozenFamilyLabel,
      freezeDecisionFromR51: String(r51ActivationReport?.freezeDecision || ""),
      freezeReasonFromR51: String(r51ActivationReport?.freezeReason || "")
    },
    frozenFamilyLabel,
    documentsAnalyzed: familyDocIds.length,
    simulationVsLiveDelta,
    queryLevelRegressionBreakdown: queryRegressions,
    familyRiskFactors,
    recommendedFrontierPolicyChanges,
    recommendedFreezeDisposition,
    mayUnfreezeWithoutModelChange,
    candidateRows
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R52 Frozen Family Postmortem (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push(`- frozenFamilyLabel: ${report.frozenFamilyLabel}`);
  lines.push(`- documentsAnalyzed: ${report.documentsAnalyzed}`);
  lines.push(`- recommendedFreezeDisposition: ${report.recommendedFreezeDisposition}`);
  lines.push(`- mayUnfreezeWithoutModelChange: ${report.mayUnfreezeWithoutModelChange}`);
  lines.push("");

  lines.push("## Simulation vs Live Delta");
  for (const [k, v] of Object.entries(report.simulationVsLiveDelta || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Query-Level Regression Breakdown");
  for (const row of (report.queryLevelRegressionBreakdown || []).filter((r) => Number(r.qualityDelta || 0) < 0)) {
    lines.push(`- ${row.queryId}: qualityDelta=${row.qualityDelta}, activatedDocTop10Hits=${row.activatedDocTop10Hits}`);
  }
  if (!(report.queryLevelRegressionBreakdown || []).some((r) => Number(r.qualityDelta || 0) < 0)) lines.push("- none");
  lines.push("");

  lines.push("## Candidate Rows");
  for (const row of report.candidateRows || []) {
    lines.push(
      `- ${row.documentId}: simulatedDelta=${row.simulatedQualityDelta}, actualDelta=${row.actualQualityDeltaIfKnown}, predictionError=${row.predictionError}, recommendedDisposition=${row.recommendedDisposition}`
    );
  }
  lines.push("");

  lines.push("## Recommended Frontier Policy Changes");
  for (const item of report.recommendedFrontierPolicyChanges || []) lines.push(`- ${item}`);
  lines.push("");

  lines.push("- Dry-run only. No activation, rollback, or runtime policy mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [r48, r50, r51Activation, r51LiveQa] = await Promise.all([
    readJson(path.resolve(reportsDir, r48Name)),
    readJson(path.resolve(reportsDir, r50Name)),
    readJson(path.resolve(reportsDir, r51ActivationName)),
    readJson(path.resolve(reportsDir, r51LiveQaName))
  ]);

  const report = buildR52Postmortem({
    r48Report: r48,
    r50Report: r50,
    r51ActivationReport: r51Activation,
    r51LiveQaReport: r51LiveQa
  });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);

  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        frozenFamilyLabel: report.frozenFamilyLabel,
        documentsAnalyzed: report.documentsAnalyzed,
        recommendedFreezeDisposition: report.recommendedFreezeDisposition,
        mayUnfreezeWithoutModelChange: report.mayUnfreezeWithoutModelChange
      },
      null,
      2
    )
  );
  console.log(`R52 postmortem report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
