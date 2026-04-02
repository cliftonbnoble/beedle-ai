import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");

const r46ReportName = process.env.RETRIEVAL_R48_R46_REPORT_NAME || "retrieval-r46-single-frontier-report.json";
const r47ReportName = process.env.RETRIEVAL_R48_R47_REPORT_NAME || "retrieval-r47-single-activation-report.json";
const outputJsonName =
  process.env.RETRIEVAL_R48_QUALITY_AUDIT_REPORT_NAME || "retrieval-r48-frontier-quality-audit-report.json";
const outputMdName =
  process.env.RETRIEVAL_R48_QUALITY_AUDIT_MARKDOWN_NAME || "retrieval-r48-frontier-quality-audit-report.md";

const QUALITY_GATE_DELTA = Number(process.env.RETRIEVAL_R48_QUALITY_GATE_DELTA || "-0.5");

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean).map((v) => String(v)))).sort((a, b) => a.localeCompare(b));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCountEntries(obj = {}) {
  return Object.entries(obj)
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .map(([key, count]) => ({ key, count }));
}

function normalize(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function isLowSignal(label) {
  const t = normalize(label);
  return /(^|_)(caption|caption_title|issue_statement|appearances|questions_presented|parties|appearance)(_|$)/.test(t);
}

function chunkProfile(chunks = []) {
  const chunkTypes = chunks.map((c) => normalize(c.chunkType || c.sectionLabel || ""));
  const sectionLabels = chunks.map((c) => normalize(c.sectionLabel || ""));
  const chunkTypeCounts = sortCountEntries(countBy(chunkTypes));
  const sectionLabelCounts = sortCountEntries(countBy(sectionLabels));
  const lowSignalCount = chunks.filter((c) => isLowSignal(c.chunkType || c.sectionLabel || "")).length;
  const lowSignalShare = chunks.length ? Number((lowSignalCount / chunks.length).toFixed(4)) : 0;
  const dominantChunkType = chunkTypeCounts[0]?.key || "unknown";
  const secondaryChunkType = chunkTypeCounts[1]?.key || "none";
  const sizeBucket = chunks.length <= 10 ? "short" : chunks.length <= 20 ? "medium" : "long";
  const lowSignalBucket = lowSignalShare >= 0.2 ? "low_signal_heavy" : lowSignalShare > 0 ? "low_signal_present" : "low_signal_absent";
  const familyLabel = `${lowSignalBucket}::${sizeBucket}::${dominantChunkType}+${secondaryChunkType}`;
  return {
    chunkCount: chunks.length,
    lowSignalChunkCount: lowSignalCount,
    lowSignalChunkShare: lowSignalShare,
    chunkTypeCounts,
    sectionLabelCounts,
    dominantChunkType,
    secondaryChunkType,
    familyLabel
  };
}

async function readJsonIfExists(filePath) {
  try {
    return JSON.parse(await fs.readFile(filePath, "utf8"));
  } catch {
    return null;
  }
}

async function readJsonStrict(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function fetchJson(url) {
  const response = await fetch(url);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url}, got non-JSON response.`);
  }
  if (!response.ok) throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  return body;
}

function knownRealOutcomeMap(r47Report = null) {
  if (!r47Report) return new Map();
  const docId = String(r47Report.docActivatedExact || "");
  if (!docId) return new Map();
  const before = Number(r47Report?.beforeLiveMetrics?.averageQualityScore || 0);
  const after = Number(r47Report?.afterLiveMetrics?.averageQualityScore || 0);
  const delta = Number((after - before).toFixed(2));
  const qualityGateFailed = (r47Report?.hardGate?.failures || []).includes("qualityNotMateriallyRegressed");
  return new Map([
    [
      docId,
      {
        docId,
        keepOrRollbackDecision: String(r47Report.keepOrRollbackDecision || ""),
        actualKnownQualityDelta: delta,
        qualityGateFailed,
        anomalyFlags: (r47Report.anomalyFlags || []).map(String)
      }
    ]
  ]);
}

function buildFamilyRiskBreakdown(rows = []) {
  const grouped = new Map();
  for (const row of rows) {
    const key = String(row.documentFamilyLabel || "unknown");
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(row);
  }

  const breakdown = [];
  for (const [family, members] of grouped.entries()) {
    const known = members.filter((row) => row.actualKnownQualityDelta !== null);
    const knownMiss = known.filter((row) => Number(row.actualKnownQualityDelta || 0) < Number(QUALITY_GATE_DELTA || -0.5));
    const avgSimDelta = members.length
      ? Number((members.reduce((sum, row) => sum + Number(row.simulatedQualityDelta || 0), 0) / members.length).toFixed(3))
      : 0;
    breakdown.push({
      documentFamilyLabel: family,
      candidateCount: members.length,
      knownOutcomeCount: known.length,
      knownQualityMissCount: knownMiss.length,
      knownQualityMissRate: known.length ? Number((knownMiss.length / known.length).toFixed(4)) : 0,
      averageSimulatedQualityDelta: avgSimDelta,
      representativeDocIds: unique(members.map((row) => row.documentId)).slice(0, 10)
    });
  }

  return breakdown.sort((a, b) => {
    if (b.knownQualityMissRate !== a.knownQualityMissRate) return b.knownQualityMissRate - a.knownQualityMissRate;
    if (a.averageSimulatedQualityDelta !== b.averageSimulatedQualityDelta)
      return a.averageSimulatedQualityDelta - b.averageSimulatedQualityDelta;
    return String(a.documentFamilyLabel).localeCompare(String(b.documentFamilyLabel));
  });
}

function classifyRisk(row, familyRiskMap) {
  const familyRisk = familyRiskMap.get(String(row.documentFamilyLabel || ""));
  if (Number(row.actualKnownQualityDelta || 0) < Number(QUALITY_GATE_DELTA || -0.5)) return "high";
  if (Number(familyRisk?.knownQualityMissRate || 0) > 0) return "high";
  if (Number(row.simulatedQualityDelta || 0) < 0.25) return "medium";
  if (Number(row.simulatedQualityDelta || 0) < 0.75) return "medium";
  return "low";
}

export function buildR48Audit({ r46Report, r47Report = null, profilesByDocId = new Map() }) {
  const baseline = {
    averageQualityScore: Number(r46Report?.averageQualityScore || r46Report?.baselineLiveMetrics?.averageQualityScore || 0),
    effectiveCitationCeiling: Number(r46Report?.effectiveCitationCeiling || r46Report?.baselineLiveMetrics?.effectiveCitationCeiling || 0)
  };

  const safeRows = Array.isArray(r46Report?.safeSingleCandidates) ? r46Report.safeSingleCandidates : [];
  const knownOutcome = knownRealOutcomeMap(r47Report);

  const candidateRows = safeRows
    .map((row) => {
      const docId = String(row.documentId || "");
      const known = knownOutcome.get(docId) || null;
      const profile = profilesByDocId.get(docId) || {
        chunkCount: null,
        lowSignalChunkShare: null,
        chunkTypeCounts: [],
        sectionLabelCounts: [],
        familyLabel: "unknown"
      };
      const simulatedDelta = Number(row.projectedQualityDelta ?? row?.metrics?.qualityDelta ?? 0);
      const actualDelta = known ? Number(known.actualKnownQualityDelta) : null;
      const predictionError = actualDelta === null ? null : Number((simulatedDelta - actualDelta).toFixed(2));
      return {
        documentId: docId,
        title: String(row.title || ""),
        simulatedAverageQualityScore: Number(row.projectedAverageQualityScore ?? row?.metrics?.averageQualityScoreAfter ?? 0),
        simulatedQualityDelta: simulatedDelta,
        actualKnownQualityDelta: actualDelta,
        qualityPredictionError: predictionError,
        projectedCitationTopDocumentShare: Number(row.projectedCitationTopDocumentShare ?? row?.metrics?.citationTopDocumentShareAfter ?? 0),
        projectedLowSignalStructuralShare: Number(row.projectedLowSignalStructuralShare ?? row?.metrics?.lowSignalStructuralShareAfter ?? 0),
        projectedOutOfCorpusHitQueryCount: Number(row.projectedOutOfCorpusHitQueryCount ?? row?.metrics?.outOfCorpusHitQueryCountAfter ?? 0),
        projectedZeroTrustedResultQueryCount: Number(
          row.projectedZeroTrustedResultQueryCount ?? row?.metrics?.zeroTrustedResultQueryCountAfter ?? 0
        ),
        projectedProvenanceCompletenessAverage: Number(
          row.projectedProvenanceCompletenessAverage ?? row?.metrics?.provenanceCompletenessAverageAfter ?? 0
        ),
        projectedCitationAnchorCoverageAverage: Number(
          row.projectedCitationAnchorCoverageAverage ?? row?.metrics?.citationAnchorCoverageAverageAfter ?? 0
        ),
        blockerFamilies: (row.blockerFamilies || []).map(String),
        improvementSignals: (row.improvementSignals || []).map(String),
        regressionSignals: (row.regressionSignals || []).map(String),
        documentFamilyLabel: String(profile.familyLabel || "unknown"),
        chunkTypeProfile: (profile.chunkTypeCounts || []).slice(0, 5),
        sectionLabelProfile: (profile.sectionLabelCounts || []).slice(0, 5),
        lowSignalChunkShareByProfile: profile.lowSignalChunkShare,
        r46WasSafe: true,
        r47KnownOutcome: known ? known.keepOrRollbackDecision : "unknown"
      };
    })
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const familyRiskBreakdown = buildFamilyRiskBreakdown(candidateRows);
  const familyRiskMap = new Map(familyRiskBreakdown.map((row) => [String(row.documentFamilyLabel), row]));

  for (const row of candidateRows) {
    row.riskLabel = classifyRisk(row, familyRiskMap);
  }

  const simulationVsRealityRows = candidateRows.filter((row) => row.actualKnownQualityDelta !== null);
  const qualityMispredictionCount = simulationVsRealityRows.filter(
    (row) => Number(row.simulatedQualityDelta || 0) >= 0 && Number(row.actualKnownQualityDelta || 0) < Number(QUALITY_GATE_DELTA || -0.5)
  ).length;

  const blockerFamilyCounts = sortCountEntries(countBy(candidateRows.flatMap((row) => row.blockerFamilies || [])));

  const failedDocId = String(r47Report?.docActivatedExact || "");
  const failedRow = candidateRows.find((row) => row.documentId === failedDocId) || null;
  const failedFamily = String(failedRow?.documentFamilyLabel || "");
  const failedFamilyStats = familyRiskMap.get(failedFamily) || null;
  const failedCandidateRepresentative = Boolean(failedFamilyStats && failedFamilyStats.candidateCount > 1);

  const eligibleRows = candidateRows
    .filter((row) => row.riskLabel !== "high")
    .filter((row) => Number(row.simulatedQualityDelta || 0) >= 0.5)
    .filter((row) => Number(row.projectedCitationTopDocumentShare || 0) <= Number(baseline.effectiveCitationCeiling || 0))
    .filter((row) => Number(row.projectedLowSignalStructuralShare || 0) <= 0)
    .filter((row) => Number(row.projectedOutOfCorpusHitQueryCount || 0) === 0)
    .filter((row) => Number(row.projectedZeroTrustedResultQueryCount || 0) === 0)
    .filter((row) => Number(row.projectedProvenanceCompletenessAverage || 0) === 1)
    .filter((row) => Number(row.projectedCitationAnchorCoverageAverage || 0) === 1)
    .sort((a, b) => {
      if (b.simulatedQualityDelta !== a.simulatedQualityDelta) return b.simulatedQualityDelta - a.simulatedQualityDelta;
      if (a.projectedCitationTopDocumentShare !== b.projectedCitationTopDocumentShare)
        return a.projectedCitationTopDocumentShare - b.projectedCitationTopDocumentShare;
      return String(a.documentId).localeCompare(String(b.documentId));
    });

  const nextBestCandidateIfAny = eligibleRows[0] || null;

  const noTrustworthyClass = eligibleRows.length === 0;
  const activationRecommendation = noTrustworthyClass ? "no" : "yes";
  const recommendedNextStep = noTrustworthyClass
    ? "do_not_activate_any_more_singles"
    : `safe_single_doc_activation_candidate:${nextBestCandidateIfAny.documentId}`;

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R48",
    summary: {
      baselineAverageQualityScore: baseline.averageQualityScore,
      baselineEffectiveCitationCeiling: baseline.effectiveCitationCeiling,
      failedR47DocId: failedDocId,
      failedCandidateRepresentative,
      failedCandidateFamily: failedFamily
    },
    candidatesScanned: Number(r46Report?.candidatesScanned || 0),
    safeCandidatesEvaluated: candidateRows.length,
    candidatesWithKnownRealOutcome: simulationVsRealityRows.length,
    qualityMispredictionCount,
    blockerFamilyCounts,
    familyRiskBreakdown,
    simulationVsRealityRows,
    nextBestCandidateIfAny,
    activationRecommendation,
    recommendedNextStep,
    candidateRows
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R48 Frontier Quality Audit (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push(`- candidatesScanned: ${report.candidatesScanned}`);
  lines.push(`- safeCandidatesEvaluated: ${report.safeCandidatesEvaluated}`);
  lines.push(`- candidatesWithKnownRealOutcome: ${report.candidatesWithKnownRealOutcome}`);
  lines.push(`- qualityMispredictionCount: ${report.qualityMispredictionCount}`);
  lines.push(`- activationRecommendation: ${report.activationRecommendation}`);
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push("");

  lines.push("## Family Risk Breakdown");
  for (const row of report.familyRiskBreakdown || []) {
    lines.push(
      `- ${row.documentFamilyLabel}: count=${row.candidateCount}, knownOutcomes=${row.knownOutcomeCount}, knownQualityMissRate=${row.knownQualityMissRate}, avgSimDelta=${row.averageSimulatedQualityDelta}`
    );
  }
  if (!(report.familyRiskBreakdown || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Simulation vs Reality");
  for (const row of report.simulationVsRealityRows || []) {
    lines.push(
      `- ${row.documentId}: simulatedDelta=${row.simulatedQualityDelta}, actualDelta=${row.actualKnownQualityDelta}, predictionError=${row.qualityPredictionError}, family=${row.documentFamilyLabel}`
    );
  }
  if (!(report.simulationVsRealityRows || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Next Best Candidate");
  if (report.nextBestCandidateIfAny) {
    lines.push(
      `- ${report.nextBestCandidateIfAny.documentId} | simulatedDelta=${report.nextBestCandidateIfAny.simulatedQualityDelta} | riskLabel=${report.nextBestCandidateIfAny.riskLabel}`
    );
  } else {
    lines.push("- none");
  }
  lines.push("");
  lines.push("- Dry-run only. No activation, rollback, or policy mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const r46 = await readJsonStrict(path.resolve(reportsDir, r46ReportName));
  const r47 = await readJsonIfExists(path.resolve(reportsDir, r47ReportName));

  const safeDocIds = unique((r46?.safeSingleCandidates || []).map((row) => row.documentId));
  const profilesByDocId = new Map();

  for (const docId of safeDocIds) {
    const preview = await fetchJson(`${apiBase}/admin/retrieval/documents/${docId}/chunks?includeText=0`);
    const profile = chunkProfile(Array.isArray(preview?.chunks) ? preview.chunks : []);
    profilesByDocId.set(docId, profile);
  }

  const report = buildR48Audit({ r46Report: r46, r47Report: r47, profilesByDocId });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);

  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, buildMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        candidatesScanned: report.candidatesScanned,
        safeCandidatesEvaluated: report.safeCandidatesEvaluated,
        candidatesWithKnownRealOutcome: report.candidatesWithKnownRealOutcome,
        qualityMispredictionCount: report.qualityMispredictionCount,
        activationRecommendation: report.activationRecommendation,
        recommendedNextStep: report.recommendedNextStep
      },
      null,
      2
    )
  );
  console.log(`R48 quality audit report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
