import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const inputName =
  process.env.RETRIEVAL_R39_FRONTIER_INPUT_NAME || "retrieval-r38-single-frontier-refresh-report.json";
const outputJsonName =
  process.env.RETRIEVAL_R39_FRONTIER_BLOCKER_BREAKDOWN_REPORT_NAME ||
  "retrieval-r39-frontier-blocker-breakdown-report.json";
const outputMdName =
  process.env.RETRIEVAL_R39_FRONTIER_BLOCKER_BREAKDOWN_MARKDOWN_NAME ||
  "retrieval-r39-frontier-blocker-breakdown-report.md";

const QUALITY_GATE = "qualityNotMateriallyRegressed";
const CITATION_GATE = "citationTopDocumentShareAtOrBelowEffectiveCeiling";
const LOW_SIGNAL_GATE = "lowSignalStructuralShareNotWorsened";

const HARD_GATES = new Set([
  QUALITY_GATE,
  CITATION_GATE,
  LOW_SIGNAL_GATE,
  "outOfCorpusHitQueryCountZero",
  "zeroTrustedResultQueryCountZero",
  "provenanceCompletenessOne",
  "citationAnchorCoverageOne"
]);

const BLOCKER_PRIORITY = [
  QUALITY_GATE,
  CITATION_GATE,
  LOW_SIGNAL_GATE,
  "outOfCorpusHitQueryCountZero",
  "zeroTrustedResultQueryCountZero",
  "provenanceCompletenessOne",
  "citationAnchorCoverageOne",
  "candidate_preview_unreadable",
  "offline_missing_candidate_simulation"
];

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCountEntries(obj) {
  return Object.entries(obj || {})
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .map(([key, count]) => ({ key, count }));
}

function normalize(text) {
  return String(text || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function priorityIndex(gate) {
  const idx = BLOCKER_PRIORITY.indexOf(String(gate || ""));
  return idx === -1 ? Number.MAX_SAFE_INTEGER : idx;
}

function sortGates(gates = []) {
  return [...gates].sort((a, b) => {
    const aIdx = priorityIndex(a);
    const bIdx = priorityIndex(b);
    if (aIdx !== bIdx) return aIdx - bIdx;
    return String(a).localeCompare(String(b));
  });
}

export function classifyDocumentFamilyLabel(row = {}) {
  const title = String(row?.title || "").toLowerCase();
  if (/retrieval messy headings/.test(title)) return "retrieval_messy_headings";
  if (/retrieval fallback/.test(title)) return "retrieval_fallback";
  if (/approval rollout/.test(title)) return "approval_rollout";
  if (/fixture runtime candidate/.test(title)) return "fixture_runtime_candidate";

  const topSection = row?.dominantFeatureDiagnosis?.sectionLabelProfile?.[0]?.key;
  if (topSection) return `section_${normalize(topSection)}`;
  const topChunk = row?.dominantFeatureDiagnosis?.chunkTypeMix?.[0]?.key;
  if (topChunk) return `chunk_${normalize(topChunk)}`;
  return "other";
}

export function evaluateSingleGateCounterfactuals(failingGates = []) {
  const sorted = sortGates(unique(failingGates));
  return {
    wouldPassIfOnlyQualityGateRelaxed: sorted.length === 1 && sorted[0] === QUALITY_GATE,
    wouldPassIfOnlyCitationConcentrationRelaxed: sorted.length === 1 && sorted[0] === CITATION_GATE,
    wouldPassIfOnlyLowSignalGateRelaxed: sorted.length === 1 && sorted[0] === LOW_SIGNAL_GATE
  };
}

export function analyzeR39Frontier(input = {}) {
  const rows = Array.isArray(input?.candidateRows) ? input.candidateRows : [];
  const baseline = input?.baselineLiveMetrics || {};

  const candidateRows = rows
    .map((row) => {
      const failingGates = sortGates(unique(row?.failingGates || []));
      const blockerFamilies = unique(row?.blockerFamilies || []);
      const primaryBlocker = failingGates[0] || "none";
      const secondaryBlockers = failingGates.slice(1);
      const cf = evaluateSingleGateCounterfactuals(failingGates);
      const documentFamilyLabel = classifyDocumentFamilyLabel(row);
      return {
        documentId: String(row?.documentId || ""),
        title: String(row?.title || ""),
        keepOrDoNotActivate: String(row?.keepOrDoNotActivate || "do_not_activate"),
        primaryBlocker,
        secondaryBlockers,
        blockerFamilies,
        projectedAverageQualityScore: Number(row?.projectedAverageQualityScore || 0),
        projectedQualityDelta: Number(row?.projectedQualityDelta || 0),
        projectedCitationTopDocumentShare: Number(row?.projectedCitationTopDocumentShare || 0),
        projectedLowSignalStructuralShare: Number(row?.projectedLowSignalStructuralShare || 0),
        projectedOutOfCorpusHitQueryCount: Number(row?.projectedOutOfCorpusHitQueryCount || 0),
        projectedZeroTrustedResultQueryCount: Number(row?.projectedZeroTrustedResultQueryCount || 0),
        projectedProvenanceCompletenessAverage: Number(row?.projectedProvenanceCompletenessAverage || 0),
        projectedCitationAnchorCoverageAverage: Number(row?.projectedCitationAnchorCoverageAverage || 0),
        wouldPassIfOnlyQualityGateRelaxed: cf.wouldPassIfOnlyQualityGateRelaxed,
        wouldPassIfOnlyCitationConcentrationRelaxed: cf.wouldPassIfOnlyCitationConcentrationRelaxed,
        wouldPassIfOnlyLowSignalGateRelaxed: cf.wouldPassIfOnlyLowSignalGateRelaxed,
        documentFamilyLabel,
        improvementSignals: unique(row?.improvementSignals || []),
        regressionSignals: unique(row?.regressionSignals || []),
        failingGates
      };
    })
    .filter((row) => row.documentId)
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const safeRows = candidateRows.filter((row) => row.keepOrDoNotActivate === "keep");
  const blockedRows = candidateRows.filter((row) => row.keepOrDoNotActivate !== "keep");

  const blockerFamilyCounts = sortCountEntries(countBy(blockedRows.flatMap((row) => row.blockerFamilies || [])));
  const primaryBlockerCounts = sortCountEntries(countBy(blockedRows.map((row) => row.primaryBlocker)));
  const secondaryBlockerCounts = sortCountEntries(countBy(blockedRows.flatMap((row) => row.secondaryBlockers || [])));

  const nearMissCandidates = blockedRows.filter((row) => (row.failingGates || []).filter((gate) => HARD_GATES.has(gate)).length === 1);

  const blockerCoOccurrenceMatrix = {};
  const families = unique(blockedRows.flatMap((row) => row.blockerFamilies || []));
  for (const a of families) {
    blockerCoOccurrenceMatrix[a] = {};
    for (const b of families) blockerCoOccurrenceMatrix[a][b] = 0;
  }
  for (const row of blockedRows) {
    const fams = unique(row.blockerFamilies || []);
    for (const a of fams) {
      for (const b of fams) {
        blockerCoOccurrenceMatrix[a][b] += 1;
      }
    }
  }

  const familyMap = new Map();
  for (const row of candidateRows) {
    const key = row.documentFamilyLabel;
    if (!familyMap.has(key)) {
      familyMap.set(key, {
        documentFamilyLabel: key,
        count: 0,
        safeCount: 0,
        blockedCount: 0,
        nearMissCount: 0,
        primaryBlockers: []
      });
    }
    const cur = familyMap.get(key);
    cur.count += 1;
    if (row.keepOrDoNotActivate === "keep") cur.safeCount += 1;
    else cur.blockedCount += 1;
    if ((row.failingGates || []).filter((gate) => HARD_GATES.has(gate)).length === 1) cur.nearMissCount += 1;
    if (row.primaryBlocker && row.primaryBlocker !== "none") cur.primaryBlockers.push(row.primaryBlocker);
  }
  const candidateFamilyBreakdown = [...familyMap.values()]
    .map((row) => ({
      ...row,
      primaryBlockerCounts: sortCountEntries(countBy(row.primaryBlockers))
    }))
    .sort((a, b) => {
      if (b.count !== a.count) return b.count - a.count;
      return String(a.documentFamilyLabel).localeCompare(String(b.documentFamilyLabel));
    });

  const wouldPassIfOnlyOneGateChangedCandidates = blockedRows
    .filter(
      (row) =>
        row.wouldPassIfOnlyQualityGateRelaxed ||
        row.wouldPassIfOnlyCitationConcentrationRelaxed ||
        row.wouldPassIfOnlyLowSignalGateRelaxed
    )
    .map((row) => ({
      documentId: row.documentId,
      title: row.title,
      primaryBlocker: row.primaryBlocker,
      documentFamilyLabel: row.documentFamilyLabel,
      wouldPassIfOnlyQualityGateRelaxed: row.wouldPassIfOnlyQualityGateRelaxed,
      wouldPassIfOnlyCitationConcentrationRelaxed: row.wouldPassIfOnlyCitationConcentrationRelaxed,
      wouldPassIfOnlyLowSignalGateRelaxed: row.wouldPassIfOnlyLowSignalGateRelaxed
    }))
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const topFamily = candidateFamilyBreakdown[0];
  let recommendedNextMove = "stop_expansion";
  if (safeRows.length > 0) recommendedNextMove = "create_new_stricter_candidate_filter";
  else if (wouldPassIfOnlyOneGateChangedCandidates.length > 0) recommendedNextMove = "retune_one_specific_gate";
  else if (topFamily && topFamily.count / Math.max(1, blockedRows.length) >= 0.35) recommendedNextMove = "repair_specific_candidate_family";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R39",
    sourceDataMode: String(input?.dataMode || "unknown"),
    summary: {
      candidatesScanned: candidateRows.length,
      safeCandidateCount: safeRows.length,
      blockedCandidateCount: blockedRows.length,
      nearMissCandidateCount: nearMissCandidates.length,
      wouldPassIfOnlyQualityGateRelaxedCount: wouldPassIfOnlyOneGateChangedCandidates.filter(
        (row) => row.wouldPassIfOnlyQualityGateRelaxed
      ).length,
      wouldPassIfOnlyCitationConcentrationRelaxedCount: wouldPassIfOnlyOneGateChangedCandidates.filter(
        (row) => row.wouldPassIfOnlyCitationConcentrationRelaxed
      ).length,
      wouldPassIfOnlyLowSignalGateRelaxedCount: wouldPassIfOnlyOneGateChangedCandidates.filter(
        (row) => row.wouldPassIfOnlyLowSignalGateRelaxed
      ).length,
      recommendedNextMove
    },
    baselineLiveMetrics: baseline,
    candidatesScanned: candidateRows.length,
    safeCandidateCount: safeRows.length,
    blockedCandidateCount: blockedRows.length,
    blockerFamilyCounts,
    primaryBlockerCounts,
    secondaryBlockerCounts,
    nearMissCandidateCount: nearMissCandidates.length,
    nearMissCandidates: nearMissCandidates.map((row) => ({
      documentId: row.documentId,
      title: row.title,
      primaryBlocker: row.primaryBlocker,
      documentFamilyLabel: row.documentFamilyLabel
    })),
    blockerCoOccurrenceMatrix,
    candidateFamilyBreakdown,
    wouldPassIfOnlyQualityGateRelaxedCount: wouldPassIfOnlyOneGateChangedCandidates.filter(
      (row) => row.wouldPassIfOnlyQualityGateRelaxed
    ).length,
    wouldPassIfOnlyCitationConcentrationRelaxedCount: wouldPassIfOnlyOneGateChangedCandidates.filter(
      (row) => row.wouldPassIfOnlyCitationConcentrationRelaxed
    ).length,
    wouldPassIfOnlyLowSignalGateRelaxedCount: wouldPassIfOnlyOneGateChangedCandidates.filter(
      (row) => row.wouldPassIfOnlyLowSignalGateRelaxed
    ).length,
    wouldPassIfOnlyOneGateChangedCandidates,
    candidateRows
  };
}

function formatMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R39 Frontier Blocker Breakdown (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push("");
  lines.push("## Blocker Families");
  for (const row of report.blockerFamilyCounts || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.blockerFamilyCounts || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Primary Blockers");
  for (const row of report.primaryBlockerCounts || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.primaryBlockerCounts || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Near Miss Candidates");
  for (const row of (report.nearMissCandidates || []).slice(0, 20)) {
    lines.push(`- ${row.documentId} | blocker=${row.primaryBlocker} | family=${row.documentFamilyLabel}`);
  }
  if (!(report.nearMissCandidates || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Would Pass If One Gate Changed");
  for (const row of (report.wouldPassIfOnlyOneGateChangedCandidates || []).slice(0, 30)) {
    lines.push(
      `- ${row.documentId} | quality=${row.wouldPassIfOnlyQualityGateRelaxed} | citation=${row.wouldPassIfOnlyCitationConcentrationRelaxed} | lowSignal=${row.wouldPassIfOnlyLowSignalGateRelaxed}`
    );
  }
  if (!(report.wouldPassIfOnlyOneGateChangedCandidates || []).length) lines.push("- none");
  lines.push("");
  lines.push(`- sourceDataMode: ${report.sourceDataMode}`);
  lines.push("- Dry-run only. No activation, rollback, or gate mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const inputPath = path.resolve(reportsDir, inputName);
  const inputRaw = await fs.readFile(inputPath, "utf8");
  const input = JSON.parse(inputRaw);

  const report = analyzeR39Frontier(input);

  const reportPath = path.resolve(reportsDir, outputJsonName);
  const markdownPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(markdownPath, formatMarkdown(report))
  ]);

  console.log(
    JSON.stringify(
      {
        candidatesScanned: report.candidatesScanned,
        safeCandidateCount: report.safeCandidateCount,
        blockedCandidateCount: report.blockedCandidateCount,
        nearMissCandidateCount: report.nearMissCandidateCount,
        recommendedNextMove: report.summary?.recommendedNextMove
      },
      null,
      2
    )
  );
  console.log(`R39 blocker breakdown report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}

