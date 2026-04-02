import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const outputJsonName =
  process.env.RETRIEVAL_R43_POLICY_ADOPTION_SIMULATION_REPORT_NAME ||
  "retrieval-r43-policy-adoption-simulation-report.json";
const outputMdName =
  process.env.RETRIEVAL_R43_POLICY_ADOPTION_SIMULATION_MARKDOWN_NAME ||
  "retrieval-r43-policy-adoption-simulation-report.md";

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

async function readJson(name) {
  const raw = await fs.readFile(path.resolve(reportsDir, name), "utf8");
  return JSON.parse(raw);
}

function hasAnyUnsafeRegression(row = {}) {
  return (
    Number(row.projectedOutOfCorpusHitQueryCount || 0) > 0 ||
    Number(row.projectedZeroTrustedResultQueryCount || 0) > 0 ||
    Number(row.projectedProvenanceCompletenessAverage || 0) < 1 ||
    Number(row.projectedCitationAnchorCoverageAverage || 0) < 1
  );
}

function isLowSignalBlocked(row = {}) {
  return (row.blockerFamilies || []).includes("low_signal_structural_share_increase");
}

function isCitationBlocked(row = {}) {
  return (row.blockerFamilies || []).includes("citation_concentration_above_effective_ceiling");
}

function mapCandidate(row = {}) {
  return {
    documentId: String(row.documentId || ""),
    title: String(row.title || ""),
    documentFamilyLabel: String(row.documentFamilyLabel || "other"),
    blockerFamilies: unique(row.blockerFamilies || []),
    projectedAverageQualityScore: Number(row.projectedAverageQualityScore || 0),
    projectedQualityDelta: Number(row.projectedQualityDelta || 0),
    projectedCitationTopDocumentShare: Number(row.projectedCitationTopDocumentShare || 0),
    projectedLowSignalStructuralShare: Number(row.projectedLowSignalStructuralShare || 0),
    projectedOutOfCorpusHitQueryCount: Number(row.projectedOutOfCorpusHitQueryCount || 0),
    projectedZeroTrustedResultQueryCount: Number(row.projectedZeroTrustedResultQueryCount || 0),
    projectedProvenanceCompletenessAverage: Number(row.projectedProvenanceCompletenessAverage || 0),
    projectedCitationAnchorCoverageAverage: Number(row.projectedCitationAnchorCoverageAverage || 0),
    improvementSignals: unique(row.improvementSignals || []),
    regressionSignals: unique(row.regressionSignals || [])
  };
}

function sortEligible(rows = []) {
  return [...rows].sort((a, b) => {
    if (b.projectedQualityDelta !== a.projectedQualityDelta) return b.projectedQualityDelta - a.projectedQualityDelta;
    if (a.projectedCitationTopDocumentShare !== b.projectedCitationTopDocumentShare) {
      return a.projectedCitationTopDocumentShare - b.projectedCitationTopDocumentShare;
    }
    if (a.projectedLowSignalStructuralShare !== b.projectedLowSignalStructuralShare) {
      return a.projectedLowSignalStructuralShare - b.projectedLowSignalStructuralShare;
    }
    return String(a.documentId).localeCompare(String(b.documentId));
  });
}

export function analyzeR43PolicyAdoption({
  r39 = {},
  r42Fix = {},
  r42Rerun = {}
} = {}) {
  const candidateRows = Array.isArray(r39.candidateRows) ? r39.candidateRows : [];
  const dynamicPolicyRow = (r42Rerun.policyRows || []).find((row) => row.policyId === "dynamic_floor") || null;
  const currentPolicyRow =
    (r42Rerun.policyRows || []).find((row) => row.policyId === "current_effective_policy") || null;

  const correctedTrustedCount = Number(r42Fix?.correctedReference?.trustedDocumentCount || 0);
  const lineageSourcesUsed = unique(r42Fix?.lineageSourcesUsed || []);

  const newlyEligibleCandidates = sortEligible(
    candidateRows
      .filter((row) => isCitationBlocked(row) && !isLowSignalBlocked(row) && !hasAnyUnsafeRegression(row))
      .map(mapCandidate)
  );

  const stillBlockedCandidates = candidateRows
    .filter((row) => !newlyEligibleCandidates.find((c) => c.documentId === row.documentId))
    .map(mapCandidate)
    .sort((a, b) => String(a.documentId).localeCompare(String(b.documentId)));

  const blockerFamilyCountsAfterAdoption = sortCountEntries(
    countBy(stillBlockedCandidates.flatMap((row) => row.blockerFamilies || []))
  );

  const qualityRiskSummary = {
    newlyEligibleWithNegativeQualityDeltaCount: newlyEligibleCandidates.filter((row) => row.projectedQualityDelta < 0).length,
    avgQualityDeltaNewlyEligible: Number(
      (
        newlyEligibleCandidates.reduce((sum, row) => sum + Number(row.projectedQualityDelta || 0), 0) /
        Math.max(1, newlyEligibleCandidates.length)
      ).toFixed(4)
    )
  };

  const provenanceSafetySummary = {
    allNewlyEligibleProvenanceComplete: newlyEligibleCandidates.every(
      (row) =>
        Number(row.projectedProvenanceCompletenessAverage || 0) === 1 &&
        Number(row.projectedCitationAnchorCoverageAverage || 0) === 1
    ),
    newlyEligibleWithProvenanceOrAnchorRegressionCount: newlyEligibleCandidates.filter(
      (row) =>
        Number(row.projectedProvenanceCompletenessAverage || 0) < 1 ||
        Number(row.projectedCitationAnchorCoverageAverage || 0) < 1
    ).length
  };

  const lowSignalSafetySummary = {
    stillBlockedByLowSignalCount: stillBlockedCandidates.filter((row) =>
      (row.blockerFamilies || []).includes("low_signal_structural_share_increase")
    ).length,
    newlyEligibleLowSignalBlockedCount: newlyEligibleCandidates.filter((row) =>
      (row.blockerFamilies || []).includes("low_signal_structural_share_increase")
    ).length
  };

  const nextBest = newlyEligibleCandidates[0] || null;
  const safeToProceed = Boolean(
    correctedTrustedCount === 33 &&
      lineageSourcesUsed.includes("retrieval-r36-next-safe-single-manifest.json") &&
      dynamicPolicyRow?.baselinePasses === true &&
      dynamicPolicyRow?.currentLivePasses === true &&
      provenanceSafetySummary.allNewlyEligibleProvenanceComplete &&
      newlyEligibleCandidates.length > 0
  );

  let recommendedNextStep = "do_not_activate_yet";
  if (safeToProceed && nextBest) recommendedNextStep = `safe_single_doc_activation_candidate:${nextBest.documentId}`;
  else if (!safeToProceed && newlyEligibleCandidates.length > 0) recommendedNextStep = "candidate_family_pruning_before_activation";
  else if (newlyEligibleCandidates.length === 0) recommendedNextStep = "no_newly_eligible_candidates_under_dynamic_floor";

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R43",
    summary: {
      correctedTrustedDocCount: correctedTrustedCount,
      currentPolicyUnlockedCount: Number(currentPolicyRow?.currentlyBlockedCandidatesUnlockedCount || 0),
      proposedPolicyUnlockedCount: Number(dynamicPolicyRow?.currentlyBlockedCandidatesUnlockedCount || 0),
      newlyEligibleCandidateCount: newlyEligibleCandidates.length,
      stillBlockedCandidateCount: stillBlockedCandidates.length,
      safeToProceed
    },
    currentPolicyReference: {
      policyId: String(currentPolicyRow?.policyId || "current_effective_policy"),
      citationCeilingResolved: Number(currentPolicyRow?.citationCeilingResolved || 0),
      unlockedCount: Number(currentPolicyRow?.currentlyBlockedCandidatesUnlockedCount || 0),
      baselinePasses: Boolean(currentPolicyRow?.baselinePasses || false),
      currentLivePasses: Boolean(currentPolicyRow?.currentLivePasses || false)
    },
    proposedPolicyReference: {
      policyId: String(dynamicPolicyRow?.policyId || "dynamic_floor"),
      citationCeilingResolved: Number(dynamicPolicyRow?.citationCeilingResolved || 0),
      unlockedCount: Number(dynamicPolicyRow?.currentlyBlockedCandidatesUnlockedCount || 0),
      baselinePasses: Boolean(dynamicPolicyRow?.baselinePasses || false),
      currentLivePasses: Boolean(dynamicPolicyRow?.currentLivePasses || false),
      lineageSourcesUsed
    },
    policyDeltaSummary: {
      unlockDelta: Number(dynamicPolicyRow?.currentlyBlockedCandidatesUnlockedCount || 0) -
        Number(currentPolicyRow?.currentlyBlockedCandidatesUnlockedCount || 0),
      baselinePassChange:
        Boolean(dynamicPolicyRow?.baselinePasses || false) !== Boolean(currentPolicyRow?.baselinePasses || false),
      currentLivePassChange:
        Boolean(dynamicPolicyRow?.currentLivePasses || false) !== Boolean(currentPolicyRow?.currentLivePasses || false)
    },
    newlyEligibleCandidateCount: newlyEligibleCandidates.length,
    stillBlockedCandidateCount: stillBlockedCandidates.length,
    blockerFamilyCountsAfterAdoption,
    newlyEligibleCandidates,
    stillBlockedCandidates,
    qualityRiskSummary,
    provenanceSafetySummary,
    lowSignalSafetySummary,
    recommendedNextStep,
    safeToProceed
  };
}

function formatMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R43 Policy Adoption Simulation (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Policy Delta");
  for (const [k, v] of Object.entries(report.policyDeltaSummary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Newly Eligible Candidates");
  for (const row of (report.newlyEligibleCandidates || []).slice(0, 20)) {
    lines.push(`- ${row.documentId} | qualityDelta=${row.projectedQualityDelta} | lowSignal=${row.projectedLowSignalStructuralShare}`);
  }
  if (!(report.newlyEligibleCandidates || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Still Blocked By Family");
  for (const row of report.blockerFamilyCountsAfterAdoption || []) lines.push(`- ${row.key}: ${row.count}`);
  if (!(report.blockerFamilyCountsAfterAdoption || []).length) lines.push("- none");
  lines.push("");
  lines.push(`- recommendedNextStep: ${report.recommendedNextStep}`);
  lines.push(`- safeToProceed: ${report.safeToProceed}`);
  lines.push("- Dry-run only. No state mutation, activation, or rollback.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r39, r42Fix, r42Rerun] = await Promise.all([
    readJson("retrieval-r39-frontier-blocker-breakdown-report.json"),
    readJson("retrieval-r42-reference-lineage-fix-report.json"),
    readJson("retrieval-r42-rerun-citation-gate-sensitivity-report.json")
  ]);

  const report = analyzeR43PolicyAdoption({ r39, r42Fix, r42Rerun });

  const jsonPath = path.resolve(reportsDir, outputJsonName);
  const mdPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, formatMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        newlyEligibleCandidateCount: report.newlyEligibleCandidateCount,
        stillBlockedCandidateCount: report.stillBlockedCandidateCount,
        recommendedNextStep: report.recommendedNextStep,
        safeToProceed: report.safeToProceed
      },
      null,
      2
    )
  );
  console.log(`R43 policy adoption simulation report written to ${jsonPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}

