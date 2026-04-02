import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const reportsDir = path.resolve(process.cwd(), "reports");
const outputJsonName =
  process.env.RETRIEVAL_R40_CITATION_GATE_SENSITIVITY_REPORT_NAME || "retrieval-r40-citation-gate-sensitivity-report.json";
const outputMdName =
  process.env.RETRIEVAL_R40_CITATION_GATE_SENSITIVITY_MARKDOWN_NAME || "retrieval-r40-citation-gate-sensitivity-report.md";

const QUALITY_GATE = "qualityNotMateriallyRegressed";
const LOW_SIGNAL_GATE = "lowSignalStructuralShareNotWorsened";
const CITATION_GATE = "citationTopDocumentShareAtOrBelowEffectiveCeiling";

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

async function readJsonIfExists(name) {
  try {
    return await readJson(name);
  } catch {
    return null;
  }
}

function pickAttainableFloor({ r34Live, r34Report, r38 }) {
  const fromR34Thresholds = Number(
    r34Live?.summary?.hardGate?.thresholds?.citationTopDocumentShareCeilingAttainableFloor ?? NaN
  );
  if (Number.isFinite(fromR34Thresholds) && fromR34Thresholds > 0) return Number(fromR34Thresholds.toFixed(4));

  const fromR34Summary = Number(r34Report?.summary?.baselineCitationTopDocumentShare ?? NaN);
  if (Number.isFinite(fromR34Summary) && fromR34Summary > 0) return Number(fromR34Summary.toFixed(4));

  const fromR38Effective = Number(r38?.summary?.effectiveCitationCeiling ?? r38?.baselineLiveMetrics?.effectiveCitationCeiling ?? NaN);
  if (Number.isFinite(fromR38Effective) && fromR38Effective > 0) return Number(fromR38Effective.toFixed(4));

  return 0.1;
}

function resolveReferences({ r38, r34Live }) {
  const r34After = r34Live?.summary?.after || {};
  const r34Thresholds = r34Live?.summary?.hardGate?.thresholds || {};
  const currentBaseline = r38?.baselineLiveMetrics || {};

  return {
    baselineReference: {
      source: "r34_kept_state",
      averageQualityScore: Number(r34After?.averageQualityScore || 0),
      citationTopDocumentShare: Number(r34Live?.summary?.hardGate?.measured?.citationTopDocumentShare || 0),
      lowSignalStructuralShare: Number(r34Live?.summary?.hardGate?.measured?.afterLowSignalStructuralShare || 0),
      outOfCorpusHitQueryCount: Number(r34After?.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(r34After?.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(r34After?.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(r34After?.citationAnchorCoverageAverage || 0)
    },
    currentLiveReference: {
      source: `r38_${String(r38?.dataMode || "unknown")}`,
      averageQualityScore: Number(currentBaseline?.averageQualityScore || 0),
      citationTopDocumentShare: Number(currentBaseline?.citationTopDocumentShare || 0),
      lowSignalStructuralShare: Number(currentBaseline?.lowSignalStructuralShare || 0),
      outOfCorpusHitQueryCount: Number(currentBaseline?.outOfCorpusHitQueryCount || 0),
      zeroTrustedResultQueryCount: Number(currentBaseline?.zeroTrustedResultQueryCount || 0),
      provenanceCompletenessAverage: Number(currentBaseline?.provenanceCompletenessAverage || 0),
      citationAnchorCoverageAverage: Number(currentBaseline?.citationAnchorCoverageAverage || 0)
    },
    thresholdReference: {
      qualityFloor: Number(r34Thresholds?.qualityFloor || 64.72),
      lowSignalStructuralShareCeiling: Number(r34Thresholds?.lowSignalStructuralShareCeiling || 0),
      configuredGlobalCeiling: Number(r34Thresholds?.citationTopDocumentShareCeilingConfigured || 0.1)
    }
  };
}

export function evaluateReferenceAgainstPolicy({ reference, thresholdReference, citationCeilingResolved }) {
  const checks = {
    qualityAtOrAboveFloor: Number(reference?.averageQualityScore || 0) >= Number(thresholdReference?.qualityFloor || 0),
    citationTopDocumentShareAtOrBelowCeiling: Number(reference?.citationTopDocumentShare || 0) <= Number(citationCeilingResolved || 0),
    lowSignalStructuralShareNotWorse:
      Number(reference?.lowSignalStructuralShare || 0) <= Number(thresholdReference?.lowSignalStructuralShareCeiling || 0),
    outOfCorpusHitQueryCountZero: Number(reference?.outOfCorpusHitQueryCount || 0) === 0,
    zeroTrustedResultQueryCountZero: Number(reference?.zeroTrustedResultQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(reference?.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(reference?.citationAnchorCoverageAverage || 0) === 1
  };
  const failures = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([key]) => key);
  return { passes: failures.length === 0, checks, failures };
}

function evalCandidateForPolicy(row, citationCeilingResolved) {
  const checks = {
    qualityNotMateriallyRegressed: !(row.failingGates || []).includes(QUALITY_GATE),
    citationTopDocumentShareAtOrBelowEffectiveCeiling:
      Number(row.projectedCitationTopDocumentShare || 0) <= Number(citationCeilingResolved || 0),
    lowSignalStructuralShareNotWorsened: !(row.failingGates || []).includes(LOW_SIGNAL_GATE),
    outOfCorpusHitQueryCountZero: Number(row.projectedOutOfCorpusHitQueryCount || 0) === 0,
    zeroTrustedResultQueryCountZero: Number(row.projectedZeroTrustedResultQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(row.projectedProvenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(row.projectedCitationAnchorCoverageAverage || 0) === 1
  };
  const failing = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([key]) => key);
  return { passes: failing.length === 0, failing };
}

function riskLabelForPolicy({ baselinePasses, currentLivePasses, citationCeilingResolved, unlockRate }) {
  if (!baselinePasses || !currentLivePasses) return "high";
  if (citationCeilingResolved <= 0.2 && unlockRate <= 0.5) return "low";
  if (citationCeilingResolved <= 0.22) return "medium";
  return "high";
}

function buildPolicyDefinitions({ currentEffectiveCeiling, attainableFloor }) {
  return [
    {
      policyId: "current_effective_policy",
      policyLabel: "Current effective policy",
      citationCeilingResolved: Number(currentEffectiveCeiling.toFixed(4))
    },
    { policyId: "fixed_0_20", policyLabel: "Fixed ceiling 0.20", citationCeilingResolved: 0.2 },
    { policyId: "fixed_0_22", policyLabel: "Fixed ceiling 0.22", citationCeilingResolved: 0.22 },
    { policyId: "fixed_0_25", policyLabel: "Fixed ceiling 0.25", citationCeilingResolved: 0.25 },
    {
      policyId: "dynamic_floor",
      policyLabel: "Dynamic max(0.1, floor@K10)",
      citationCeilingResolved: Number(Math.max(0.1, attainableFloor).toFixed(4))
    },
    {
      policyId: "dynamic_floor_plus_0_02",
      policyLabel: "Dynamic max(0.1, floor@K10 + 0.02)",
      citationCeilingResolved: Number(Math.max(0.1, attainableFloor + 0.02).toFixed(4))
    },
    {
      policyId: "dynamic_floor_plus_0_05",
      policyLabel: "Dynamic max(0.1, floor@K10 + 0.05)",
      citationCeilingResolved: Number(Math.max(0.1, attainableFloor + 0.05).toFixed(4))
    }
  ];
}

export function analyzeR40Sensitivity({ r38, r39, r34Live, r34Report }) {
  const { baselineReference, currentLiveReference, thresholdReference } = resolveReferences({ r38, r34Live });
  const attainableFloor = pickAttainableFloor({ r34Live, r34Report, r38 });
  const currentEffectiveCeiling = Number(r38?.summary?.effectiveCitationCeiling || baselineReference.citationTopDocumentShare || 0.1);

  const candidateRows = (r39?.candidateRows || []).map((row) => ({ ...row }));
  const blockedCandidates = candidateRows.filter((row) => row.keepOrDoNotActivate !== "keep");
  const nearMissSet = new Set((r39?.nearMissCandidates || []).map((row) => String(row.documentId || "")));

  const policies = buildPolicyDefinitions({ currentEffectiveCeiling, attainableFloor });
  const policyRows = [];
  const policyUnlockMap = new Map();

  for (const policy of policies) {
    const baselineEval = evaluateReferenceAgainstPolicy({
      reference: baselineReference,
      thresholdReference,
      citationCeilingResolved: policy.citationCeilingResolved
    });
    const currentEval = evaluateReferenceAgainstPolicy({
      reference: currentLiveReference,
      thresholdReference,
      citationCeilingResolved: policy.citationCeilingResolved
    });

    const evaluated = blockedCandidates.map((row) => {
      const candidateEval = evalCandidateForPolicy(row, policy.citationCeilingResolved);
      return { row, ...candidateEval };
    });

    const unlocked = evaluated.filter((entry) => entry.passes).map((entry) => entry.row);
    const unlockedNearMiss = unlocked.filter((row) => nearMissSet.has(String(row.documentId || "")));
    const stillBlocked = evaluated.filter((entry) => !entry.passes);

    const candidatesStillBlockedByLowSignalCount = stillBlocked.filter((entry) =>
      entry.failing.includes("lowSignalStructuralShareNotWorsened")
    ).length;
    const candidatesStillBlockedByQualityCount = stillBlocked.filter((entry) =>
      entry.failing.includes("qualityNotMateriallyRegressed")
    ).length;
    const candidatesStillBlockedByMultipleGatesCount = stillBlocked.filter((entry) => entry.failing.length > 1).length;
    const unlockRate = blockedCandidates.length
      ? Number((unlocked.length / blockedCandidates.length).toFixed(4))
      : 0;

    const notes = [];
    if (!baselineEval.passes) notes.push(`baseline_fail:${baselineEval.failures.join("|")}`);
    if (!currentEval.passes) notes.push(`current_live_fail:${currentEval.failures.join("|")}`);
    if (unlocked.length === 0) notes.push("no_unlocks");

    const row = {
      policyId: policy.policyId,
      policyLabel: policy.policyLabel,
      citationCeilingResolved: policy.citationCeilingResolved,
      baselinePasses: baselineEval.passes,
      currentLivePasses: currentEval.passes,
      currentlyBlockedCandidatesUnlockedCount: unlocked.length,
      nearMissCandidatesUnlockedCount: unlockedNearMiss.length,
      candidatesStillBlockedByLowSignalCount,
      candidatesStillBlockedByQualityCount,
      candidatesStillBlockedByMultipleGatesCount,
      unlockRate,
      riskLabel: riskLabelForPolicy({
        baselinePasses: baselineEval.passes,
        currentLivePasses: currentEval.passes,
        citationCeilingResolved: policy.citationCeilingResolved,
        unlockRate
      }),
      notes
    };
    policyRows.push(row);
    policyUnlockMap.set(policy.policyId, unlocked.map((r) => String(r.documentId || "")));
  }

  policyRows.sort((a, b) => {
    if (a.citationCeilingResolved !== b.citationCeilingResolved) return a.citationCeilingResolved - b.citationCeilingResolved;
    return String(a.policyId).localeCompare(String(b.policyId));
  });

  const safePolicyRows = policyRows.filter((row) => row.baselinePasses && row.currentLivePasses);
  const recommended = safePolicyRows
    .slice()
    .sort((a, b) => {
      if (b.currentlyBlockedCandidatesUnlockedCount !== a.currentlyBlockedCandidatesUnlockedCount) {
        return b.currentlyBlockedCandidatesUnlockedCount - a.currentlyBlockedCandidatesUnlockedCount;
      }
      if (a.citationCeilingResolved !== b.citationCeilingResolved) return a.citationCeilingResolved - b.citationCeilingResolved;
      return String(a.policyId).localeCompare(String(b.policyId));
    })[0];

  const recommendedPolicy = recommended?.policyId || "none";
  const recommendedPolicyReason = recommended
    ? `max_unlock_with_reference_passes:${recommended.currentlyBlockedCandidatesUnlockedCount}`
    : "no_policy_simultaneously_passes_baseline_and_current_live_references";

  const candidateUnlockSummary = policyRows.map((row) => ({
    policyId: row.policyId,
    unlocked: row.currentlyBlockedCandidatesUnlockedCount,
    unlockRate: row.unlockRate
  }));

  const unlockByDoc = new Map();
  for (const row of candidateRows) {
    const id = String(row.documentId || "");
    if (!id) continue;
    unlockByDoc.set(id, {
      documentId: id,
      title: String(row.title || ""),
      documentFamilyLabel: String(row.documentFamilyLabel || "other"),
      projectedAverageQualityScore: Number(row.projectedAverageQualityScore || 0),
      projectedCitationTopDocumentShare: Number(row.projectedCitationTopDocumentShare || 0),
      projectedLowSignalStructuralShare: Number(row.projectedLowSignalStructuralShare || 0),
      wouldUnlockUnderPolicies: [],
      stillBlockedUnderPolicies: []
    });
  }

  for (const policy of policyRows) {
    const unlockedSet = new Set(policyUnlockMap.get(policy.policyId) || []);
    for (const [id, rec] of unlockByDoc.entries()) {
      if (unlockedSet.has(id)) rec.wouldUnlockUnderPolicies.push(policy.policyId);
      else rec.stillBlockedUnderPolicies.push(policy.policyId);
    }
  }

  const safestUnlockCandidates = [...unlockByDoc.values()]
    .filter((row) => row.wouldUnlockUnderPolicies.length > 0)
    .sort((a, b) => {
      if (b.wouldUnlockUnderPolicies.length !== a.wouldUnlockUnderPolicies.length) {
        return b.wouldUnlockUnderPolicies.length - a.wouldUnlockUnderPolicies.length;
      }
      if (b.projectedAverageQualityScore !== a.projectedAverageQualityScore) {
        return b.projectedAverageQualityScore - a.projectedAverageQualityScore;
      }
      return String(a.documentId).localeCompare(String(b.documentId));
    })
    .slice(0, 25);

  const unsafePolicyWarnings = policyRows
    .filter((row) => !row.baselinePasses || !row.currentLivePasses || row.riskLabel === "high")
    .map((row) => ({
      policyId: row.policyId,
      reason: row.notes.join("; ") || "high_risk_or_reference_fail"
    }));

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    phase: "R40",
    summary: {
      policiesEvaluated: policyRows.length,
      candidatesScanned: Number(r39?.candidatesScanned || candidateRows.length),
      blockedCandidates: blockedCandidates.length,
      recommendedPolicy,
      recommendedPolicyReason
    },
    policiesEvaluated: policyRows.map((row) => row.policyId),
    baselineReference: {
      ...baselineReference,
      thresholdReference,
      attainableFloorGivenUniqueDocsAtK10: attainableFloor
    },
    currentLiveReference,
    policyRows,
    recommendedPolicy,
    recommendedPolicyReason,
    candidateUnlockSummary,
    safestUnlockCandidates,
    unsafePolicyWarnings
  };
}

function formatMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R40 Citation Gate Sensitivity Audit (Dry Run)");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Policy Rows");
  for (const row of report.policyRows || []) {
    lines.push(
      `- ${row.policyId} (${row.citationCeilingResolved}) | unlocked=${row.currentlyBlockedCandidatesUnlockedCount} | baselinePasses=${row.baselinePasses} | currentLivePasses=${row.currentLivePasses} | risk=${row.riskLabel}`
    );
  }
  if (!(report.policyRows || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Safest Unlock Candidates");
  for (const row of report.safestUnlockCandidates || []) {
    lines.push(`- ${row.documentId} | unlockPolicies=${row.wouldUnlockUnderPolicies.join(", ") || "<none>"}`);
  }
  if (!(report.safestUnlockCandidates || []).length) lines.push("- none");
  lines.push("");
  lines.push("## Unsafe Policy Warnings");
  for (const row of report.unsafePolicyWarnings || []) lines.push(`- ${row.policyId}: ${row.reason}`);
  if (!(report.unsafePolicyWarnings || []).length) lines.push("- none");
  lines.push("");
  lines.push("- Dry-run only. No activation, rollback, or runtime gate mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [r38, r39, r34Live, r34Report] = await Promise.all([
    readJson("retrieval-r38-single-frontier-refresh-report.json"),
    readJson("retrieval-r39-frontier-blocker-breakdown-report.json"),
    readJsonIfExists("retrieval-r34-gate-revision-live-qa-report.json"),
    readJsonIfExists("retrieval-r34-gate-revision-report.json")
  ]);

  const report = analyzeR40Sensitivity({ r38, r39, r34Live, r34Report });
  const reportPath = path.resolve(reportsDir, outputJsonName);
  const markdownPath = path.resolve(reportsDir, outputMdName);
  await Promise.all([
    fs.writeFile(reportPath, JSON.stringify(report, null, 2)),
    fs.writeFile(markdownPath, formatMarkdown(report))
  ]);

  console.log(
    JSON.stringify(
      {
        policiesEvaluated: report.policiesEvaluated.length,
        recommendedPolicy: report.recommendedPolicy,
        recommendedPolicyReason: report.recommendedPolicyReason
      },
      null,
      2
    )
  );
  console.log(`R40 sensitivity report written to ${reportPath}`);
}

const isEntrypoint = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isEntrypoint) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}

