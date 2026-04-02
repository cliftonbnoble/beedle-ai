const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}

function splitFamily(value) {
  if (Array.isArray(value)) return value.map((v) => String(v).trim()).filter(Boolean);
  return String(value || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean);
}

function rowKey(row) {
  return `${String(row.batchKey || "")}::${String(row.subBucket || "")}::${String(row.documentId || "")}`;
}

function decisionFromPosture(posture) {
  const p = String(posture || "");
  if (p === "possible_manual_context_fix_but_no_auto_apply") return "possible_manual_context_fix_but_no_auto_apply";
  if (p === "escalate_to_legal_context_review") return "escalate_to_legal_context_review";
  return "keep_blocked";
}

function decisionFromDisposition(disposition) {
  const d = String(disposition || "");
  if (d === "possible_manual_context_fix_but_no_auto_apply") return "possible_manual_context_fix_but_no_auto_apply";
  if (d === "escalate_to_legal_context_review") return "escalate_to_legal_context_review";
  return "keep_blocked";
}

function conservativeness(decision) {
  const d = String(decision || "");
  if (d === "keep_blocked") return 3;
  if (d === "escalate_to_legal_context_review") return 2;
  if (d === "manual_no_action") return 1;
  if (d === "possible_manual_context_fix_but_no_auto_apply") return 0;
  return -1;
}

function compareOutcome(reviewerDecision, recommendedDecision, simulatedDecision, requiresLegalEscalation, blocked37xFamily) {
  const reviewer = String(reviewerDecision || "");
  const blockedUnsafe = blocked37xFamily.some((f) => UNSAFE_37X.has(f));
  const matchesRecommended = reviewer === recommendedDecision;
  const matchesSimulated = reviewer === simulatedDecision;

  if (blockedUnsafe && reviewer === "keep_blocked") return "conservative_policy_match";
  if (reviewer === "keep_blocked") return "blocked_remains_blocked";
  if (reviewer === "escalate_to_legal_context_review") return "requires_legal_attention";
  if (reviewer === "possible_manual_context_fix_but_no_auto_apply") return "manual_context_fix_candidate";
  if (matchesRecommended) return "matches_recommended_posture";
  if (matchesSimulated) return "matches_simulated_disposition";

  const reviewerRank = conservativeness(reviewer);
  const recommendedRank = conservativeness(recommendedDecision);
  if (reviewerRank > recommendedRank) return "more_conservative_than_recommendation";
  if (reviewerRank < recommendedRank) return "less_conservative_than_recommendation";

  if (requiresLegalEscalation || blockedUnsafe) return "requires_legal_attention";
  return "matches_recommended_posture";
}

function comparisonNotes({
  reviewerDecision,
  recommendedReviewerPosture,
  recommendedSimulatedDisposition,
  outcome,
  blocked37xFamily,
  matchesRecommended,
  matchesSimulated
}) {
  const unsafe = blocked37xFamily.filter((f) => UNSAFE_37X.has(f));
  const notes = [];
  if (unsafe.length) notes.push(`blocked_unsafe_37x:${unsafe.join(",")}`);
  notes.push(`recommended_posture:${recommendedReviewerPosture}`);
  notes.push(`simulated_disposition:${recommendedSimulatedDisposition}`);
  notes.push(`reviewer_decision:${reviewerDecision}`);
  if (matchesRecommended) notes.push("matches_recommended_posture");
  if (matchesSimulated) notes.push("matches_simulated_disposition");
  if (outcome === "less_conservative_than_recommendation") notes.push("human_recheck_recommended");
  return notes.join(" | ");
}

function sortRows(a, b) {
  const qA = Number(a.queueOrder || 0);
  const qB = Number(b.queueOrder || 0);
  if (qA !== qB) return qA - qB;
  const rA = Number(a.rowNumber || 0);
  const rB = Number(b.rowNumber || 0);
  if (rA !== rB) return rA - rB;
  return rowKey(a).localeCompare(rowKey(b));
}

export function buildReviewerDecisionCompare(validateReport, actionQueueReport = null, simReport = null) {
  const queueMap = new Map((actionQueueReport?.rows || []).map((row) => [rowKey(row), row]));
  const simMap = new Map((simReport?.batches || []).map((batch) => [String(batch.batchKey || ""), batch]));

  const comparableRows = (validateReport?.rows || [])
    .filter((row) => row.readyForDryRunComparison === true)
    .map((row) => {
      const key = rowKey(row);
      const queueRow = queueMap.get(key);
      const simBatch = simMap.get(String(row.batchKey || ""));

      const reviewerDecision = String(row.reviewerDecision || "");
      const recommendedReviewerPosture = String(row.recommendedReviewerPosture || queueRow?.recommendedReviewerPosture || "keep_blocked");
      const recommendedSimulatedDisposition = String(
        row.recommendedSimulatedDisposition ||
          queueRow?.recommendedSimulatedDisposition ||
          simBatch?.recommendedSimulatedDisposition ||
          "keep_blocked"
      );

      const recommendedDecision = decisionFromPosture(recommendedReviewerPosture);
      const simulatedDecision = decisionFromDisposition(recommendedSimulatedDisposition);
      const blocked37xFamily = splitFamily(row.blocked37xFamily || queueRow?.blocked37xFamily || "");
      const requiresLegalEscalation =
        String(row.requiresLegalEscalation) === "true" ||
        row.requiresLegalEscalation === true ||
        queueRow?.requiresLegalEscalation === true;
      const matchesRecommended = reviewerDecision === recommendedDecision;
      const matchesSimulated = reviewerDecision === simulatedDecision;
      const outcome = compareOutcome(
        reviewerDecision,
        recommendedDecision,
        simulatedDecision,
        requiresLegalEscalation,
        blocked37xFamily
      );
      const conservativePolicyMatch = outcome === "conservative_policy_match";
      const exactMatch = matchesRecommended || matchesSimulated;
      const trueDivergence = !exactMatch && !conservativePolicyMatch;

      return {
        rowNumber: row.rowNumber ?? null,
        queueOrder: Number(row.queueOrder || queueRow?.queueOrder || 0),
        documentId: String(row.documentId || ""),
        title: String(row.title || ""),
        batchKey: String(row.batchKey || ""),
        priorityLane: String(row.priorityLane || queueRow?.priorityLane || ""),
        reviewerDecision,
        recommendedReviewerPosture,
        recommendedSimulatedDisposition,
        comparisonOutcome: outcome,
        comparisonNotes: comparisonNotes({
          reviewerDecision,
          recommendedReviewerPosture,
          recommendedSimulatedDisposition,
          outcome,
          blocked37xFamily,
          matchesRecommended,
          matchesSimulated
        }),
        doNotAutoApply: row.doNotAutoApply !== false,
        blocked37xFamily,
        requiresLegalEscalation,
        conservativePolicyMatch,
        exactMatch,
        trueDivergence,
        matchesRecommendedPosture: matchesRecommended,
        matchesSimulatedDisposition: matchesSimulated,
        moreConservativeThanRecommendation: conservativeness(reviewerDecision) > conservativeness(recommendedDecision),
        lessConservativeThanRecommendation: conservativeness(reviewerDecision) < conservativeness(recommendedDecision)
      };
    })
    .sort(sortRows);

  const rowsNeedingHumanRecheck = comparableRows.filter((row) => row.lessConservativeThanRecommendation || row.trueDivergence);

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      totalComparableRows: comparableRows.length,
      rowsExactMatches: comparableRows.filter((row) => row.exactMatch).length,
      rowsConservativePolicyMatches: comparableRows.filter((row) => row.conservativePolicyMatch).length,
      rowsTrueDivergencesNeedingHumanAttention: comparableRows.filter((row) => row.trueDivergence).length,
      rowsMatchingRecommendedPosture: comparableRows.filter((row) => row.matchesRecommendedPosture).length,
      rowsMatchingSimulatedDisposition: comparableRows.filter((row) => row.matchesSimulatedDisposition).length,
      rowsMoreConservativeThanRecommendation: comparableRows.filter((row) => row.moreConservativeThanRecommendation).length,
      rowsLessConservativeThanRecommendation: comparableRows.filter((row) => row.lessConservativeThanRecommendation).length,
      rowsStillBlocked: comparableRows.filter((row) => row.reviewerDecision === "keep_blocked").length,
      rowsEscalatedToLegal: comparableRows.filter((row) => row.reviewerDecision === "escalate_to_legal_context_review").length,
      rowsMarkedManualContextFix: comparableRows.filter((row) => row.reviewerDecision === "possible_manual_context_fix_but_no_auto_apply").length,
      rowsNeedingHumanRecheck: rowsNeedingHumanRecheck.length
    },
    guidance: {
      divergesFromQueuePosture: comparableRows.filter((row) => !row.matchesRecommendedPosture && !row.conservativePolicyMatch),
      divergesFromSimulatedDisposition: comparableRows.filter((row) => !row.matchesSimulatedDisposition && !row.conservativePolicyMatch),
      involvingUnsafeBlocked37xFamilies: comparableRows.filter((row) => row.blocked37xFamily.some((f) => UNSAFE_37X.has(f))),
      safeForNextDryRunOnlyComparisonReview: comparableRows.filter(
        (row) => (row.matchesRecommendedPosture || row.conservativePolicyMatch) && row.doNotAutoApply && !row.lessConservativeThanRecommendation
      )
    },
    matches: comparableRows.filter((row) => row.exactMatch || row.conservativePolicyMatch),
    divergences: comparableRows.filter((row) => row.trueDivergence),
    stillBlocked: comparableRows.filter((row) => row.reviewerDecision === "keep_blocked"),
    escalatedRows: comparableRows.filter((row) => row.reviewerDecision === "escalate_to_legal_context_review"),
    manualContextFixCandidates: comparableRows.filter((row) => row.reviewerDecision === "possible_manual_context_fix_but_no_auto_apply"),
    countsByComparisonOutcome: countBy(comparableRows.map((row) => row.comparisonOutcome)),
    rows: comparableRows
  };
}

export function formatReviewerDecisionCompareMarkdown(report) {
  const lines = [];
  lines.push("# Reviewer Decision Compare");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("- countsByComparisonOutcome:");
  for (const item of report.countsByComparisonOutcome || []) lines.push(`  - ${item.key}: ${item.count}`);
  lines.push("");

  const sections = [
    ["Matches", report.matches || []],
    ["Divergences", report.divergences || []],
    ["Still blocked", report.stillBlocked || []],
    ["Escalated rows", report.escalatedRows || []],
    ["Manual context fix candidates", report.manualContextFixCandidates || []]
  ];

  for (const [title, rows] of sections) {
    lines.push(`## ${title}`);
    if (!rows.length) {
      lines.push("- none");
      lines.push("");
      continue;
    }
    for (const row of rows.slice(0, 200)) {
      lines.push(
        `- row=${row.rowNumber ?? "n/a"} | #${row.queueOrder} | ${row.documentId} | ${row.title} | outcome=${row.comparisonOutcome}`
      );
      lines.push(`  - reviewerDecision=${row.reviewerDecision} | posture=${row.recommendedReviewerPosture} | disposition=${row.recommendedSimulatedDisposition}`);
      lines.push(`  - notes=${row.comparisonNotes}`);
    }
    lines.push("");
  }

  lines.push("## Reviewer follow-up checklist");
  lines.push(`- Diverges from queue posture: ${report.guidance.divergesFromQueuePosture.length}`);
  lines.push(`- Diverges from simulated disposition: ${report.guidance.divergesFromSimulatedDisposition.length}`);
  lines.push(`- Involving blocked 37.3/37.7/37.9 families: ${report.guidance.involvingUnsafeBlocked37xFamilies.length}`);
  lines.push(`- Safe for next dry-run-only comparison review: ${report.guidance.safeForNextDryRunOnlyComparisonReview.length}`);
  lines.push("");
  lines.push("- doNotAutoApply remains true; this report is read-only.");

  return lines.join("\n");
}
