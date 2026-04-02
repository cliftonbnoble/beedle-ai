function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

export const DEFAULT_ALLOWED_SOURCES = ["crosswalk", "dhs_phrase"];
export const DEFAULT_MIN_TOP_SCORE = 6.5;
export const DEFAULT_MIN_MARGIN = 2;

export function evaluateInferenceCandidate(row, options = {}) {
  const allowedSources = new Set((options.allowedSources || DEFAULT_ALLOWED_SOURCES).map((value) => String(value)));
  const minTopScore = Number(options.minTopScore ?? DEFAULT_MIN_TOP_SCORE);
  const minMargin = Number(options.minMargin ?? DEFAULT_MIN_MARGIN);
  const candidates = Array.isArray(row?.candidateCodes) ? row.candidateCodes : [];
  const top = candidates[0] || null;
  const second = candidates[1] || null;

  if (!top?.code) {
    return {
      eligible: false,
      reason: "no_candidate",
      selectedCode: null,
      topCandidate: null,
      secondCandidate: second
    };
  }

  const topSources = new Set((top.sources || []).map(String));
  if (!Array.from(topSources).some((source) => allowedSources.has(source))) {
    return {
      eligible: false,
      reason: "source_not_allowed",
      selectedCode: null,
      topCandidate: top,
      secondCandidate: second
    };
  }

  if (Number(top.score || 0) < minTopScore) {
    return {
      eligible: false,
      reason: "top_score_below_threshold",
      selectedCode: null,
      topCandidate: top,
      secondCandidate: second
    };
  }

  const margin = second ? Number((Number(top.score || 0) - Number(second.score || 0)).toFixed(2)) : null;
  if (margin !== null && margin < minMargin) {
    return {
      eligible: false,
      reason: "ambiguous_second_candidate",
      selectedCode: null,
      topCandidate: top,
      secondCandidate: second,
      margin
    };
  }

  return {
    eligible: true,
    reason: "selected",
    selectedCode: String(top.code),
    topCandidate: top,
    secondCandidate: second,
    margin,
    selectedIndexCodes: [String(top.code)],
    selectedSources: uniqueSorted(Array.from(topSources))
  };
}

export function selectInferenceCandidates(rows, options = {}) {
  const evaluated = (rows || []).map((row) => ({
    row,
    evaluation: evaluateInferenceCandidate(row, options)
  }));

  const selected = evaluated
    .filter((item) => item.evaluation.eligible)
    .sort((a, b) => {
      const scoreDelta = Number(b.evaluation.topCandidate?.score || 0) - Number(a.evaluation.topCandidate?.score || 0);
      if (scoreDelta !== 0) return scoreDelta;
      const marginDelta = Number(b.evaluation.margin ?? Number.POSITIVE_INFINITY) - Number(a.evaluation.margin ?? Number.POSITIVE_INFINITY);
      if (marginDelta !== 0) return marginDelta;
      const dateA = String(a.row?.decisionDate || "");
      const dateB = String(b.row?.decisionDate || "");
      return dateB.localeCompare(dateA) || String(a.row?.citation || "").localeCompare(String(b.row?.citation || ""));
    });

  return {
    selected,
    skipped: evaluated.filter((item) => !item.evaluation.eligible)
  };
}

