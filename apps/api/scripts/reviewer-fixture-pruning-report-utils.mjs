const TITLE_PREFIX_PATTERNS = [/^approval rollout/i, /^fixture runtime candidate/i, /^metadata cleanup/i, /^missing index remediation/i];
const TITLE_SUFFIX_PATTERNS = [/(?:ref|cleanup|rollout)[a-z0-9_-]{4,}$/i, /\b\d{10,}\b/];
const CITATION_PATTERNS = [/^BEE-ROLL-/i, /^KNOWN-REF-/i];
const UNSAFE_37X_FAMILIES = new Set(["37.3", "37.7", "37.9"]);

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || '<none>');
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function titleSignal(title) {
  return TITLE_PREFIX_PATTERNS.some((pattern) => pattern.test(String(title || '')));
}

function titleSuffixSignal(title) {
  return TITLE_SUFFIX_PATTERNS.some((pattern) => pattern.test(String(title || "")));
}

function citationSignal(citation) {
  return CITATION_PATTERNS.some((pattern) => pattern.test(String(citation || '')));
}

function parseIsoMs(value) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function resolveAge(createdAt, updatedAt, nowIso) {
  const created = parseIsoMs(createdAt);
  const updated = parseIsoMs(updatedAt);
  const now = Date.parse(nowIso);
  if (!Number.isFinite(now)) return { ageDays: null, ageEvidenceSource: "none" };
  if (created !== null) {
    return { ageDays: Math.max(0, Math.floor((now - created) / 86400000)), ageEvidenceSource: "createdAt" };
  }
  if (updated !== null) {
    return { ageDays: Math.max(0, Math.floor((now - updated) / 86400000)), ageEvidenceSource: "updatedAt" };
  }
  return { ageDays: null, ageEvidenceSource: "none" };
}

function addDaysIso(iso, days) {
  const baseMs = Date.parse(String(iso || ""));
  if (!Number.isFinite(baseMs)) return null;
  return new Date(baseMs + days * 86400000).toISOString();
}

function extractRolloutSuffixToken(title) {
  const text = String(title || "").toLowerCase().trim();
  const compact = text.match(/\brollout([a-z0-9_-]{4,})\b/i)?.[1];
  if (compact) return compact;
  const spaced = text.match(/\brollout\b[\s:_-]*([a-z0-9_-]{4,})\b/i)?.[1];
  if (spaced) return spaced;
  return null;
}

function deriveRolloutFamilyKey(row) {
  const title = String(row?.title || "").toLowerCase().trim();
  const citation = String(row?.citation || "").toLowerCase().trim();
  const rolloutSuffixToken = extractRolloutSuffixToken(title);
  if (rolloutSuffixToken) {
    return { rolloutFamilyKey: `rollout:${rolloutSuffixToken}`, rolloutSuffixToken, rolloutFamilyKeySource: "title_rollout_suffix" };
  }
  const fromCitation = citation.match(/(?:bee-roll-|known-ref-)([a-z0-9_-]+)/i)?.[1] || null;
  if (fromCitation) return { rolloutFamilyKey: `citation:${fromCitation}`, rolloutSuffixToken: null, rolloutFamilyKeySource: "citation_token" };
  const fromTitleToken = title.match(/\b(ref[a-z0-9_-]{4,}|cleanup[a-z0-9_-]{4,}|rollout[a-z0-9_-]{4,})\b/i)?.[1];
  if (fromTitleToken) return { rolloutFamilyKey: `title:${fromTitleToken}`, rolloutSuffixToken: null, rolloutFamilyKeySource: "title_token" };
  return { rolloutFamilyKey: null, rolloutSuffixToken: null, rolloutFamilyKeySource: "none" };
}

function classifyRow(row, options) {
  const { nowIso, minFixtureAgeDays, realFamilyIndex, fixtureFamilyIndex, familyFixtureCounts } = options;
  const rowId = String(row.id || "");
  const tSignal = titleSignal(row.title);
  const tSuffixSignal = titleSuffixSignal(row.title);
  const cSignal = citationSignal(row.citation);
  const likelyFixture = Boolean(row.isLikelyFixture);
  const ageResolved = resolveAge(row.createdAt, row.updatedAt, nowIso);
  const age = ageResolved.ageDays;
  const ageEvidenceSource = ageResolved.ageEvidenceSource;
  const family = deriveRolloutFamilyKey(row);
  const familyKey = family.rolloutFamilyKey;
  const rolloutSuffixToken = family.rolloutSuffixToken;
  const rolloutFamilyKeySource = family.rolloutFamilyKeySource;
  const siblingRealDocIds = familyKey
    ? Array.from(realFamilyIndex.get(familyKey) || []).filter((id) => id !== rowId).sort()
    : [];
  const siblingFixtureDocIds = familyKey
    ? Array.from(fixtureFamilyIndex.get(familyKey) || []).filter((id) => id !== rowId).sort()
    : [];
  const siblingRealMatchCount = siblingRealDocIds.length;
  const siblingFixtureMatchCount = siblingFixtureDocIds.length;
  const siblingMatchStatus = !familyKey
    ? "none"
    : siblingRealMatchCount > 0
      ? "paired_real_match"
      : siblingFixtureMatchCount === 0
        ? "self_only"
        : "none";
  const blockedFamilies = Array.from(
    new Set((row.blocked37xReferences || []).map((item) => String(item.family || "")).filter(Boolean))
  ).sort();
  const recurringFamilies = Array.from(new Set((row.recurringCitationFamilies || []).map((item) => String(item)).filter(Boolean)));
  const unresolvedBuckets = Array.from(new Set((row.unresolvedBuckets || []).map((item) => String(item)).filter(Boolean)));
  const hasUnsafeFamily = blockedFamilies.some((family) => UNSAFE_37X_FAMILIES.has(family));
  const runtimeSurface = Boolean(row.runtimeSurfaceForManualReview);
  const runtimeDoNotAutoApply = row.runtimeDoNotAutoApply !== false;
  const runtimeReason = String(row.runtimeManualReasonCode || "none");
  const oldEnough = age === null ? false : age >= minFixtureAgeDays;
  const fixtureFamilyCount = familyKey ? Number(familyFixtureCounts.get(familyKey) || 0) : 0;
  const repeatedFixtureFamilyNoRealVisibility = Boolean(familyKey && fixtureFamilyCount >= 2 && siblingRealMatchCount === 0);
  const fixtureEvidence = [];
  const keepOperationalEvidence = [];
  const promotionFailureReasons = [];
  let ambiguousDerivedStatus = null;
  let daysUntilEligible = null;
  let eligibleOnOrAfter = null;
  let removableIfAgedToday = false;

  if (likelyFixture) fixtureEvidence.push("likely_fixture_flag");
  if (tSignal) fixtureEvidence.push("fixture_title_prefix_pattern");
  if (tSuffixSignal) fixtureEvidence.push("fixture_title_suffix_pattern");
  if (cSignal) fixtureEvidence.push("fixture_citation_pattern");
  if (familyKey) fixtureEvidence.push("fixture_rollout_family_key_present");
  if (siblingRealMatchCount > 0) fixtureEvidence.push("fixture_sibling_real_pair_detected");
  if (siblingFixtureMatchCount > 0) fixtureEvidence.push("fixture_sibling_fixture_pair_detected");
  if (repeatedFixtureFamilyNoRealVisibility) fixtureEvidence.push("repeated_fixture_family_no_real_visibility");
  if (oldEnough) fixtureEvidence.push("age_threshold_met");

  const lowRisk37xPath =
    recurringFamilies.includes("37.2") &&
    !hasUnsafeFamily &&
    unresolvedBuckets.some((bucket) => bucket === "likely_parenthetical_or_prefix_fix" || bucket === "duplicate_or_redundant_reference");
  if (!likelyFixture) keepOperationalEvidence.push("non_fixture_flag");
  if (runtimeSurface) keepOperationalEvidence.push("runtime_surface_for_manual_review");
  if (runtimeDoNotAutoApply) keepOperationalEvidence.push("runtime_do_not_auto_apply");
  if (runtimeReason === "parenthetical_prefix_fix_candidate" || runtimeReason === "low_risk_not_found_residue") {
    keepOperationalEvidence.push("runtime_reason_code_safe_narrow");
  }
  if (lowRisk37xPath) keepOperationalEvidence.push("low_risk_37_2_residue_path");

  let bucket = 'keep_operational';
  let reason = 'No deterministic fixture evidence.';
  const classificationRulesTriggered = [];

  const removableFixtureSignals = fixtureEvidence.filter((signal) => !["age_threshold_met", "fixture_rollout_family_key_present"].includes(signal));
  const hasTwoIndependentFixtureSignals = removableFixtureSignals.length >= 2;
  const explicitFixtureEvidence = likelyFixture && hasTwoIndependentFixtureSignals;
  const siblingRuleSatisfied = explicitFixtureEvidence && siblingRealMatchCount > 0 && oldEnough;
  const keepOperationalDeterministic =
    runtimeSurface &&
    !likelyFixture &&
    runtimeDoNotAutoApply &&
    (runtimeReason === "parenthetical_prefix_fix_candidate" || runtimeReason === "low_risk_not_found_residue") &&
    lowRisk37xPath;

  if (keepOperationalDeterministic) {
    bucket = "keep_operational";
    reason = "Real surfaced runtime-manual candidate on low-risk 37.2 residue path.";
    classificationRulesTriggered.push("keep_operational_low_risk_37_2_runtime_manual");
  } else if (siblingRuleSatisfied) {
    bucket = 'likely_removable_fixture';
    reason = "Likely fixture with explicit fixture evidence, paired real sibling, and age threshold met.";
    classificationRulesTriggered.push("likely_removable_fixture_multi_signal_age_gated");
    classificationRulesTriggered.push("likely_removable_fixture_sibling_pair_rule");
  } else if (likelyFixture || tSignal || cSignal) {
    bucket = 'ambiguous_fixture_like_row';
    reason = likelyFixture
      ? 'Likely fixture but missing age/pattern threshold for removable classification.'
      : 'Fixture-like naming/citation pattern without likelyFixture confirmation.';
    classificationRulesTriggered.push("ambiguous_fixture_like_insufficient_fixture_signals");
  }

  if (bucket === "ambiguous_fixture_like_row") {
    if (!likelyFixture) promotionFailureReasons.push("missing_explicit_fixture_flag");
    if (!hasTwoIndependentFixtureSignals) promotionFailureReasons.push("insufficient_explicit_fixture_signals");
    if (siblingRealMatchCount === 0) promotionFailureReasons.push("missing_paired_real_sibling");
    if (!tSignal) promotionFailureReasons.push("missing_fixture_specific_title_pattern");
    if (ageEvidenceSource === "none") {
      promotionFailureReasons.push("missing_age_evidence");
    } else if (!oldEnough) {
      promotionFailureReasons.push("age_threshold_not_satisfied");
      if (siblingRealMatchCount > 0) promotionFailureReasons.push("paired_sibling_detected_but_too_young");
    }

    const hasMissingPair = promotionFailureReasons.includes("missing_paired_real_sibling");
    const hasMissingAgeEvidence = promotionFailureReasons.includes("missing_age_evidence");
    const hasAgeNotSatisfied = promotionFailureReasons.includes("age_threshold_not_satisfied");
    const onlyAgeBlocker =
      siblingRealMatchCount > 0 &&
      hasAgeNotSatisfied &&
      !hasMissingPair &&
      !hasMissingAgeEvidence &&
      promotionFailureReasons.every((reason) => reason === "age_threshold_not_satisfied" || reason === "paired_sibling_detected_but_too_young");

    removableIfAgedToday = onlyAgeBlocker;
    if (hasMissingAgeEvidence) {
      ambiguousDerivedStatus = "missing_age_evidence";
    } else if (hasMissingPair) {
      ambiguousDerivedStatus = "missing_real_pair";
    } else if (onlyAgeBlocker) {
      ambiguousDerivedStatus = "awaiting_age_threshold";
      daysUntilEligible = Math.max(0, minFixtureAgeDays - Number(age || 0));
      eligibleOnOrAfter = addDaysIso(nowIso, daysUntilEligible);
    } else {
      ambiguousDerivedStatus = "other_ambiguous";
    }
  }

  return {
    id: String(row.id || ''),
    title: String(row.title || ''),
    citation: String(row.citation || ''),
    createdAt: row.createdAt || null,
    updatedAt: row.updatedAt || null,
    ageEvidenceSource,
    ageDays: age,
    requiredAgeDays: minFixtureAgeDays,
    daysUntilEligible,
    eligibleOnOrAfter,
    removableIfAgedToday,
    isLikelyFixture: likelyFixture,
    runtimeSurfaceForManualReview: runtimeSurface,
    runtimeDisposition: String(row.runtimeDisposition || 'keep_blocked'),
    runtimeDoNotAutoApply,
    runtimeManualReasonCode: runtimeReason,
    runtimeOperatorReviewSummary: String(row.runtimeOperatorReviewSummary || ''),
    rolloutSuffixToken,
    rolloutFamilyKeySource,
    rolloutFamilyKey: familyKey,
    siblingMatchStatus,
    siblingRealDocIds,
    siblingFixtureDocIds,
    siblingRealMatchCount,
    siblingFixtureMatchCount,
    ageThresholdSatisfied: oldEnough,
    removableFixtureConfidence: bucket === "likely_removable_fixture" ? "high" : "none",
    blocked37xFamilies: blockedFamilies,
    unresolvedBuckets,
    recurringCitationFamilies: recurringFamilies,
    fixtureEvidence,
    keepOperationalEvidence,
    promotionFailureReasons,
    ambiguousDerivedStatus,
    classificationRulesTriggered,
    explicitFixtureEvidence,
    titlePatternSignal: tSignal,
    titleSuffixPatternSignal: tSuffixSignal,
    citationPatternSignal: cSignal,
    bucket,
    classificationReason: reason
  };
}

export function buildReviewerFixturePruningReport({ rows = [], nowIso = new Date().toISOString(), minFixtureAgeDays = 2, topLimit = 40 }) {
  const rowList = rows || [];
  const realFamilyIndex = new Map();
  const fixtureFamilyIndex = new Map();
  const familyFixtureCounts = new Map();
  const familyRealCounts = new Map();
  const suffixPairCounts = new Map();
  for (const row of rowList) {
    const family = deriveRolloutFamilyKey(row);
    const familyKey = family.rolloutFamilyKey;
    if (!familyKey) continue;
    if (family.rolloutSuffixToken) {
      if (!suffixPairCounts.has(family.rolloutSuffixToken)) {
        suffixPairCounts.set(family.rolloutSuffixToken, { totalRows: 0, realRows: 0, fixtureRows: 0 });
      }
      const entry = suffixPairCounts.get(family.rolloutSuffixToken);
      entry.totalRows += 1;
      if (row?.isLikelyFixture) entry.fixtureRows += 1;
      else entry.realRows += 1;
    }
    if (row?.isLikelyFixture) {
      if (!fixtureFamilyIndex.has(familyKey)) fixtureFamilyIndex.set(familyKey, new Set());
      fixtureFamilyIndex.get(familyKey).add(String(row.id || ""));
      familyFixtureCounts.set(familyKey, Number(familyFixtureCounts.get(familyKey) || 0) + 1);
    } else {
      if (!realFamilyIndex.has(familyKey)) realFamilyIndex.set(familyKey, new Set());
      realFamilyIndex.get(familyKey).add(String(row.id || ""));
      familyRealCounts.set(familyKey, Number(familyRealCounts.get(familyKey) || 0) + 1);
    }
  }
  const classified = rowList.map((row) =>
    classifyRow(row, { nowIso, minFixtureAgeDays, realFamilyIndex, fixtureFamilyIndex, familyFixtureCounts })
  );

  const keepOperational = classified.filter((row) => row.bucket === 'keep_operational');
  const removable = classified.filter((row) => row.bucket === 'likely_removable_fixture');
  const ambiguous = classified.filter((row) => row.bucket === 'ambiguous_fixture_like_row');
  const awaitingAge = ambiguous.filter((row) => row.ambiguousDerivedStatus === "awaiting_age_threshold");
  const missingPair = ambiguous.filter((row) => row.ambiguousDerivedStatus === "missing_real_pair");
  const missingAge = ambiguous.filter((row) => row.ambiguousDerivedStatus === "missing_age_evidence");
  const rowsWithExtractedRolloutSuffixButNoPair = classified.filter(
    (row) => Boolean(row.rolloutSuffixToken) && row.siblingRealMatchCount === 0
  );
  const rowsWithPairedRealSibling = classified.filter((row) => row.siblingRealMatchCount > 0);

  return {
    generatedAt: nowIso,
    readOnly: true,
    minFixtureAgeDays,
    summary: {
      totalRowsAnalyzed: classified.length,
      keepOperationalCount: keepOperational.length,
      likelyRemovableFixtureCount: removable.length,
      ambiguousFixtureLikeCount: ambiguous.length,
      surfacedRuntimeManualCandidates: classified.filter((row) => row.runtimeSurfaceForManualReview).length,
      surfacedRuntimeManualRealCandidates: classified.filter((row) => row.runtimeSurfaceForManualReview && !row.isLikelyFixture).length,
      surfacedRuntimeManualFixtureCandidates: classified.filter((row) => row.runtimeSurfaceForManualReview && row.isLikelyFixture).length,
      unsafeRuntimeManualSurfacedViolations: classified.filter(
        (row) => row.runtimeSurfaceForManualReview && row.blocked37xFamilies.some((family) => ['37.3', '37.7', '37.9'].includes(family))
      ).length,
      awaitingAgeThresholdCount: awaitingAge.length,
      missingRealPairCount: missingPair.length,
      missingAgeEvidenceCount: missingAge.length
    },
    siblingPairEvidence: classified
      .filter((row) => (row.siblingRealDocIds || []).length > 0)
      .map((row) => ({
        id: row.id,
        title: row.title,
        rolloutFamilyKey: row.rolloutFamilyKey,
        siblingRealDocIds: row.siblingRealDocIds,
        siblingFixtureDocIds: row.siblingFixtureDocIds
      }))
      .slice(0, topLimit),
    rolloutSuffixPairCounts: Object.fromEntries(
      Array.from(suffixPairCounts.entries())
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([token, value]) => {
          const familyKey = `rollout:${token}`;
          return [
            token,
            {
              totalRows: value.totalRows,
              realRows: value.realRows,
              fixtureRows: value.fixtureRows,
              pairedRealSiblingRows: classified.filter(
                (row) => row.rolloutSuffixToken === token && row.siblingRealMatchCount > 0
              ).length,
              familyHasRealRows: Number(familyRealCounts.get(familyKey) || 0) > 0
            }
          ];
        })
    ),
    rowsWithExtractedRolloutSuffixButNoPair: rowsWithExtractedRolloutSuffixButNoPair.slice(0, topLimit),
    rowsWithPairedRealSibling: rowsWithPairedRealSibling.slice(0, topLimit),
    rowsAwaitingAgeThreshold: awaitingAge.slice(0, topLimit),
    rowsMissingRealPair: missingPair.slice(0, topLimit),
    pairedFamiliesAwaitingAge: countBy(awaitingAge.map((row) => row.rolloutSuffixToken || "<none>")),
    removableByAgeThreshold: removable.filter((row) => row.ageThresholdSatisfied).slice(0, topLimit),
    removableByExplicitFixtureSignals: removable
      .filter((row) => (row.fixtureEvidence || []).filter((signal) => !["age_threshold_met", "fixture_rollout_family_key_present"].includes(signal)).length >= 2)
      .slice(0, topLimit),
    promotionFailureReasonCounts: countBy(ambiguous.flatMap((row) => row.promotionFailureReasons || [])),
    rowsMissingAgeEvidence: ambiguous.filter((row) => row.ageEvidenceSource === "none").slice(0, topLimit),
    rowsWithOnlySelfFamilyMatch: ambiguous.filter((row) => row.siblingMatchStatus === "self_only").slice(0, topLimit),
    rowsWithPairedSiblingButTooYoung: ambiguous
      .filter((row) => row.siblingMatchStatus === "paired_real_match" && !row.ageThresholdSatisfied)
      .slice(0, topLimit),
    classificationRulesTriggered: countBy(classified.flatMap((row) => row.classificationRulesTriggered || [])),
    countsByBucket: countBy(classified.map((row) => row.bucket)),
    keepOperational: keepOperational.slice(0, topLimit),
    likelyRemovableFixture: removable.slice(0, topLimit),
    ambiguousFixtureLike: ambiguous.slice(0, topLimit)
  };
}

export function formatReviewerFixturePruningMarkdown(report) {
  const lines = [];
  lines.push('# Reviewer Fixture Pruning Report (Read-only)');
  lines.push('');
  lines.push('## Summary');
  lines.push(`- totalRowsAnalyzed: ${report.summary.totalRowsAnalyzed}`);
  lines.push(`- keepOperationalCount: ${report.summary.keepOperationalCount}`);
  lines.push(`- likelyRemovableFixtureCount: ${report.summary.likelyRemovableFixtureCount}`);
  lines.push(`- ambiguousFixtureLikeCount: ${report.summary.ambiguousFixtureLikeCount}`);
  lines.push(`- surfacedRuntimeManualCandidates: ${report.summary.surfacedRuntimeManualCandidates}`);
  lines.push(`- surfacedRuntimeManualRealCandidates: ${report.summary.surfacedRuntimeManualRealCandidates}`);
  lines.push(`- surfacedRuntimeManualFixtureCandidates: ${report.summary.surfacedRuntimeManualFixtureCandidates}`);
  lines.push(`- unsafeRuntimeManualSurfacedViolations: ${report.summary.unsafeRuntimeManualSurfacedViolations}`);
  lines.push(`- awaitingAgeThresholdCount: ${report.summary.awaitingAgeThresholdCount}`);
  lines.push(`- missingRealPairCount: ${report.summary.missingRealPairCount}`);
  lines.push(`- missingAgeEvidenceCount: ${report.summary.missingAgeEvidenceCount}`);
  lines.push('');

  lines.push('## Likely Removable Fixture');
  if (!report.likelyRemovableFixture.length) lines.push('- none');
  for (const row of report.likelyRemovableFixture || []) {
    lines.push(
      `- ${row.id} | ${row.title} | ageDays=${row.ageDays} | reason=${row.classificationReason} | fixtureEvidence=${(row.fixtureEvidence || []).join(", ")}`
    );
  }
  lines.push('');

  lines.push('## Keep Operational');
  if (!report.keepOperational.length) lines.push('- none');
  for (const row of report.keepOperational || []) {
    lines.push(
      `- ${row.id} | ${row.title} | reason=${row.classificationReason} | keepOperationalEvidence=${(row.keepOperationalEvidence || []).join(", ")}`
    );
  }
  lines.push('');

  lines.push('## Ambiguous Fixture-like Rows');
  if (!report.ambiguousFixtureLike.length) lines.push('- none');
  for (const row of report.ambiguousFixtureLike || []) {
    lines.push(
      `- ${row.id} | ${row.title} | likelyFixture=${row.isLikelyFixture} | reason=${row.classificationReason} | siblingMatchStatus=${row.siblingMatchStatus} | age=${row.ageDays ?? "<none>"}/${row.requiredAgeDays} (${row.ageEvidenceSource}) | promotionFailureReasons=${(row.promotionFailureReasons || []).join(", ") || "<none>"}`
    );
  }
  lines.push('');
  lines.push('## Rows Awaiting Age Threshold');
  if (!(report.rowsAwaitingAgeThreshold || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.rowsAwaitingAgeThreshold || []) {
      lines.push(
        `- ${row.id} | ${row.title} | daysUntilEligible=${row.daysUntilEligible} | eligibleOnOrAfter=${row.eligibleOnOrAfter || "<none>"} | removableIfAgedToday=${row.removableIfAgedToday}`
      );
    }
  }
  lines.push('');
  lines.push('## Rows Missing Real Pair');
  if (!(report.rowsMissingRealPair || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.rowsMissingRealPair || []) {
      lines.push(`- ${row.id} | ${row.title} | rolloutSuffixToken=${row.rolloutSuffixToken || "<none>"}`);
    }
  }
  lines.push('');
  lines.push('## Paired Families Awaiting Age');
  const familiesAwaiting = Object.entries(report.pairedFamiliesAwaitingAge || {});
  if (!familiesAwaiting.length) {
    lines.push('- none');
  } else {
    for (const [family, count] of familiesAwaiting) lines.push(`- ${family}: ${count}`);
  }
  lines.push('');

  lines.push('## Promotion Failure Reason Counts');
  const failureCounts = Object.entries(report.promotionFailureReasonCounts || {});
  if (!failureCounts.length) {
    lines.push('- none');
  } else {
    for (const [reason, count] of failureCounts) lines.push(`- ${reason}: ${count}`);
  }
  lines.push('');

  lines.push('## Classification Rules Triggered');
  const rules = Object.entries(report.classificationRulesTriggered || {});
  if (!rules.length) {
    lines.push('- none');
  } else {
    for (const [rule, count] of rules) lines.push(`- ${rule}: ${count}`);
  }
  lines.push('');
  lines.push('## Sibling Pair Evidence');
  if (!(report.siblingPairEvidence || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.siblingPairEvidence || []) {
      lines.push(
        `- ${row.id} | ${row.title} | family=${row.rolloutFamilyKey || "<none>"} | siblingRealDocIds=${(row.siblingRealDocIds || []).join(", ") || "<none>"} | siblingFixtureDocIds=${(row.siblingFixtureDocIds || []).join(", ") || "<none>"}`
      );
    }
  }
  lines.push('');
  lines.push('## Rollout Suffix Pair Counts');
  const suffixCounts = Object.entries(report.rolloutSuffixPairCounts || {});
  if (!suffixCounts.length) {
    lines.push('- none');
  } else {
    for (const [token, counts] of suffixCounts) {
      lines.push(
        `- ${token}: total=${counts.totalRows}, real=${counts.realRows}, fixture=${counts.fixtureRows}, pairedRealSiblingRows=${counts.pairedRealSiblingRows}, familyHasRealRows=${counts.familyHasRealRows}`
      );
    }
  }
  lines.push('');
  lines.push('## Rows With Extracted Rollout Suffix But No Pair');
  if (!(report.rowsWithExtractedRolloutSuffixButNoPair || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.rowsWithExtractedRolloutSuffixButNoPair || []) {
      lines.push(`- ${row.id} | ${row.title} | rolloutSuffixToken=${row.rolloutSuffixToken} | siblingMatchStatus=${row.siblingMatchStatus}`);
    }
  }
  lines.push('');
  lines.push('## Rows With Paired Real Sibling');
  if (!(report.rowsWithPairedRealSibling || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.rowsWithPairedRealSibling || []) {
      lines.push(
        `- ${row.id} | ${row.title} | rolloutSuffixToken=${row.rolloutSuffixToken} | siblingRealDocIds=${(row.siblingRealDocIds || []).join(", ") || "<none>"}`
      );
    }
  }
  lines.push('');
  lines.push('## Removable by Age Threshold');
  if (!(report.removableByAgeThreshold || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.removableByAgeThreshold || []) {
      lines.push(`- ${row.id} | ${row.title} | ageThresholdSatisfied=${row.ageThresholdSatisfied} | confidence=${row.removableFixtureConfidence}`);
    }
  }
  lines.push('');
  lines.push('## Removable by Explicit Fixture Signals');
  if (!(report.removableByExplicitFixtureSignals || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.removableByExplicitFixtureSignals || []) {
      lines.push(`- ${row.id} | ${row.title} | fixtureEvidence=${(row.fixtureEvidence || []).join(", ") || "<none>"}`);
    }
  }
  lines.push('');
  lines.push('## Rows Missing Age Evidence');
  if (!(report.rowsMissingAgeEvidence || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.rowsMissingAgeEvidence || []) {
      lines.push(`- ${row.id} | ${row.title} | ageEvidenceSource=${row.ageEvidenceSource}`);
    }
  }
  lines.push('');
  lines.push('## Rows With Only Self Family Match');
  if (!(report.rowsWithOnlySelfFamilyMatch || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.rowsWithOnlySelfFamilyMatch || []) {
      lines.push(`- ${row.id} | ${row.title} | family=${row.rolloutFamilyKey || "<none>"}`);
    }
  }
  lines.push('');
  lines.push('## Rows With Paired Sibling But Too Young');
  if (!(report.rowsWithPairedSiblingButTooYoung || []).length) {
    lines.push('- none');
  } else {
    for (const row of report.rowsWithPairedSiblingButTooYoung || []) {
      lines.push(
        `- ${row.id} | ${row.title} | age=${row.ageDays ?? "<none>"}/${row.requiredAgeDays} | siblingRealDocIds=${(row.siblingRealDocIds || []).join(", ") || "<none>"}`
      );
    }
  }
  lines.push('');
  lines.push('- Read-only only. No deletion or mutation is performed.');
  return `${lines.join('\n')}\n`;
}
