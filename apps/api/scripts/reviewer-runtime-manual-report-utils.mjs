function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function normalizeCandidate(raw) {
  const blockedFamilies = Array.from(new Set((raw.blocked37xReferences || []).map((item) => String(item.family || "")).filter(Boolean))).sort();
  return {
    id: String(raw.id || ""),
    title: String(raw.title || ""),
    runtimeDisposition: String(raw.runtimeDisposition || "keep_blocked"),
    runtimeSurfaceForManualReview: Boolean(raw.runtimeSurfaceForManualReview),
    runtimeDoNotAutoApply: raw.runtimeDoNotAutoApply !== false,
    runtimeManualReasonCode: String(raw.runtimeManualReasonCode || "none"),
    runtimeManualReasonSummary: String(raw.runtimeManualReasonSummary || ""),
    runtimeSuggestedOperatorAction: String(raw.runtimeSuggestedOperatorAction || ""),
    runtimeOperatorReviewSummary: String(raw.runtimeOperatorReviewSummary || ""),
    runtimeReviewDiagnostic: raw.runtimeReviewDiagnostic ?? null,
    unresolvedBuckets: (raw.unresolvedBuckets || []).map((item) => String(item)).filter(Boolean),
    recurringCitationFamilies: (raw.recurringCitationFamilies || []).map((item) => String(item)).filter(Boolean),
    isLikelyFixture: Boolean(raw.isLikelyFixture),
    blocked37xFamilies: blockedFamilies
  };
}

export function buildReviewerRuntimeManualReport({ allDocuments = [], topLimit = 25, includeFixtures = false }) {
  const candidates = allDocuments.map(normalizeCandidate).filter((row) => row.runtimeSurfaceForManualReview);
  const realCandidates = candidates.filter((row) => !row.isLikelyFixture);
  const fixtureCandidates = candidates.filter((row) => row.isLikelyFixture);
  const operationalCandidates = includeFixtures ? candidates : realCandidates;

  const unsafeSurfaced = candidates.filter((row) => row.blocked37xFamilies.some((family) => ["37.3", "37.7", "37.9"].includes(family)));

  return {
    generatedAt: new Date().toISOString(),
    mode: includeFixtures ? "mixed_include_fixtures" : "real_operational_default",
    summary: {
      totalRuntimeManualCandidates: candidates.length,
      realRuntimeManualCandidates: realCandidates.length,
      fixtureRuntimeManualCandidates: fixtureCandidates.length,
      operationalRuntimeManualCandidates: operationalCandidates.length,
      includeFixtures,
      unsafe37xSurfacedCount: unsafeSurfaced.length,
      unsafe37xSuppressionOk: unsafeSurfaced.length === 0,
      rowsLeftForManualReview: realCandidates.length,
      surfacedRuntimeManualCandidates: candidates.length,
      surfacedRuntimeManualRealCandidates: realCandidates.length,
      surfacedRuntimeManualFixtureCandidates: fixtureCandidates.length,
      unsafeRuntimeManualSurfacedViolations: unsafeSurfaced.length
    },
    countsByRecurringCitationFamily: countBy(operationalCandidates.flatMap((row) => row.recurringCitationFamilies)),
    countsByUnresolvedBucket: countBy(operationalCandidates.flatMap((row) => row.unresolvedBuckets)),
    countsByRecurringCitationFamilyAll: countBy(candidates.flatMap((row) => row.recurringCitationFamilies)),
    countsByUnresolvedBucketAll: countBy(candidates.flatMap((row) => row.unresolvedBuckets)),
    topCandidateDocs: operationalCandidates.slice(0, topLimit).map((row) => ({
      id: row.id,
      title: row.title,
      runtimeDisposition: row.runtimeDisposition,
      runtimeSurfaceForManualReview: row.runtimeSurfaceForManualReview,
      runtimeDoNotAutoApply: row.runtimeDoNotAutoApply,
      runtimeManualReasonCode: row.runtimeManualReasonCode,
      runtimeManualReasonSummary: row.runtimeManualReasonSummary,
      runtimeSuggestedOperatorAction: row.runtimeSuggestedOperatorAction,
      runtimeOperatorReviewSummary: row.runtimeOperatorReviewSummary,
      runtimeReviewDiagnostic: row.runtimeReviewDiagnostic,
      unresolvedBuckets: row.unresolvedBuckets,
      recurringCitationFamilies: row.recurringCitationFamilies,
      isLikelyFixture: row.isLikelyFixture
    })),
    realTopCandidateDocs: realCandidates.slice(0, topLimit),
    fixtureTopCandidateDocs: fixtureCandidates.slice(0, topLimit),
    unsafe37xSurfaceViolations: unsafeSurfaced.slice(0, topLimit)
  };
}

export function buildRuntimeManualCountAlignment(params) {
  const mixedSummary = params?.mixedSummary || {};
  const realSummary = params?.realSummary || {};
  const reportSummary = params?.reportSummary || {};

  const mixedApiTotal = Number(mixedSummary.surfacedRuntimeManualCandidates || 0);
  const mixedApiReal = Number(mixedSummary.surfacedRuntimeManualRealCandidates || 0);
  const mixedApiFixture = Number(mixedSummary.surfacedRuntimeManualFixtureCandidates || 0);
  const realApiTotal = Number(realSummary.surfacedRuntimeManualCandidates || 0);

  const reportTotal = Number(reportSummary.surfacedRuntimeManualCandidates || 0);
  const reportReal = Number(reportSummary.surfacedRuntimeManualRealCandidates || 0);
  const reportFixture = Number(reportSummary.surfacedRuntimeManualFixtureCandidates || 0);

  return {
    mixedApiVsReportTotalMatch: mixedApiTotal === reportTotal,
    mixedApiVsReportRealMatch: mixedApiReal === reportReal,
    mixedApiVsReportFixtureMatch: mixedApiFixture === reportFixture,
    realOnlyApiVsReportRealMatch: realApiTotal === reportReal,
    allMatches: mixedApiTotal === reportTotal && mixedApiReal === reportReal && mixedApiFixture === reportFixture && realApiTotal === reportReal,
    mixedApi: {
      total: mixedApiTotal,
      real: mixedApiReal,
      fixture: mixedApiFixture,
      unsafeViolations: Number(mixedSummary.unsafeRuntimeManualSurfacedViolations || 0)
    },
    realOnlyApi: {
      total: realApiTotal,
      unsafeViolations: Number(realSummary.unsafeRuntimeManualSurfacedViolations || 0)
    },
    report: {
      total: reportTotal,
      real: reportReal,
      fixture: reportFixture,
      unsafeViolations: Number(reportSummary.unsafeRuntimeManualSurfacedViolations || 0)
    }
  };
}

export function formatReviewerRuntimeManualReportMarkdown(report) {
  const lines = [];
  lines.push("# Runtime Manual Candidate Verification (Read-only)");
  lines.push("");
  lines.push(`- mode: ${report.mode}`);
  lines.push(`- includeFixtures: ${report.summary.includeFixtures}`);
  lines.push("");
  lines.push("## Summary");
  lines.push(`- totalRuntimeManualCandidates: ${report.summary.totalRuntimeManualCandidates}`);
  lines.push(`- realRuntimeManualCandidates: ${report.summary.realRuntimeManualCandidates}`);
  lines.push(`- fixtureRuntimeManualCandidates: ${report.summary.fixtureRuntimeManualCandidates}`);
  lines.push(`- operationalRuntimeManualCandidates: ${report.summary.operationalRuntimeManualCandidates}`);
  lines.push(`- unsafe37xSurfacedCount: ${report.summary.unsafe37xSurfacedCount}`);
  lines.push(`- unsafe37xSuppressionOk: ${report.summary.unsafe37xSuppressionOk}`);
  lines.push(`- rowsLeftForManualReview: ${report.summary.rowsLeftForManualReview}`);
  lines.push(`- surfacedRuntimeManualCandidates: ${report.summary.surfacedRuntimeManualCandidates}`);
  lines.push(`- surfacedRuntimeManualRealCandidates: ${report.summary.surfacedRuntimeManualRealCandidates}`);
  lines.push(`- surfacedRuntimeManualFixtureCandidates: ${report.summary.surfacedRuntimeManualFixtureCandidates}`);
  lines.push(`- unsafeRuntimeManualSurfacedViolations: ${report.summary.unsafeRuntimeManualSurfacedViolations}`);
  lines.push("");
  lines.push("## Count Alignment");
  if (report.countAlignment) {
    lines.push(`- mixedApiVsReportTotalMatch: ${report.countAlignment.mixedApiVsReportTotalMatch}`);
    lines.push(`- mixedApiVsReportRealMatch: ${report.countAlignment.mixedApiVsReportRealMatch}`);
    lines.push(`- mixedApiVsReportFixtureMatch: ${report.countAlignment.mixedApiVsReportFixtureMatch}`);
    lines.push(`- realOnlyApiVsReportRealMatch: ${report.countAlignment.realOnlyApiVsReportRealMatch}`);
    lines.push(`- allMatches: ${report.countAlignment.allMatches}`);
  } else {
    lines.push("- none");
  }
  lines.push("");

  lines.push("## Operational Counts by Recurring Citation Family");
  const byFamily = Object.entries(report.countsByRecurringCitationFamily || {});
  if (byFamily.length === 0) {
    lines.push("- none");
  } else {
    for (const [key, value] of byFamily) lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Operational Counts by Unresolved Bucket");
  const byBucket = Object.entries(report.countsByUnresolvedBucket || {});
  if (byBucket.length === 0) {
    lines.push("- none");
  } else {
    for (const [key, value] of byBucket) lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Top Candidate Docs (Operational View)");
  if ((report.topCandidateDocs || []).length === 0) {
    lines.push("- none");
  } else {
    for (const row of report.topCandidateDocs || []) {
      lines.push(`- ${row.id} | ${row.title}`);
      lines.push(`  - runtimeDisposition=${row.runtimeDisposition} | runtimeSurfaceForManualReview=${row.runtimeSurfaceForManualReview} | runtimeDoNotAutoApply=${row.runtimeDoNotAutoApply}`);
      lines.push(`  - runtimeManualReasonCode=${row.runtimeManualReasonCode} | runtimeManualReasonSummary=${row.runtimeManualReasonSummary || "<none>"}`);
      lines.push(`  - runtimeSuggestedOperatorAction=${row.runtimeSuggestedOperatorAction || "<none>"}`);
      lines.push(`  - runtimeOperatorReviewSummary=${row.runtimeOperatorReviewSummary || "<none>"}`);
      lines.push(`  - unresolvedBuckets=${(row.unresolvedBuckets || []).join(", ") || "<none>"}`);
      lines.push(`  - recurringCitationFamilies=${(row.recurringCitationFamilies || []).join(", ") || "<none>"}`);
      lines.push(`  - isLikelyFixture=${row.isLikelyFixture}`);
    }
  }
  lines.push("");
  lines.push("## Fixture Candidate Docs");
  if ((report.fixtureTopCandidateDocs || []).length === 0) {
    lines.push("- none");
  } else {
    for (const row of report.fixtureTopCandidateDocs || []) {
      lines.push(`- ${row.id} | ${row.title}`);
      lines.push(`  - runtimeDisposition=${row.runtimeDisposition} | runtimeDoNotAutoApply=${row.runtimeDoNotAutoApply}`);
      lines.push(`  - runtimeManualReasonCode=${row.runtimeManualReasonCode} | runtimeManualReasonSummary=${row.runtimeManualReasonSummary || "<none>"}`);
      lines.push(`  - runtimeSuggestedOperatorAction=${row.runtimeSuggestedOperatorAction || "<none>"}`);
      lines.push(`  - runtimeOperatorReviewSummary=${row.runtimeOperatorReviewSummary || "<none>"}`);
      lines.push(`  - unresolvedBuckets=${(row.unresolvedBuckets || []).join(", ") || "<none>"}`);
      lines.push(`  - recurringCitationFamilies=${(row.recurringCitationFamilies || []).join(", ") || "<none>"}`);
    }
  }
  lines.push("");
  lines.push("- Read-only report only. No approval/QC/citation mutation is performed.");
  lines.push("- doNotAutoApply remains true for surfaced candidates.");
  return `${lines.join("\n")}\n`;
}
