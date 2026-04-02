import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildReviewerRuntimeManualReport,
  buildRuntimeManualCountAlignment,
  formatReviewerRuntimeManualReportMarkdown
} from '../scripts/reviewer-runtime-manual-report-utils.mjs';

function doc(overrides = {}) {
  return {
    id: 'doc_1',
    title: 'Doc 1',
    runtimeDisposition: 'possible_manual_context_fix_but_no_auto_apply',
    runtimeSurfaceForManualReview: true,
    runtimeDoNotAutoApply: true,
    runtimeManualReasonCode: 'parenthetical_prefix_fix_candidate',
    runtimeManualReasonSummary: 'Formatting signal detected.',
    runtimeSuggestedOperatorAction: 'Review citation formatting during app workflow.',
    runtimeOperatorReviewSummary: 'parenthetical_prefix_fix_candidate: Formatting signal detected.',
    runtimeReviewDiagnostic: { runtimeDoNotAutoApply: true },
    unresolvedBuckets: ['likely_parenthetical_or_prefix_fix'],
    recurringCitationFamilies: ['37.2'],
    isLikelyFixture: false,
    blocked37xReferences: [],
    ...overrides
  };
}

test('report separates real vs fixture manual candidates and keeps unsafe suppressed', () => {
  const report = buildReviewerRuntimeManualReport({
    allDocuments: [
      doc({ id: 'real_1' }),
      doc({ id: 'fix_1', isLikelyFixture: true }),
      doc({ id: 'unsafe_hidden', runtimeSurfaceForManualReview: false, runtimeDisposition: 'keep_blocked', blocked37xReferences: [{ family: '37.3' }] })
    ],
    topLimit: 10
  });

  assert.equal(report.summary.totalRuntimeManualCandidates, 2);
  assert.equal(report.summary.realRuntimeManualCandidates, 1);
  assert.equal(report.summary.fixtureRuntimeManualCandidates, 1);
  assert.equal(report.summary.operationalRuntimeManualCandidates, 1);
  assert.equal(report.summary.includeFixtures, false);
  assert.equal(report.summary.surfacedRuntimeManualCandidates, 2);
  assert.equal(report.summary.surfacedRuntimeManualRealCandidates, 1);
  assert.equal(report.summary.surfacedRuntimeManualFixtureCandidates, 1);
  assert.equal(report.summary.unsafeRuntimeManualSurfacedViolations, 0);
  assert.equal(report.mode, 'real_operational_default');
  assert.equal(report.topCandidateDocs.length, 1);
  assert.equal(report.topCandidateDocs[0].id, 'real_1');
  assert.equal(report.fixtureTopCandidateDocs.length, 1);
  assert.equal(report.summary.unsafe37xSurfacedCount, 0);
  assert.equal(report.summary.unsafe37xSuppressionOk, true);
  assert.equal(report.topCandidateDocs[0].runtimeManualReasonCode, 'parenthetical_prefix_fix_candidate');
  assert.equal(typeof report.topCandidateDocs[0].runtimeManualReasonSummary, 'string');
  assert.equal(typeof report.topCandidateDocs[0].runtimeSuggestedOperatorAction, 'string');
  assert.equal(typeof report.topCandidateDocs[0].runtimeOperatorReviewSummary, 'string');
  assert.ok(report.topCandidateDocs[0].runtimeReviewDiagnostic);
});

test('unsafe surfaced rows are flagged as violations', () => {
  const report = buildReviewerRuntimeManualReport({
    allDocuments: [
      doc({ id: 'unsafe_1', blocked37xReferences: [{ family: '37.7' }] })
    ]
  });

  assert.equal(report.summary.totalRuntimeManualCandidates, 1);
  assert.equal(report.summary.unsafe37xSurfacedCount, 1);
  assert.equal(report.summary.unsafe37xSuppressionOk, false);
  assert.equal(report.unsafe37xSurfaceViolations[0].id, 'unsafe_1');
});

test('include fixtures mode shows mixed operational view', () => {
  const report = buildReviewerRuntimeManualReport({
    allDocuments: [doc({ id: 'real_1' }), doc({ id: 'fix_1', isLikelyFixture: true })],
    includeFixtures: true
  });
  assert.equal(report.summary.includeFixtures, true);
  assert.equal(report.mode, 'mixed_include_fixtures');
  assert.equal(report.summary.operationalRuntimeManualCandidates, 2);
  assert.equal(report.topCandidateDocs.length, 2);
});

test('markdown output includes core summary sections', () => {
  const report = buildReviewerRuntimeManualReport({ allDocuments: [doc()] });
  const md = formatReviewerRuntimeManualReportMarkdown(report);
  assert.match(md, /# Runtime Manual Candidate Verification/);
  assert.match(md, /## Summary/);
  assert.match(md, /## Operational Counts by Recurring Citation Family/);
  assert.match(md, /## Operational Counts by Unresolved Bucket/);
  assert.match(md, /## Top Candidate Docs \(Operational View\)/);
  assert.match(md, /## Fixture Candidate Docs/);
  assert.match(md, /runtimeManualReasonCode=/);
  assert.match(md, /runtimeSuggestedOperatorAction=/);
  assert.match(md, /runtimeOperatorReviewSummary=/);
  assert.match(md, /## Count Alignment/);
});

test('count alignment helper is deterministic and exact when inputs match', () => {
  const alignment = buildRuntimeManualCountAlignment({
    mixedSummary: {
      surfacedRuntimeManualCandidates: 10,
      surfacedRuntimeManualRealCandidates: 4,
      surfacedRuntimeManualFixtureCandidates: 6,
      unsafeRuntimeManualSurfacedViolations: 0
    },
    realSummary: {
      surfacedRuntimeManualCandidates: 4,
      unsafeRuntimeManualSurfacedViolations: 0
    },
    reportSummary: {
      surfacedRuntimeManualCandidates: 10,
      surfacedRuntimeManualRealCandidates: 4,
      surfacedRuntimeManualFixtureCandidates: 6,
      unsafeRuntimeManualSurfacedViolations: 0
    }
  });
  assert.equal(alignment.mixedApiVsReportTotalMatch, true);
  assert.equal(alignment.mixedApiVsReportRealMatch, true);
  assert.equal(alignment.mixedApiVsReportFixtureMatch, true);
  assert.equal(alignment.realOnlyApiVsReportRealMatch, true);
  assert.equal(alignment.allMatches, true);
});
