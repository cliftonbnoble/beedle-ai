import test from 'node:test';
import assert from 'node:assert/strict';
import { buildReviewerFixturePruningReport, formatReviewerFixturePruningMarkdown } from '../scripts/reviewer-fixture-pruning-report-utils.mjs';

function row(overrides = {}) {
  return {
    id: 'doc_1',
    title: 'Operational Real Candidate',
    citation: 'REAL-1',
    createdAt: '2026-03-01T00:00:00.000Z',
    updatedAt: '2026-03-01T00:00:00.000Z',
    isLikelyFixture: false,
    runtimeSurfaceForManualReview: true,
    runtimeDisposition: 'possible_manual_context_fix_but_no_auto_apply',
    runtimeDoNotAutoApply: true,
    runtimeManualReasonCode: 'low_risk_not_found_residue',
    runtimeOperatorReviewSummary: 'low_risk_not_found_residue: ...',
    blocked37xReferences: [],
    ...overrides
  };
}

test('real docs are not removable without explicit fixture evidence', () => {
  const report = buildReviewerFixturePruningReport({
    rows: [
      row({ title: 'Approval Rollout Maybe Real', isLikelyFixture: false, citation: 'BEE-ROLL-REAL' })
    ],
    nowIso: '2026-03-10T00:00:00.000Z',
    minFixtureAgeDays: 2
  });
  assert.equal(report.summary.likelyRemovableFixtureCount, 0);
  assert.equal(report.summary.ambiguousFixtureLikeCount, 1);
  assert.equal(report.ambiguousFixtureLike[0].bucket, 'ambiguous_fixture_like_row');
  assert.ok(report.ambiguousFixtureLike[0].fixtureEvidence.includes('fixture_title_prefix_pattern'));
  assert.ok(report.ambiguousFixtureLike[0].fixtureEvidence.includes('fixture_citation_pattern'));
});

test('fixture rows with deterministic signals and age are removable candidates', () => {
  const report = buildReviewerFixturePruningReport({
    rows: [
      row({
        id: 'real_old_pair',
        title: 'Approval Rollout SafeManual rolloutfixx',
        citation: 'L182777-DECISION',
        isLikelyFixture: false,
        runtimeManualReasonCode: 'parenthetical_prefix_fix_candidate',
        unresolvedBuckets: ['likely_parenthetical_or_prefix_fix'],
        recurringCitationFamilies: ['37.2']
      }),
      row({
        id: 'fix_1',
        title: 'Fixture Runtime Candidate rolloutfixx',
        citation: 'BEE-ROLL-FIX',
        isLikelyFixture: true,
        createdAt: '2026-03-01T00:00:00.000Z'
      })
    ],
    nowIso: '2026-03-10T00:00:00.000Z',
    minFixtureAgeDays: 2
  });
  assert.equal(report.summary.likelyRemovableFixtureCount, 1);
  assert.equal(report.likelyRemovableFixture[0].bucket, 'likely_removable_fixture');
  assert.equal(report.likelyRemovableFixture[0].runtimeDoNotAutoApply, true);
  assert.equal(report.likelyRemovableFixture[0].removableFixtureConfidence, 'high');
  assert.equal(report.likelyRemovableFixture[0].ageThresholdSatisfied, true);
  assert.equal(report.likelyRemovableFixture[0].ambiguousDerivedStatus, null);
  assert.equal(report.likelyRemovableFixture[0].daysUntilEligible, null);
  assert.equal(report.likelyRemovableFixture[0].removableIfAgedToday, false);
  assert.ok(report.likelyRemovableFixture[0].fixtureEvidence.includes('likely_fixture_flag'));
  assert.ok(report.likelyRemovableFixture[0].classificationRulesTriggered.includes('likely_removable_fixture_multi_signal_age_gated'));
});

test('real surfaced low-risk 37.2 runtime candidates stay keep_operational', () => {
  const report = buildReviewerFixturePruningReport({
    rows: [
      row({
        id: 'real_keep_1',
        isLikelyFixture: false,
        runtimeSurfaceForManualReview: true,
        runtimeDoNotAutoApply: true,
        runtimeManualReasonCode: 'parenthetical_prefix_fix_candidate',
        unresolvedBuckets: ['likely_parenthetical_or_prefix_fix'],
        recurringCitationFamilies: ['37.2'],
        blocked37xReferences: []
      })
    ],
    nowIso: '2026-03-10T00:00:00.000Z'
  });
  assert.equal(report.summary.keepOperationalCount, 1);
  assert.equal(report.summary.ambiguousFixtureLikeCount, 0);
  assert.equal(report.keepOperational[0].bucket, 'keep_operational');
  assert.ok(report.keepOperational[0].keepOperationalEvidence.includes('low_risk_37_2_residue_path'));
  assert.equal(report.keepOperational[0].removableFixtureConfidence, 'none');
  assert.deepEqual(report.keepOperational[0].siblingRealDocIds, []);
  assert.deepEqual(report.keepOperational[0].siblingFixtureDocIds, []);
  assert.notEqual(report.keepOperational[0].siblingMatchStatus, 'paired_real_match');
});

test('clear old fixture sibling becomes likely_removable_fixture via sibling rule', () => {
  const report = buildReviewerFixturePruningReport({
    rows: [
      row({
        id: 'real_1',
        title: 'Approval Rollout SafeManual rolloutmmidn5ja',
        citation: 'L182368-DECISION',
        isLikelyFixture: false,
        runtimeSurfaceForManualReview: true,
        runtimeManualReasonCode: 'parenthetical_prefix_fix_candidate',
        unresolvedBuckets: ['likely_parenthetical_or_prefix_fix'],
        recurringCitationFamilies: ['37.2']
      }),
      row({
        id: 'fixture_1',
        title: 'Fixture Runtime Candidate rolloutmmidn5ja',
        citation: 'KNOWN-REF-different-token',
        isLikelyFixture: true,
        createdAt: '2026-03-01T00:00:00.000Z'
      })
    ],
    nowIso: '2026-03-10T00:00:00.000Z',
    minFixtureAgeDays: 2
  });
  assert.equal(report.summary.likelyRemovableFixtureCount, 1);
  const fixture = report.likelyRemovableFixture[0];
  assert.equal(fixture.id, 'fixture_1');
  assert.ok(fixture.siblingRealDocIds.includes('real_1'));
  assert.ok(!fixture.siblingRealDocIds.includes('fixture_1'));
  assert.equal(fixture.siblingRealMatchCount, 1);
  assert.equal(fixture.siblingMatchStatus, 'paired_real_match');
  assert.equal(fixture.rolloutSuffixToken, 'mmidn5ja');
  assert.equal(fixture.rolloutFamilyKeySource, 'title_rollout_suffix');
  assert.equal(fixture.ageThresholdSatisfied, true);
  assert.ok(fixture.classificationRulesTriggered.includes('likely_removable_fixture_sibling_pair_rule'));
});

test('fixture row without age threshold remains ambiguous', () => {
  const report = buildReviewerFixturePruningReport({
    rows: [
      row({
        id: 'real_fresh_pair',
        title: 'Approval Rollout SafeManual rolloutfresh',
        citation: 'L182999-DECISION',
        isLikelyFixture: false,
        runtimeManualReasonCode: 'parenthetical_prefix_fix_candidate',
        unresolvedBuckets: ['likely_parenthetical_or_prefix_fix'],
        recurringCitationFamilies: ['37.2']
      }),
      row({
        id: 'fixture_young',
        title: 'Fixture Runtime Candidate rolloutfresh',
        citation: 'KNOWN-REF-rolloutfresh',
        isLikelyFixture: true,
        createdAt: '2026-03-09T00:00:00.000Z'
      })
    ],
    nowIso: '2026-03-10T00:00:00.000Z',
    minFixtureAgeDays: 2
  });
  assert.equal(report.summary.likelyRemovableFixtureCount, 0);
  assert.equal(report.summary.ambiguousFixtureLikeCount, 1);
  const row0 = report.ambiguousFixtureLike[0];
  assert.equal(row0.ageThresholdSatisfied, false);
  assert.equal(row0.removableFixtureConfidence, 'none');
  assert.equal(row0.ageEvidenceSource, 'createdAt');
  assert.ok(row0.promotionFailureReasons.includes('age_threshold_not_satisfied'));
  assert.equal(row0.ambiguousDerivedStatus, 'awaiting_age_threshold');
  assert.equal(row0.daysUntilEligible, 1);
  assert.equal(row0.removableIfAgedToday, true);
});

test('paired real sibling but too young remains ambiguous with explicit failure reasons', () => {
  const report = buildReviewerFixturePruningReport({
    rows: [
      row({
        id: 'real_pair',
        title: 'Approval Rollout SafeManual rolloutfamilyx',
        citation: 'L182415-DECISION',
        isLikelyFixture: false,
        runtimeManualReasonCode: 'parenthetical_prefix_fix_candidate',
        unresolvedBuckets: ['likely_parenthetical_or_prefix_fix'],
        recurringCitationFamilies: ['37.2']
      }),
      row({
        id: 'fixture_pair_young',
        title: 'Fixture Runtime Candidate rolloutfamilyx',
        citation: 'KNOWN-REF-notmatchingcitation',
        isLikelyFixture: true,
        createdAt: '2026-03-09T00:00:00.000Z'
      })
    ],
    nowIso: '2026-03-10T00:00:00.000Z',
    minFixtureAgeDays: 2
  });
  assert.equal(report.summary.likelyRemovableFixtureCount, 0);
  assert.equal(report.summary.ambiguousFixtureLikeCount, 1);
  const candidate = report.ambiguousFixtureLike[0];
  assert.equal(candidate.siblingMatchStatus, 'paired_real_match');
  assert.equal(candidate.rolloutSuffixToken, 'familyx');
  assert.ok(candidate.promotionFailureReasons.includes('paired_sibling_detected_but_too_young'));
  assert.ok(candidate.promotionFailureReasons.includes('age_threshold_not_satisfied'));
  assert.equal(candidate.ambiguousDerivedStatus, 'awaiting_age_threshold');
  assert.equal(candidate.removableIfAgedToday, true);
  assert.equal(report.rowsWithPairedSiblingButTooYoung.length, 1);
  assert.equal(report.summary.awaitingAgeThresholdCount, 1);
});

test('missing-age fixture row remains ambiguous with missing_age_evidence', () => {
  const report = buildReviewerFixturePruningReport({
    rows: [
      row({
        id: 'fixture_no_age',
        title: 'Fixture Runtime Candidate refnoage',
        citation: 'KNOWN-REF-refnoage',
        isLikelyFixture: true,
        createdAt: null,
        updatedAt: null
      })
    ],
    nowIso: '2026-03-10T00:00:00.000Z',
    minFixtureAgeDays: 2
  });
  assert.equal(report.summary.ambiguousFixtureLikeCount, 1);
  const candidate = report.ambiguousFixtureLike[0];
  assert.equal(candidate.ageEvidenceSource, 'none');
  assert.ok(candidate.promotionFailureReasons.includes('missing_age_evidence'));
  assert.equal(candidate.ambiguousDerivedStatus, 'missing_age_evidence');
  assert.equal(candidate.removableIfAgedToday, false);
  assert.equal(report.rowsMissingAgeEvidence.length, 1);
  assert.equal(report.summary.missingAgeEvidenceCount, 1);
});

test('unpaired fixture row classifies as missing_real_pair', () => {
  const report = buildReviewerFixturePruningReport({
    rows: [
      row({
        id: 'fixture_unpaired',
        title: 'Fixture Runtime Candidate rolloutsolo1234',
        citation: 'KNOWN-REF-rolloutsolo1234',
        isLikelyFixture: true,
        createdAt: '2026-03-01T00:00:00.000Z'
      })
    ],
    nowIso: '2026-03-10T00:00:00.000Z',
    minFixtureAgeDays: 2
  });
  assert.equal(report.summary.ambiguousFixtureLikeCount, 1);
  const candidate = report.ambiguousFixtureLike[0];
  assert.equal(candidate.ambiguousDerivedStatus, 'missing_real_pair');
  assert.ok(candidate.promotionFailureReasons.includes('missing_paired_real_sibling'));
  assert.equal(report.summary.missingRealPairCount, 1);
});

test('unsafe surfaced count remains zero when no unsafe families are surfaced', () => {
  const report = buildReviewerFixturePruningReport({
    rows: [row({ blocked37xReferences: [{ family: '37.3' }], runtimeSurfaceForManualReview: false })]
  });
  assert.equal(report.summary.unsafeRuntimeManualSurfacedViolations, 0);
});

test('markdown includes required sections', () => {
  const report = buildReviewerFixturePruningReport({ rows: [row()] });
  const md = formatReviewerFixturePruningMarkdown(report);
  assert.match(md, /# Reviewer Fixture Pruning Report/);
  assert.match(md, /## Summary/);
  assert.match(md, /## Likely Removable Fixture/);
  assert.match(md, /## Keep Operational/);
  assert.match(md, /## Ambiguous Fixture-like Rows/);
  assert.match(md, /## Classification Rules Triggered/);
  assert.match(md, /## Sibling Pair Evidence/);
  assert.match(md, /## Removable by Age Threshold/);
  assert.match(md, /## Removable by Explicit Fixture Signals/);
  assert.match(md, /## Promotion Failure Reason Counts/);
  assert.match(md, /## Rows Missing Age Evidence/);
  assert.match(md, /## Rows With Only Self Family Match/);
  assert.match(md, /## Rows With Paired Sibling But Too Young/);
  assert.match(md, /## Rollout Suffix Pair Counts/);
  assert.match(md, /## Rows With Extracted Rollout Suffix But No Pair/);
  assert.match(md, /## Rows With Paired Real Sibling/);
  assert.match(md, /## Rows Awaiting Age Threshold/);
  assert.match(md, /## Rows Missing Real Pair/);
  assert.match(md, /## Paired Families Awaiting Age/);
});

test('classifier is deterministic for same input', () => {
  const rows = [
    row({ id: 'a', title: 'Fixture Runtime Candidate zzz', citation: 'BEE-ROLL-X', isLikelyFixture: true, createdAt: '2026-03-01T00:00:00.000Z' }),
    row({
      id: 'b',
      isLikelyFixture: false,
      runtimeManualReasonCode: 'parenthetical_prefix_fix_candidate',
      unresolvedBuckets: ['likely_parenthetical_or_prefix_fix'],
      recurringCitationFamilies: ['37.2'],
      blocked37xReferences: []
    }),
    row({ id: 'c', title: 'Approval Rollout Maybe Real', isLikelyFixture: false, citation: 'BEE-ROLL-REAL' })
  ];
  const a = buildReviewerFixturePruningReport({ rows, nowIso: '2026-03-10T00:00:00.000Z' });
  const b = buildReviewerFixturePruningReport({ rows, nowIso: '2026-03-10T00:00:00.000Z' });
  assert.deepEqual(a.countsByBucket, b.countsByBucket);
  assert.deepEqual(a.classificationRulesTriggered, b.classificationRulesTriggered);
});
