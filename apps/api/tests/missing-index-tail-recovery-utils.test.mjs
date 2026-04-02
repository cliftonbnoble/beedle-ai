import test from 'node:test';
import assert from 'node:assert/strict';
import {
  extractCaseTokens,
  evaluateTailInferenceCandidate,
  selectTailInferenceCandidates
} from '../scripts/lib/missing-index-tail-recovery-utils.mjs';

test('extractCaseTokens returns stable unique case ids', () => {
  assert.deepEqual(
    extractCaseTokens('AT230060-T230498-DECISION-TECH-CORR T230498 Decision'),
    ['AT230060', 'T230498']
  );
});

test('evaluateTailInferenceCandidate accepts phrase-only exact hits over generic crosswalks', () => {
  const result = evaluateTailInferenceCandidate({
    candidateCodes: [
      {
        code: 'N21',
        score: 7.25,
        sources: ['phrase'],
        evidence: ['exact_phrase:Roommate', 'findings_phrase:Roommate']
      },
      {
        code: 'J10',
        score: 7,
        sources: ['crosswalk'],
        evidence: ['linked_ordinance:Ordinance 37.3']
      }
    ]
  });

  assert.equal(result.eligible, true);
  assert.equal(result.reason, 'phrase_only_exact_beats_generic');
  assert.deepEqual(result.selectedIndexCodes, ['N21']);
});

test('evaluateTailInferenceCandidate accepts sibling-like crosswalk margin winners', () => {
  const result = evaluateTailInferenceCandidate({
    candidateCodes: [
      {
        code: 'C6',
        score: 10.25,
        sources: ['crosswalk'],
        evidence: ['linked_rules:Rule 12.15']
      },
      {
        code: 'D21',
        score: 9.25,
        sources: ['crosswalk', 'phrase'],
        evidence: ['exact_phrase:Condition']
      }
    ]
  });

  assert.equal(result.eligible, true);
  assert.equal(result.reason, 'crosswalk_margin');
  assert.deepEqual(result.selectedIndexCodes, ['C6']);
});

test('selectTailInferenceCandidates sorts higher-confidence rows first', () => {
  const result = selectTailInferenceCandidates([
    {
      citation: 'DOC-2',
      decisionDate: '2024-01-01',
      candidateCodes: [
        {
          code: 'N21',
          score: 7.25,
          sources: ['phrase'],
          evidence: ['exact_phrase:Roommate', 'findings_phrase:Roommate']
        },
        {
          code: 'J10',
          score: 7,
          sources: ['crosswalk'],
          evidence: ['linked_ordinance:Ordinance 37.3']
        }
      ]
    },
    {
      citation: 'DOC-1',
      decisionDate: '2024-02-01',
      candidateCodes: [
        {
          code: 'J39',
          score: 15.75,
          sources: ['crosswalk', 'phrase'],
          evidence: ['exact_phrase:General Obligation Bond Passthrough', 'findings_phrase:General Obligation Bond Passthrough']
        },
        {
          code: 'J8',
          score: 14.75,
          sources: ['crosswalk', 'phrase'],
          evidence: ['exact_phrase:Fixed Income']
        }
      ]
    }
  ]);

  assert.equal(result.selected.length, 2);
  assert.equal(result.selected[0].row.citation, 'DOC-1');
  assert.equal(result.selected[1].row.citation, 'DOC-2');
});
