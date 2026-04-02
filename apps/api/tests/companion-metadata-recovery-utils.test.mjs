import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildCompanionRecoveryCandidates,
  classifyCompanionFamily,
  deriveCompanionBaseKey
} from '../scripts/lib/companion-metadata-recovery-utils.mjs';

test('deriveCompanionBaseKey strips companion suffixes', () => {
  assert.equal(deriveCompanionBaseKey('T141307-POST-HEARING-ORDER'), 'T141307');
  assert.equal(deriveCompanionBaseKey('L160990-DECISION-TECH-CORR'), 'L160990');
  assert.equal(deriveCompanionBaseKey('L141717-DECISION-TECH-CORR-2'), 'L141717');
});

test('classifyCompanionFamily recognizes derivative families', () => {
  assert.equal(classifyCompanionFamily({ title: 'Decision Tech Corr', citation: '' }), 'tech_corr');
  assert.equal(classifyCompanionFamily({ title: 'Post hearing order', citation: '' }), 'post_hearing_order');
  assert.equal(classifyCompanionFamily({ title: 'Minute Order', citation: '' }), 'minute_order');
});

test('buildCompanionRecoveryCandidates selects unanimous sibling payloads', () => {
  const targets = [
    {
      documentId: 'doc_target',
      title: 'T141307 Post hearing order',
      citation: 'T141307-POST-HEARING-ORDER',
      decisionDate: '2014-07-17',
      indexCodesJson: '[]',
      rulesSectionsJson: '[]',
      ordinanceSectionsJson: '[]'
    }
  ];
  const siblings = [
    {
      documentId: 'doc_sibling_1',
      title: 'T141307 Decision',
      citation: 'T141307-DECISION',
      searchableAt: '2026-03-01T00:00:00.000Z',
      qcPassed: 1,
      extractionConfidence: 0.9,
      updatedAt: '2026-03-01T00:00:00.000Z',
      indexCodesJson: '["G27"]',
      rulesSectionsJson: '["Rule 37.2","Rule 37.8"]',
      ordinanceSectionsJson: '["Ordinance 37.3"]'
    },
    {
      documentId: 'doc_sibling_2',
      title: 'T141307 Decision Tech Corr',
      citation: 'T141307-DECISION-TECH-CORR',
      searchableAt: '2026-03-02T00:00:00.000Z',
      qcPassed: 1,
      extractionConfidence: 0.85,
      updatedAt: '2026-03-02T00:00:00.000Z',
      indexCodesJson: '["G27"]',
      rulesSectionsJson: '["Rule 37.2","Rule 37.8"]',
      ordinanceSectionsJson: '["Ordinance 37.3"]'
    }
  ];

  const result = buildCompanionRecoveryCandidates(targets, siblings);
  assert.equal(result.selected.length, 1);
  assert.equal(result.selected[0].evaluation.family, 'post_hearing_order');
  assert.deepEqual(result.selected[0].evaluation.payload.index_codes, ['G27']);
  assert.deepEqual(result.selected[0].evaluation.payload.rules_sections, ['Rule 37.2', 'Rule 37.8']);
  assert.deepEqual(result.selected[0].evaluation.payload.ordinance_sections, ['Ordinance 37.3']);
  assert.equal(result.selected[0].evaluation.payload.confirm_required_metadata, true);
});

test('buildCompanionRecoveryCandidates skips non-unanimous sibling payloads', () => {
  const targets = [
    {
      documentId: 'doc_target',
      title: 'L160990 Decision Tech Corr',
      citation: 'L160990-DECISION-TECH-CORR',
      decisionDate: '2016-03-10',
      indexCodesJson: '[]',
      rulesSectionsJson: '[]',
      ordinanceSectionsJson: '[]'
    }
  ];
  const siblings = [
    {
      documentId: 'doc_sibling_1',
      title: 'L160990 Decision',
      citation: 'L160990-DECISION',
      searchableAt: '2026-03-01T00:00:00.000Z',
      qcPassed: 1,
      extractionConfidence: 0.9,
      updatedAt: '2026-03-01T00:00:00.000Z',
      indexCodesJson: '["G27"]',
      rulesSectionsJson: '["Rule 37.2"]',
      ordinanceSectionsJson: '["Ordinance 37.3"]'
    },
    {
      documentId: 'doc_sibling_2',
      title: 'L160990 Post hearing order',
      citation: 'L160990-POST-HEARING-ORDER',
      searchableAt: '2026-03-02T00:00:00.000Z',
      qcPassed: 1,
      extractionConfidence: 0.85,
      updatedAt: '2026-03-02T00:00:00.000Z',
      indexCodesJson: '["G28"]',
      rulesSectionsJson: '["Rule 37.2"]',
      ordinanceSectionsJson: '["Ordinance 37.3"]'
    }
  ];

  const result = buildCompanionRecoveryCandidates(targets, siblings);
  assert.equal(result.selected.length, 0);
  assert.equal(result.skipped.length, 1);
  assert.equal(result.skipped[0].evaluation.reason, 'index_payload_not_unanimous');
});
