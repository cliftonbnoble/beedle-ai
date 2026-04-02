import test from "node:test";
import assert from "node:assert/strict";
import {
  evaluateInferenceCandidate,
  selectInferenceCandidates
} from "../scripts/lib/missing-index-inference-utils.mjs";

test("evaluateInferenceCandidate selects a strong crosswalk-backed winner", () => {
  const result = evaluateInferenceCandidate({
    candidateCodes: [
      { code: "A32", score: 12.5, sources: ["crosswalk", "phrase"] },
      { code: "H18.1", score: 7.25, sources: ["phrase"] }
    ]
  });

  assert.equal(result.eligible, true);
  assert.equal(result.selectedCode, "A32");
  assert.equal(result.margin, 5.25);
});

test("evaluateInferenceCandidate blocks ambiguous second-place candidates", () => {
  const result = evaluateInferenceCandidate({
    candidateCodes: [
      { code: "C6", score: 8.1, sources: ["crosswalk"] },
      { code: "C80", score: 7.2, sources: ["crosswalk"] }
    ]
  });

  assert.equal(result.eligible, false);
  assert.equal(result.reason, "ambiguous_second_candidate");
});

test("evaluateInferenceCandidate blocks phrase-only candidates by default", () => {
  const result = evaluateInferenceCandidate({
    candidateCodes: [{ code: "H18.1", score: 9.0, sources: ["phrase"] }]
  });

  assert.equal(result.eligible, false);
  assert.equal(result.reason, "source_not_allowed");
});

test("selectInferenceCandidates returns selected rows sorted by confidence", () => {
  const result = selectInferenceCandidates([
    {
      citation: "DOC-2",
      decisionDate: "2024-01-01",
      candidateCodes: [{ code: "M1", score: 6.75, sources: ["crosswalk"] }]
    },
    {
      citation: "DOC-1",
      decisionDate: "2024-02-01",
      candidateCodes: [{ code: "A32", score: 12.0, sources: ["crosswalk", "phrase"] }]
    }
  ]);

  assert.equal(result.selected.length, 2);
  assert.equal(result.selected[0].row.citation, "DOC-1");
  assert.equal(result.selected[1].row.citation, "DOC-2");
});

