import test from "node:test";
import assert from "node:assert/strict";
import {
  computeTopicSignals,
  computeWorthReprocessing,
  isLikelyFixtureDoc,
  isLikelyFixtureSourceKey,
  scoreSourceImportCandidate
} from "../scripts/provisional-topic-candidate-utils.mjs";

test("fixture heuristic excludes harness and BEE docs", () => {
  assert.equal(isLikelyFixtureDoc({ title: "Harness Decision Invalid", citation: "T123" }), true);
  assert.equal(isLikelyFixtureDoc({ title: "Real Decision", citation: "BEE-DEC-123" }), true);
  assert.equal(isLikelyFixtureDoc({ title: "T192015 Decision", citation: "T192015-DECISION" }), false);
  assert.equal(isLikelyFixtureSourceKey("decision_docx/2026-03-09/f625b1f9-f3aa-423d-9655-1d468edb9511-retrieval-fallback.docx"), true);
  assert.equal(isLikelyFixtureSourceKey("decision_docx/2026-03-08/c58e1883-2b91-4c90-b42a-bd24d32584dc-T192015 Decision.docx"), false);
});

test("topic signals capture direct and synonym hits", () => {
  const signals = computeTopicSignals({
    coolingDirectHits: 1,
    coolingSynonymHits: 2,
    cooling_0_cooling_hit: 1,
    cooling_4_ventilation_hit: 1,
    cooling_7_air_flow_hit: 1,
    ventilationDirectHits: 0,
    ventilationSynonymHits: 1,
    ventilation_0_ventilation_hit: 1,
    moldDirectHits: 0,
    moldSynonymHits: 0
  });

  assert.equal(signals.cooling.totalHits, 3);
  assert.deepEqual(signals.cooling.matchedTerms, ["cooling", "ventilation", "air flow"]);
  assert.equal(signals.ventilation.totalHits, 1);
});

test("worth reprocessing blocks structurally bad docs even when topical", () => {
  const result = computeWorthReprocessing({
    chunkCount: 20,
    xmlChunkCount: 14,
    tinyChunkCount: 1,
    lowValueSectionCount: 15,
    usefulSectionCount: 0,
    unresolvedReferenceCount: 2,
    unsafe37xReferenceCount: 1,
    extractionConfidence: 0.68,
    r2ObjectPresent: 1,
    coolingDirectHits: 1,
    coolingSynonymHits: 2
  });

  assert.equal(result.worthReprocessing, false);
  assert.ok(result.blockers.includes("xml_artifact_ratio_too_high"));
  assert.ok(result.blockers.includes("no_useful_sections"));
});

test("worth reprocessing accepts topic-bearing cleaner docs", () => {
  const result = computeWorthReprocessing({
    chunkCount: 30,
    xmlChunkCount: 0,
    tinyChunkCount: 1,
    lowValueSectionCount: 3,
    usefulSectionCount: 10,
    unresolvedReferenceCount: 1,
    unsafe37xReferenceCount: 0,
    extractionConfidence: 0.78,
    r2ObjectPresent: 1,
    moldDirectHits: 1,
    moldSynonymHits: 2
  });

  assert.equal(result.worthReprocessing, true);
  assert.equal(result.strongestTopic, "mold");
  assert.ok(result.score >= 35);
});

test("source import scoring rejects fixture-like keys and prefers topical names", () => {
  const bad = scoreSourceImportCandidate({
    key: "decision_docx/2026-03-07/7fb8e8a3-c793-491a-82ba-a0c40405ebf4-decision_fail.docx"
  });
  const good = scoreSourceImportCandidate({
    key: "decision_docx/2026-03-10/abc123-mold-ventilation-decision.docx"
  });

  assert.equal(bad.worthImporting, false);
  assert.equal(good.worthImporting, true);
  assert.equal(good.strongestTopic, "cooling");
});
