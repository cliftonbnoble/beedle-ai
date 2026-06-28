import test from "node:test";
import assert from "node:assert/strict";

import { buildMarkdown, summarizeStageBottlenecks } from "../scripts/phrase-search-qa-report.mjs";

test("phrase search QA report aggregates dominant bottleneck stages", () => {
  assert.deepEqual(
    summarizeStageBottlenecks([
      { slowestStage: { stage: "decisionLayerFetch", ms: 2800 } },
      { slowestStage: { stage: "lexicalSearch", ms: 1400 } },
      { slowestStage: { stage: "decisionLayerFetch", ms: 1800 } },
      { slowestStage: { stage: "rerank", ms: 2400 } }
    ]),
    [
      { stage: "decisionLayerFetch", queryCount: 2, totalMs: 4600, maxMs: 2800, averageMs: 2300 },
      { stage: "rerank", queryCount: 1, totalMs: 2400, maxMs: 2400, averageMs: 2400 },
      { stage: "lexicalSearch", queryCount: 1, totalMs: 1400, maxMs: 1400, averageMs: 1400 }
    ]
  );
});

test("phrase search QA markdown includes aggregate bottleneck summary", () => {
  const markdown = buildMarkdown({
    generatedAt: "2026-06-27T00:00:00.000Z",
    apiBase: "http://127.0.0.1:8787",
    corpusMode: "trusted_only",
    summary: {
      queryCount: 1,
      passed: 1,
      failed: 0,
      stageBottlenecks: [{ stage: "decisionLayerFetch", queryCount: 1, totalMs: 2800, maxMs: 2800, averageMs: 2800 }]
    },
    results: [
      {
        query: "pipe noise",
        passed: true,
        lexicalMs: 900,
        totalMs: 2800,
        wallMs: 2900,
        slowestStages: [{ stage: "decisionLayerFetch", ms: 2800 }],
        failures: [],
        top1ExpectedHits: ["pipe"],
        expectedHits: ["pipe"],
        topResults: [{ rank: 1, title: "Pipe Decision", score: 12.5, snippet: "pipe noise evidence" }]
      }
    ]
  });

  assert.match(markdown, /Dominant bottleneck stages: `decisionLayerFetch` \(1 queries, avg `2800ms`, max `2800ms`\)/);
  assert.match(markdown, /## PASS pipe noise/);
  assert.match(markdown, /slowest stages: `decisionLayerFetch=2800ms`/);
});
