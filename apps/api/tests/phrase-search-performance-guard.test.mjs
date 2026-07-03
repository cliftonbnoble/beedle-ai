import test from "node:test";
import assert from "node:assert/strict";

import {
  evaluatePhraseSearchPerformance,
  formatPhraseSearchPerformanceMarkdown,
  PHRASE_SEARCH_PERFORMANCE_TASKS,
  PHRASE_SEARCH_TARGET_TOTAL_MS,
  summarizeStageBottlenecks,
  summarizeSlowestStages
} from "../scripts/phrase-search-performance-guard.mjs";

test("phrase search performance guard ranks slowest stage timings", () => {
  assert.deepEqual(
    summarizeSlowestStages({
      total: 9000,
      lexicalSearch: 1700,
      vectorSearch: 300,
      decisionLayerFetch: 2400,
      rerank: 2400
    }),
    [
      { stage: "decisionLayerFetch", ms: 2400 },
      { stage: "rerank", ms: 2400 },
      { stage: "lexicalSearch", ms: 1700 },
      { stage: "vectorSearch", ms: 300 }
    ]
  );
});

test("phrase search performance guard aggregates bottleneck stages across tasks", () => {
  assert.deepEqual(
    summarizeStageBottlenecks([
      { slowestStage: { stage: "decisionLayerFetch", ms: 2500 } },
      { slowestStage: { stage: "lexicalSearch", ms: 1700 } },
      { slowestStage: { stage: "decisionLayerFetch", ms: 1500 } },
      { slowestStage: { stage: "rerank", ms: 2400 } }
    ]),
    [
      { stage: "decisionLayerFetch", queryCount: 2, totalMs: 4000, maxMs: 2500, averageMs: 2000 },
      { stage: "rerank", queryCount: 1, totalMs: 2400, maxMs: 2400, averageMs: 2400 },
      { stage: "lexicalSearch", queryCount: 1, totalMs: 1700, maxMs: 1700, averageMs: 1700 }
    ]
  );
});

test("phrase search performance guard classifies slow lexical and total timings", () => {
  assert.equal(PHRASE_SEARCH_TARGET_TOTAL_MS, 3000);
  const evaluated = evaluatePhraseSearchPerformance(
    { id: "pipe_noise", query: "pipe noise" },
    {
      ok: true,
      httpStatus: 200,
      wallMs: 8300,
      body: {
        total: 2,
        runtimeDiagnostics: {
          stageTimingsMs: {
            total: 8100,
            lexicalSearch: 1700,
            decisionLayerFetch: 2500
          }
        },
        results: [
          { citation: "T123", title: "Pipe Noise", documentId: "doc-1", sectionLabel: "Findings", score: 42 },
          { citation: "T456", title: "More Pipe Noise", documentId: "doc-2", sectionLabel: "Analysis", score: 21 }
        ]
      }
    },
    { warnTotalMs: 7000, warnLexicalMs: 1500 }
  );

  assert.equal(evaluated.status, "returned");
  assert.deepEqual(evaluated.warnings, ["total_over_7000ms", "lexical_over_1500ms"]);
  assert.equal(evaluated.totalResults, 2);
  assert.deepEqual(evaluated.slowestStage, { stage: "decisionLayerFetch", ms: 2500 });
  assert.deepEqual(evaluated.slowestStages.slice(0, 2), [
    { stage: "decisionLayerFetch", ms: 2500 },
    { stage: "lexicalSearch", ms: 1700 }
  ]);
  assert.deepEqual(evaluated.topCitations, ["T123", "T456"]);
  assert.deepEqual(evaluated.topResults[0], {
    rank: 1,
    citation: "T123",
    title: "Pipe Noise",
    documentId: "doc-1",
    sectionLabel: "Findings",
    score: 42
  });
});

test("phrase search performance guard reports HTTP failures and known task set", () => {
  const evaluated = evaluatePhraseSearchPerformance(
    PHRASE_SEARCH_PERFORMANCE_TASKS[0],
    {
      ok: false,
      httpStatus: 503,
      wallMs: 1200,
      body: { error: "unavailable" }
    },
    { warnTotalMs: 7000, warnLexicalMs: 1500 }
  );

  assert.equal(PHRASE_SEARCH_PERFORMANCE_TASKS.length, 4);
  assert.equal(evaluated.status, "http_error");
  assert.deepEqual(evaluated.warnings, ["http_503"]);
  assert.match(evaluated.error, /unavailable/);
});

test("phrase search performance markdown includes warning summary and citations", () => {
  const markdown = formatPhraseSearchPerformanceMarkdown({
    generatedAt: "2026-06-26T00:00:00.000Z",
    apiBase: "http://127.0.0.1:8787",
    corpusMode: "trusted_only",
    limit: 8,
    targetTotalMs: 3000,
    warningThresholds: { totalMs: 7000, lexicalMs: 1500 },
    summary: {
      queryCount: 1,
      warningCount: 1,
      stageBottlenecks: [{ stage: "decisionLayerFetch", queryCount: 1, totalMs: 2500, maxMs: 2500, averageMs: 2500 }]
    },
    results: [
      {
        query: "pipe noise",
        status: "returned",
        totalResults: 2,
        wallMs: 8300,
        totalMs: 8100,
        lexicalMs: 1700,
        slowestStages: [
          { stage: "decisionLayerFetch", ms: 2500 },
          { stage: "lexicalSearch", ms: 1700 }
        ],
        warnings: ["total_over_7000ms"],
        topCitations: ["T123", "T456"]
      }
    ]
  });

  assert.match(markdown, /Queries with warnings: 1\/1/);
  assert.match(markdown, /Dominant bottleneck stages: decisionLayerFetch \(1 queries, avg 2500ms, max 2500ms\)/);
  assert.match(markdown, /Target: common phrase searches under 3000ms total/);
  assert.match(markdown, /## WARN pipe noise/);
  assert.match(markdown, /slowest stages: decisionLayerFetch=2500ms, lexicalSearch=1700ms/);
  assert.match(markdown, /top citations: T123 \| T456/);
});
