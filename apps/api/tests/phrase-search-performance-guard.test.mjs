import test from "node:test";
import assert from "node:assert/strict";

import {
  evaluatePhraseSearchPerformance,
  formatPhraseSearchPerformanceMarkdown,
  PHRASE_SEARCH_PERFORMANCE_TASKS
} from "../scripts/phrase-search-performance-guard.mjs";

test("phrase search performance guard classifies slow lexical and total timings", () => {
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
            lexicalSearch: 1700
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
    warningThresholds: { totalMs: 7000, lexicalMs: 1500 },
    summary: { queryCount: 1, warningCount: 1 },
    results: [
      {
        query: "pipe noise",
        status: "returned",
        totalResults: 2,
        wallMs: 8300,
        totalMs: 8100,
        lexicalMs: 1700,
        warnings: ["total_over_7000ms"],
        topCitations: ["T123", "T456"]
      }
    ]
  });

  assert.match(markdown, /Queries with warnings: 1\/1/);
  assert.match(markdown, /## WARN pipe noise/);
  assert.match(markdown, /top citations: T123 \| T456/);
});
