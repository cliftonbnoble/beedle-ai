import test from "node:test";
import assert from "node:assert/strict";
import {
  buildRetrievalLiveSearchQaReport,
  formatRetrievalLiveSearchQaMarkdown
} from "../scripts/retrieval-live-search-qa-utils.mjs";

const trustedDocumentIds = ["doc_a", "doc_b"];

const queries = [
  {
    id: "q_authority",
    group: "ordinance_rule_authority_lookup",
    query: "ordinance 37.2 rule 37.8 authority discussion",
    queryType: "rules_ordinance",
    expectedChunkTypes: ["authority_discussion", "analysis_reasoning"],
    intent: "authority"
  },
  {
    id: "q_findings",
    group: "findings_credibility_evidence",
    query: "findings of fact credibility evidence",
    queryType: "keyword",
    expectedChunkTypes: ["findings"],
    intent: "findings"
  }
];

function mkResult({
  documentId,
  chunkId,
  title,
  sectionLabel,
  score = 9.5,
  rerankScore = 6,
  sourceLink = "https://example.test/doc",
  sourceFileRef = "source/ref.docx",
  citationAnchor = "p1"
}) {
  return {
    documentId,
    chunkId,
    title,
    citation: "X",
    fileType: "decision_docx",
    snippet: "sample",
    sectionLabel,
    sourceFileRef,
    sourceLink,
    citationAnchor,
    sectionHeading: sectionLabel,
    paragraphAnchor: citationAnchor,
    score,
    lexicalScore: 0.8,
    vectorScore: 0.2,
    diagnostics: {
      lexicalScore: 0.8,
      vectorScore: 0.2,
      exactPhraseBoost: 0,
      citationBoost: 0,
      metadataBoost: 0,
      sectionBoost: 0,
      partyNameBoost: 0,
      rerankScore,
      why: []
    }
  };
}

const mockByQuery = {
  "ordinance 37.2 rule 37.8 authority discussion": {
    total: 4,
    results: [
      mkResult({ documentId: "doc_a", chunkId: "a1", title: "A", sectionLabel: "authority_discussion", rerankScore: 6.2 }),
      mkResult({ documentId: "doc_a", chunkId: "a2", title: "A", sectionLabel: "authority_discussion", rerankScore: 5.8 }),
      mkResult({ documentId: "doc_b", chunkId: "b1", title: "B", sectionLabel: "analysis_reasoning", rerankScore: 5.1 }),
      mkResult({ documentId: "doc_out", chunkId: "o1", title: "OUT", sectionLabel: "authority_discussion", rerankScore: 7.1 })
    ]
  },
  "findings of fact credibility evidence": {
    total: 3,
    results: [
      mkResult({ documentId: "doc_b", chunkId: "b2", title: "B", sectionLabel: "findings", rerankScore: 4.4 }),
      mkResult({ documentId: "doc_b", chunkId: "b3", title: "B", sectionLabel: "analysis_reasoning", rerankScore: 4.1 }),
      mkResult({
        documentId: "doc_b",
        chunkId: "b4",
        title: "B",
        sectionLabel: "findings",
        rerankScore: 3.8,
        citationAnchor: "",
        sourceLink: ""
      })
    ]
  }
};

async function fakeFetchSearchDebug(payload) {
  return mockByQuery[payload.query] || { total: 0, results: [] };
}

function stripGeneratedAt(value) {
  if (Array.isArray(value)) return value.map((v) => stripGeneratedAt(v));
  if (!value || typeof value !== "object") return value;
  const out = {};
  for (const [k, v] of Object.entries(value)) {
    out[k] = k === "generatedAt" ? "<ignored>" : stripGeneratedAt(v);
  }
  return out;
}

test("live search QA report is deterministic and trusted-corpus scoped", async () => {
  const one = await buildRetrievalLiveSearchQaReport({
    apiBase: "http://local",
    trustedDocumentIds,
    queries,
    fetchSearchDebug: fakeFetchSearchDebug,
    limit: 10,
    realOnly: true
  });
  const two = await buildRetrievalLiveSearchQaReport({
    apiBase: "http://local",
    trustedDocumentIds,
    queries,
    fetchSearchDebug: fakeFetchSearchDebug,
    limit: 10,
    realOnly: true
  });

  assert.deepEqual(stripGeneratedAt(one), stripGeneratedAt(two));

  assert.equal(one.summary.queriesEvaluated, 2);
  assert.equal(one.summary.trustedDocumentCount, 2);

  const authority = one.queryResults.find((row) => row.queryId === "q_authority");
  assert.ok(authority);
  assert.equal(authority.trustedResultCount, 3);
  assert.equal(authority.metrics.outOfCorpusHits, 1);
  assert.ok(authority.topResults.every((row) => ["doc_a", "doc_b"].includes(row.documentId)));

  const findings = one.queryResults.find((row) => row.queryId === "q_findings");
  assert.ok(findings);
  assert.ok(findings.metrics.citationAnchorCoverage < 1);

  assert.ok(Array.isArray(one.recommendedTuningActions));
  assert.ok(one.recommendedTuningActions.length >= 1);
});

test("markdown output includes required QA sections", async () => {
  const report = await buildRetrievalLiveSearchQaReport({
    apiBase: "http://local",
    trustedDocumentIds,
    queries,
    fetchSearchDebug: fakeFetchSearchDebug,
    limit: 10,
    realOnly: true
  });

  const markdown = formatRetrievalLiveSearchQaMarkdown(report);
  assert.match(markdown, /# Retrieval Live Search QA Report/);
  assert.match(markdown, /## Summary/);
  assert.match(markdown, /## Strongest Queries/);
  assert.match(markdown, /## Weakest Queries/);
  assert.match(markdown, /## Recommended Tuning Actions/);
});
