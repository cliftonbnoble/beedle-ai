import { loadTrustedActivatedDocumentIds } from "./retrieval-live-search-qa-utils.mjs";
import {
  BENCHMARK_INTENT_TO_QUERY_TYPE,
  buildBenchmarkDebugPayload,
  buildScoringInputs,
  normalizeSectionTypeRuntime,
  toTrustedRows
} from "./retrieval-benchmark-contract-utils.mjs";

export const R60_GOLDSET_TASKS = [
  {
    queryId: "gold_authority_001",
    query: "ordinance 37.2 authority discussion for permit denial",
    intent: "authority_lookup",
    expectedDecisionIds: ["doc_115e68c9-edcc-473c-bbfc-7d3b7538605a"],
    expectedSectionTypes: ["authority_discussion", "analysis_reasoning"],
    minimumAcceptableRank: 5,
    notes: "Core ordinance authority lookup used in judge memo prep."
  },
  {
    queryId: "gold_authority_002",
    query: "rule 37.8 legal standard authority section",
    intent: "authority_lookup",
    expectedDecisionIds: ["doc_03a67369-8a19-4264-81c5-21424ebb5cf4"],
    expectedSectionTypes: ["authority_discussion"],
    minimumAcceptableRank: 5,
    notes: "Direct rules citation retrieval."
  },
  {
    queryId: "gold_authority_003",
    query: "index code reference and ordinance cross-reference analysis",
    intent: "authority_lookup",
    expectedDecisionIds: ["doc_23ac8cee-72f8-4ff1-8123-bdd7169016a7"],
    expectedSectionTypes: ["authority_discussion", "analysis_reasoning"],
    minimumAcceptableRank: 8,
    notes: "Cross-reference style legal citation query."
  },
  {
    queryId: "gold_findings_001",
    query: "findings of fact regarding witness credibility and evidence weight",
    intent: "findings",
    expectedDecisionIds: ["doc_2b0ccae5-2111-4180-8376-76e79b82c5d1"],
    expectedSectionTypes: ["findings", "analysis_reasoning"],
    minimumAcceptableRank: 5,
    notes: "Findings/credibility workflow."
  },
  {
    queryId: "gold_findings_002",
    query: "substantial evidence findings and factual determinations",
    intent: "findings",
    expectedDecisionIds: ["doc_2dfe0da2-e42e-41be-bcb9-57ca805b83bc"],
    expectedSectionTypes: ["findings", "facts_background"],
    minimumAcceptableRank: 5,
    notes: "Evidence-focused findings retrieval."
  },
  {
    queryId: "gold_findings_003",
    query: "credibility determination in administrative hearing record",
    intent: "findings",
    expectedDecisionIds: ["doc_1e1f711f-a1f6-40ba-b3d7-d0541171fd69"],
    expectedSectionTypes: ["findings", "analysis_reasoning"],
    minimumAcceptableRank: 8,
    notes: "Hearing credibility precedents."
  },
  {
    queryId: "gold_procedural_001",
    query: "procedural history hearing notice and continuance timeline",
    intent: "procedural_history",
    expectedDecisionIds: ["doc_345fd497-a82c-40ca-a45d-c0aca1b17826"],
    expectedSectionTypes: ["procedural_history", "facts_background"],
    minimumAcceptableRank: 5,
    notes: "Procedural chronology for clerk brief."
  },
  {
    queryId: "gold_procedural_002",
    query: "service of notice and procedural due process hearing steps",
    intent: "procedural_history",
    expectedDecisionIds: ["doc_496366f7-dea1-4246-8333-326f629cca57"],
    expectedSectionTypes: ["procedural_history"],
    minimumAcceptableRank: 8,
    notes: "Notice/service procedural compliance."
  },
  {
    queryId: "gold_procedural_003",
    query: "continuance request handling in prior administrative decisions",
    intent: "procedural_history",
    expectedDecisionIds: ["doc_6497e48b-69b3-44e3-b0c9-e7eb370067e2"],
    expectedSectionTypes: ["procedural_history", "analysis_reasoning"],
    minimumAcceptableRank: 8,
    notes: "Continuance procedure comparison."
  },
  {
    queryId: "gold_issue_disposition_001",
    query: "issue presented and final holding on ordinance violation",
    intent: "issue_holding_disposition",
    expectedDecisionIds: ["doc_115e68c9-edcc-473c-bbfc-7d3b7538605a"],
    expectedSectionTypes: ["issue_statement", "holding_disposition", "analysis_reasoning"],
    minimumAcceptableRank: 5,
    notes: "Issue-to-holding retrieval."
  },
  {
    queryId: "gold_issue_disposition_002",
    query: "order and disposition language for permit revocation",
    intent: "issue_holding_disposition",
    expectedDecisionIds: ["doc_03a67369-8a19-4264-81c5-21424ebb5cf4"],
    expectedSectionTypes: ["holding_disposition", "order"],
    minimumAcceptableRank: 5,
    notes: "Order/disposition drafting support."
  },
  {
    queryId: "gold_issue_disposition_003",
    query: "questions presented and outcome of legal issue",
    intent: "issue_holding_disposition",
    expectedDecisionIds: ["doc_23ac8cee-72f8-4ff1-8123-bdd7169016a7"],
    expectedSectionTypes: ["issue_statement", "holding_disposition"],
    minimumAcceptableRank: 8,
    notes: "Issue-framing and outcome linkage."
  },
  {
    queryId: "gold_analysis_001",
    query: "analysis reasoning applying legal standard to facts",
    intent: "analysis_reasoning",
    expectedDecisionIds: ["doc_2b0ccae5-2111-4180-8376-76e79b82c5d1"],
    expectedSectionTypes: ["analysis_reasoning", "authority_discussion"],
    minimumAcceptableRank: 5,
    notes: "Core legal reasoning lookup."
  },
  {
    queryId: "gold_analysis_002",
    query: "application of ordinance standard in analysis section",
    intent: "analysis_reasoning",
    expectedDecisionIds: ["doc_2dfe0da2-e42e-41be-bcb9-57ca805b83bc"],
    expectedSectionTypes: ["analysis_reasoning", "authority_discussion"],
    minimumAcceptableRank: 5,
    notes: "Ordinance application analysis."
  },
  {
    queryId: "gold_analysis_003",
    query: "legal reasoning discussion comparing prior rulings",
    intent: "analysis_reasoning",
    expectedDecisionIds: ["doc_1e1f711f-a1f6-40ba-b3d7-d0541171fd69"],
    expectedSectionTypes: ["analysis_reasoning"],
    minimumAcceptableRank: 8,
    notes: "Comparative legal reasoning."
  },
  {
    queryId: "gold_comparative_001",
    query: "compare how prior decisions treated ordinance 37.2 evidence",
    intent: "comparative_reasoning",
    expectedDecisionIds: [
      "doc_115e68c9-edcc-473c-bbfc-7d3b7538605a",
      "doc_03a67369-8a19-4264-81c5-21424ebb5cf4"
    ],
    expectedSectionTypes: ["analysis_reasoning", "authority_discussion", "findings"],
    minimumAcceptableRank: 10,
    notes: "Cross-decision comparison workflow."
  },
  {
    queryId: "gold_comparative_002",
    query: "similar decisions discussing hearing notice and continuance",
    intent: "comparative_reasoning",
    expectedDecisionIds: [
      "doc_345fd497-a82c-40ca-a45d-c0aca1b17826",
      "doc_496366f7-dea1-4246-8333-326f629cca57"
    ],
    expectedSectionTypes: ["procedural_history", "analysis_reasoning"],
    minimumAcceptableRank: 10,
    notes: "Procedural comparative search."
  },
  {
    queryId: "gold_comparative_003",
    query: "compare disposition outcomes in ordinance citation cases",
    intent: "comparative_reasoning",
    expectedDecisionIds: [
      "doc_23ac8cee-72f8-4ff1-8123-bdd7169016a7",
      "doc_2b0ccae5-2111-4180-8376-76e79b82c5d1"
    ],
    expectedSectionTypes: ["holding_disposition", "analysis_reasoning"],
    minimumAcceptableRank: 10,
    notes: "Outcome comparison lookup."
  },
  {
    queryId: "gold_citation_001",
    query: "Rule 37.8",
    intent: "citation_direct",
    expectedDecisionIds: ["doc_03a67369-8a19-4264-81c5-21424ebb5cf4"],
    expectedSectionTypes: ["authority_discussion"],
    minimumAcceptableRank: 5,
    notes: "Direct rule citation."
  },
  {
    queryId: "gold_citation_002",
    query: "Ordinance 37.2",
    intent: "citation_direct",
    expectedDecisionIds: ["doc_115e68c9-edcc-473c-bbfc-7d3b7538605a"],
    expectedSectionTypes: ["authority_discussion"],
    minimumAcceptableRank: 5,
    notes: "Direct ordinance citation."
  },
  {
    queryId: "gold_citation_003",
    query: "index code section in prior decisions",
    intent: "citation_direct",
    expectedDecisionIds: ["doc_23ac8cee-72f8-4ff1-8123-bdd7169016a7"],
    expectedSectionTypes: ["authority_discussion", "analysis_reasoning"],
    minimumAcceptableRank: 8,
    notes: "Direct index code retrieval."
  },
  {
    queryId: "gold_citation_004",
    query: "ordinance citation and holding language",
    intent: "citation_direct",
    expectedDecisionIds: ["doc_2dfe0da2-e42e-41be-bcb9-57ca805b83bc"],
    expectedSectionTypes: ["authority_discussion", "holding_disposition"],
    minimumAcceptableRank: 8,
    notes: "Citation + disposition combined lookup."
  },
  {
    queryId: "gold_evidence_001",
    query: "weight of documentary evidence in findings section",
    intent: "findings",
    expectedDecisionIds: ["doc_2b0ccae5-2111-4180-8376-76e79b82c5d1"],
    expectedSectionTypes: ["findings", "analysis_reasoning"],
    minimumAcceptableRank: 8,
    notes: "Evidence weighting precedents."
  },
  {
    queryId: "gold_evidence_002",
    query: "credibility analysis and factual findings in hearing decisions",
    intent: "findings",
    expectedDecisionIds: ["doc_1e1f711f-a1f6-40ba-b3d7-d0541171fd69"],
    expectedSectionTypes: ["findings", "analysis_reasoning"],
    minimumAcceptableRank: 8,
    notes: "Credibility/factual synthesis."
  }
];

const LOW_SIGNAL_CHUNK_TYPES = new Set([
  "caption",
  "caption_title",
  "issue_statement",
  "appearances",
  "questions_presented",
  "parties",
  "questions presented",
  "APPEARANCES",
  "QUESTIONS PRESENTED"
]);

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function topKDecisionHit(task, rows, k) {
  const expected = new Set((task.expectedDecisionIds || []).map(String));
  if (!expected.size) return false;
  return rows.slice(0, k).some((row) => expected.has(String(row.documentId || "")));
}

function firstExpectedRank(task, rows) {
  const expected = new Set((task.expectedDecisionIds || []).map(String));
  for (let i = 0; i < rows.length; i += 1) {
    if (expected.has(String(rows[i]?.documentId || ""))) return i + 1;
  }
  return null;
}

function sectionHit(task, rows, k = 5) {
  const expected = new Set((task.expectedSectionTypes || []).map(String));
  if (!expected.size) return false;
  return rows.slice(0, k).some((row) => expected.has(String(row.chunkType || row.sectionLabel || "")));
}

function noisyDomination(rows, k = 5) {
  const top = rows.slice(0, k);
  if (!top.length) return false;
  const noisy = top.filter((row) => LOW_SIGNAL_CHUNK_TYPES.has(String(row.chunkType || row.sectionLabel || ""))).length;
  return noisy / top.length >= 0.6;
}

export async function runR60GoldsetEvaluation({
  apiBase,
  reportsDir,
  tasks = R60_GOLDSET_TASKS,
  limit = 10,
  trustedDocumentIdsOverride = null,
  fetchSearchDebug
}) {
  const trusted = trustedDocumentIdsOverride
    ? { trustedDocumentIds: trustedDocumentIdsOverride, sources: ["override"] }
    : await loadTrustedActivatedDocumentIds({ reportsDir });
  const trustedSet = new Set((trusted.trustedDocumentIds || []).map(String));

  const queryResults = [];
  for (const task of tasks) {
    const queryType = BENCHMARK_INTENT_TO_QUERY_TYPE[String(task.intent || "")] || "keyword";
    const payload = buildBenchmarkDebugPayload({
      query: task.query,
      intent: task.intent,
      queryType,
      limit
    });
    const response = await fetchSearchDebug(payload);
    const rows = toTrustedRows(response.results || [], trustedSet, limit).map((row) => ({
      documentId: row.documentId,
      title: row.title,
      chunkId: row.chunkId,
      chunkType: row.sectionType,
      rankScore: row.score,
      citationAnchor: row.citationAnchor,
      sourceLink: row.sourceLink
    }));
    const scoring = buildScoringInputs({
      task: {
        ...task,
        expectedSectionTypes: (task.expectedSectionTypes || []).map((value) => normalizeSectionTypeRuntime(value))
      },
      trustedRows: rows.map((row) => ({
        documentId: row.documentId,
        sectionType: row.chunkType
      })),
      topK: 5
    });

    const row = {
      ...task,
      topResults: rows.slice(0, limit),
      top1DecisionHit: scoring.top1Hit,
      top3DecisionHit: scoring.top3Hit,
      top5DecisionHit: scoring.top5Hit,
      sectionTypeHit: scoring.sectionTypeHit,
      firstExpectedRank: scoring.firstExpectedRank,
      noisyChunkDominated: noisyDomination(rows, 5),
      meetsMinimumRank:
        scoring.firstExpectedRank !== null && Number(scoring.firstExpectedRank) <= Number(task.minimumAcceptableRank || 999),
      falsePositiveChunkTypes: rows
        .slice(0, 5)
        .map((result) => String(result.chunkType || ""))
        .filter((type) => !(task.expectedSectionTypes || []).map((value) => normalizeSectionTypeRuntime(value)).includes(type))
    };
    queryResults.push(row);
  }

  const total = Math.max(1, queryResults.length);
  const top1DecisionHitRate = Number(
    (queryResults.filter((row) => row.top1DecisionHit).length / total).toFixed(4)
  );
  const top3DecisionHitRate = Number(
    (queryResults.filter((row) => row.top3DecisionHit).length / total).toFixed(4)
  );
  const top5DecisionHitRate = Number(
    (queryResults.filter((row) => row.top5DecisionHit).length / total).toFixed(4)
  );
  const sectionTypeHitRate = Number(
    (queryResults.filter((row) => row.sectionTypeHit).length / total).toFixed(4)
  );
  const noisyChunkDominationRate = Number(
    (queryResults.filter((row) => row.noisyChunkDominated).length / total).toFixed(4)
  );

  const intentBreakdown = Object.entries(
    queryResults.reduce((acc, row) => {
      const key = String(row.intent || "unknown");
      if (!acc[key]) {
        acc[key] = {
          tasks: 0,
          top1Hits: 0,
          top3Hits: 0,
          top5Hits: 0,
          sectionHits: 0
        };
      }
      acc[key].tasks += 1;
      if (row.top1DecisionHit) acc[key].top1Hits += 1;
      if (row.top3DecisionHit) acc[key].top3Hits += 1;
      if (row.top5DecisionHit) acc[key].top5Hits += 1;
      if (row.sectionTypeHit) acc[key].sectionHits += 1;
      return acc;
    }, {})
  )
    .map(([intent, row]) => ({
      intent,
      tasks: row.tasks,
      top1DecisionHitRate: Number((row.top1Hits / Math.max(1, row.tasks)).toFixed(4)),
      top3DecisionHitRate: Number((row.top3Hits / Math.max(1, row.tasks)).toFixed(4)),
      top5DecisionHitRate: Number((row.top5Hits / Math.max(1, row.tasks)).toFixed(4)),
      sectionTypeHitRate: Number((row.sectionHits / Math.max(1, row.tasks)).toFixed(4))
    }))
    .sort((a, b) => a.intent.localeCompare(b.intent));

  const falsePositiveChunkTypeCounts = Object.entries(
    countBy(queryResults.flatMap((row) => row.falsePositiveChunkTypes))
  )
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    })
    .map(([chunkType, count]) => ({ chunkType, count }));

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    summary: {
      tasksEvaluated: queryResults.length,
      trustedDocumentCount: trustedSet.size
    },
    top1DecisionHitRate,
    top3DecisionHitRate,
    top5DecisionHitRate,
    sectionTypeHitRate,
    intentBreakdown,
    falsePositiveChunkTypeCounts,
    noisyChunkDominationRate,
    queryResults,
    trustedCorpus: {
      trustedDocumentIds: Array.from(trustedSet).sort((a, b) => a.localeCompare(b)),
      sources: trusted.sources || []
    }
  };
}
