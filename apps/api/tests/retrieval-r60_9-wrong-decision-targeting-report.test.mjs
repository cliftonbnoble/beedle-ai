import test from "node:test";
import assert from "node:assert/strict";
import { buildR60_9WrongDecisionTargetingReport } from "../scripts/retrieval-r60_9-wrong-decision-targeting-report.mjs";

test("R60.9 extracts recovered-but-wrong tasks and emits intent-specific refinements", () => {
  const source = {
    wrongDecisionTaskIds: ["q1", "q3"],
    normalizedPackages: [
      {
        queryId: "q1",
        intent: "citation_direct",
        originalQuery: "Rule 37.8 authority",
        normalizedQuery: "rule 37.8 authority",
        compressedKeywordQuery: "rule 37.8 authority",
        citationFocusedQuery: "Rule 37.8",
        proceduralQuery: "",
        findingsCredibilityQuery: "",
        dispositionQuery: "",
        legalConceptTerms: ["rule", "37.8", "authority"]
      },
      {
        queryId: "q3",
        intent: "findings",
        originalQuery: "findings credibility",
        normalizedQuery: "findings credibility evidence",
        compressedKeywordQuery: "findings credibility",
        citationFocusedQuery: "",
        proceduralQuery: "",
        findingsCredibilityQuery: "findings of fact credibility witness evidence weight",
        dispositionQuery: "",
        legalConceptTerms: ["findings", "credibility", "evidence"]
      }
    ],
    taskEvaluations: [
      {
        queryId: "q1",
        intent: "citation_direct",
        expectedDecisionIds: ["doc_a"],
        expectedSectionTypes: ["authority_discussion"],
        bestVariantType: "normalized",
        variantRows: [
          {
            variantType: "normalized",
            query: "rule 37.8 authority",
            topReturnedDecisionIds: ["doc_x"],
            topReturnedSectionTypes: ["analysis_reasoning"],
            returnedCount: 2
          }
        ]
      },
      {
        queryId: "q3",
        intent: "findings",
        expectedDecisionIds: ["doc_b"],
        expectedSectionTypes: ["findings"],
        bestVariantType: "findings_credibility",
        variantRows: [
          {
            variantType: "findings_credibility",
            query: "findings of fact credibility witness evidence weight",
            topReturnedDecisionIds: ["doc_y"],
            topReturnedSectionTypes: ["analysis_reasoning"],
            returnedCount: 2
          }
        ]
      },
      {
        queryId: "q2",
        intent: "analysis_reasoning",
        expectedDecisionIds: ["doc_c"],
        expectedSectionTypes: ["analysis_reasoning"],
        bestVariantType: "normalized",
        variantRows: []
      }
    ]
  };

  const out = buildR60_9WrongDecisionTargetingReport(source);
  assert.equal(out.phase, "R60.9");
  assert.equal(out.recoveredButWrongTaskCount, 2);
  assert.ok(out.primaryMissReasonClassification);
  assert.ok(Array.isArray(out.candidateIntentSpecificRewriteRules));
  assert.ok(out.perTaskRows.every((row) => row.queryRefinementCandidates.length > 0));
});
