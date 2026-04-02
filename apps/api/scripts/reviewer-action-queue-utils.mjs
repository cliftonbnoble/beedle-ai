function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}
const UNSAFE_37X = new Set(["37.3", "37.7", "37.9"]);

function hasUnsafe37xFamily(families) {
  return (families || []).some((family) => UNSAFE_37X.has(String(family)));
}

function defaultReviewerNotesTemplate() {
  return [
    "Context decision (ordinance/rules/ambiguous):",
    "Evidence snippet references reviewed:",
    "Decision: keep blocked / escalate / possible manual context fix (no auto-apply):",
    "Reviewer notes:"
  ];
}

function postureFromSubBucket(subBucket) {
  if (subBucket === "likely_ordinance_manual_review") return "possible_manual_context_fix_but_no_auto_apply";
  if (subBucket === "mixed_ambiguous_escalate") return "escalate_to_legal_context_review";
  if (subBucket === "insufficient_context_hold") return "keep_blocked";
  if (subBucket === "not_found_hold") return "keep_blocked";
  if (subBucket === "unclassified_hold") return "keep_blocked";
  return "keep_blocked";
}

function rootCauseFromSubBucket(subBucket) {
  if (subBucket === "not_found_hold") return "not_found";
  if (subBucket === "mixed_ambiguous_escalate") return "cross_context_or_ambiguous";
  if (subBucket === "insufficient_context_hold") return "insufficient_context";
  if (subBucket === "likely_ordinance_manual_review") return "ordinance_context_candidate";
  return "unclassified";
}

function suggestedDecisionOptions(posture) {
  if (posture === "possible_manual_context_fix_but_no_auto_apply") {
    return ["possible_manual_context_fix_but_no_auto_apply", "keep_blocked", "escalate_to_legal_context_review"];
  }
  if (posture === "escalate_to_legal_context_review") {
    return ["escalate_to_legal_context_review", "keep_blocked"];
  }
  return ["keep_blocked", "escalate_to_legal_context_review"];
}

function evidenceMaps(evidenceReport) {
  const byBatch = new Map();
  const byBatchDoc = new Map();
  for (const packet of evidenceReport?.packets || []) {
    const batchKey = String(packet.batchKey || "");
    if (!batchKey) continue;
    byBatch.set(batchKey, {
      reviewerNotesTemplate: Array.isArray(packet.reviewerNotesTemplate) ? packet.reviewerNotesTemplate : defaultReviewerNotesTemplate(),
      blocked37xFamily: Array.isArray(packet.blocked37xFamily) ? packet.blocked37xFamily : []
    });
    const docMap = new Map();
    const snippets = (packet.patternEvidence || []).flatMap((pattern) => pattern.representativeSnippets || []);
    for (const snippet of snippets) {
      const docId = String(snippet.documentId || "");
      if (!docId || docMap.has(docId)) continue;
      docMap.set(docId, {
        rootCause: String(snippet.rootCause || ""),
        contextClass: String(snippet.contextClass || "no_useful_context"),
        localTextSnippet: String(snippet.localTextSnippet || ""),
        rawCitation: String(snippet.rawCitation || "")
      });
    }
    byBatchDoc.set(batchKey, docMap);
  }
  return { byBatch, byBatchDoc };
}

function laneForRow(row) {
  const hasEvidence = Boolean(row.topEvidenceSnippet && row.topEvidenceSnippet.trim());
  if (row.recommendedReviewerPosture === "possible_manual_context_fix_but_no_auto_apply" && hasEvidence) return "review_first";
  if (row.subBucket === "insufficient_context_hold") return "review_after";
  if (row.subBucket === "not_found_hold") return "hold_blocked";
  if (row.recommendedReviewerPosture === "escalate_to_legal_context_review") return "hold_blocked";
  if (row.requiresLegalEscalation) return "hold_blocked";
  if (hasEvidence) return "review_after";
  return "review_after";
}

function rankTuple(row) {
  const laneRank = row.priorityLane === "review_first" ? 0 : row.priorityLane === "review_after" ? 1 : 2;
  const hasEvidence = row.topEvidenceSnippet ? 0 : 1;
  const afterRank = row.subBucket === "insufficient_context_hold" ? 1 : hasEvidence;
  const holdRank = row.subBucket === "not_found_hold" ? 1 : 2;
  const second = laneRank === 1 ? afterRank : laneRank === 2 ? holdRank : hasEvidence;
  return [laneRank, second, String(row.batchKey), String(row.subBucket || ""), String(row.title || ""), String(row.documentId)];
}

function compareRows(a, b) {
  const ra = rankTuple(a);
  const rb = rankTuple(b);
  for (let i = 0; i < ra.length; i += 1) {
    if (ra[i] < rb[i]) return -1;
    if (ra[i] > rb[i]) return 1;
  }
  return 0;
}

function addSplitRows(rows, splitReport, decisionByBatch, evidenceMapsByBatch, evidenceByBatchDoc) {
  for (const batch of splitReport?.splitBatches || []) {
    const batchKey = String(batch.batchKey || "");
    if (!batchKey) continue;
    const decision = decisionByBatch.get(batchKey);
    const batchEvidenceMeta = evidenceMapsByBatch.get(batchKey);
    const blocked37xFamily = Array.isArray(batch.blocked37xFamily) ? batch.blocked37xFamily : batchEvidenceMeta?.blocked37xFamily || [];
    const unsafeFamily = hasUnsafe37xFamily(blocked37xFamily);
    const notesTemplate = batchEvidenceMeta?.reviewerNotesTemplate || defaultReviewerNotesTemplate();
    const docEvidenceMap = evidenceByBatchDoc.get(batchKey) || new Map();
    for (const packet of batch.subBucketPackets || []) {
      const subBucket = String(packet.subBucket || "");
      const docs = Array.isArray(packet.docs) ? packet.docs : (packet.sampleDocs || []).map((doc) => ({
        documentId: doc.documentId,
        title: doc.title,
        simulatedRecommendation: "insufficient_context",
        contextSignal: "no_useful_context"
      }));
      for (const doc of docs) {
        const documentId = String(doc.documentId || "");
        if (!documentId) continue;
        const evidence = docEvidenceMap.get(documentId);
        const posture = unsafeFamily ? "keep_blocked" : String(packet.recommendedReviewerPosture || postureFromSubBucket(subBucket));
        const row = {
          queueOrder: 0,
          batchKey,
          subBucket,
          documentId,
          title: String(doc.title || ""),
          recommendedReviewerPosture: posture,
          recommendedSimulatedDisposition: unsafeFamily ? "keep_blocked" : String(decision?.recommendedSimulatedDisposition || "split_batch_before_review"),
          doNotAutoApply: true,
          blocked37xFamily,
          rootCauseSummary: String(evidence?.rootCause || rootCauseFromSubBucket(subBucket)),
          contextClass: String(evidence?.contextClass || doc.contextSignal || "no_useful_context"),
          topEvidenceSnippet: String(evidence?.localTextSnippet || ""),
          reviewerNotesTemplate: notesTemplate,
          suggestedDecisionOptions: suggestedDecisionOptions(posture),
          requiresLegalEscalation: posture === "escalate_to_legal_context_review",
          priorityLane: "review_after"
        };
        row.priorityLane = laneForRow(row);
        rows.push(row);
      }
    }
  }
}

function addNonSplitRows(rows, decisionReport, splitBatchKeys, evidenceMapsByBatch, evidenceByBatchDoc) {
  for (const batch of decisionReport?.batches || []) {
    const batchKey = String(batch.batchKey || "");
    if (!batchKey || splitBatchKeys.has(batchKey)) continue;
    const batchEvidenceMeta = evidenceMapsByBatch.get(batchKey);
    const blocked37xFamily = Array.isArray(batch.blocked37xFamily) ? batch.blocked37xFamily : batchEvidenceMeta?.blocked37xFamily || [];
    const unsafeFamily = hasUnsafe37xFamily(blocked37xFamily);
    const notesTemplate = batchEvidenceMeta?.reviewerNotesTemplate || defaultReviewerNotesTemplate();
    const docEvidenceMap = evidenceByBatchDoc.get(batchKey) || new Map();
    for (const doc of batch.docSimulations || []) {
      const documentId = String(doc.documentId || "");
      if (!documentId) continue;
      const evidence = docEvidenceMap.get(documentId);
      const posture = unsafeFamily
        ? "keep_blocked"
        : String(batch.recommendedSimulatedDisposition || "") === "possible_manual_context_fix_but_no_auto_apply"
          ? "possible_manual_context_fix_but_no_auto_apply"
          : String(batch.recommendedSimulatedDisposition || "") === "escalate_to_legal_context_review"
            ? "escalate_to_legal_context_review"
            : "keep_blocked";
      const row = {
        queueOrder: 0,
        batchKey,
        subBucket: null,
        documentId,
        title: String(doc.title || ""),
        recommendedReviewerPosture: posture,
        recommendedSimulatedDisposition: unsafeFamily ? "keep_blocked" : String(batch.recommendedSimulatedDisposition || ""),
        doNotAutoApply: true,
        blocked37xFamily,
        rootCauseSummary: String(evidence?.rootCause || "batch_non_split"),
        contextClass: String(evidence?.contextClass || doc.contextSignal || "no_useful_context"),
        topEvidenceSnippet: String(evidence?.localTextSnippet || ""),
        reviewerNotesTemplate: notesTemplate,
        suggestedDecisionOptions: suggestedDecisionOptions(posture),
        requiresLegalEscalation: posture === "escalate_to_legal_context_review",
        priorityLane: "review_after"
      };
      row.priorityLane = laneForRow(row);
      rows.push(row);
    }
  }
}

function dedupeRows(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const key = `${row.batchKey}::${row.subBucket || "<none>"}::${row.documentId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

export function buildReviewerActionQueue(splitReport, decisionReport, evidenceReport) {
  const rows = [];
  const decisionByBatch = new Map((decisionReport?.batches || []).map((batch) => [String(batch.batchKey || ""), batch]));
  const { byBatch, byBatchDoc } = evidenceMaps(evidenceReport || {});
  const splitBatchKeys = new Set((splitReport?.splitBatches || []).map((batch) => String(batch.batchKey || "")).filter(Boolean));

  addSplitRows(rows, splitReport || {}, decisionByBatch, byBatch, byBatchDoc);
  addNonSplitRows(rows, decisionReport || {}, splitBatchKeys, byBatch, byBatchDoc);

  const deduped = dedupeRows(rows).sort(compareRows).map((row, index) => ({ ...row, queueOrder: index + 1 }));

  const top20 = deduped.slice(0, 20).map((row) => ({
    queueOrder: row.queueOrder,
    batchKey: row.batchKey,
    subBucket: row.subBucket,
    documentId: row.documentId,
    title: row.title,
    priorityLane: row.priorityLane,
    recommendedReviewerPosture: row.recommendedReviewerPosture
  }));

  const splitCoverage = (splitReport?.splitBatches || []).map((batch) => {
    const batchKey = String(batch.batchKey || "");
    const queueCount = deduped.filter((row) => row.batchKey === batchKey).length;
    const expectedCount = Number(batch.docCount || 0);
    return {
      batchKey,
      expectedDocCount: expectedCount,
      queueRowCount: queueCount,
      coverageStatus: queueCount === expectedCount ? "complete" : "incomplete"
    };
  });

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      totalQueueRows: deduped.length,
      splitBatchesRepresented: splitCoverage.length,
      splitCoverage,
      countsByPriorityLane: countBy(deduped.map((row) => row.priorityLane)),
      countsByRecommendedReviewerPosture: countBy(deduped.map((row) => row.recommendedReviewerPosture)),
      countsBySubBucket: countBy(deduped.map((row) => row.subBucket || "<none>")),
      countsByBlocked37xFamily: countBy(
        deduped.flatMap((row) => (Array.isArray(row.blocked37xFamily) && row.blocked37xFamily.length ? row.blocked37xFamily : ["<none>"]))
      ),
      top20DocsToReviewFirst: top20
    },
    reviewFirstQueue: deduped.filter((row) => row.priorityLane === "review_first"),
    reviewAfterQueue: deduped.filter((row) => row.priorityLane === "review_after"),
    holdBlockedQueue: deduped.filter((row) => row.priorityLane === "hold_blocked"),
    rows: deduped
  };
}
