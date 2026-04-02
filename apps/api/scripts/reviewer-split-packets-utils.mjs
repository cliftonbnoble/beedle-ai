function countBy(items) {
  const map = new Map();
  for (const item of items) map.set(item, (map.get(item) || 0) + 1);
  return Array.from(map.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || String(a.key).localeCompare(String(b.key)));
}

function parseUnresolvedReferences(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function toSubBucket(doc, evidenceByDoc) {
  if (doc.syntheticUnmapped) return "unclassified_hold";
  const recommendation = String(doc.simulatedRecommendation || "");
  const signal = String(doc.contextSignal || "");
  const evidence = evidenceByDoc.get(doc.documentId);
  const rootCause = String(evidence?.rootCause || "");
  if (recommendation === "insufficient_context") return "insufficient_context_hold";
  if (recommendation === "keep_blocked" || rootCause === "not_found") return "not_found_hold";
  if (recommendation === "manual_context_review_candidate" && signal === "likely_ordinance_wording") {
    return "likely_ordinance_manual_review";
  }
  return "mixed_ambiguous_escalate";
}

function postureForSubBucket(subBucket) {
  if (subBucket === "likely_ordinance_manual_review") return "possible_manual_context_fix_but_no_auto_apply";
  if (subBucket === "insufficient_context_hold") return "keep_blocked";
  if (subBucket === "not_found_hold") return "keep_blocked";
  if (subBucket === "unclassified_hold") return "keep_blocked";
  return "escalate_to_legal_context_review";
}

function evidenceForBatch(evidenceBatch, queueRowsForBatch = []) {
  const map = new Map();
  const snippets = evidenceBatch
    ? (evidenceBatch.patternEvidence || []).flatMap((pattern) => pattern.representativeSnippets || [])
    : [];
  for (const snippet of snippets) {
    const existing = map.get(snippet.documentId);
    if (!existing) {
      map.set(snippet.documentId, {
        rawCitation: snippet.rawCitation,
        normalizedValue: snippet.normalizedValue,
        referenceType: snippet.referenceType,
        rootCause: snippet.rootCause,
        localTextSnippet: snippet.localTextSnippet,
        contextClass: snippet.contextClass
      });
    }
  }
  for (const row of queueRowsForBatch) {
    const documentId = String(row.documentId || "");
    if (!documentId || map.has(documentId)) continue;
    const unresolved = parseUnresolvedReferences(row.exactUnresolvedReferences);
    const first = unresolved[0];
    if (!first) continue;
    map.set(documentId, {
      rawCitation: String(first.rawValue || ""),
      normalizedValue: String(first.normalizedValue || ""),
      referenceType: String(first.referenceType || ""),
      rootCause: String(first.rootCause || ""),
      localTextSnippet: "",
      contextClass: "no_useful_context"
    });
  }
  return map;
}

function topEvidenceForDocs(docIds, evidenceMap) {
  const out = [];
  for (const id of docIds) {
    const evidence = evidenceMap.get(id);
    if (!evidence) continue;
    out.push({
      documentId: id,
      rawCitation: evidence.rawCitation,
      normalizedValue: evidence.normalizedValue,
      referenceType: evidence.referenceType,
      rootCause: evidence.rootCause,
      contextClass: evidence.contextClass,
      localTextSnippet: evidence.localTextSnippet
    });
    if (out.length >= 6) break;
  }
  return out;
}

function docRecommendationForSupplement(docId, evidenceMap) {
  const evidence = evidenceMap.get(docId);
  const contextClass = String(evidence?.contextClass || "no_useful_context");
  if (contextClass === "likely_ordinance_wording") {
    return { simulatedRecommendation: "manual_context_review_candidate", contextSignal: contextClass };
  }
  if (contextClass === "likely_rules_wording" || contextClass === "mixed_ambiguous_wording") {
    return { simulatedRecommendation: "escalate", contextSignal: contextClass };
  }
  const rootCause = String(evidence?.rootCause || "");
  if (rootCause === "not_found") {
    return { simulatedRecommendation: "keep_blocked", contextSignal: contextClass };
  }
  return { simulatedRecommendation: "insufficient_context", contextSignal: contextClass };
}

function rowsByBatchKey(queueRows = []) {
  const map = new Map();
  for (const row of queueRows || []) {
    const batchKey = String(row.batchKey || "");
    if (!batchKey) continue;
    const list = map.get(batchKey) || [];
    list.push({
      documentId: String(row.documentId || ""),
      title: String(row.title || ""),
      exactUnresolvedReferences: row.exactUnresolvedReferences
    });
    map.set(batchKey, list);
  }
  for (const list of map.values()) {
    list.sort((a, b) => a.documentId.localeCompare(b.documentId) || a.title.localeCompare(b.title));
  }
  return map;
}

export function buildReviewerSplitPackets(simReport, evidenceReport = null, queueRows = []) {
  const batches = Array.isArray(simReport?.batches) ? simReport.batches : [];
  const evidenceBatches = new Map(
    (Array.isArray(evidenceReport?.packets) ? evidenceReport.packets : []).map((packet) => [String(packet.batchKey), packet])
  );
  const queueByBatch = rowsByBatchKey(queueRows);
  const splitBatches = batches
    .filter((batch) => batch.recommendedSimulatedDisposition === "split_batch_before_review")
    .map((batch) => {
      const evidenceBatch = evidenceBatches.get(String(batch.batchKey));
      const queueRowsForBatch = queueByBatch.get(String(batch.batchKey)) || [];
      const evidenceMap = evidenceForBatch(evidenceBatch, queueRowsForBatch);
      const docsById = new Map();
      let sourceDuplicateDocCount = 0;
      for (const doc of batch.docSimulations || []) {
        const docId = String(doc.documentId || "");
        if (!docId) continue;
        if (docsById.has(docId)) {
          sourceDuplicateDocCount += 1;
          continue;
        }
        docsById.set(docId, {
          documentId: docId,
          title: String(doc.title || ""),
          simulatedRecommendation: String(doc.simulatedRecommendation || ""),
          contextSignal: String(doc.contextSignal || "no_useful_context")
        });
      }
      for (const row of queueRowsForBatch) {
        const docId = String(row.documentId || "");
        if (!docId || docsById.has(docId)) continue;
        const inferred = docRecommendationForSupplement(docId, evidenceMap);
        docsById.set(docId, {
          documentId: docId,
          title: String(row.title || ""),
          simulatedRecommendation: inferred.simulatedRecommendation,
          contextSignal: inferred.contextSignal
        });
      }

      const expectedDocCount = Number(batch.docCount || docsById.size);
      const missingCount = Math.max(0, expectedDocCount - docsById.size);
      for (let i = 0; i < missingCount; i += 1) {
        const syntheticId = `unmapped:${batch.batchKey}:${String(i + 1).padStart(4, "0")}`;
        docsById.set(syntheticId, {
          documentId: syntheticId,
          title: "<unmapped>",
          simulatedRecommendation: "insufficient_context",
          contextSignal: "no_useful_context",
          syntheticUnmapped: true
        });
      }

      const allDocs = Array.from(docsById.values()).sort((a, b) => a.documentId.localeCompare(b.documentId));
      const subBuckets = new Map();
      for (const doc of allDocs) {
        const key = toSubBucket(doc, evidenceMap);
        const list = subBuckets.get(key) || [];
        list.push(doc);
        subBuckets.set(key, list);
      }
      const ordered = ["likely_ordinance_manual_review", "mixed_ambiguous_escalate", "insufficient_context_hold", "not_found_hold", "unclassified_hold"];
      const subBucketPackets = ordered
        .filter((key) => subBuckets.has(key))
        .map((key) => {
          const docs = subBuckets.get(key) || [];
          const docIds = docs.map((doc) => doc.documentId);
          return {
            subBucket: key,
            docCount: docs.length,
            recommendedReviewerPosture: postureForSubBucket(key),
            doNotAutoApply: true,
            docs: docs.map((doc) => ({
              documentId: doc.documentId,
              title: doc.title,
              simulatedRecommendation: doc.simulatedRecommendation,
              contextSignal: doc.contextSignal,
              syntheticUnmapped: Boolean(doc.syntheticUnmapped)
            })),
            sampleDocs: docs.slice(0, 8).map((doc) => ({ documentId: doc.documentId, title: doc.title })),
            representativeCitationContextEvidence: topEvidenceForDocs(docIds, evidenceMap)
          };
        });
      const allOutcomes = countBy(allDocs.map((doc) => doc.simulatedRecommendation));
      const totalAssigned = subBucketPackets.reduce((sum, item) => sum + Number(item.docCount || 0), 0);
      const coveredDocCount = totalAssigned;
      const duplicateDocCount = Math.max(0, totalAssigned - allDocs.length) + sourceDuplicateDocCount;
      const uncoveredDocCount = Math.max(0, expectedDocCount - coveredDocCount);
      const coverageStatus = duplicateDocCount > 0 ? "invalid" : uncoveredDocCount > 0 ? "incomplete" : "complete";
      const coverageWarnings = [];
      if (uncoveredDocCount > 0) coverageWarnings.push(`coverage_incomplete:${uncoveredDocCount}`);
      if (duplicateDocCount > 0) coverageWarnings.push(`duplicate_assignment:${duplicateDocCount}`);
      if (missingCount > 0) coverageWarnings.push(`synthetic_unmapped_docs_added:${missingCount}`);

      return {
        batchKey: batch.batchKey,
        docCount: expectedDocCount,
        blocked37xFamily: batch.blocked37xFamily || [],
        splitReason: batch.splitReason || "",
        splitConfidence: batch.splitConfidence || "low",
        proposedSubBuckets: batch.proposedSubBuckets || subBucketPackets.map((item) => item.subBucket),
        subBucketCounts: subBucketPackets.map((item) => ({ key: item.subBucket, count: item.docCount })),
        docSimulationOutcomeCounts: allOutcomes,
        coveredDocCount,
        uncoveredDocCount,
        duplicateDocCount,
        coverageStatus,
        coverageWarnings,
        syntheticUnmappedDocCount: missingCount,
        uniqueAssignedDocCount: allDocs.length,
        subBucketPackets,
        doNotAutoApply: true
      };
    })
    .sort((a, b) => b.docCount - a.docCount || a.batchKey.localeCompare(b.batchKey));

  const nonSplitBatches = batches
    .filter((batch) => batch.recommendedSimulatedDisposition !== "split_batch_before_review")
    .map((batch) => ({
      batchKey: batch.batchKey,
      recommendedSimulatedDisposition: batch.recommendedSimulatedDisposition,
      docCount: batch.docCount
    }));

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    summary: {
      splitBatchesFound: splitBatches.length,
      splitBatchDocTotal: splitBatches.reduce((sum, batch) => sum + Number(batch.docCount || 0), 0),
      splitBatchKeys: splitBatches.map((batch) => batch.batchKey),
      excludedNonSplitBatches: nonSplitBatches.length
    },
    splitBatches,
    excludedNonSplitBatches: nonSplitBatches
  };
}
