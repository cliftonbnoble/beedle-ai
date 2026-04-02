import { LIVE_SEARCH_QA_QUERIES, buildRetrievalLiveSearchQaReport } from "./retrieval-live-search-qa-utils.mjs";

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function sortCountEntries(obj) {
  return Object.entries(obj || {})
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return String(a[0]).localeCompare(String(b[0]));
    })
    .map(([key, count]) => ({ key, count }));
}

function stableHash(value) {
  const text = typeof value === "string" ? value : JSON.stringify(value);
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

function mkBatchId(nextBatchDocIds, nextBatchChunkIds) {
  return `activation_batch_${stableHash(`${nextBatchDocIds.join("|")}::${nextBatchChunkIds.join("|")}`).slice(0, 16)}`;
}

function buildPayloadRowsFromPreviews(previewsById, nextBatchDocIds) {
  const embeddingRows = [];
  const searchRows = [];
  const chunkIds = [];
  const docsMissingPreview = [];

  for (const documentId of nextBatchDocIds) {
    const preview = previewsById.get(documentId);
    if (!preview) {
      docsMissingPreview.push(documentId);
      continue;
    }

    const title = String(preview?.document?.title || "Untitled");
    const sourceLink = String(preview?.document?.sourceLink || "");

    for (const chunk of preview?.chunks || []) {
      if (!chunk?.chunkId) continue;

      const chunkId = String(chunk.chunkId);
      const citationAnchorStart = String(chunk.citationAnchorStart || "");
      const citationAnchorEnd = String(chunk.citationAnchorEnd || citationAnchorStart);
      const sourceText = String(chunk.sourceText || "");
      const chunkType = String(chunk.chunkType || "general_body");
      const retrievalPriority = String(chunk.retrievalPriority || "low");

      embeddingRows.push({
        embeddingId: `batch_emb_${chunkId}`,
        documentId,
        chunkId,
        sourceText,
        chunkType,
        retrievalPriority,
        hasCanonicalReferenceAlignment: Boolean(chunk.hasCanonicalReferenceAlignment),
        citationAnchorStart,
        citationAnchorEnd,
        sourceLink
      });

      searchRows.push({
        searchId: `batch_search_${chunkId}`,
        documentId,
        chunkId,
        title,
        chunkType,
        retrievalPriority,
        citationAnchorStart,
        citationAnchorEnd,
        sourceLink,
        hasCanonicalReferenceAlignment: Boolean(chunk.hasCanonicalReferenceAlignment)
      });

      chunkIds.push(chunkId);
    }
  }

  return {
    embeddingRows,
    searchRows,
    nextBatchChunkIds: uniqueSorted(chunkIds),
    docsMissingPreview
  };
}

export function buildBatchActivationArtifacts({
  previews,
  nextBatchDocIds,
  existingTrustedDocIds,
  activationManifestSource = "retrieval-next-batch-manifest.json"
}) {
  const filteredBatchDocIds = uniqueSorted(nextBatchDocIds || []);
  const trustedBefore = uniqueSorted(existingTrustedDocIds || []);
  const previewsById = new Map(
    (previews || [])
      .map((preview) => [String(preview?.document?.documentId || ""), preview])
      .filter(([id]) => Boolean(id))
  );

  const rows = buildPayloadRowsFromPreviews(previewsById, filteredBatchDocIds);
  const nextBatchChunkIds = rows.nextBatchChunkIds;

  const activationBatchId = mkBatchId(filteredBatchDocIds, nextBatchChunkIds);
  const rollbackBatchId = `rollback_${stableHash(activationBatchId).slice(0, 16)}`;

  const activationManifest = {
    readOnly: false,
    source: activationManifestSource,
    activationBatchIds: [activationBatchId],
    documentsToActivate: filteredBatchDocIds,
    chunksToActivate: nextBatchChunkIds,
    baselineAdmittedDocIds: [],
    promotedEnrichmentDocIds: filteredBatchDocIds,
    integrity: {
      activationChecksum: stableHash(JSON.stringify({ docs: filteredBatchDocIds, chunks: nextBatchChunkIds })),
      documentsChecksum: stableHash(filteredBatchDocIds.join("|")),
      chunksChecksum: stableHash(nextBatchChunkIds.join("|"))
    }
  };

  const rollbackManifest = {
    readOnly: false,
    rollbackBatchIds: [rollbackBatchId],
    activationBatchIdsReversed: [activationBatchId],
    documentsToRemove: [...filteredBatchDocIds],
    chunksToRemove: [...nextBatchChunkIds],
    integrity: {
      rollbackChecksum: stableHash(JSON.stringify({ docs: filteredBatchDocIds, chunks: nextBatchChunkIds }))
    }
  };

  return {
    activationBatchId,
    trustedBefore,
    trustedAfter: uniqueSorted([...trustedBefore, ...filteredBatchDocIds]),
    nextBatchDocIds: filteredBatchDocIds,
    nextBatchChunkIds,
    docsMissingPreview: rows.docsMissingPreview,
    payload: {
      embeddingPayload: { rowCount: rows.embeddingRows.length, rows: rows.embeddingRows },
      searchPayload: { rowCount: rows.searchRows.length, rows: rows.searchRows },
      activationManifest,
      rollbackManifest,
      dryRun: false,
      performVectorUpsert: true
    }
  };
}

function summarizeLiveQa(report) {
  return {
    queriesEvaluated: Number(report?.summary?.queriesEvaluated || 0),
    totalApiResultsAcrossQueries: Number(report?.summary?.totalApiResultsAcrossQueries || 0),
    zeroTrustedResultQueryCount: Number(report?.summary?.zeroTrustedResultQueryCount || 0),
    averageQualityScore: Number(report?.summary?.averageQualityScore || 0),
    outOfCorpusHitQueryCount: Number(report?.summary?.outOfCorpusHitQueryCount || 0),
    provenanceCompletenessAverage: Number(report?.summary?.provenanceCompletenessAverage || 0),
    citationAnchorCoverageAverage: Number(report?.summary?.citationAnchorCoverageAverage || 0),
    duplicateFloodingQueryCount: Number(report?.summary?.duplicateFloodingQueryCount || 0)
  };
}

function delta(before, after) {
  return Number((Number(after || 0) - Number(before || 0)).toFixed(4));
}

export function compareLiveQa(beforeReport, afterReport) {
  const before = summarizeLiveQa(beforeReport);
  const after = summarizeLiveQa(afterReport);

  return {
    before,
    after,
    deltas: {
      totalApiResultsAcrossQueries: delta(before.totalApiResultsAcrossQueries, after.totalApiResultsAcrossQueries),
      zeroTrustedResultQueryCount: delta(before.zeroTrustedResultQueryCount, after.zeroTrustedResultQueryCount),
      averageQualityScore: delta(before.averageQualityScore, after.averageQualityScore),
      outOfCorpusHitQueryCount: delta(before.outOfCorpusHitQueryCount, after.outOfCorpusHitQueryCount),
      provenanceCompletenessAverage: delta(before.provenanceCompletenessAverage, after.provenanceCompletenessAverage),
      citationAnchorCoverageAverage: delta(before.citationAnchorCoverageAverage, after.citationAnchorCoverageAverage)
    }
  };
}

export function validateBatchActivationOutcome({
  batchDocIds,
  activationWriteReport,
  corpusAdmissionById,
  trustedBeforeIds,
  trustedAfterIds,
  beforeLiveQa,
  afterLiveQa
}) {
  const activatedDocs = activationWriteReport?.documentsActivated || [];
  const activatedDocIds = uniqueSorted(activatedDocs.map((row) => row.documentId));
  const activatedChunks = activationWriteReport?.chunksActivated || [];

  const batchSet = new Set((batchDocIds || []).map(String));
  const nonBatchActivatedDocIds = activatedDocIds.filter((id) => !batchSet.has(id));

  const classification = activatedDocIds.map((id) => ({
    documentId: id,
    corpusAdmissionStatus: String(corpusAdmissionById.get(id)?.corpusAdmissionStatus || "unknown"),
    isLikelyFixture: Boolean(corpusAdmissionById.get(id)?.isLikelyFixture)
  }));

  const heldDocsWrittenCount = classification.filter((row) => row.corpusAdmissionStatus === "hold_for_repair_review").length;
  const excludedDocsWrittenCount = classification.filter((row) => row.corpusAdmissionStatus === "exclude_from_initial_corpus").length;
  const fixtureDocsWrittenCount = classification.filter((row) => row.isLikelyFixture).length;

  const checks = {
    onlyManifestDocsActivated: nonBatchActivatedDocIds.length === 0,
    activatedDocCountMatchesManifest: activatedDocIds.length === uniqueSorted(batchDocIds || []).length,
    batchDocsRemainHeldScope: heldDocsWrittenCount === activatedDocIds.length,
    noExcludedDocsWritten: excludedDocsWrittenCount === 0,
    noFixtureDocsWritten: fixtureDocsWrittenCount === 0,
    provenanceComplete: Number(activationWriteReport?.summary?.provenanceFailuresCount || 0) === 0,
    outOfCorpusLeakageZero: Number(afterLiveQa?.summary?.outOfCorpusHitQueryCount || 0) === 0,
    provenanceCompletenessOne: Number(afterLiveQa?.summary?.provenanceCompletenessAverage || 0) === 1,
    citationAnchorCoverageOne: Number(afterLiveQa?.summary?.citationAnchorCoverageAverage || 0) === 1,
    zeroTrustedResultZero: Number(afterLiveQa?.summary?.zeroTrustedResultQueryCount || 0) === 0,
    trustedSetExpandedByBatch:
      uniqueSorted(trustedAfterIds || []).length === uniqueSorted([...(trustedBeforeIds || []), ...(batchDocIds || [])]).length
  };

  const failures = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([key]) => key);

  return {
    passed: failures.length === 0,
    checks,
    failures,
    activatedDocIds,
    activatedChunkCount: activatedChunks.length,
    nonBatchActivatedDocIds,
    heldDocsWrittenCount,
    excludedDocsWrittenCount,
    fixtureDocsWrittenCount
  };
}

export async function runLiveQa({ apiBase, trustedDocumentIds, fetchSearchDebug, limit = 20 }) {
  return buildRetrievalLiveSearchQaReport({
    apiBase,
    trustedDocumentIds,
    queries: LIVE_SEARCH_QA_QUERIES,
    fetchSearchDebug,
    limit,
    realOnly: true
  });
}

export function buildCorpusAdmissionMap(corpusAdmissionReport) {
  const map = new Map();
  for (const row of corpusAdmissionReport?.documents || []) {
    if (row?.documentId) map.set(String(row.documentId), row);
  }
  return map;
}

export function buildBatchActivationMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Batch Activation Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Activated Docs");
  for (const row of report.documentsActivated || []) {
    lines.push(`- ${row.documentId} | ${row.title} | status=${row.corpusAdmissionStatus}`);
  }
  if (!(report.documentsActivated || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Live QA Before vs After");
  for (const [k, v] of Object.entries(report.liveQaComparison?.deltas || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Validation Checks");
  for (const [k, v] of Object.entries(report.validation?.checks || {})) lines.push(`- ${k}: ${v}`);
  if ((report.validation?.failures || []).length) lines.push(`- failures: ${report.validation.failures.join(", ")}`);
  lines.push("");

  lines.push("## Rollback");
  lines.push(`- rollbackManifest: ${report.rollbackManifestFile || "retrieval-batch-rollback-manifest.json"}`);
  lines.push(`- rollbackBatchId: ${report.rollbackManifest?.rollbackBatchIds?.[0] || "<none>"}`);
  lines.push("");

  lines.push("- Real write executed for this batch only. No trust-gate weakening performed.");
  return `${lines.join("\n")}\n`;
}

export function buildBatchLiveQaMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Batch Live QA Report");
  lines.push("");
  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");
  lines.push("## Top Results (3 Queries)");
  for (const row of (report.queryResults || []).slice(0, 3)) {
    lines.push(`- ${row.query}`);
    for (const top of (row.topResults || []).slice(0, 5)) {
      lines.push(`  - ${top.documentId} | ${top.chunkId} | ${top.chunkType} | score=${top.score}`);
    }
  }
  lines.push("");
  lines.push("- Live runtime QA report only.");
  return `${lines.join("\n")}\n`;
}

export function aggregateReasonCounts(rows, field) {
  return sortCountEntries(countBy((rows || []).flatMap((row) => row?.[field] || [])));
}
