import crypto from "node:crypto";
import { buildRetrievalExpandedCorpusEvalReport } from "./retrieval-expanded-corpus-eval-utils.mjs";
import { buildRetrievalCorpusAdmissionReport } from "./retrieval-corpus-admission-utils.mjs";
import { buildRetrievalReferenceEnrichmentReport } from "./retrieval-reference-enrichment-utils.mjs";

function uniqueSorted(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b)));
}

function stableHash(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex");
}

function countBy(values) {
  const out = {};
  for (const value of values || []) {
    const key = String(value || "<none>");
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function chunkProvenanceComplete(chunk, doc) {
  const sourceLink = String(chunk?.sourceLink || doc?.document?.sourceLink || "").trim();
  return Boolean(
    chunk?.chunkId &&
      chunk?.documentId &&
      chunk?.citationAnchorStart &&
      chunk?.citationAnchorEnd &&
      sourceLink
  );
}

function documentProvenanceComplete(doc, trustedChunks) {
  const sourceLink = String(doc?.document?.sourceLink || "").trim();
  const sourceFileRef = String(doc?.document?.sourceFileRef || "").trim();
  if (!sourceLink || !sourceFileRef) return false;
  return trustedChunks.every((chunk) => chunk.provenanceComplete);
}

function docMapById(documents) {
  const map = new Map();
  for (const doc of documents || []) {
    const id = doc?.document?.documentId;
    if (id) map.set(id, doc);
  }
  return map;
}

function admissionStatusMap(report) {
  const map = new Map();
  for (const row of report?.documents || []) {
    if (row?.documentId) map.set(row.documentId, row);
  }
  return map;
}

function enrichmentStatusMap(report) {
  const map = new Map();
  for (const row of report?.documents || []) {
    if (row?.documentId) map.set(row.documentId, row);
  }
  return map;
}

function buildBatchId(docIds, chunkIds) {
  const signature = `${(docIds || []).join("|")}::${(chunkIds || []).join("|")}`;
  return `activation_${stableHash(signature).slice(0, 16)}`;
}

function buildActivationManifest({
  baselineAdmittedDocIds,
  promotedEnrichmentDocIds,
  expandedAdmittedDocIds,
  trustedChunkIds,
  embeddingPayloadFile,
  searchPayloadFile
}) {
  const activationBatchId = buildBatchId(expandedAdmittedDocIds, trustedChunkIds);
  const activationChecksum = stableHash(JSON.stringify({ expandedAdmittedDocIds, trustedChunkIds }));
  return {
    readOnly: true,
    activationBatchIds: [activationBatchId],
    documentsToActivate: expandedAdmittedDocIds,
    chunksToActivate: trustedChunkIds,
    embeddingPayloadFile,
    searchPayloadFile,
    baselineAdmittedDocIds,
    promotedEnrichmentDocIds,
    integrity: {
      activationChecksum,
      documentsChecksum: stableHash(expandedAdmittedDocIds.join("|")),
      chunksChecksum: stableHash(trustedChunkIds.join("|"))
    }
  };
}

function buildRollbackManifest({ activationManifest }) {
  const rollbackBatchId = `rollback_${stableHash((activationManifest.activationBatchIds || []).join("|")).slice(0, 16)}`;
  return {
    readOnly: true,
    rollbackBatchIds: [rollbackBatchId],
    activationBatchIdsReversed: [...(activationManifest.activationBatchIds || [])].reverse(),
    documentsToRemove: [...(activationManifest.documentsToActivate || [])],
    chunksToRemove: [...(activationManifest.chunksToActivate || [])],
    integrity: {
      rollbackChecksum: stableHash(
        JSON.stringify({ documentsToRemove: activationManifest.documentsToActivate || [], chunksToRemove: activationManifest.chunksToActivate || [] })
      )
    }
  };
}

export function buildRetrievalActivationBundleReport({
  apiBase,
  input,
  documents,
  includeText = true,
  promotedEnrichmentDocIdsOverride = null,
  outputFileNames = {}
}) {
  const expandedEval = buildRetrievalExpandedCorpusEvalReport({
    apiBase,
    input,
    documents,
    includeText,
    promotedEnrichmentDocIdsOverride
  });

  const admission = buildRetrievalCorpusAdmissionReport({ apiBase, input, documents });
  const enrichment = Array.isArray(promotedEnrichmentDocIdsOverride)
    ? { documents: [] }
    : buildRetrievalReferenceEnrichmentReport({ apiBase, input, documents });

  const baselineAdmittedDocIds = uniqueSorted(expandedEval.expandedCorpusBundles?.baselineAdmittedDocIds || []);
  const promotedEnrichmentDocIds = uniqueSorted(expandedEval.expandedCorpusBundles?.promotedEnrichmentDocIds || []);
  const expandedAdmittedDocIds = uniqueSorted(expandedEval.expandedCorpusBundles?.expandedAdmittedDocIds || []);

  const docsById = docMapById(documents);
  const admissionById = admissionStatusMap(admission);
  const enrichmentById = enrichmentStatusMap(enrichment);

  const trustedDocRows = [];
  const trustedChunkRows = [];
  const embeddingPayloadRows = [];
  const searchPayloadRows = [];

  for (const docId of expandedAdmittedDocIds) {
    const doc = docsById.get(docId);
    if (!doc || doc.isLikelyFixture) continue;

    const trustSource = baselineAdmittedDocIds.includes(docId) ? "baseline_admit_now" : "promoted_after_enrichment";
    const admissionRow = admissionById.get(docId);
    const enrichmentRow = enrichmentById.get(docId);

    const chunks = (doc.chunks || []).map((chunk) => {
      const provenanceComplete = chunkProvenanceComplete(chunk, doc);
      const sourceLink = String(chunk?.sourceLink || doc?.document?.sourceLink || "");
      const embeddingEligible = provenanceComplete;
      const searchEligible = provenanceComplete;

      const chunkRow = {
        chunkId: chunk?.chunkId,
        documentId: docId,
        chunkType: chunk?.chunkType || "general_body",
        retrievalPriority: chunk?.retrievalPriority || "low",
        citationAnchorStart: chunk?.citationAnchorStart || "",
        citationAnchorEnd: chunk?.citationAnchorEnd || "",
        hasCanonicalReferenceAlignment: Boolean(chunk?.hasCanonicalReferenceAlignment),
        sourceLink,
        embeddingEligible,
        searchEligible,
        provenanceComplete
      };

      trustedChunkRows.push(chunkRow);

      if (embeddingEligible) {
        embeddingPayloadRows.push({
          embeddingId: `trusted_emb_${chunk?.chunkId}`,
          documentId: docId,
          chunkId: chunk?.chunkId,
          sourceText: includeText ? String(chunk?.sourceText || "") : "",
          chunkType: chunk?.chunkType || "general_body",
          retrievalPriority: chunk?.retrievalPriority || "low",
          hasCanonicalReferenceAlignment: Boolean(chunk?.hasCanonicalReferenceAlignment),
          citationAnchorStart: chunk?.citationAnchorStart || "",
          citationAnchorEnd: chunk?.citationAnchorEnd || "",
          sourceLink
        });
      }

      if (searchEligible) {
        searchPayloadRows.push({
          searchId: `trusted_search_${chunk?.chunkId}`,
          documentId: docId,
          chunkId: chunk?.chunkId,
          title: doc?.document?.title || "Untitled",
          chunkType: chunk?.chunkType || "general_body",
          retrievalPriority: chunk?.retrievalPriority || "low",
          citationAnchorStart: chunk?.citationAnchorStart || "",
          citationAnchorEnd: chunk?.citationAnchorEnd || "",
          sourceLink,
          hasCanonicalReferenceAlignment: Boolean(chunk?.hasCanonicalReferenceAlignment)
        });
      }

      return chunkRow;
    });

    const alignedChunks = chunks.filter((chunk) => chunk.hasCanonicalReferenceAlignment).length;
    const canonicalAlignmentRate = chunks.length > 0 ? Number((alignedChunks / chunks.length).toFixed(4)) : 0;
    const provenanceComplete = documentProvenanceComplete(doc, chunks);
    const activationEligible = provenanceComplete && chunks.length > 0 && chunks.every((chunk) => chunk.embeddingEligible && chunk.searchEligible);

    trustedDocRows.push({
      documentId: docId,
      title: doc?.document?.title || "Untitled",
      trustSource,
      corpusAdmissionStatus:
        trustSource === "baseline_admit_now"
          ? String(admissionRow?.corpusAdmissionStatus || "admit_now")
          : String(enrichmentRow?.postEnrichmentCorpusAdmissionStatus || "admit_now"),
      sourceLink: String(doc?.document?.sourceLink || ""),
      sourceFileRef: String(doc?.document?.sourceFileRef || ""),
      chunkCount: chunks.length,
      canonicalAlignmentRate,
      provenanceComplete,
      activationEligible,
      rollbackEligible: activationEligible
    });
  }

  const trustedChunkIds = uniqueSorted(trustedChunkRows.map((row) => row.chunkId));
  const trustedDocumentIds = uniqueSorted(trustedDocRows.map((row) => row.documentId));

  const embeddingPayloadFile = outputFileNames.embeddingPayloadFile || "retrieval-trusted-embedding-payload.json";
  const searchPayloadFile = outputFileNames.searchPayloadFile || "retrieval-trusted-search-payload.json";

  const activationManifest = buildActivationManifest({
    baselineAdmittedDocIds,
    promotedEnrichmentDocIds,
    expandedAdmittedDocIds: trustedDocumentIds,
    trustedChunkIds,
    embeddingPayloadFile,
    searchPayloadFile
  });
  const rollbackManifest = buildRollbackManifest({ activationManifest });

  const documentsMissingProvenanceCount = trustedDocRows.filter((row) => !row.provenanceComplete).length;
  const chunksMissingProvenanceCount = trustedChunkRows.filter((row) => !row.provenanceComplete).length;
  const documentsFailingTrustRequirementsCount = trustedDocRows.filter((row) => !row.activationEligible).length;

  const allRealDocs = (documents || []).filter((doc) => !doc?.isLikelyFixture).map((doc) => doc?.document?.documentId).filter(Boolean);
  const documentsExcludedFromActivationCount = uniqueSorted(allRealDocs).filter((id) => !trustedDocumentIds.includes(id)).length;

  const summary = {
    trustedDocumentCount: trustedDocRows.length,
    trustedChunkCount: trustedChunkRows.length,
    baselineTrustedDocumentCount: trustedDocRows.filter((row) => row.trustSource === "baseline_admit_now").length,
    promotedTrustedDocumentCount: trustedDocRows.filter((row) => row.trustSource === "promoted_after_enrichment").length,
    documentsExcludedFromActivationCount,
    embeddingRowCount: embeddingPayloadRows.length,
    searchRowCount: searchPayloadRows.length,
    documentsMissingProvenanceCount,
    chunksMissingProvenanceCount,
    documentsFailingTrustRequirementsCount,
    activationReady: trustedDocRows.length > 0 && documentsFailingTrustRequirementsCount === 0 && chunksMissingProvenanceCount === 0,
    rollbackReady: Boolean(rollbackManifest.rollbackBatchIds?.length && rollbackManifest.documentsToRemove?.length >= 0)
  };

  return {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    apiBase,
    input,
    summary,
    trustedCorpusSummary: {
      trustedDocumentCount: summary.trustedDocumentCount,
      trustedChunkCount: summary.trustedChunkCount,
      baselineTrustedDocumentCount: summary.baselineTrustedDocumentCount,
      promotedTrustedDocumentCount: summary.promotedTrustedDocumentCount,
      trustSourceCounts: countBy(trustedDocRows.map((row) => row.trustSource))
    },
    embeddingPayloadSummary: {
      embeddingRowCount: embeddingPayloadRows.length,
      documentsCoveredCount: uniqueSorted(embeddingPayloadRows.map((row) => row.documentId)).length,
      checksum: stableHash(JSON.stringify(embeddingPayloadRows.map((row) => `${row.documentId}:${row.chunkId}`)))
    },
    searchPayloadSummary: {
      searchRowCount: searchPayloadRows.length,
      documentsCoveredCount: uniqueSorted(searchPayloadRows.map((row) => row.documentId)).length,
      checksum: stableHash(JSON.stringify(searchPayloadRows.map((row) => `${row.documentId}:${row.chunkId}`)))
    },
    activationManifestSummary: {
      activationBatchIds: activationManifest.activationBatchIds,
      documentCount: (activationManifest.documentsToActivate || []).length,
      chunkCount: (activationManifest.chunksToActivate || []).length,
      checksum: activationManifest.integrity?.activationChecksum || ""
    },
    rollbackManifestSummary: {
      rollbackBatchIds: rollbackManifest.rollbackBatchIds,
      documentCount: (rollbackManifest.documentsToRemove || []).length,
      chunkCount: (rollbackManifest.chunksToRemove || []).length,
      checksum: rollbackManifest.integrity?.rollbackChecksum || ""
    },
    documents: trustedDocRows.sort((a, b) => String(a.documentId).localeCompare(String(b.documentId))),
    chunks: trustedChunkRows.sort((a, b) => String(a.chunkId).localeCompare(String(b.chunkId))),
    trustedCorpusBundles: {
      baselineAdmittedDocIds,
      promotedEnrichmentDocIds,
      expandedAdmittedDocIds: trustedDocumentIds,
      trustedChunkIds,
      embeddingPayloadFile,
      searchPayloadFile
    },
    payloads: {
      trustedEmbeddingPayload: {
        generatedAt: new Date().toISOString(),
        readOnly: true,
        rowCount: embeddingPayloadRows.length,
        rows: embeddingPayloadRows.sort((a, b) => String(a.chunkId).localeCompare(String(b.chunkId)))
      },
      trustedSearchPayload: {
        generatedAt: new Date().toISOString(),
        readOnly: true,
        rowCount: searchPayloadRows.length,
        rows: searchPayloadRows.sort((a, b) => String(a.chunkId).localeCompare(String(b.chunkId)))
      },
      activationManifest,
      rollbackManifest
    },
    expandedCorpusEvalSummary: expandedEval.summary
  };
}

export function formatRetrievalActivationBundleMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval Activation Bundle Report (Dry-Run)");
  lines.push("");

  lines.push("## Summary");
  for (const [k, v] of Object.entries(report.summary || {})) lines.push(`- ${k}: ${v}`);
  lines.push("");

  lines.push("## Trusted Corpus Summary");
  for (const [k, v] of Object.entries(report.trustedCorpusSummary || {})) {
    if (typeof v === "object") continue;
    lines.push(`- ${k}: ${v}`);
  }
  for (const [k, v] of Object.entries(report.trustedCorpusSummary?.trustSourceCounts || {})) {
    lines.push(`- trustSource.${k}: ${v}`);
  }
  lines.push("");

  lines.push("## Activation Manifest Summary");
  for (const [k, v] of Object.entries(report.activationManifestSummary || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(",") : v}`);
  }
  lines.push("");

  lines.push("## Rollback Manifest Summary");
  for (const [k, v] of Object.entries(report.rollbackManifestSummary || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(",") : v}`);
  }
  lines.push("");

  lines.push("## Trusted Documents (Sample)");
  for (const row of (report.documents || []).slice(0, 25)) {
    lines.push(
      `- ${row.documentId} | ${row.title} | source=${row.trustSource} | chunks=${row.chunkCount} | provenance=${row.provenanceComplete} | activate=${row.activationEligible}`
    );
  }
  if (!(report.documents || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Trusted Bundle IDs");
  lines.push(`- baselineAdmittedDocIds: ${(report.trustedCorpusBundles?.baselineAdmittedDocIds || []).length}`);
  lines.push(`- promotedEnrichmentDocIds: ${(report.trustedCorpusBundles?.promotedEnrichmentDocIds || []).length}`);
  lines.push(`- expandedAdmittedDocIds: ${(report.trustedCorpusBundles?.expandedAdmittedDocIds || []).length}`);
  lines.push(`- trustedChunkIds: ${(report.trustedCorpusBundles?.trustedChunkIds || []).length}`);
  lines.push("");

  lines.push("- Dry-run only. No embedding writes, no vector/search-index writes, and no corpus-admission/QC/citation mutations.");
  return `${lines.join("\n")}\n`;
}
