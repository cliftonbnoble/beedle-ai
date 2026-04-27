import path from "node:path";
import {
  buildRealDecisionPredicate,
  checkHealth,
  defaultDbPath,
  ensureDir,
  fetchJson,
  formatTimestamp,
  queryCorpusSnapshot,
  writeJson,
  writeText,
  runSqlJson
} from "./lib/overnight-corpus-lift-utils.mjs";

const apiBaseUrl = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_BUSY_TIMEOUT_MS || "5000", 10);
const batchLimit = Math.max(1, Number.parseInt(process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_LIMIT || "25", 10));
const batchOffset = Math.max(0, Number.parseInt(process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_OFFSET || "0", 10));
const batchOrder = String(process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_ORDER || "decision_like_searchable_asc");
const dryRunOnly = (process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_DRY_RUN || "0") === "1";
const performVectorUpsert = (process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_PERFORM_VECTOR_UPSERT || "0") === "1";
const outputDir = path.resolve(
  process.cwd(),
  process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_OUTPUT_DIR || "reports/searchable-retrieval-activation"
);
const reportName = process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_REPORT_NAME || "searchable-retrieval-activation-report.json";
const markdownName =
  process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_MARKDOWN_NAME || "searchable-retrieval-activation-report.md";
const requestTimeoutMs = Math.max(
  1_000,
  Number.parseInt(process.env.SEARCHABLE_RETRIEVAL_ACTIVATION_WRITE_TIMEOUT_MS || "300000", 10)
);

function decisionLikePriorityClause() {
  return `
    CASE
      WHEN lower(COALESCE(d.title, '')) LIKE '%minute order%' THEN 4
      WHEN lower(COALESCE(d.title, '')) LIKE '%notice%' THEN 4
      WHEN lower(COALESCE(d.title, '')) LIKE '%stipulation%' THEN 3
      WHEN lower(COALESCE(d.title, '')) LIKE '%calendar%' THEN 3
      WHEN lower(COALESCE(d.title, '')) LIKE '%tech corr%' THEN 3
      WHEN lower(COALESCE(d.title, '')) LIKE '%dismissal%' THEN 2
      WHEN lower(COALESCE(d.title, '')) LIKE '%decision%' THEN 0
      WHEN lower(COALESCE(d.title, '')) LIKE '%remand%' THEN 0
      WHEN upper(COALESCE(d.citation, '')) LIKE '%DECISION%' THEN 0
      ELSE 1
    END ASC,
    CASE
      WHEN COALESCE(d.qc_has_index_codes, 0) = 1
       AND COALESCE(d.qc_has_rules_section, 0) = 1
       AND COALESCE(d.qc_has_ordinance_section, 0) = 1 THEN 0
      WHEN COALESCE(d.qc_has_rules_section, 0) = 1
        OR COALESCE(d.qc_has_ordinance_section, 0) = 1
        OR COALESCE(d.qc_has_index_codes, 0) = 1 THEN 1
      ELSE 2
    END ASC,
    CASE
      WHEN COALESCE(json_extract(d.metadata_json, '$.plainTextLength'), 0) >= 20000 THEN 0
      WHEN COALESCE(json_extract(d.metadata_json, '$.plainTextLength'), 0) >= 8000 THEN 1
      WHEN COALESCE(json_extract(d.metadata_json, '$.plainTextLength'), 0) >= 3000 THEN 2
      ELSE 3
    END ASC,
    COALESCE(json_extract(d.metadata_json, '$.plainTextLength'), 0) DESC
  `;
}

function resolveOrderClause(value) {
  switch (value) {
    case "coverage_rich_decision_like_searchable_asc":
      return `${decisionLikePriorityClause()}, COALESCE(d.searchable_at, '') ASC, COALESCE(d.updated_at, '') ASC, COALESCE(d.decision_date, '') ASC, d.citation ASC`;
    case "coverage_rich_decision_like_searchable_desc":
      return `${decisionLikePriorityClause()}, COALESCE(d.searchable_at, '') DESC, COALESCE(d.updated_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.citation ASC`;
    case "decision_like_searchable_desc":
      return `${decisionLikePriorityClause()}, COALESCE(d.searchable_at, '') DESC, COALESCE(d.updated_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.citation ASC`;
    case "searchable_asc":
      return `COALESCE(d.searchable_at, '') ASC, COALESCE(d.updated_at, '') ASC, COALESCE(d.decision_date, '') ASC, d.citation ASC`;
    case "decision_desc":
      return `COALESCE(d.decision_date, '') DESC, COALESCE(d.updated_at, '') DESC, COALESCE(d.searchable_at, '') DESC, d.citation ASC`;
    case "searchable_desc":
      return `COALESCE(d.searchable_at, '') DESC, COALESCE(d.updated_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.citation ASC`;
    case "decision_like_searchable_asc":
    default:
      return `${decisionLikePriorityClause()}, COALESCE(d.searchable_at, '') ASC, COALESCE(d.updated_at, '') ASC, COALESCE(d.decision_date, '') ASC, d.citation ASC`;
  }
}

async function selectGapDocuments() {
  return runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      WITH active_docs AS (
        SELECT DISTINCT document_id FROM retrieval_search_chunks WHERE active = 1
      )
      SELECT
        d.id AS documentId,
        d.title,
        d.citation,
        d.searchable_at AS searchableAt,
        d.updated_at AS updatedAt,
        d.decision_date AS decisionDate
      FROM documents d
      LEFT JOIN active_docs a ON a.document_id = d.id
      WHERE ${buildRealDecisionPredicate("d")}
        AND d.searchable_at IS NOT NULL
        AND a.document_id IS NULL
      ORDER BY ${resolveOrderClause(batchOrder)}
      LIMIT ${batchLimit}
      OFFSET ${batchOffset}
    `
  });
}

async function fetchPreview(documentId) {
  return fetchJson(`${apiBaseUrl}/admin/retrieval/documents/${encodeURIComponent(documentId)}/chunks?includeText=1`);
}

function buildIds(chunkId) {
  const safeChunkId = String(chunkId || "");
  return {
    embeddingId: `emb_${safeChunkId}`,
    searchId: `srch_${safeChunkId}`
  };
}

function buildWritePayload(previews) {
  const docsToActivate = [];
  const chunksToActivate = [];
  const embeddingRows = [];
  const searchRows = [];
  const activatedDocuments = [];
  const skippedDocuments = [];

  for (const preview of previews) {
    const document = preview?.document || null;
    const chunks = Array.isArray(preview?.chunks) ? preview.chunks : [];
    if (!document?.documentId) {
      skippedDocuments.push({
        documentId: null,
        citation: null,
        title: null,
        reason: "missing_document_payload"
      });
      continue;
    }

    const validRows = chunks.filter((chunk) => {
      const paragraphAnchorStart = chunk?.paragraphAnchorStart || chunk?.provenance?.paragraphAnchorStart;
      return Boolean(
        chunk?.chunkId &&
          chunk?.sourceText &&
          chunk?.citationAnchorStart &&
          chunk?.citationAnchorEnd &&
          paragraphAnchorStart &&
          (chunk?.provenance?.sourceLink || document?.sourceLink)
      );
    });

    if (!validRows.length) {
      skippedDocuments.push({
        documentId: document.documentId,
        citation: document.citation || "",
        title: document.title || "",
        reason: chunks.length ? "no_provenance_complete_chunks" : "no_preview_chunks"
      });
      continue;
    }

    docsToActivate.push(document.documentId);
    activatedDocuments.push({
      documentId: document.documentId,
      citation: document.citation || "",
      title: document.title || "",
      chunkCount: validRows.length,
      sourceLink: document.sourceLink || ""
    });

    for (const chunk of validRows) {
      const paragraphAnchorStart = chunk.paragraphAnchorStart || chunk.provenance?.paragraphAnchorStart;
      const paragraphAnchorEnd = chunk.paragraphAnchorEnd || chunk.provenance?.paragraphAnchorEnd || paragraphAnchorStart;
      const sourceLink = chunk.provenance?.sourceLink || document.sourceLink || "";
      const ids = buildIds(chunk.chunkId);

      chunksToActivate.push(chunk.chunkId);
      embeddingRows.push({
        embeddingId: ids.embeddingId,
        documentId: document.documentId,
        chunkId: chunk.chunkId,
        sourceText: chunk.sourceText,
        chunkType: chunk.chunkType,
        sectionLabel: chunk.sectionLabel || chunk.provenance?.sectionLabel || chunk.chunkType,
        retrievalPriority: chunk.retrievalPriority || "medium",
        hasCanonicalReferenceAlignment: Boolean(chunk.hasCanonicalReferenceAlignment),
        paragraphAnchorStart,
        paragraphAnchorEnd,
        citationAnchorStart: chunk.citationAnchorStart,
        citationAnchorEnd: chunk.citationAnchorEnd,
        sourceLink
      });
      searchRows.push({
        searchId: ids.searchId,
        documentId: document.documentId,
        chunkId: chunk.chunkId,
        title: document.title || "",
        chunkType: chunk.chunkType,
        sectionLabel: chunk.sectionLabel || chunk.provenance?.sectionLabel || chunk.chunkType,
        retrievalPriority: chunk.retrievalPriority || "medium",
        paragraphAnchorStart,
        paragraphAnchorEnd,
        citationAnchorStart: chunk.citationAnchorStart,
        citationAnchorEnd: chunk.citationAnchorEnd,
        sourceLink,
        hasCanonicalReferenceAlignment: Boolean(chunk.hasCanonicalReferenceAlignment)
      });
    }
  }

  const activationBatchId = `searchable_${formatTimestamp(new Date())}`;
  const rollbackBatchId = `rollback_${activationBatchId}`;

  return {
    payload: {
      embeddingPayload: { rows: embeddingRows },
      searchPayload: { rows: searchRows },
      activationManifest: {
        activationBatchIds: [activationBatchId],
        documentsToActivate: docsToActivate,
        chunksToActivate,
        baselineAdmittedDocIds: docsToActivate,
        promotedEnrichmentDocIds: [],
        integrity: {
          activationChecksum: `${activationBatchId}_${chunksToActivate.length}`
        }
      },
      rollbackManifest: {
        rollbackBatchIds: [rollbackBatchId],
        activationBatchIdsReversed: [activationBatchId],
        documentsToRemove: docsToActivate,
        chunksToRemove: chunksToActivate,
        integrity: {
          rollbackChecksum: `${rollbackBatchId}_${chunksToActivate.length}`
        }
      },
      dryRun: dryRunOnly,
      performVectorUpsert
    },
    activatedDocuments,
    skippedDocuments
  };
}

async function postWrite(payload) {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(`Searchable activation write timed out after ${requestTimeoutMs}ms`),
    requestTimeoutMs
  );
  const response = await fetch(`${apiBaseUrl}/admin/retrieval/activation/write`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
    signal: controller.signal
  }).finally(() => clearTimeout(timeout));

  const raw = await response.text();
  let body = null;
  try {
    body = raw ? JSON.parse(raw) : null;
  } catch {
    throw new Error(`Expected JSON from activation write endpoint, got: ${raw.slice(0, 500)}`);
  }

  if (!response.ok) {
    throw new Error(`Searchable activation write failed (${response.status}): ${JSON.stringify(body)}`);
  }

  return body;
}

function formatMarkdown(report) {
  const beforeSnapshot = report.beforeSnapshot || {};
  const afterSnapshot = report.afterSnapshot || beforeSnapshot;
  const summary = report.summary || {};
  const lines = [
    "# Searchable Retrieval Activation",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Dry run: \`${report.readOnly}\``,
    `- Stage status: \`${report.stageStatus}\``,
    `- Batch limit: \`${report.batchLimit}\``,
    `- Batch offset: \`${report.batchOffset}\``,
    `- Batch order: \`${report.batchOrder}\``,
    ""
  ];

  lines.push("## Snapshot");
  lines.push(`- Searchable but not active before: \`${beforeSnapshot.searchableButNotActiveCount ?? "n/a"}\``);
  lines.push(`- Searchable but not active after: \`${afterSnapshot.searchableButNotActiveCount ?? "n/a"}\``);
  lines.push(`- Active retrieval docs before: \`${beforeSnapshot.activeRetrievalDecisionCount ?? "n/a"}\``);
  lines.push(`- Active retrieval docs after: \`${afterSnapshot.activeRetrievalDecisionCount ?? "n/a"}\``);
  lines.push("");

  lines.push("## Selection");
  lines.push(`- Selected documents: \`${summary.selectedDocumentCount ?? "n/a"}\``);
  lines.push(`- Preview-ready documents: \`${summary.previewReadyDocumentCount ?? "n/a"}\``);
  lines.push(`- Activated documents: \`${summary.activatedDocumentDelta ?? "n/a"}\``);
  lines.push(`- Activated chunks attempted: \`${summary.chunksAttempted ?? "n/a"}\``);
  lines.push("");

  lines.push("## Activated Documents");
  if ((report.activatedDocuments || []).length) {
    for (const row of report.activatedDocuments) {
      lines.push(`- ${row.citation || row.documentId}: ${row.title} (chunks=${row.chunkCount})`);
    }
  } else {
    lines.push("- none");
  }
  lines.push("");

  lines.push("## Skipped Documents");
  if ((report.skippedDocuments || []).length) {
    for (const row of report.skippedDocuments) {
      lines.push(`- ${row.citation || row.documentId || "<unknown>"}: ${row.reason}`);
    }
  } else {
    lines.push("- none");
  }
  lines.push("");

  if (report.writeReport?.summary) {
    lines.push("## Write Summary");
    for (const [key, value] of Object.entries(report.writeReport.summary)) {
      lines.push(`- ${key}: \`${value}\``);
    }
    lines.push("");
  }

  if (report.error) {
    lines.push("## Error");
    lines.push(`- ${report.error}`);
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

export async function main() {
  await ensureDir(outputDir);

  const health = await checkHealth(apiBaseUrl);
  if (!health.ok) {
    throw new Error(`API health check failed for ${apiBaseUrl}: ${health.error}`);
  }

  const beforeSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const selectedDocuments = await selectGapDocuments();

  if (!selectedDocuments.length) {
    const noopReport = {
      generatedAt: new Date().toISOString(),
      apiBase: apiBaseUrl,
      readOnly: dryRunOnly,
      stageStatus: "noop",
      batchLimit,
      batchOffset,
      batchOrder,
      beforeSnapshot,
      afterSnapshot: beforeSnapshot,
      summary: {
        selectedDocumentCount: 0,
        previewReadyDocumentCount: 0,
        chunksAttempted: 0,
        activatedDocumentDelta: 0
      },
      activatedDocuments: [],
      skippedDocuments: [],
      writeReport: null,
      error: null
    };
    await Promise.all([
      writeJson(path.resolve(outputDir, reportName), noopReport),
      writeText(path.resolve(outputDir, markdownName), formatMarkdown(noopReport))
    ]);
    console.log(JSON.stringify(noopReport.summary, null, 2));
    return;
  }

  const previews = [];
  const previewErrors = [];
  for (const row of selectedDocuments) {
    try {
      const preview = await fetchPreview(row.documentId);
      previews.push(preview);
    } catch (error) {
      previewErrors.push({
        documentId: row.documentId,
        citation: row.citation || "",
        title: row.title || "",
        reason: error instanceof Error ? error.message : String(error)
      });
    }
  }

  const build = buildWritePayload(previews);
  const writeReport =
    build.payload.activationManifest.documentsToActivate.length > 0 ? await postWrite(build.payload) : null;
  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase: apiBaseUrl,
    readOnly: dryRunOnly,
    stageStatus: writeReport
      ? dryRunOnly
        ? "dry_run_validated"
        : "written"
      : build.activatedDocuments.length > 0
        ? "payload_ready"
        : "blocked",
    batchLimit,
    batchOffset,
    batchOrder,
    beforeSnapshot,
    afterSnapshot,
    summary: {
      selectedDocumentCount: selectedDocuments.length,
      previewReadyDocumentCount: previews.length,
      previewFailureCount: previewErrors.length,
      chunksAttempted: build.payload.activationManifest.chunksToActivate.length,
      documentsAttempted: build.payload.activationManifest.documentsToActivate.length,
      activatedDocumentDelta:
        Number(afterSnapshot.activeRetrievalDecisionCount || 0) - Number(beforeSnapshot.activeRetrievalDecisionCount || 0),
      searchableButNotActiveDelta:
        Number(afterSnapshot.searchableButNotActiveCount || 0) - Number(beforeSnapshot.searchableButNotActiveCount || 0)
    },
    selectedDocuments,
    activatedDocuments: build.activatedDocuments,
    skippedDocuments: [...build.skippedDocuments, ...previewErrors],
    payloadSummary: {
      activationBatchId: build.payload.activationManifest.activationBatchIds[0] || null,
      rollbackBatchId: build.payload.rollbackManifest.rollbackBatchIds[0] || null,
      documentCount: build.payload.activationManifest.documentsToActivate.length,
      chunkCount: build.payload.activationManifest.chunksToActivate.length
    },
    writeReport,
    error: null
  };

  await Promise.all([
    writeJson(path.resolve(outputDir, reportName), report),
    writeText(path.resolve(outputDir, markdownName), formatMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Searchable retrieval activation JSON report written to ${path.resolve(outputDir, reportName)}`);
  console.log(`Searchable retrieval activation Markdown report written to ${path.resolve(outputDir, markdownName)}`);
}

main().catch(async (error) => {
  const failure = {
    generatedAt: new Date().toISOString(),
    apiBase: apiBaseUrl,
    readOnly: dryRunOnly,
    stageStatus: "failed",
    batchLimit,
    batchOffset,
    batchOrder,
    error: error instanceof Error ? error.stack || error.message : String(error)
  };

  try {
    await Promise.all([
      writeJson(path.resolve(outputDir, reportName), failure),
      writeText(path.resolve(outputDir, markdownName), formatMarkdown(failure))
    ]);
  } catch {}

  console.error(failure.error);
  process.exitCode = 1;
});
