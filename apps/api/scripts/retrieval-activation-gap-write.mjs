import path from "node:path";
import {
  buildRealDecisionPredicate,
  defaultDbPath,
  ensureDir,
  queryCorpusSnapshot,
  readJson,
  runNodeScript,
  runSqlJson,
  writeJson,
  writeText
} from "./lib/overnight-corpus-lift-utils.mjs";
import {
  evaluateDryRunForWrite,
  formatRetrievalActivationGapMarkdown
} from "./lib/retrieval-activation-gap-utils.mjs";

const apiBaseUrl = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.RETRIEVAL_ACTIVATION_GAP_BUSY_TIMEOUT_MS || "5000", 10);
const batchLimit = Math.max(1, Number.parseInt(process.env.RETRIEVAL_ACTIVATION_GAP_LIMIT || "100", 10));
const batchOffset = Math.max(0, Number.parseInt(process.env.RETRIEVAL_ACTIVATION_GAP_OFFSET || "0", 10));
const batchOrder = String(process.env.RETRIEVAL_ACTIVATION_GAP_ORDER || "searchable_desc");
const outputDir = path.resolve(process.cwd(), process.env.RETRIEVAL_ACTIVATION_GAP_OUTPUT_DIR || "reports/retrieval-activation-gap");
const reportsDir = path.resolve(outputDir, "artifacts");
const reportName = process.env.RETRIEVAL_ACTIVATION_GAP_REPORT_NAME || "retrieval-activation-gap-report.json";
const markdownName = process.env.RETRIEVAL_ACTIVATION_GAP_MARKDOWN_NAME || "retrieval-activation-gap-report.md";
const dryRunOnly = (process.env.RETRIEVAL_ACTIVATION_GAP_WRITE_DRY_RUN || "0") === "1";
const performVectorUpsert = (process.env.RETRIEVAL_ACTIVATION_GAP_PERFORM_VECTOR_UPSERT || "0") === "1";
const activationWriteTimeoutMs = Math.max(
  1_000,
  Number.parseInt(process.env.RETRIEVAL_ACTIVATION_GAP_WRITE_TIMEOUT_MS || "300000", 10)
);

const bundleReportName = "retrieval-activation-bundle-report.json";
const bundleMarkdownName = "retrieval-activation-bundle-report.md";
const embeddingPayloadName = "retrieval-trusted-embedding-payload.json";
const searchPayloadName = "retrieval-trusted-search-payload.json";
const activationManifestName = "retrieval-trusted-activation-manifest.json";
const rollbackManifestName = "retrieval-trusted-rollback-manifest.json";
const debugReportName = "retrieval-activation-debug-report.json";
const debugMarkdownName = "retrieval-activation-debug-report.md";
const writeReportName = "retrieval-activation-write-report.json";
const writeMarkdownName = "retrieval-activation-write-report.md";
const verificationName = "retrieval-activation-verification.json";

function scriptPath(name) {
  return path.resolve(process.cwd(), "scripts", name);
}

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
    case "decision_like_searchable_asc":
      return `${decisionLikePriorityClause()}, COALESCE(d.searchable_at, '') ASC, COALESCE(d.updated_at, '') ASC, COALESCE(d.decision_date, '') ASC, d.citation ASC`;
    case "decision_like_searchable_desc":
      return `${decisionLikePriorityClause()}, COALESCE(d.searchable_at, '') DESC, COALESCE(d.updated_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.citation ASC`;
    case "searchable_asc":
      return `COALESCE(d.searchable_at, '') ASC, COALESCE(d.updated_at, '') ASC, COALESCE(d.decision_date, '') ASC, d.citation ASC`;
    case "decision_desc":
      return `COALESCE(d.decision_date, '') DESC, COALESCE(d.updated_at, '') DESC, COALESCE(d.searchable_at, '') DESC, d.citation ASC`;
    case "decision_asc":
      return `COALESCE(d.decision_date, '') ASC, COALESCE(d.updated_at, '') ASC, COALESCE(d.searchable_at, '') ASC, d.citation ASC`;
    case "searchable_desc":
    default:
      return `COALESCE(d.searchable_at, '') DESC, COALESCE(d.updated_at, '') DESC, COALESCE(d.decision_date, '') DESC, d.citation ASC`;
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

function buildSharedEnv(documentIds) {
  return {
    API_BASE_URL: apiBaseUrl,
    RETRIEVAL_DOC_IDS: documentIds.join(","),
    RETRIEVAL_DOC_LIMIT: String(batchLimit),
    RETRIEVAL_REAL_ONLY: "1",
    RETRIEVAL_INCLUDE_TEXT: "1",
    RETRIEVAL_REPORTS_DIR: reportsDir,
    RETRIEVAL_ACTIVATION_BUNDLE_REPORT_NAME: bundleReportName,
    RETRIEVAL_ACTIVATION_BUNDLE_MARKDOWN_NAME: bundleMarkdownName,
    RETRIEVAL_TRUSTED_EMBEDDING_PAYLOAD_NAME: embeddingPayloadName,
    RETRIEVAL_TRUSTED_SEARCH_PAYLOAD_NAME: searchPayloadName,
    RETRIEVAL_TRUSTED_ACTIVATION_MANIFEST_NAME: activationManifestName,
    RETRIEVAL_TRUSTED_ROLLBACK_MANIFEST_NAME: rollbackManifestName,
    RETRIEVAL_ACTIVATION_DEBUG_REPORT_NAME: debugReportName,
    RETRIEVAL_ACTIVATION_DEBUG_MARKDOWN_NAME: debugMarkdownName,
    RETRIEVAL_ACTIVATION_WRITE_REPORT_NAME: writeReportName,
    RETRIEVAL_ACTIVATION_WRITE_MARKDOWN_NAME: writeMarkdownName,
    RETRIEVAL_ACTIVATION_VERIFICATION_NAME: verificationName,
    RETRIEVAL_PERFORM_VECTOR_UPSERT: performVectorUpsert ? "1" : "0",
    RETRIEVAL_ACTIVATION_WRITE_TIMEOUT_MS: String(activationWriteTimeoutMs)
  };
}

function stageError(message, detail = {}) {
  return {
    generatedAt: new Date().toISOString(),
    readOnly: dryRunOnly,
    apiBase: apiBaseUrl,
    stageStatus: "failed",
    summary: {
      error: message,
      ...detail
    },
    selectedDocuments: [],
    skippedDocuments: [],
    dryRunEvaluation: {
      canWrite: false,
      reasons: [message],
      verificationChecks: {}
    }
  };
}

export async function main() {
  await ensureDir(outputDir);
  await ensureDir(reportsDir);

  const beforeSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const selectedDocuments = await selectGapDocuments();

  if (!selectedDocuments.length) {
    const report = {
      generatedAt: new Date().toISOString(),
      readOnly: dryRunOnly,
      apiBase: apiBaseUrl,
      stageStatus: "noop",
      summary: {
        selectedOffset: batchOffset,
        selectedOrder: batchOrder,
        selectedDocumentCount: 0,
        attemptedDocumentCount: 0,
        activatedDocumentDelta: 0,
        searchableButNotActiveBefore: beforeSnapshot.searchableButNotActiveCount,
        searchableButNotActiveAfter: beforeSnapshot.searchableButNotActiveCount,
        activeRetrievalDecisionCountBefore: beforeSnapshot.activeRetrievalDecisionCount,
        activeRetrievalDecisionCountAfter: beforeSnapshot.activeRetrievalDecisionCount
      },
      selectedDocuments: [],
      skippedDocuments: [],
      dryRunEvaluation: {
        canWrite: false,
        reasons: ["no_gap_documents_available"],
        verificationChecks: {}
      },
      beforeSnapshot,
      afterSnapshot: beforeSnapshot
    };

    await Promise.all([
      writeJson(path.resolve(outputDir, reportName), report),
      writeText(path.resolve(outputDir, markdownName), formatRetrievalActivationGapMarkdown(report))
    ]);

    console.log(JSON.stringify(report.summary, null, 2));
    console.log(`Retrieval activation gap JSON report written to ${path.resolve(outputDir, reportName)}`);
    console.log(`Retrieval activation gap Markdown report written to ${path.resolve(outputDir, markdownName)}`);
    return;
  }

  const sharedEnv = buildSharedEnv(selectedDocuments.map((row) => row.documentId));
  const bundleRun = await runNodeScript(scriptPath("retrieval-activation-bundle-report.mjs"), {
    cwd: process.cwd(),
    env: sharedEnv
  });
  if (!bundleRun.ok) {
    throw new Error(`Bundle stage failed:\n${bundleRun.stderr || bundleRun.stdout}`);
  }

  const bundleReport = await readJson(path.resolve(reportsDir, bundleReportName));
  const activationManifest = await readJson(path.resolve(reportsDir, activationManifestName));
  const skippedDocuments = (bundleReport.documents || [])
    .filter((row) => !row.activationEligible)
    .map((row) => ({ documentId: row.documentId, reason: "activation_ineligible", title: row.title || "" }));

  const debugRun = await runNodeScript(scriptPath("retrieval-activation-debug.mjs"), {
    cwd: process.cwd(),
    env: sharedEnv
  });
  if (!debugRun.ok) {
    throw new Error(`Dry-run activation stage failed:\n${debugRun.stderr || debugRun.stdout}`);
  }

  const debugReport = await readJson(path.resolve(reportsDir, debugReportName));
  const dryRunEvaluation = evaluateDryRunForWrite(debugReport, activationManifest);

  let writeReport = null;
  let stageStatus = dryRunEvaluation.canWrite ? (dryRunOnly ? "dry_run_only" : "write_ready") : "blocked";

  if (dryRunEvaluation.canWrite && !dryRunOnly) {
    const writeRun = await runNodeScript(scriptPath("retrieval-activation-write.mjs"), {
      cwd: process.cwd(),
      env: sharedEnv
    });
    if (!writeRun.ok) {
      throw new Error(`Activation write stage failed:\n${writeRun.stderr || writeRun.stdout}`);
    }
    writeReport = await readJson(path.resolve(reportsDir, writeReportName));
    stageStatus = "written";
  }

  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const activatedDocumentCount = Number(writeReport?.summary?.activatedDocumentCount || 0);
  const activatedChunkCount = Number(writeReport?.summary?.activatedChunkCount || 0);

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: dryRunOnly,
    apiBase: apiBaseUrl,
    dbPath,
    stageStatus,
    summary: {
      selectedOffset: batchOffset,
      selectedOrder: batchOrder,
      selectedDocumentCount: selectedDocuments.length,
      bundleTrustedDocumentCount: Number(bundleReport.summary?.trustedDocumentCount || 0),
      bundleTrustedChunkCount: Number(bundleReport.summary?.trustedChunkCount || 0),
      activationReady: Boolean(bundleReport.summary?.activationReady),
      performVectorUpsert,
      attemptedDocumentCount: Number(debugReport.summary?.attemptedTrustedDocumentCount || 0),
      attemptedChunkCount: Number(debugReport.summary?.attemptedTrustedChunkCount || 0),
      activatedDocumentCount,
      activatedChunkCount,
      activatedDocumentDelta: afterSnapshot.activeRetrievalDecisionCount - beforeSnapshot.activeRetrievalDecisionCount,
      searchableButNotActiveBefore: beforeSnapshot.searchableButNotActiveCount,
      searchableButNotActiveAfter: afterSnapshot.searchableButNotActiveCount,
      activeRetrievalDecisionCountBefore: beforeSnapshot.activeRetrievalDecisionCount,
      activeRetrievalDecisionCountAfter: afterSnapshot.activeRetrievalDecisionCount
    },
    selectedDocuments,
    skippedDocuments,
    dryRunEvaluation,
    beforeSnapshot,
    afterSnapshot,
    bundleSummary: bundleReport.summary,
    debugSummary: debugReport.summary,
    writeSummary: writeReport?.summary || null,
    artifactFiles: {
      reportsDir,
      bundleReport: path.resolve(reportsDir, bundleReportName),
      debugReport: path.resolve(reportsDir, debugReportName),
      writeReport: path.resolve(reportsDir, writeReportName),
      activationManifest: path.resolve(reportsDir, activationManifestName),
      rollbackManifest: path.resolve(reportsDir, rollbackManifestName),
      embeddingPayload: path.resolve(reportsDir, embeddingPayloadName),
      searchPayload: path.resolve(reportsDir, searchPayloadName)
    }
  };

  await Promise.all([
    writeJson(path.resolve(outputDir, reportName), report),
    writeText(path.resolve(outputDir, markdownName), formatRetrievalActivationGapMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Retrieval activation gap JSON report written to ${path.resolve(outputDir, reportName)}`);
  console.log(`Retrieval activation gap Markdown report written to ${path.resolve(outputDir, markdownName)}`);
}

main().catch(async (error) => {
  const report = stageError(error instanceof Error ? error.message : String(error));
  await ensureDir(outputDir);
  await Promise.all([
    writeJson(path.resolve(outputDir, reportName), report),
    writeText(path.resolve(outputDir, markdownName), formatRetrievalActivationGapMarkdown(report))
  ]);
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
