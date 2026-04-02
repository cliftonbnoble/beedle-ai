import path from "node:path";
import {
  buildRealDecisionPredicate,
  defaultDbPath,
  ensureDir,
  queryCorpusSnapshot,
  runSqlJson,
  writeJson,
  writeText
} from "./lib/overnight-corpus-lift-utils.mjs";
import {
  DEFAULT_ALLOWED_UNKNOWN_ORDINANCE,
  DEFAULT_ALLOWED_UNKNOWN_RULES,
  DEFAULT_MIN_EXTRACTION_CONFIDENCE,
  selectStaleQcCandidates
} from "./lib/stale-qc-remediation-utils.mjs";

const apiBaseUrl = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.STALE_QC_REMEDIATION_BUSY_TIMEOUT_MS || "5000", 10);
const batchLimit = Math.max(1, Number.parseInt(process.env.STALE_QC_REMEDIATION_LIMIT || "100", 10));
const requestTimeoutMs = Math.max(1_000, Number.parseInt(process.env.STALE_QC_REMEDIATION_TIMEOUT_MS || "60000", 10));
const minExtractionConfidence = Number.parseFloat(
  process.env.STALE_QC_REMEDIATION_MIN_CONFIDENCE || String(DEFAULT_MIN_EXTRACTION_CONFIDENCE)
);
const allowedUnknownRules = String(
  process.env.STALE_QC_REMEDIATION_ALLOWED_UNKNOWN_RULES || DEFAULT_ALLOWED_UNKNOWN_RULES.join(",")
)
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const allowedUnknownOrdinance = String(
  process.env.STALE_QC_REMEDIATION_ALLOWED_UNKNOWN_ORDINANCE || DEFAULT_ALLOWED_UNKNOWN_ORDINANCE.join(",")
)
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const outputDir = path.resolve(
  process.cwd(),
  process.env.STALE_QC_REMEDIATION_OUTPUT_DIR || "reports/stale-qc-remediation"
);
const reportName = process.env.STALE_QC_REMEDIATION_REPORT_NAME || "stale-qc-remediation-report.json";
const markdownName = process.env.STALE_QC_REMEDIATION_MARKDOWN_NAME || "stale-qc-remediation-report.md";

async function postMetadataUpdate(documentId, payload) {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(`Metadata update timed out after ${requestTimeoutMs}ms for ${documentId}`),
    requestTimeoutMs
  );

  try {
    const response = await fetch(`${apiBaseUrl}/admin/ingestion/documents/${documentId}/metadata`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal
    });

    const raw = await response.text();
    let body = null;
    try {
      body = raw ? JSON.parse(raw) : null;
    } catch {
      body = { raw };
    }

    if (!response.ok) {
      throw new Error(`Metadata update failed (${response.status}) for ${documentId}: ${JSON.stringify(body)}`);
    }

    return body;
  } finally {
    clearTimeout(timeout);
  }
}

async function selectCandidateRows() {
  return runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      SELECT
        d.id AS documentId,
        d.title,
        d.citation,
        d.decision_date AS decisionDate,
        d.extraction_confidence AS extractionConfidence,
        d.qc_required_confirmed AS qcRequiredConfirmed,
        d.qc_passed AS qcPassed,
        d.extraction_warnings_json AS extractionWarningsJson,
        d.metadata_json AS metadataJson,
        d.index_codes_json AS indexCodesJson,
        d.rules_sections_json AS rulesSectionsJson,
        d.ordinance_sections_json AS ordinanceSectionsJson
      FROM documents d
      WHERE ${buildRealDecisionPredicate("d")}
        AND d.searchable_at IS NULL
        AND COALESCE(d.qc_has_index_codes, 0) = 1
        AND COALESCE(d.qc_has_rules_section, 0) = 1
        AND COALESCE(d.qc_has_ordinance_section, 0) = 1
        AND COALESCE(d.qc_passed, 0) = 0
        AND COALESCE(d.qc_required_confirmed, 0) = 0
        AND d.approved_at IS NULL
      ORDER BY COALESCE(d.extraction_confidence, 0) DESC, COALESCE(d.decision_date, '') DESC, d.citation ASC
    `
  });
}

function formatMarkdown(report) {
  const lines = [
    "# Stale QC Remediation Batch",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Database: \`${report.dbPath}\``,
    `- Stage status: \`${report.stageStatus}\``,
    "",
    "## Summary",
    ""
  ];

  for (const [key, value] of Object.entries(report.summary || {})) {
    lines.push(`- ${key}: ${Array.isArray(value) ? value.join(", ") : value}`);
  }

  lines.push("");
  lines.push("## Updated Documents");
  lines.push("");
  for (const row of report.updatedDocuments || []) {
    lines.push(
      `- \`${row.documentId}\` | \`${row.citation}\` | confidence=\`${row.extractionConfidence}\` | unknownRules=\`${row.unknownRules.join(", ")}\` | unknownOrdinance=\`${row.unknownOrdinance.join(", ")}\``
    );
  }
  if (!(report.updatedDocuments || []).length) lines.push("- none");

  lines.push("");
  lines.push("## Skipped (Sample)");
  lines.push("");
  for (const row of (report.skippedDocuments || []).slice(0, 75)) {
    lines.push(`- \`${row.documentId}\` | \`${row.citation}\` | reason=\`${row.reason}\``);
  }
  if (!(report.skippedDocuments || []).length) lines.push("- none");

  lines.push("");
  lines.push("## Failures");
  lines.push("");
  for (const row of report.failures || []) {
    lines.push(`- \`${row.documentId}\` | \`${row.citation}\` | ${row.error}`);
  }
  if (!(report.failures || []).length) lines.push("- none");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

export async function main() {
  await ensureDir(outputDir);

  const beforeSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const candidateRows = await selectCandidateRows();
  const evaluation = selectStaleQcCandidates(candidateRows, {
    minExtractionConfidence,
    allowedUnknownRules,
    allowedUnknownOrdinance
  });
  const selectedBatch = evaluation.selected.slice(0, batchLimit);

  if (!selectedBatch.length) {
    const report = {
      generatedAt: new Date().toISOString(),
      apiBase: apiBaseUrl,
      dbPath,
      stageStatus: "noop",
      summary: {
        batchLimit,
        minExtractionConfidence,
        candidateDocCount: candidateRows.length,
        eligibleCandidateCount: 0,
        selectedDocumentCount: 0,
        updatedDocumentCount: 0,
        qcPassedNotSearchableBefore: beforeSnapshot.qcPassedNotSearchableCount,
        qcPassedNotSearchableAfter: beforeSnapshot.qcPassedNotSearchableCount
      },
      updatedDocuments: [],
      skippedDocuments: evaluation.skipped.slice(0, 250).map(({ row, evaluation: item }) => ({
        documentId: row.documentId,
        citation: row.citation,
        reason: item.reason
      })),
      failures: [],
      beforeSnapshot,
      afterSnapshot: beforeSnapshot
    };

    await Promise.all([
      writeJson(path.resolve(outputDir, reportName), report),
      writeText(path.resolve(outputDir, markdownName), formatMarkdown(report))
    ]);

    console.log(JSON.stringify(report.summary, null, 2));
    console.log(`Stale QC remediation JSON report written to ${path.resolve(outputDir, reportName)}`);
    console.log(`Stale QC remediation Markdown report written to ${path.resolve(outputDir, markdownName)}`);
    return;
  }

  const updatedDocuments = [];
  const failures = [];

  for (const item of selectedBatch) {
    try {
      const detail = await postMetadataUpdate(item.row.documentId, item.evaluation.payload);
      updatedDocuments.push({
        documentId: item.row.documentId,
        citation: item.row.citation,
        title: item.row.title,
        extractionConfidence: Number(item.row.extractionConfidence || 0),
        unknownRules: item.evaluation.unknownRules || [],
        unknownOrdinance: item.evaluation.unknownOrdinance || [],
        warningCategories: item.evaluation.warningCategories || [],
        qcPassed: Boolean(detail?.qcGateDiagnostics?.passed),
        qcRequiredConfirmed: Boolean(detail?.qcRequiredConfirmed)
      });
    } catch (error) {
      failures.push({
        documentId: item.row.documentId,
        citation: item.row.citation,
        title: item.row.title,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const report = {
    generatedAt: new Date().toISOString(),
    apiBase: apiBaseUrl,
    dbPath,
    stageStatus: failures.length > 0 ? (updatedDocuments.length > 0 ? "partial" : "failed") : "written",
    summary: {
      batchLimit,
      minExtractionConfidence,
      allowedUnknownRules,
      allowedUnknownOrdinance,
      candidateDocCount: candidateRows.length,
      eligibleCandidateCount: evaluation.selected.length,
      selectedDocumentCount: selectedBatch.length,
      updatedDocumentCount: updatedDocuments.length,
      failuresCount: failures.length,
      qcPassedCount: updatedDocuments.filter((row) => row.qcPassed).length,
      qcRequiredConfirmedCount: updatedDocuments.filter((row) => row.qcRequiredConfirmed).length,
      qcPassedNotSearchableBefore: beforeSnapshot.qcPassedNotSearchableCount,
      qcPassedNotSearchableAfter: afterSnapshot.qcPassedNotSearchableCount,
      searchableBefore: beforeSnapshot.searchableDecisionDocs,
      searchableAfter: afterSnapshot.searchableDecisionDocs
    },
    updatedDocuments,
    skippedDocuments: evaluation.skipped.slice(0, 250).map(({ row, evaluation: item }) => ({
      documentId: row.documentId,
      citation: row.citation,
      reason: item.reason
    })),
    failures,
    beforeSnapshot,
    afterSnapshot
  };

  await Promise.all([
    writeJson(path.resolve(outputDir, reportName), report),
    writeText(path.resolve(outputDir, markdownName), formatMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Stale QC remediation JSON report written to ${path.resolve(outputDir, reportName)}`);
  console.log(`Stale QC remediation Markdown report written to ${path.resolve(outputDir, markdownName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
