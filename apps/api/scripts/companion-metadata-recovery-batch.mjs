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
import { buildCompanionRecoveryCandidates } from "./lib/companion-metadata-recovery-utils.mjs";

const apiBaseUrl = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.COMPANION_METADATA_BUSY_TIMEOUT_MS || "5000", 10);
const batchLimit = Math.max(1, Number.parseInt(process.env.COMPANION_METADATA_LIMIT || "100", 10));
const requestTimeoutMs = Math.max(1000, Number.parseInt(process.env.COMPANION_METADATA_TIMEOUT_MS || "60000", 10));
const outputDir = path.resolve(
  process.cwd(),
  process.env.COMPANION_METADATA_OUTPUT_DIR || "reports/companion-metadata-recovery"
);
const reportName = process.env.COMPANION_METADATA_REPORT_NAME || "companion-metadata-recovery-report.json";
const markdownName = process.env.COMPANION_METADATA_MARKDOWN_NAME || "companion-metadata-recovery-report.md";

async function selectTargetRows() {
  return runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      SELECT
        d.id AS documentId,
        d.title,
        d.citation,
        d.decision_date AS decisionDate,
        d.updated_at AS updatedAt,
        d.extraction_confidence AS extractionConfidence,
        d.index_codes_json AS indexCodesJson,
        d.rules_sections_json AS rulesSectionsJson,
        d.ordinance_sections_json AS ordinanceSectionsJson,
        d.qc_has_index_codes AS qcHasIndexCodes,
        d.qc_has_rules_section AS qcHasRulesSection,
        d.qc_has_ordinance_section AS qcHasOrdinanceSection
      FROM documents d
      WHERE ${buildRealDecisionPredicate("d")}
        AND d.searchable_at IS NULL
        AND COALESCE(d.qc_passed, 0) = 0
        AND (
          COALESCE(d.qc_has_index_codes, 0) = 0
          OR COALESCE(d.qc_has_rules_section, 0) = 0
          OR COALESCE(d.qc_has_ordinance_section, 0) = 0
        )
      ORDER BY COALESCE(d.decision_date, '') DESC, COALESCE(d.updated_at, '') DESC, d.citation ASC
    `
  });
}

async function selectSiblingRows() {
  return runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      SELECT
        d.id AS documentId,
        d.title,
        d.citation,
        d.searchable_at AS searchableAt,
        d.qc_passed AS qcPassed,
        d.extraction_confidence AS extractionConfidence,
        d.updated_at AS updatedAt,
        d.index_codes_json AS indexCodesJson,
        d.rules_sections_json AS rulesSectionsJson,
        d.ordinance_sections_json AS ordinanceSectionsJson
      FROM documents d
      WHERE ${buildRealDecisionPredicate("d")}
        AND COALESCE(d.index_codes_json, '') NOT IN ('', '[]')
        AND COALESCE(d.rules_sections_json, '') NOT IN ('', '[]')
        AND COALESCE(d.ordinance_sections_json, '') NOT IN ('', '[]')
    `
  });
}

async function postMetadataUpdate(documentId, payload) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(`Metadata update timed out after ${requestTimeoutMs}ms for ${documentId}`), requestTimeoutMs);

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

function formatMarkdown(report) {
  const lines = [
    "# Companion Metadata Recovery Batch",
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
      `- \`${row.documentId}\` | \`${row.citation}\` | family=\`${row.family}\` | siblingCount=\`${row.siblingCount}\` | qcPassed=\`${row.qcPassed}\``
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
  const [targetRows, siblingRows] = await Promise.all([selectTargetRows(), selectSiblingRows()]);
  const evaluation = buildCompanionRecoveryCandidates(targetRows, siblingRows);
  const selectedBatch = evaluation.selected.slice(0, batchLimit);

  if (!selectedBatch.length) {
    const report = {
      generatedAt: new Date().toISOString(),
      apiBase: apiBaseUrl,
      dbPath,
      stageStatus: "noop",
      summary: {
        batchLimit,
        targetDocCount: targetRows.length,
        siblingDocCount: siblingRows.length,
        eligibleCandidateCount: evaluation.selected.length,
        selectedDocumentCount: 0,
        updatedDocumentCount: 0,
        qcPassedCount: 0,
        qcPassedNotSearchableBefore: beforeSnapshot.qcPassedNotSearchableCount,
        qcPassedNotSearchableAfter: beforeSnapshot.qcPassedNotSearchableCount
      },
      updatedDocuments: [],
      skippedDocuments: evaluation.skipped.map((item) => ({
        documentId: item.row.documentId,
        citation: item.row.citation,
        reason: item.evaluation.reason
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
    console.log(`Companion metadata recovery JSON report written to ${path.resolve(outputDir, reportName)}`);
    console.log(`Companion metadata recovery Markdown report written to ${path.resolve(outputDir, markdownName)}`);
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
        family: item.evaluation.family,
        siblingCount: item.evaluation.siblingCount,
        qcPassed: Boolean(detail?.qcGateDiagnostics?.passed),
        qcRequiredConfirmed: Boolean(detail?.qcRequiredConfirmed),
        resultingIndexCodes: Array.isArray(detail?.indexCodes) ? detail.indexCodes.map(String) : []
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
      targetDocCount: targetRows.length,
      siblingDocCount: siblingRows.length,
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
    skippedDocuments: evaluation.skipped.map((item) => ({
      documentId: item.row.documentId,
      citation: item.row.citation,
      reason: item.evaluation.reason
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
  console.log(`Companion metadata recovery JSON report written to ${path.resolve(outputDir, reportName)}`);
  console.log(`Companion metadata recovery Markdown report written to ${path.resolve(outputDir, markdownName)}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
