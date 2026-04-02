import path from "node:path";
import {
  buildMissingIndexAuditReport
} from "./lib/missing-index-audit-utils.mjs";
import {
  DEFAULT_ALLOWED_SOURCES,
  DEFAULT_MIN_MARGIN,
  DEFAULT_MIN_TOP_SCORE,
  selectInferenceCandidates
} from "./lib/missing-index-inference-utils.mjs";
import {
  defaultDbPath,
  ensureDir,
  queryCorpusSnapshot,
  writeJson,
  writeText
} from "./lib/overnight-corpus-lift-utils.mjs";

const apiBaseUrl = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.MISSING_INDEX_INFERENCE_BUSY_TIMEOUT_MS || "5000", 10);
const batchLimit = Math.max(1, Number.parseInt(process.env.MISSING_INDEX_INFERENCE_LIMIT || "100", 10));
const outputDir = path.resolve(process.cwd(), process.env.MISSING_INDEX_INFERENCE_OUTPUT_DIR || "reports/missing-index-inference-batch");
const reportName = process.env.MISSING_INDEX_INFERENCE_REPORT_NAME || "missing-index-inference-report.json";
const markdownName = process.env.MISSING_INDEX_INFERENCE_MARKDOWN_NAME || "missing-index-inference-report.md";
const requestTimeoutMs = Math.max(1_000, Number.parseInt(process.env.MISSING_INDEX_INFERENCE_TIMEOUT_MS || "60000", 10));
const minTopScore = Number.parseFloat(process.env.MISSING_INDEX_INFERENCE_MIN_TOP_SCORE || String(DEFAULT_MIN_TOP_SCORE));
const minMargin = Number.parseFloat(process.env.MISSING_INDEX_INFERENCE_MIN_MARGIN || String(DEFAULT_MIN_MARGIN));
const allowedSources = String(process.env.MISSING_INDEX_INFERENCE_ALLOWED_SOURCES || DEFAULT_ALLOWED_SOURCES.join(","))
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

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
    "# Missing-index Inference Batch",
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
      `- \`${row.documentId}\` | \`${row.citation}\` | code=\`${row.selectedCode}\` | score=\`${row.topScore}\` | margin=\`${row.margin ?? "<single>"}\` | qcPassed=\`${row.qcPassed}\``
    );
  }
  if (!(report.updatedDocuments || []).length) lines.push("- none");

  lines.push("");
  lines.push("## Skipped (Sample)");
  lines.push("");
  for (const row of (report.skippedDocuments || []).slice(0, 50)) {
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
  const auditReport = await buildMissingIndexAuditReport({ dbPath, busyTimeoutMs, limit: 0 });
  const { selected, skipped } = selectInferenceCandidates(auditReport.rows || [], {
    allowedSources,
    minTopScore,
    minMargin
  });
  const selectedBatch = selected.slice(0, batchLimit);

  if (!selectedBatch.length) {
    const report = {
      generatedAt: new Date().toISOString(),
      apiBase: apiBaseUrl,
      dbPath,
      stageStatus: "noop",
      summary: {
        allowedSources,
        minTopScore,
        minMargin,
        batchLimit,
        candidateDocCount: Number(auditReport.summary?.candidateDocCount || 0),
        eligibleInferenceDocCount: 0,
        selectedDocumentCount: 0,
        updatedDocumentCount: 0,
        qcPassedNotSearchableBefore: beforeSnapshot.qcPassedNotSearchableCount,
        qcPassedNotSearchableAfter: beforeSnapshot.qcPassedNotSearchableCount
      },
      updatedDocuments: [],
      skippedDocuments: skipped.slice(0, 100).map((item) => ({
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
    console.log(`Missing-index inference JSON report written to ${path.resolve(outputDir, reportName)}`);
    console.log(`Missing-index inference Markdown report written to ${path.resolve(outputDir, markdownName)}`);
    return;
  }

  const updatedDocuments = [];
  const failures = [];

  for (const item of selectedBatch) {
    const row = item.row;
    const evaluation = item.evaluation;
    const payload = {
      index_codes: evaluation.selectedIndexCodes,
      rules_sections: row.rulesSections,
      ordinance_sections: row.ordinanceSections
    };

    try {
      const detail = await postMetadataUpdate(row.documentId, payload);
      updatedDocuments.push({
        documentId: row.documentId,
        citation: row.citation,
        title: row.title,
        selectedCode: evaluation.selectedCode,
        topScore: Number(evaluation.topCandidate?.score || 0),
        margin: evaluation.margin,
        selectedSources: evaluation.selectedSources || [],
        issueFamilies: (row.issueFamilies || []).map((item) => item.family),
        qcPassed: Boolean(detail?.qcGateDiagnostics?.passed),
        resultingIndexCodes: Array.isArray(detail?.indexCodes) ? detail.indexCodes.map(String) : []
      });
    } catch (error) {
      failures.push({
        documentId: row.documentId,
        citation: row.citation,
        title: row.title,
        selectedCode: evaluation.selectedCode,
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
      allowedSources,
      minTopScore,
      minMargin,
      batchLimit,
      auditMissingIndexOnlyCount: Number(auditReport.summary?.missingIndexOnlyCount || 0),
      candidateDocCount: Number(auditReport.summary?.candidateDocCount || 0),
      eligibleInferenceDocCount: selected.length,
      selectedDocumentCount: selectedBatch.length,
      updatedDocumentCount: updatedDocuments.length,
      failuresCount: failures.length,
      qcPassedCount: updatedDocuments.filter((row) => row.qcPassed).length,
      qcPassedNotSearchableBefore: beforeSnapshot.qcPassedNotSearchableCount,
      qcPassedNotSearchableAfter: afterSnapshot.qcPassedNotSearchableCount,
      searchableBefore: beforeSnapshot.searchableDecisionDocs,
      searchableAfter: afterSnapshot.searchableDecisionDocs
    },
    updatedDocuments,
    skippedDocuments: skipped.slice(0, 250).map((item) => ({
      documentId: item.row.documentId,
      citation: item.row.citation,
      reason: item.evaluation.reason,
      topCandidate: item.evaluation.topCandidate,
      secondCandidate: item.evaluation.secondCandidate,
      issueFamilies: item.row.issueFamilies
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
  console.log(`Missing-index inference JSON report written to ${path.resolve(outputDir, reportName)}`);
  console.log(`Missing-index inference Markdown report written to ${path.resolve(outputDir, markdownName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});

