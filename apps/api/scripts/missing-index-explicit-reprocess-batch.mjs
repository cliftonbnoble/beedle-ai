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

const apiBaseUrl = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.MISSING_INDEX_REPROCESS_BUSY_TIMEOUT_MS || "5000", 10);
const batchLimit = Math.max(1, Number.parseInt(process.env.MISSING_INDEX_REPROCESS_LIMIT || "100", 10));
const outputDir = path.resolve(process.cwd(), process.env.MISSING_INDEX_REPROCESS_OUTPUT_DIR || "reports/missing-index-explicit-reprocess");
const reportName = process.env.MISSING_INDEX_REPROCESS_REPORT_NAME || "missing-index-explicit-reprocess-report.json";
const markdownName = process.env.MISSING_INDEX_REPROCESS_MARKDOWN_NAME || "missing-index-explicit-reprocess-report.md";
const requestTimeoutMs = Math.max(1_000, Number.parseInt(process.env.MISSING_INDEX_REPROCESS_TIMEOUT_MS || "60000", 10));

async function selectCandidates() {
  return runSqlJson({
    dbPath,
    busyTimeoutMs,
    sql: `
      WITH header_hits AS (
        SELECT
          c.document_id AS documentId,
          MIN(c.chunk_order) AS headerChunkOrder
        FROM document_chunks c
        JOIN documents d ON d.id = c.document_id
        WHERE ${buildRealDecisionPredicate("d")}
          AND d.qc_has_index_codes = 0
          AND d.qc_has_rules_section = 1
          AND d.qc_has_ordinance_section = 1
          AND d.searchable_at IS NULL
          AND c.chunk_order <= 2
          AND lower(c.chunk_text) LIKE '%index code%'
        GROUP BY c.document_id
      )
      SELECT
        d.id AS documentId,
        d.citation,
        d.title,
        d.author_name AS authorName,
        d.decision_date AS decisionDate,
        d.updated_at AS updatedAt,
        h.headerChunkOrder,
        substr(replace(replace(c.chunk_text, char(10), ' '), char(13), ' '), 1, 240) AS headerSnippet
      FROM header_hits h
      JOIN documents d ON d.id = h.documentId
      JOIN document_chunks c
        ON c.document_id = h.documentId
       AND c.chunk_order = h.headerChunkOrder
      ORDER BY
        COALESCE(d.updated_at, '') DESC,
        COALESCE(d.decision_date, '') DESC,
        d.citation ASC
      LIMIT ${batchLimit}
    `
  });
}

async function postReprocess(documentId) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(`Reprocess timed out after ${requestTimeoutMs}ms for ${documentId}`), requestTimeoutMs);
  try {
    const response = await fetch(`${apiBaseUrl}/admin/ingestion/documents/${documentId}/reprocess`, {
      method: "POST",
      headers: { "content-type": "application/json" },
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
      throw new Error(`Reprocess failed (${response.status}) for ${documentId}: ${JSON.stringify(body)}`);
    }

    return body;
  } finally {
    clearTimeout(timeout);
  }
}

function formatMarkdown(report) {
  const lines = [
    "# Missing-index Explicit Reprocess Batch",
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
    lines.push(`- ${key}: ${value}`);
  }

  lines.push("");
  lines.push("## Reprocessed");
  lines.push("");
  for (const row of report.reprocessedDocuments || []) {
    lines.push(
      `- \`${row.documentId}\` | \`${row.citation}\` | recovered=\`${row.recoveredIndexCodes.join(", ") || "<none>"}\` | qcPassed=\`${row.qcPassed}\``
    );
  }
  if (!(report.reprocessedDocuments || []).length) lines.push("- none");

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
  const candidates = await selectCandidates();

  if (!candidates.length) {
    const report = {
      generatedAt: new Date().toISOString(),
      apiBase: apiBaseUrl,
      dbPath,
      stageStatus: "noop",
      summary: {
        selectedDocumentCount: 0,
        attemptedDocumentCount: 0,
        recoveredIndexCodeCount: 0,
        newlyQcPassedCount: 0,
        qcPassedNotSearchableBefore: beforeSnapshot.qcPassedNotSearchableCount,
        qcPassedNotSearchableAfter: beforeSnapshot.qcPassedNotSearchableCount
      },
      selectedDocuments: [],
      reprocessedDocuments: [],
      failures: [],
      beforeSnapshot,
      afterSnapshot: beforeSnapshot
    };

    await Promise.all([
      writeJson(path.resolve(outputDir, reportName), report),
      writeText(path.resolve(outputDir, markdownName), formatMarkdown(report))
    ]);

    console.log(JSON.stringify(report.summary, null, 2));
    console.log(`Missing-index reprocess JSON report written to ${path.resolve(outputDir, reportName)}`);
    console.log(`Missing-index reprocess Markdown report written to ${path.resolve(outputDir, markdownName)}`);
    return;
  }

  const reprocessedDocuments = [];
  const failures = [];

  for (const candidate of candidates) {
    try {
      const detail = await postReprocess(candidate.documentId);
      reprocessedDocuments.push({
        documentId: candidate.documentId,
        citation: candidate.citation,
        title: candidate.title,
        headerSnippet: candidate.headerSnippet,
        recoveredIndexCodes: Array.isArray(detail?.indexCodes) ? detail.indexCodes.map((value) => String(value)) : [],
        qcPassed: Boolean(detail?.qcGateDiagnostics?.passed),
        extractionWarnings: Array.isArray(detail?.extractionWarnings) ? detail.extractionWarnings.map((value) => String(value)) : []
      });
    } catch (error) {
      failures.push({
        documentId: candidate.documentId,
        citation: candidate.citation,
        title: candidate.title,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const report = {
    generatedAt: new Date().toISOString(),
    apiBase: apiBaseUrl,
    dbPath,
    stageStatus: failures.length > 0 ? (reprocessedDocuments.length > 0 ? "partial" : "failed") : "written",
    summary: {
      selectedDocumentCount: candidates.length,
      attemptedDocumentCount: candidates.length,
      reprocessedDocumentCount: reprocessedDocuments.length,
      recoveredIndexCodeCount: reprocessedDocuments.filter((row) => row.recoveredIndexCodes.length > 0).length,
      newlyQcPassedCount: reprocessedDocuments.filter((row) => row.qcPassed).length,
      failuresCount: failures.length,
      qcPassedNotSearchableBefore: beforeSnapshot.qcPassedNotSearchableCount,
      qcPassedNotSearchableAfter: afterSnapshot.qcPassedNotSearchableCount,
      searchableBefore: beforeSnapshot.searchableDecisionDocs,
      searchableAfter: afterSnapshot.searchableDecisionDocs
    },
    selectedDocuments: candidates,
    reprocessedDocuments,
    failures,
    beforeSnapshot,
    afterSnapshot
  };

  await Promise.all([
    writeJson(path.resolve(outputDir, reportName), report),
    writeText(path.resolve(outputDir, markdownName), formatMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Missing-index reprocess JSON report written to ${path.resolve(outputDir, reportName)}`);
  console.log(`Missing-index reprocess Markdown report written to ${path.resolve(outputDir, markdownName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
