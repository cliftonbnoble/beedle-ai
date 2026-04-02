import path from 'node:path';
import { buildMissingIndexAuditReport } from './lib/missing-index-audit-utils.mjs';
import {
  buildSiblingInheritanceCandidates,
  selectTailInferenceCandidates
} from './lib/missing-index-tail-recovery-utils.mjs';
import {
  defaultDbPath,
  ensureDir,
  queryCorpusSnapshot,
  writeJson,
  writeText
} from './lib/overnight-corpus-lift-utils.mjs';

const apiBaseUrl = process.env.API_BASE_URL || 'http://127.0.0.1:8787';
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.MISSING_INDEX_TAIL_BUSY_TIMEOUT_MS || '5000', 10);
const batchLimit = Math.max(1, Number.parseInt(process.env.MISSING_INDEX_TAIL_LIMIT || '100', 10));
const outputDir = path.resolve(process.cwd(), process.env.MISSING_INDEX_TAIL_OUTPUT_DIR || 'reports/missing-index-tail-recovery');
const reportName = process.env.MISSING_INDEX_TAIL_REPORT_NAME || 'missing-index-tail-recovery-report.json';
const markdownName = process.env.MISSING_INDEX_TAIL_MARKDOWN_NAME || 'missing-index-tail-recovery-report.md';
const requestTimeoutMs = Math.max(1000, Number.parseInt(process.env.MISSING_INDEX_TAIL_TIMEOUT_MS || '60000', 10));

async function postMetadataUpdate(documentId, payload) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(`Metadata update timed out after ${requestTimeoutMs}ms for ${documentId}`), requestTimeoutMs);

  try {
    const response = await fetch(`${apiBaseUrl}/admin/ingestion/documents/${documentId}/metadata`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
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
    '# Missing-index Tail Recovery Batch',
    '',
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Database: \`${report.dbPath}\``,
    `- Stage status: \`${report.stageStatus}\``,
    '',
    '## Summary',
    ''
  ];

  for (const [key, value] of Object.entries(report.summary || {})) {
    lines.push(`- ${key}: ${Array.isArray(value) ? value.join(', ') : value}`);
  }

  lines.push('');
  lines.push('## Updated Documents');
  lines.push('');
  for (const row of report.updatedDocuments || []) {
    lines.push(
      `- \`${row.documentId}\` | \`${row.citation}\` | method=\`${row.selectionMethod}\` | codes=\`${(row.selectedIndexCodes || []).join(', ')}\` | qcPassed=\`${row.qcPassed}\``
    );
  }
  if (!(report.updatedDocuments || []).length) lines.push('- none');

  lines.push('');
  lines.push('## Skipped Inference (Sample)');
  lines.push('');
  for (const row of (report.skippedDocuments || []).slice(0, 50)) {
    lines.push(`- \`${row.documentId}\` | \`${row.citation}\` | reason=\`${row.reason}\``);
  }
  if (!(report.skippedDocuments || []).length) lines.push('- none');

  lines.push('');
  lines.push('## Failures');
  lines.push('');
  for (const row of report.failures || []) {
    lines.push(`- \`${row.documentId}\` | \`${row.citation}\` | ${row.error}`);
  }
  if (!(report.failures || []).length) lines.push('- none');
  lines.push('');

  return `${lines.join('\n')}\n`;
}

function buildSelectedBatch({ siblingCandidates, inferenceSelected, batchLimit: limit }) {
  const combined = [
    ...siblingCandidates.map((item) => ({
      row: item.row,
      selectionMethod: 'sibling_inheritance',
      selectedIndexCodes: item.selectedIndexCodes,
      selectedCode: item.selectedCode,
      confidenceScore: Number.POSITIVE_INFINITY,
      metadata: {
        siblingCaseTokens: item.siblingCaseTokens,
        siblingCount: item.siblingCount,
        siblingSample: item.siblingSample
      }
    })),
    ...inferenceSelected.map((item) => ({
      row: item.row,
      selectionMethod: 'tail_inference',
      selectedIndexCodes: item.evaluation.selectedIndexCodes,
      selectedCode: item.evaluation.selectedCode,
      confidenceScore: Number(item.evaluation.topCandidate?.score || 0),
      metadata: {
        reason: item.evaluation.reason,
        margin: item.evaluation.margin,
        topCandidate: item.evaluation.topCandidate,
        secondCandidate: item.evaluation.secondCandidate,
        selectedSources: item.evaluation.selectedSources || []
      }
    }))
  ];

  combined.sort((a, b) => {
    const methodRank = (value) => (value.selectionMethod === 'sibling_inheritance' ? 0 : 1);
    const methodDelta = methodRank(a) - methodRank(b);
    if (methodDelta !== 0) return methodDelta;
    const scoreDelta = Number(b.confidenceScore || 0) - Number(a.confidenceScore || 0);
    if (scoreDelta !== 0) return scoreDelta;
    const dateA = String(a.row?.decisionDate || '');
    const dateB = String(b.row?.decisionDate || '');
    return dateB.localeCompare(dateA) || String(a.row?.citation || '').localeCompare(String(b.row?.citation || ''));
  });

  return combined.slice(0, limit);
}

export async function main() {
  await ensureDir(outputDir);

  const beforeSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const auditReport = await buildMissingIndexAuditReport({ dbPath, busyTimeoutMs, limit: 0 });
  const siblingCandidates = await buildSiblingInheritanceCandidates(auditReport.rows || [], { dbPath, busyTimeoutMs });
  const siblingDocIds = new Set(siblingCandidates.map((item) => item.row.documentId));
  const inferenceResult = selectTailInferenceCandidates(
    (auditReport.rows || []).filter((row) => !siblingDocIds.has(row.documentId))
  );
  const selectedBatch = buildSelectedBatch({
    siblingCandidates,
    inferenceSelected: inferenceResult.selected,
    batchLimit
  });

  if (!selectedBatch.length) {
    const report = {
      generatedAt: new Date().toISOString(),
      apiBase: apiBaseUrl,
      dbPath,
      stageStatus: 'noop',
      summary: {
        batchLimit,
        auditMissingIndexOnlyCount: Number(auditReport.summary?.missingIndexOnlyCount || 0),
        candidateDocCount: Number(auditReport.summary?.candidateDocCount || 0),
        siblingCandidateCount: siblingCandidates.length,
        tailInferenceEligibleCount: inferenceResult.selected.length,
        selectedDocumentCount: 0,
        updatedDocumentCount: 0,
        qcPassedCount: 0,
        qcPassedNotSearchableBefore: beforeSnapshot.qcPassedNotSearchableCount,
        qcPassedNotSearchableAfter: beforeSnapshot.qcPassedNotSearchableCount
      },
      updatedDocuments: [],
      skippedDocuments: inferenceResult.skipped.slice(0, 100).map((item) => ({
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
    console.log(`Missing-index tail recovery JSON report written to ${path.resolve(outputDir, reportName)}`);
    console.log(`Missing-index tail recovery Markdown report written to ${path.resolve(outputDir, markdownName)}`);
    return;
  }

  const updatedDocuments = [];
  const failures = [];

  for (const item of selectedBatch) {
    const payload = {
      index_codes: item.selectedIndexCodes,
      rules_sections: item.row.rulesSections,
      ordinance_sections: item.row.ordinanceSections
    };

    try {
      const detail = await postMetadataUpdate(item.row.documentId, payload);
      updatedDocuments.push({
        documentId: item.row.documentId,
        citation: item.row.citation,
        title: item.row.title,
        selectionMethod: item.selectionMethod,
        selectedCode: item.selectedCode,
        selectedIndexCodes: item.selectedIndexCodes,
        margin: item.metadata.margin ?? null,
        siblingCount: item.metadata.siblingCount ?? null,
        qcPassed: Boolean(detail?.qcGateDiagnostics?.passed),
        resultingIndexCodes: Array.isArray(detail?.indexCodes) ? detail.indexCodes.map(String) : []
      });
    } catch (error) {
      failures.push({
        documentId: item.row.documentId,
        citation: item.row.citation,
        title: item.row.title,
        selectionMethod: item.selectionMethod,
        selectedIndexCodes: item.selectedIndexCodes,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const report = {
    generatedAt: new Date().toISOString(),
    apiBase: apiBaseUrl,
    dbPath,
    stageStatus: failures.length > 0 ? (updatedDocuments.length > 0 ? 'partial' : 'failed') : 'written',
    summary: {
      batchLimit,
      auditMissingIndexOnlyCount: Number(auditReport.summary?.missingIndexOnlyCount || 0),
      candidateDocCount: Number(auditReport.summary?.candidateDocCount || 0),
      unresolvedDocCount: Number(auditReport.summary?.unresolvedDocCount || 0),
      siblingCandidateCount: siblingCandidates.length,
      tailInferenceEligibleCount: inferenceResult.selected.length,
      selectedDocumentCount: selectedBatch.length,
      updatedDocumentCount: updatedDocuments.length,
      failuresCount: failures.length,
      qcPassedCount: updatedDocuments.filter((row) => row.qcPassed).length,
      siblingUpdatedCount: updatedDocuments.filter((row) => row.selectionMethod === 'sibling_inheritance').length,
      tailInferenceUpdatedCount: updatedDocuments.filter((row) => row.selectionMethod === 'tail_inference').length,
      qcPassedNotSearchableBefore: beforeSnapshot.qcPassedNotSearchableCount,
      qcPassedNotSearchableAfter: afterSnapshot.qcPassedNotSearchableCount,
      searchableBefore: beforeSnapshot.searchableDecisionDocs,
      searchableAfter: afterSnapshot.searchableDecisionDocs
    },
    updatedDocuments,
    skippedDocuments: inferenceResult.skipped.slice(0, 250).map((item) => ({
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
  console.log(`Missing-index tail recovery JSON report written to ${path.resolve(outputDir, reportName)}`);
  console.log(`Missing-index tail recovery Markdown report written to ${path.resolve(outputDir, markdownName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
