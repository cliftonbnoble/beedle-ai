import path from 'node:path';
import {
  buildRealDecisionPredicate,
  defaultDbPath,
  ensureDir,
  queryCorpusSnapshot,
  runSqlFirst,
  runSqlJson,
  writeJson,
  writeText
} from './lib/overnight-corpus-lift-utils.mjs';

const apiBaseUrl = process.env.API_BASE_URL || 'http://127.0.0.1:8787';
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.SEARCHABLE_CHUNK_BACKFILL_BUSY_TIMEOUT_MS || '5000', 10);
const batchLimit = Math.max(1, Number.parseInt(process.env.SEARCHABLE_CHUNK_BACKFILL_LIMIT || '50', 10));
const requestTimeoutMs = Math.max(1000, Number.parseInt(process.env.SEARCHABLE_CHUNK_BACKFILL_TIMEOUT_MS || '60000', 10));
const outputDir = path.resolve(
  process.cwd(),
  process.env.SEARCHABLE_CHUNK_BACKFILL_OUTPUT_DIR || 'reports/searchable-chunk-backfill'
);
const reportName = process.env.SEARCHABLE_CHUNK_BACKFILL_REPORT_NAME || 'searchable-chunk-backfill-report.json';
const markdownName = process.env.SEARCHABLE_CHUNK_BACKFILL_MARKDOWN_NAME || 'searchable-chunk-backfill-report.md';

async function queryChunkGapSummary() {
  return runSqlFirst({
    dbPath,
    busyTimeoutMs,
    sql: `
      SELECT
        (SELECT COUNT(*)
         FROM documents d
         WHERE ${buildRealDecisionPredicate('d')}
           AND d.searchable_at IS NOT NULL
           AND EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id)
        ) AS searchableWithChunkRows,
        (SELECT COUNT(*)
         FROM documents d
         WHERE ${buildRealDecisionPredicate('d')}
           AND d.searchable_at IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id)
        ) AS searchableWithoutChunkRows
    `
  });
}

async function selectCandidates() {
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
        d.searchable_at AS searchableAt,
        d.extraction_confidence AS extractionConfidence,
        COALESCE(CAST(json_extract(d.metadata_json, '$.plainTextLength') AS INTEGER), 0) AS plainTextLength,
        d.source_r2_key AS sourceR2Key
      FROM documents d
      WHERE ${buildRealDecisionPredicate('d')}
        AND d.searchable_at IS NOT NULL
        AND COALESCE(d.source_r2_key, '') <> ''
        AND NOT EXISTS (SELECT 1 FROM document_chunks c WHERE c.document_id = d.id)
      ORDER BY
        plainTextLength DESC,
        COALESCE(d.searchable_at, '') ASC,
        COALESCE(d.updated_at, '') DESC,
        d.citation ASC
      LIMIT ${batchLimit}
    `
  });
}

async function postReprocess(documentId) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(`Chunk backfill timed out after ${requestTimeoutMs}ms for ${documentId}`), requestTimeoutMs);

  try {
    const response = await fetch(`${apiBaseUrl}/admin/ingestion/documents/${documentId}/reprocess`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
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
    '# Searchable Chunk Backfill Batch',
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
    lines.push(`- ${key}: ${value}`);
  }

  lines.push('');
  lines.push('## Rebuilt Documents');
  lines.push('');
  for (const row of report.rebuiltDocuments || []) {
    lines.push(
      `- \`${row.documentId}\` | \`${row.citation}\` | chunksBefore=\`${row.chunkCountBefore}\` | chunksAfter=\`${row.chunkCountAfter}\` | plainTextLength=\`${row.plainTextLength}\``
    );
  }
  if (!(report.rebuiltDocuments || []).length) lines.push('- none');

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

export async function main() {
  await ensureDir(outputDir);

  const beforeSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const beforeChunkGap = await queryChunkGapSummary();
  const candidates = await selectCandidates();

  if (!candidates.length) {
    const report = {
      generatedAt: new Date().toISOString(),
      apiBase: apiBaseUrl,
      dbPath,
      stageStatus: 'noop',
      summary: {
        selectedDocumentCount: 0,
        attemptedDocumentCount: 0,
        rebuiltDocumentCount: 0,
        searchableWithChunkRowsBefore: Number(beforeChunkGap?.searchableWithChunkRows || 0),
        searchableWithChunkRowsAfter: Number(beforeChunkGap?.searchableWithChunkRows || 0),
        searchableWithoutChunkRowsBefore: Number(beforeChunkGap?.searchableWithoutChunkRows || 0),
        searchableWithoutChunkRowsAfter: Number(beforeChunkGap?.searchableWithoutChunkRows || 0)
      },
      selectedDocuments: [],
      rebuiltDocuments: [],
      failures: [],
      beforeSnapshot,
      afterSnapshot: beforeSnapshot,
      beforeChunkGap,
      afterChunkGap: beforeChunkGap
    };

    await Promise.all([
      writeJson(path.resolve(outputDir, reportName), report),
      writeText(path.resolve(outputDir, markdownName), formatMarkdown(report))
    ]);

    console.log(JSON.stringify(report.summary, null, 2));
    console.log(`Searchable chunk backfill JSON report written to ${path.resolve(outputDir, reportName)}`);
    console.log(`Searchable chunk backfill Markdown report written to ${path.resolve(outputDir, markdownName)}`);
    return;
  }

  const rebuiltDocuments = [];
  const failures = [];

  for (const candidate of candidates) {
    try {
      const detail = await postReprocess(candidate.documentId);
      const artifacts = detail?.reprocessArtifacts || {};
      rebuiltDocuments.push({
        documentId: candidate.documentId,
        citation: candidate.citation,
        title: candidate.title,
        plainTextLength: Number(candidate.plainTextLength || 0),
        chunkCountBefore: Number(artifacts.chunkCountBefore || 0),
        chunkCountAfter: Number(artifacts.chunkCountAfter || 0),
        sectionCountAfter: Number(artifacts.sectionCountAfter || 0),
        paragraphCountAfter: Number(artifacts.paragraphCountAfter || 0),
        rebuilt: Boolean(artifacts.rebuilt)
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
  const afterChunkGap = await queryChunkGapSummary();
  const report = {
    generatedAt: new Date().toISOString(),
    apiBase: apiBaseUrl,
    dbPath,
    stageStatus: failures.length > 0 ? (rebuiltDocuments.length > 0 ? 'partial' : 'failed') : 'written',
    summary: {
      selectedDocumentCount: candidates.length,
      attemptedDocumentCount: candidates.length,
      rebuiltDocumentCount: rebuiltDocuments.filter((row) => row.rebuilt && row.chunkCountAfter > row.chunkCountBefore).length,
      failuresCount: failures.length,
      searchableWithChunkRowsBefore: Number(beforeChunkGap?.searchableWithChunkRows || 0),
      searchableWithChunkRowsAfter: Number(afterChunkGap?.searchableWithChunkRows || 0),
      searchableWithoutChunkRowsBefore: Number(beforeChunkGap?.searchableWithoutChunkRows || 0),
      searchableWithoutChunkRowsAfter: Number(afterChunkGap?.searchableWithoutChunkRows || 0),
      searchableBefore: beforeSnapshot.searchableDecisionDocs,
      searchableAfter: afterSnapshot.searchableDecisionDocs
    },
    selectedDocuments: candidates,
    rebuiltDocuments,
    failures,
    beforeSnapshot,
    afterSnapshot,
    beforeChunkGap,
    afterChunkGap
  };

  await Promise.all([
    writeJson(path.resolve(outputDir, reportName), report),
    writeText(path.resolve(outputDir, markdownName), formatMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Searchable chunk backfill JSON report written to ${path.resolve(outputDir, reportName)}`);
  console.log(`Searchable chunk backfill Markdown report written to ${path.resolve(outputDir, markdownName)}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
