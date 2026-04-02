import { execFile } from 'node:child_process';
import path from 'node:path';
import { promisify } from 'node:util';
import {
  defaultDbPath,
  ensureDir,
  queryCorpusSnapshot,
  readJson,
  writeJson,
  writeText
} from './lib/overnight-corpus-lift-utils.mjs';

const execFileAsync = promisify(execFile);
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.REMAINING_TAIL_CURATION_BUSY_TIMEOUT_MS || '5000', 10);
const dryRun = (process.env.REMAINING_TAIL_CURATION_DRY_RUN || '1') !== '0';
const reportPath = path.resolve(
  process.cwd(),
  process.env.REMAINING_TAIL_CURATION_SOURCE_REPORT || 'reports/remaining-searchability-tail-review-report.json'
);
const outputDir = path.resolve(
  process.cwd(),
  process.env.REMAINING_TAIL_CURATION_OUTPUT_DIR || 'reports/remaining-tail-curation'
);
const reportName = process.env.REMAINING_TAIL_CURATION_REPORT_NAME || 'remaining-tail-curation-report.json';
const markdownName = process.env.REMAINING_TAIL_CURATION_MARKDOWN_NAME || 'remaining-tail-curation-report.md';
const rejectReason = process.env.REMAINING_TAIL_CURATION_REJECT_REASON || 'unknown_reference_like_tail_curation';
const apiBaseUrl = process.env.API_BASE_URL || 'http://127.0.0.1:8787';

function sqlQuote(value) {
  return `'${String(value ?? '').replace(/'/g, "''")}'`;
}

async function runSqlChange(sql) {
  const statement = `${sql}; SELECT changes() AS changes;`;
  const { stdout } = await execFileAsync('sqlite3', ['-json', '-cmd', `.timeout ${busyTimeoutMs}`, dbPath, statement], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  const parsed = JSON.parse(stdout || '[]');
  const lastRow = Array.isArray(parsed) ? parsed[parsed.length - 1] || {} : {};
  return Number(lastRow.changes || 0);
}

function toMarkdown(report) {
  const lines = [];
  lines.push('# Remaining Tail Curation Batch');
  lines.push('');
  lines.push(`- generatedAt: ${report.generatedAt}`);
  lines.push(`- dryRun: ${report.dryRun}`);
  lines.push(`- confirmTargetCount: ${report.summary.confirmTargetCount}`);
  lines.push(`- rejectTargetCount: ${report.summary.rejectTargetCount}`);
  lines.push(`- confirmedRowsChanged: ${report.summary.confirmedRowsChanged}`);
  lines.push(`- rejectedRowsChanged: ${report.summary.rejectedRowsChanged}`);
  lines.push(`- searchableRowsChanged: ${report.summary.searchableRowsChanged}`);
  lines.push(`- searchableBefore: ${report.summary.searchableBefore}`);
  lines.push(`- searchableAfter: ${report.summary.searchableAfter}`);
  lines.push(`- qcFailedNotSearchableBefore: ${report.summary.qcFailedNotSearchableBefore}`);
  lines.push(`- qcFailedNotSearchableAfter: ${report.summary.qcFailedNotSearchableAfter}`);
  lines.push('');
  lines.push('## Confirm Targets');
  lines.push('');
  for (const row of report.confirmTargets) {
    lines.push(`- ${row.citation} | ${row.title}`);
  }
  if (!report.confirmTargets.length) lines.push('- none');
  lines.push('');
  lines.push('## Reject Targets');
  lines.push('');
  for (const row of report.rejectTargets) {
    lines.push(`- ${row.citation} | ${row.title}`);
  }
  if (!report.rejectTargets.length) lines.push('- none');
  lines.push('');
  if (report.searchabilityEnableSummary) {
    lines.push('## Searchability Enable');
    lines.push('');
    for (const [key, value] of Object.entries(report.searchabilityEnableSummary)) {
      lines.push(`- ${key}: ${Array.isArray(value) ? value.join(', ') : value}`);
    }
    lines.push('');
  }
  return `${lines.join('\n')}\n`;
}

async function main() {
  await ensureDir(outputDir);
  const source = await readJson(reportPath);
  const rows = Array.isArray(source?.rows) ? source.rows : [];
  const confirmTargets = rows.filter((row) => row.recommendedAction === 'manual_confirm_and_enable');
  const rejectTargets = rows.filter((row) => row.recommendedAction === 'exclude_unknown_reference_like');
  const beforeSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  let confirmedRowsChanged = 0;
  let rejectedRowsChanged = 0;
  let searchableRowsChanged = 0;
  let searchabilityEnableSummary = null;

  if (!dryRun) {
    const now = new Date().toISOString();
    for (const row of confirmTargets) {
      confirmedRowsChanged += await runSqlChange(`
        UPDATE documents
        SET qc_required_confirmed = 1,
            qc_confirmed_at = COALESCE(qc_confirmed_at, ${sqlQuote(now)}),
            qc_passed = 1,
            updated_at = ${sqlQuote(now)}
        WHERE id = ${sqlQuote(row.documentId)}
          AND searchable_at IS NULL
          AND rejected_at IS NULL
      `);
    }

    for (const row of rejectTargets) {
      rejectedRowsChanged += await runSqlChange(`
        UPDATE documents
        SET rejected_at = COALESCE(rejected_at, ${sqlQuote(now)}),
            rejected_reason = ${sqlQuote(rejectReason)},
            searchable_at = NULL,
            updated_at = ${sqlQuote(now)}
        WHERE id = ${sqlQuote(row.documentId)}
      `);
    }

    for (const row of confirmTargets) {
      searchableRowsChanged += await runSqlChange(`
        UPDATE documents
        SET searchable_at = COALESCE(searchable_at, ${sqlQuote(now)}),
            updated_at = ${sqlQuote(now)}
        WHERE id = ${sqlQuote(row.documentId)}
          AND qc_passed = 1
          AND rejected_at IS NULL
      `);
    }

    searchabilityEnableSummary = {
      mode: 'inline_confirmed_docs_enable',
      attemptedDocumentCount: confirmTargets.length,
      searchableRowsChanged
    };
  }

  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const report = {
    generatedAt: new Date().toISOString(),
    dryRun,
    dbPath,
    sourceReport: reportPath,
    confirmTargets,
    rejectTargets,
    searchabilityEnableSummary,
    beforeSnapshot,
    afterSnapshot,
    summary: {
      confirmTargetCount: confirmTargets.length,
      rejectTargetCount: rejectTargets.length,
      confirmedRowsChanged,
      rejectedRowsChanged,
      searchableRowsChanged,
      searchableBefore: beforeSnapshot.searchableDecisionDocs,
      searchableAfter: afterSnapshot.searchableDecisionDocs,
      qcFailedNotSearchableBefore: beforeSnapshot.qcFailedNotSearchableCount,
      qcFailedNotSearchableAfter: afterSnapshot.qcFailedNotSearchableCount
    }
  };

  const jsonPath = path.resolve(outputDir, reportName);
  const markdownPath = path.resolve(outputDir, markdownName);
  await writeJson(jsonPath, report);
  await writeText(markdownPath, toMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Remaining tail curation JSON written to ${jsonPath}`);
  console.log(`Remaining tail curation Markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
