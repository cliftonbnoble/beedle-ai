import fs from 'node:fs/promises';
import path from 'node:path';
import { buildReviewerFixturePruningReport, formatReviewerFixturePruningMarkdown } from './reviewer-fixture-pruning-report-utils.mjs';

const apiBase = process.env.API_BASE_URL || 'http://127.0.0.1:8787';
const reportName = process.env.REVIEWER_FIXTURE_PRUNING_REPORT_NAME || 'reviewer-fixture-pruning-report.json';
const markdownName = process.env.REVIEWER_FIXTURE_PRUNING_MARKDOWN_NAME || 'reviewer-fixture-pruning-report.md';
const limit = Number.parseInt(process.env.REVIEWER_FIXTURE_PRUNING_LIMIT || '1200', 10);
const topLimit = Number.parseInt(process.env.REVIEWER_FIXTURE_PRUNING_TOP_LIMIT || '40', 10);
const minFixtureAgeDays = Number.parseInt(process.env.REVIEWER_FIXTURE_PRUNING_MIN_AGE_DAYS || '2', 10);
const scope = (process.env.REVIEWER_FIXTURE_PRUNING_SCOPE || 'runtime_manual').toLowerCase();

async function fetchJson(url) {
  const response = await fetch(url);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url} but received non-JSON payload.`);
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) for ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

function listUrl() {
  if (scope === 'all') {
    return `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx&limit=${limit}`;
  }
  return `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx&runtimeManualCandidatesOnly=1&limit=${limit}`;
}

async function main() {
  const query = listUrl();
  const payload = await fetchJson(query);
  const report = buildReviewerFixturePruningReport({
    rows: payload.documents || [],
    nowIso: new Date().toISOString(),
    minFixtureAgeDays,
    topLimit
  });
  report.apiBase = apiBase;
  report.scope = scope;
  report.sourceQuery = query;
  report.sourceSummary = payload.summary || {};

  const reportsDir = path.resolve(process.cwd(), 'reports');
  await fs.mkdir(reportsDir, { recursive: true });

  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(mdPath, formatReviewerFixturePruningMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Fixture-pruning JSON report written to ${jsonPath}`);
  console.log(`Fixture-pruning Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
