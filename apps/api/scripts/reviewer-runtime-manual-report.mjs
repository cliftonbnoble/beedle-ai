import fs from 'node:fs/promises';
import path from 'node:path';
import { buildReviewerRuntimeManualReport, buildRuntimeManualCountAlignment, formatReviewerRuntimeManualReportMarkdown } from './reviewer-runtime-manual-report-utils.mjs';

const apiBase = process.env.API_BASE_URL || 'http://127.0.0.1:8787';
const reportName = process.env.REVIEWER_RUNTIME_MANUAL_REPORT_NAME || 'reviewer-runtime-manual-report.json';
const markdownName = process.env.REVIEWER_RUNTIME_MANUAL_MARKDOWN_NAME || 'reviewer-runtime-manual-report.md';
const limit = Number.parseInt(process.env.REVIEWER_RUNTIME_MANUAL_LIMIT || '1200', 10);
const topLimit = Number.parseInt(process.env.REVIEWER_RUNTIME_MANUAL_TOP_LIMIT || '25', 10);
const includeFixtures = process.env.REVIEWER_RUNTIME_MANUAL_INCLUDE_FIXTURES === '1';

async function fetchJson(url) {
  const res = await fetch(url);
  const raw = await res.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON from ${url} but received non-JSON payload.`);
  }
  if (!res.ok) {
    throw new Error(`Request failed (${res.status}) for ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function main() {
  const listUrl = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx&runtimeManualCandidatesOnly=1&limit=${limit}`;
  const realOnlyUrl = `${apiBase}/admin/ingestion/documents?status=all&fileType=decision_docx&runtimeManualCandidatesOnly=1&realOnly=1&limit=${limit}`;
  const payload = await fetchJson(listUrl);
  const realOnlyPayload = await fetchJson(realOnlyUrl);
  const report = buildReviewerRuntimeManualReport({ allDocuments: payload.documents || [], topLimit, includeFixtures });
  report.apiBase = apiBase;
  report.sourceQuery = listUrl;
  report.sourceSummary = payload.summary || {};
  report.sourceRealOnlyQuery = realOnlyUrl;
  report.sourceRealOnlySummary = realOnlyPayload.summary || {};
  report.countAlignment = buildRuntimeManualCountAlignment({
    mixedSummary: payload.summary || {},
    realSummary: realOnlyPayload.summary || {},
    reportSummary: report.summary
  });

  const reportsDir = path.resolve(process.cwd(), 'reports');
  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(mdPath, formatReviewerRuntimeManualReportMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Runtime manual candidate JSON report written to ${jsonPath}`);
  console.log(`Runtime manual candidate Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
