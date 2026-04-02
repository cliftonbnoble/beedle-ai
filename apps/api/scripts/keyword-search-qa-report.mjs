import fs from 'node:fs/promises';
import path from 'node:path';

const apiBase = (process.env.API_BASE_URL || 'http://127.0.0.1:8787').replace(/\/$/, '');
const reportsDir = path.resolve(process.cwd(), 'reports');
const jsonName = process.env.KEYWORD_SEARCH_QA_JSON_NAME || 'keyword-search-qa-report.json';
const markdownName = process.env.KEYWORD_SEARCH_QA_MARKDOWN_NAME || 'keyword-search-qa-report.md';
const csvName = process.env.KEYWORD_SEARCH_QA_CSV_NAME || 'keyword-search-qa-report.csv';
const requestTimeoutMs = Number(process.env.KEYWORD_SEARCH_QA_TIMEOUT_MS || '90000');
const retries = Math.max(1, Number(process.env.KEYWORD_SEARCH_QA_RETRIES || '3'));
const pauseBetweenQueriesMs = Number(process.env.KEYWORD_SEARCH_QA_PAUSE_MS || '1000');
const resultLimit = Math.max(1, Number(process.env.KEYWORD_SEARCH_QA_LIMIT || '10'));
const corpusMode = process.env.KEYWORD_SEARCH_QA_CORPUS_MODE || 'trusted_plus_provisional';
const tasksFile = process.env.KEYWORD_SEARCH_QA_TASKS_FILE
  ? path.resolve(process.cwd(), process.env.KEYWORD_SEARCH_QA_TASKS_FILE)
  : path.resolve(process.cwd(), 'scripts/keyword-search-qa-tasks.sample.json');
const taskIdsFilter = new Set(
  String(process.env.KEYWORD_SEARCH_QA_TASK_IDS || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)
);

const STOPWORDS = new Set([
  'a', 'an', 'the', 'of', 'and', 'or', 'to', 'in', 'on', 'at', 'for', 'from', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
  'that', 'this', 'those', 'these', 'it', 'its', 'as', 'but', 'if', 'then', 'than', 'because', 'into', 'after', 'before', 'over', 'under', 'did',
  'not', 'no', 'up', 'out', 'off', 'per', 'via'
]);

function normalize(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9.\s_-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function csvEscape(value) {
  const text = String(value ?? '');
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function describeFilters(filters) {
  const parts = [];
  if (Array.isArray(filters?.judgeNames) && filters.judgeNames.length) parts.push(`judges=${filters.judgeNames.join(' | ')}`);
  if (Array.isArray(filters?.indexCodes) && filters.indexCodes.length) parts.push(`index=${filters.indexCodes.join(' | ')}`);
  if (filters?.rulesSection) parts.push(`rules=${filters.rulesSection}`);
  if (filters?.ordinanceSection) parts.push(`ordinance=${filters.ordinanceSection}`);
  if (filters?.fromDate || filters?.toDate) parts.push(`dates=${filters.fromDate || 'any'}..${filters.toDate || 'any'}`);
  return parts.join(' ; ') || '<none>';
}

function extractTerms(value) {
  return Array.from(
    new Set(
      normalize(value)
        .split(' ')
        .filter((token) => token && token.length >= 2 && !STOPWORDS.has(token))
    )
  );
}

function buildHaystack(result) {
  return normalize([
    result?.title,
    result?.citation,
    result?.snippet,
    result?.sectionLabel,
    result?.sectionHeading,
    result?.chunkType,
    Array.isArray(result?.retrievalReason) ? result.retrievalReason.join(' ') : ''
  ].join(' '));
}

function evaluateResultMatch(result, queryTerms, expectedSignals) {
  const haystack = buildHaystack(result);
  const matchedTerms = queryTerms.filter((term) => haystack.includes(term));
  const matchedSignals = expectedSignals.filter((term) => haystack.includes(term));
  const coverage = queryTerms.length ? Number((matchedTerms.length / queryTerms.length).toFixed(4)) : 0;
  const exactPhraseHit = queryTerms.length >= 2 && haystack.includes(normalize(queryTerms.join(' ')));
  return {
    matchedTerms,
    matchedSignals,
    coverage,
    exactPhraseHit,
    fullCoverage: queryTerms.length > 0 && matchedTerms.length === queryTerms.length,
    strongCoverage: coverage >= 0.67 || exactPhraseHit
  };
}

function qualityBand(metrics) {
  if (metrics.status === 'aborted') return 'aborted';
  if (metrics.status === 'error') return 'error';
  if (!metrics.returnedAny) return 'no_results';
  if (metrics.top1Coverage >= 0.67 || metrics.top5FullCoverageCount >= 1 || metrics.top5StrongCoverageCount >= 3) return 'strong';
  if (metrics.top10StrongCoverageCount >= 2 || metrics.top5AverageCoverage >= 0.45) return 'mixed';
  return 'weak';
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function loadTasks() {
  const raw = await fs.readFile(tasksFile, 'utf8');
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) throw new Error(`Expected an array of tasks in ${tasksFile}`);
  if (!taskIdsFilter.size) return parsed;
  return parsed.filter((task) => taskIdsFilter.has(String(task?.id || '').trim()));
}

async function fetchJson(url, payload, label) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      const startedAt = Date.now();
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          accept: 'application/json'
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      });
      const text = await response.text();
      if (!response.ok) {
        throw new Error(`Request failed (${response.status}) ${url}: ${text.slice(0, 500)}`);
      }
      return {
        elapsedMs: Date.now() - startedAt,
        body: JSON.parse(text || '{}')
      };
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        console.warn(`[keyword-search-qa] ${label} attempt ${attempt}/${retries} failed: ${error instanceof Error ? error.message : String(error)}`);
        await sleep(400 * attempt);
      }
    } finally {
      clearTimeout(timeout);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError || `Request failed for ${label}`));
}

function toMarkdown(report) {
  const lines = [];
  lines.push('# Keyword Search QA Report');
  lines.push('');
  lines.push(`- queryCount: ${report.queryCount}`);
  lines.push(`- returnedQueryCount: ${report.returnedQueryCount}`);
  lines.push(`- zeroResultQueryCount: ${report.zeroResultQueryCount}`);
  lines.push(`- abortedQueryCount: ${report.abortedQueryCount}`);
  lines.push(`- erroredQueryCount: ${report.erroredQueryCount}`);
  lines.push(`- strongQualityCount: ${report.strongQualityCount}`);
  lines.push(`- mixedQualityCount: ${report.mixedQualityCount}`);
  lines.push(`- weakQualityCount: ${report.weakQualityCount}`);
  lines.push(`- noResultCount: ${report.noResultCount}`);
  lines.push(`- top1StrongCoverageCount: ${report.top1StrongCoverageCount}`);
  lines.push(`- top5StrongCoverageCount: ${report.top5StrongCoverageCount}`);
  lines.push('');
  lines.push('## Tasks');
  lines.push('');
  for (const row of report.rows) {
    lines.push(`- ${row.id} | category=${row.category} | quality=${row.qualityBand} | top1Coverage=${row.top1Coverage} | top5Avg=${row.top5AverageCoverage} | top10Strong=${row.top10StrongCoverageCount} | top1=${row.top1Citation || '<none>'} | filters=${row.filtersSummary}${row.error ? ` | error=${row.error}` : ''}`);
  }
  return `${lines.join('\n')}\n`;
}

function toCsv(report) {
  const rows = [
    ['id', 'label', 'category', 'query', 'filters', 'status', 'qualityBand', 'returnedAny', 'totalResults', 'uniqueDecisionCount', 'top1Citation', 'top1Coverage', 'top5AverageCoverage', 'top5FullCoverageCount', 'top5StrongCoverageCount', 'top10StrongCoverageCount', 'top10ExpectedSignalHitCount', 'requestElapsedMs', 'error'],
    ...report.rows.map((row) => [
      row.id,
      row.label,
      row.category,
      row.query,
      row.filtersSummary,
      row.status,
      row.qualityBand,
      row.returnedAny ? '1' : '0',
      row.totalResults,
      row.uniqueDecisionCount,
      row.top1Citation || '',
      row.top1Coverage,
      row.top5AverageCoverage,
      row.top5FullCoverageCount,
      row.top5StrongCoverageCount,
      row.top10StrongCoverageCount,
      row.top10ExpectedSignalHitCount,
      row.requestElapsedMs,
      row.error || ''
    ])
  ];
  return `${rows.map((row) => row.map(csvEscape).join(',')).join('\n')}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const tasks = await loadTasks();
  const rows = [];

  for (const task of tasks) {
    if (rows.length > 0 && pauseBetweenQueriesMs > 0) await sleep(pauseBetweenQueriesMs);
    const queryTerms = extractTerms(task.query);
    const expectedSignals = extractTerms((task.expectedSignals || []).join(' '));

    try {
      const payload = {
        query: task.query,
        limit: resultLimit,
        corpusMode,
        filters: {
          approvedOnly: false,
          ...(task.filters || {})
        }
      };
      if (task.queryType) payload.queryType = task.queryType;

      const { body: response, elapsedMs } = await fetchJson(`${apiBase}/search`, payload, task.id);
      const results = Array.isArray(response?.results) ? response.results : [];
      const top5 = results.slice(0, 5);
      const top10 = results.slice(0, 10);
      const top1 = results[0] || null;
      const top1Match = top1 ? evaluateResultMatch(top1, queryTerms, expectedSignals) : null;
      const top5Matches = top5.map((row) => evaluateResultMatch(row, queryTerms, expectedSignals));
      const top10Matches = top10.map((row) => evaluateResultMatch(row, queryTerms, expectedSignals));

      const metrics = {
        top1Coverage: Number((top1Match?.coverage || 0).toFixed(4)),
        top5AverageCoverage: top5Matches.length
          ? Number((top5Matches.reduce((sum, item) => sum + item.coverage, 0) / top5Matches.length).toFixed(4))
          : 0,
        top5FullCoverageCount: top5Matches.filter((item) => item.fullCoverage).length,
        top5StrongCoverageCount: top5Matches.filter((item) => item.strongCoverage).length,
        top10StrongCoverageCount: top10Matches.filter((item) => item.strongCoverage).length,
        top10ExpectedSignalHitCount: top10Matches.reduce((sum, item) => sum + (item.matchedSignals.length > 0 ? 1 : 0), 0),
        returnedAny: results.length > 0,
        status: results.length > 0 ? 'returned' : 'zero_results'
      };

      rows.push({
        id: task.id,
        label: task.label || task.id,
        category: task.category || 'uncategorized',
        query: task.query,
        filtersSummary: describeFilters(task.filters || {}),
        expectation: task.expectation || '',
        status: metrics.status,
        returnedAny: results.length > 0,
        totalResults: Number(response?.total || results.length || 0),
        uniqueDecisionCount: new Set(results.map((item) => item.documentId || item.citation || item.id).filter(Boolean)).size,
        top1Citation: top1?.citation || '',
        top1SectionLabel: top1?.sectionLabel || '',
        top1Coverage: metrics.top1Coverage,
        top5AverageCoverage: metrics.top5AverageCoverage,
        top5FullCoverageCount: metrics.top5FullCoverageCount,
        top5StrongCoverageCount: metrics.top5StrongCoverageCount,
        top10StrongCoverageCount: metrics.top10StrongCoverageCount,
        top10ExpectedSignalHitCount: metrics.top10ExpectedSignalHitCount,
        top1LexicalScore: Number(top1?.lexicalScore || 0),
        top1VectorScore: Number(top1?.vectorScore || 0),
        top1Score: Number(top1?.score || 0),
        requestElapsedMs: elapsedMs,
        stageTimingsMs: {},
        qualityBand: qualityBand(metrics),
        topResults: top10.map((result, index) => ({
          rank: index + 1,
          citation: result?.citation || '',
          title: result?.title || '',
          sectionLabel: result?.sectionLabel || '',
          score: Number(result?.score || 0),
          lexicalScore: Number(result?.lexicalScore || 0),
          vectorScore: Number(result?.vectorScore || 0)
        })),
        error: ''
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      rows.push({
        id: task.id,
        label: task.label || task.id,
        category: task.category || 'uncategorized',
        query: task.query,
        filtersSummary: describeFilters(task.filters || {}),
        expectation: task.expectation || '',
        status: /aborted/i.test(message) ? 'aborted' : 'error',
        returnedAny: false,
        totalResults: 0,
        uniqueDecisionCount: 0,
        top1Citation: '',
        top1SectionLabel: '',
        top1Coverage: 0,
        top5AverageCoverage: 0,
        top5FullCoverageCount: 0,
        top5StrongCoverageCount: 0,
        top10StrongCoverageCount: 0,
        top10ExpectedSignalHitCount: 0,
        top1LexicalScore: 0,
        top1VectorScore: 0,
        top1Score: 0,
        requestElapsedMs: 0,
        stageTimingsMs: {},
        qualityBand: /aborted/i.test(message) ? 'aborted' : 'error',
        topResults: [],
        error: message
      });
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    corpusMode,
    queryCount: rows.length,
    returnedQueryCount: rows.filter((row) => row.status === 'returned').length,
    zeroResultQueryCount: rows.filter((row) => row.status === 'zero_results').length,
    abortedQueryCount: rows.filter((row) => row.status === 'aborted').length,
    erroredQueryCount: rows.filter((row) => row.status === 'error').length,
    strongQualityCount: rows.filter((row) => row.qualityBand === 'strong').length,
    mixedQualityCount: rows.filter((row) => row.qualityBand === 'mixed').length,
    weakQualityCount: rows.filter((row) => row.qualityBand === 'weak').length,
    noResultCount: rows.filter((row) => row.qualityBand === 'no_results').length,
    top1StrongCoverageCount: rows.filter((row) => row.top1Coverage >= 0.67).length,
    top5StrongCoverageCount: rows.filter((row) => row.top5StrongCoverageCount >= 3).length,
    rows
  };

  await Promise.all([
    fs.writeFile(path.join(reportsDir, jsonName), JSON.stringify(report, null, 2)),
    fs.writeFile(path.join(reportsDir, markdownName), toMarkdown(report)),
    fs.writeFile(path.join(reportsDir, csvName), toCsv(report))
  ]);

  console.log(JSON.stringify({
    queryCount: report.queryCount,
    returnedQueryCount: report.returnedQueryCount,
    zeroResultQueryCount: report.zeroResultQueryCount,
    abortedQueryCount: report.abortedQueryCount,
    erroredQueryCount: report.erroredQueryCount,
    strongQualityCount: report.strongQualityCount,
    mixedQualityCount: report.mixedQualityCount,
    weakQualityCount: report.weakQualityCount,
    noResultCount: report.noResultCount
  }, null, 2));
  console.log(`Keyword search QA JSON report written to ${path.join(reportsDir, jsonName)}`);
  console.log(`Keyword search QA Markdown report written to ${path.join(reportsDir, markdownName)}`);
  console.log(`Keyword search QA CSV report written to ${path.join(reportsDir, csvName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
