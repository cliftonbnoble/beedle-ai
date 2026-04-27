import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.CONCLUSIONS_TERM_QA_JSON_NAME || "conclusions-term-search-qa-report.json";
const markdownName = process.env.CONCLUSIONS_TERM_QA_MARKDOWN_NAME || "conclusions-term-search-qa-report.md";
const csvName = process.env.CONCLUSIONS_TERM_QA_CSV_NAME || "conclusions-term-search-qa-report.csv";
const resultLimit = Math.max(1, Number(process.env.CONCLUSIONS_TERM_QA_LIMIT || "5"));
const corpusMode = process.env.CONCLUSIONS_TERM_QA_CORPUS_MODE || "trusted_only";
const requestTimeoutMs = Number(process.env.CONCLUSIONS_TERM_QA_TIMEOUT_MS || "30000");
const sqliteTimeoutMs = Number(process.env.CONCLUSIONS_TERM_QA_SQLITE_TIMEOUT_MS || "45000");
const retries = Math.max(1, Number(process.env.CONCLUSIONS_TERM_QA_RETRIES || "2"));
const pauseBetweenQueriesMs = Number(process.env.CONCLUSIONS_TERM_QA_PAUSE_MS || "120");
const configuredDbPath = process.env.CONCLUSIONS_TERM_QA_DB_PATH || "";

const rawTerms = [
  "Elevator",
  "Refrigerator",
  "Rodents",
  "Rats",
  "Mice",
  "Ants",
  "Sink",
  "Fireplace",
  "Excessive",
  "Co-living",
  "Community",
  "Common areas",
  "Supplies",
  "Cleaning service",
  "Cleaning supplies",
  "Cable",
  "Internet",
  "Missed work",
  "Allergies",
  "Asthma",
  "Breathing",
  "Sleep",
  "Cockroaches",
  "Mold",
  "Mildew",
  "Heat",
  "Heater",
  "Stairs",
  "Porch",
  "Fire escape",
  "Windows",
  "Dog",
  "Leak",
  "Ceiling",
  "College",
  "Divorce",
  "Music",
  "Kids",
  "Children",
  "Banging",
  "Screaming",
  "Moving furniture",
  "Threatened",
  "Larson",
  "Moot",
  "Adjudicated",
  "Fleas",
  "Washing machine",
  "Homeowner's exemption",
  "Vacation home",
  "Social media",
  "Caregiver",
  "Temporary absence",
  "Remote work",
  "Assume",
  "Presume",
  "Favorable",
  "Unlawful detainer",
  "Civil court",
  "Tax return",
  "Self employed",
  "Self-employed",
  "Cancer",
  "Surgery",
  "Elderly",
  "Rebuttable presumption",
  "Deed of trust",
  "Borrower",
  "Mortgage",
  "Insurance",
  "Fastback",
  "Vehicle sightings",
  "Recovering",
  "Reasonable accommodation",
  "Service animal",
  "Oven",
  "Stove",
  "Ventilation",
  "Construction noise",
  "Dust",
  "Asbestos",
  "Garage space",
  "Coin-operated",
  "Coin operated",
  "Roof",
  "Camera",
  "Privacy",
  "Package",
  "Packages",
  "Lock box",
  "Intercom",
  "Poop",
  "Urine",
  "Chute",
  "Boarding",
  "Separate rental agreements",
  "Individual room",
  "Excessive heat"
];

const seenTerms = new Set();
const terms = rawTerms.filter((term) => {
  const key = String(term || "").trim();
  if (!key || seenTerms.has(key)) return false;
  seenTerms.add(key);
  return true;
});

function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeSqlLike(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[’']/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeForMatch(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[’']/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizedWholeWordSqlExpr(expr) {
  return `(' ' || lower(
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(coalesce(${expr}, ''), char(10), ' '), char(13), ' '), char(9), ' '), char(8217), ' '), '''', ' '), '.', ' '), ',', ' '), ';', ' '), ':', ' '), '(', ' '), ')', ' '), '-', ' ')
  ) || ' ')`;
}

function buildTermMatchClause(expr, term) {
  const normalized = normalizeSqlLike(term).replace(/'/g, "''");
  if (!normalized) return "0";
  return `instr(${normalizedWholeWordSqlExpr(expr)}, ' ${normalized} ') > 0`;
}

function buildConclusionSectionClause(expr) {
  return `(
    lower(coalesce(${expr}, '')) like '%conclusions of law%'
    or lower(coalesce(${expr}, '')) in ('conclusions_of_law', 'authority_discussion', 'analysis_reasoning')
  )`;
}

function isConclusionLikeSection(sectionLabel) {
  const raw = String(sectionLabel || "");
  const normalized = normalize(raw).replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return (
    /conclusions? of law/i.test(raw) ||
    normalized === "conclusions_of_law" ||
    normalized === "authority_discussion" ||
    normalized === "analysis_reasoning"
  );
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function execSqlite(dbPath, sql) {
  const immutablePath = `file:${dbPath}?mode=ro&immutable=1`;
  const { stdout } = await execFileAsync("sqlite3", [immutablePath, "-json", sql], {
    timeout: sqliteTimeoutMs,
    maxBuffer: 1024 * 1024 * 16
  });
  const trimmed = String(stdout || "").trim();
  return trimmed ? JSON.parse(trimmed) : [];
}

async function detectDatabasePath() {
  if (configuredDbPath) return configuredDbPath;
  const stateDir = path.resolve(process.cwd(), ".wrangler/state/v3/d1/miniflare-D1DatabaseObject");
  const entries = await fs.readdir(stateDir);
  const candidates = await Promise.all(
    entries
      .filter((entry) => entry.endsWith(".sqlite") && !entry.startsWith("backup-"))
      .map(async (entry) => {
        const fullPath = path.join(stateDir, entry);
        const stat = await fs.stat(fullPath);
        return { fullPath, size: stat.size };
      })
  );

  candidates.sort((a, b) => b.size - a.size);

  for (const candidate of candidates) {
    try {
      const rows = await execSqlite(
        candidate.fullPath,
        "select name from sqlite_master where type = 'table' and name in ('documents','document_chunks','retrieval_search_chunks') order by name;"
      );
      const names = new Set(rows.map((row) => row.name));
      if (names.has("documents") && names.has("document_chunks") && names.has("retrieval_search_chunks")) {
        return candidate.fullPath;
      }
    } catch {
      // keep scanning
    }
  }

  throw new Error("Could not find a usable local D1 sqlite database for conclusions term QA.");
}

async function fetchJson(url, payload, label) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify(payload),
        signal: controller.signal
      });
      const text = await response.text();
      if (!response.ok) {
        throw new Error(`Request failed (${response.status}) ${url}: ${text.slice(0, 400)}`);
      }
      return JSON.parse(text || "{}");
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        console.warn(`[conclusions-term-qa] ${label} attempt ${attempt}/${retries} failed: ${error instanceof Error ? error.message : String(error)}`);
        await sleep(400 * attempt);
      }
    } finally {
      clearTimeout(timeout);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError || `Request failed for ${label}`));
}

function passageContainsTerm(sectionLabel, snippet, term) {
  return isConclusionLikeSection(sectionLabel) && normalizeForMatch(snippet).includes(normalizeForMatch(term));
}

function resultHasConclusionLike(result) {
  return [
    result?.sectionLabel,
    result?.matchedPassage?.sectionLabel,
    result?.primaryAuthorityPassage?.sectionLabel
  ].some((value) => isConclusionLikeSection(value));
}

function resultHasConclusionTermHit(result, term) {
  return [
    { sectionLabel: result?.sectionLabel, snippet: result?.snippet },
    { sectionLabel: result?.matchedPassage?.sectionLabel, snippet: result?.matchedPassage?.snippet },
    { sectionLabel: result?.primaryAuthorityPassage?.sectionLabel, snippet: result?.primaryAuthorityPassage?.snippet }
  ].some((passage) => passageContainsTerm(passage.sectionLabel, passage.snippet, term));
}

function resultMentionsTerm(result, term) {
  const text = normalizeForMatch(
    [
      result?.title,
      result?.citation,
      result?.sectionLabel,
      result?.snippet,
      result?.matchedPassage?.sectionLabel,
      result?.matchedPassage?.snippet,
      result?.primaryAuthorityPassage?.sectionLabel,
      result?.primaryAuthorityPassage?.snippet,
      result?.supportingFactPassage?.sectionLabel,
      result?.supportingFactPassage?.snippet
    ]
      .filter(Boolean)
      .join(" ")
  );
  return text.includes(normalizeForMatch(term));
}

async function fetchCorpusCounts(dbPath, term) {
  const anyMatchDoc = buildTermMatchClause("dc.chunk_text", term);
  const anyMatchRs = buildTermMatchClause("rs.chunk_text", term);
  const conclusionDoc = buildConclusionSectionClause("dc.section_label");
  const conclusionRs = buildConclusionSectionClause("rs.section_label");

  const sql = `
    select
      count(distinct case when ${anyMatchDoc} then dc.document_id end) as searchableAnyDocCount,
      sum(case when ${anyMatchDoc} then 1 else 0 end) as searchableAnyChunkCount,
      count(distinct case when ${conclusionDoc} and ${anyMatchDoc} then dc.document_id end) as searchableConclusionDocCount,
      sum(case when ${conclusionDoc} and ${anyMatchDoc} then 1 else 0 end) as searchableConclusionChunkCount,
      (
        select count(distinct rs.document_id)
        from retrieval_search_chunks rs
        join documents d2 on d2.id = rs.document_id
        where d2.file_type = 'decision_docx'
          and d2.searchable_at is not null
          and d2.rejected_at is null
          and rs.active = 1
          and ${anyMatchRs}
      ) as activeAnyDocCount,
      (
        select count(*)
        from retrieval_search_chunks rs
        join documents d2 on d2.id = rs.document_id
        where d2.file_type = 'decision_docx'
          and d2.searchable_at is not null
          and d2.rejected_at is null
          and rs.active = 1
          and ${anyMatchRs}
      ) as activeAnyChunkCount,
      (
        select count(distinct rs.document_id)
        from retrieval_search_chunks rs
        join documents d2 on d2.id = rs.document_id
        where d2.file_type = 'decision_docx'
          and d2.searchable_at is not null
          and d2.rejected_at is null
          and rs.active = 1
          and ${conclusionRs}
          and ${anyMatchRs}
      ) as activeConclusionDocCount,
      (
        select count(*)
        from retrieval_search_chunks rs
        join documents d2 on d2.id = rs.document_id
        where d2.file_type = 'decision_docx'
          and d2.searchable_at is not null
          and d2.rejected_at is null
          and rs.active = 1
          and ${conclusionRs}
          and ${anyMatchRs}
      ) as activeConclusionChunkCount
    from document_chunks dc
    join documents d on d.id = dc.document_id
    where d.file_type = 'decision_docx'
      and d.searchable_at is not null
      and d.rejected_at is null;
  `;

  const rows = await execSqlite(dbPath, sql);
  const row = rows[0] || {};
  return {
    searchableAnyDocCount: Number(row.searchableAnyDocCount || 0),
    searchableAnyChunkCount: Number(row.searchableAnyChunkCount || 0),
    searchableConclusionDocCount: Number(row.searchableConclusionDocCount || 0),
    searchableConclusionChunkCount: Number(row.searchableConclusionChunkCount || 0),
    activeAnyDocCount: Number(row.activeAnyDocCount || 0),
    activeAnyChunkCount: Number(row.activeAnyChunkCount || 0),
    activeConclusionDocCount: Number(row.activeConclusionDocCount || 0),
    activeConclusionChunkCount: Number(row.activeConclusionChunkCount || 0)
  };
}

function classifyTerm(row) {
  if (row.error) return "request_error";
  if (row.searchableAnyChunkCount === 0) return "corpus_gap";
  if (row.searchableConclusionChunkCount === 0) return "conclusion_gap";
  if (row.activeConclusionChunkCount === 0) return "activation_gap";
  if (!row.returnedAny) return "search_gap";
  if (!row.top3ConclusionLike) return "ranking_gap";
  if (!row.top3ConclusionTermHit) return "term_alignment_gap";
  if (!row.top1ConclusionLike || !row.top1ConclusionTermHit) return "good_but_not_top1";
  return "strong";
}

function buildRecommendation(classification) {
  switch (classification) {
    case "corpus_gap":
      return "No evidence found in searchable decision chunks. This likely needs corpus acquisition or different terminology coverage.";
    case "conclusion_gap":
      return "The term appears in the corpus but not in conclusion-like sections. This is a section-coverage gap, not just ranking.";
    case "activation_gap":
      return "The term exists in searchable conclusion-like chunks but not in active retrieval chunks. Topic-guided activation or more backfill should help.";
    case "search_gap":
      return "Conclusion-like active chunks exist, but live search still returned zero trusted hits. This points to query/scoping behavior.";
    case "ranking_gap":
      return "Search returned results, but none of the top three surfaced conclusion-like passages. Ranking/scoping needs help.";
    case "term_alignment_gap":
      return "Conclusion-like results are surfacing, but the returned passages are not explicitly carrying the searched term yet.";
    case "good_but_not_top1":
      return "The term is searchable in conclusion-like results, but not consistently as the top conclusion hit yet.";
    case "strong":
      return "The term is searchable, conclusion-like, and directly present in the surfaced trusted results.";
    default:
      return "The query could not be evaluated cleanly in this run.";
  }
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Conclusions Term Search QA Report");
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(`- termCount: ${report.summary.termCount}`);
  lines.push(`- returnedAnyCount: ${report.summary.returnedAnyCount}`);
  lines.push(`- top1ConclusionLikeCount: ${report.summary.top1ConclusionLikeCount}`);
  lines.push(`- top3ConclusionLikeCount: ${report.summary.top3ConclusionLikeCount}`);
  lines.push(`- top1ConclusionTermHitCount: ${report.summary.top1ConclusionTermHitCount}`);
  lines.push(`- top3ConclusionTermHitCount: ${report.summary.top3ConclusionTermHitCount}`);
  lines.push(`- strongCount: ${report.summary.strongCount}`);
  lines.push(`- goodButNotTop1Count: ${report.summary.goodButNotTop1Count}`);
  lines.push(`- rankingGapCount: ${report.summary.rankingGapCount}`);
  lines.push(`- searchGapCount: ${report.summary.searchGapCount}`);
  lines.push(`- activationGapCount: ${report.summary.activationGapCount}`);
  lines.push(`- conclusionGapCount: ${report.summary.conclusionGapCount}`);
  lines.push(`- corpusGapCount: ${report.summary.corpusGapCount}`);
  lines.push("");

  const grouped = new Map();
  for (const row of report.rows) {
    const bucket = grouped.get(row.classification) || [];
    bucket.push(row);
    grouped.set(row.classification, bucket);
  }

  for (const [classification, rows] of grouped.entries()) {
    lines.push(`## ${classification}`);
    lines.push("");
    for (const row of rows) {
      lines.push(`- ${row.term} | results=${row.resultCount} | top1=${row.top1Citation || "<none>"} / ${row.top1SectionLabel || "<none>"} | activeConclusionChunks=${row.activeConclusionChunkCount} | searchableConclusionChunks=${row.searchableConclusionChunkCount}`);
    }
    lines.push("");
  }

  lines.push("## Top Rows");
  lines.push("");
  for (const row of report.rows.slice(0, 20)) {
    lines.push(`- ${row.term} | class=${row.classification} | recommendation=${row.recommendation}`);
  }

  return `${lines.join("\n")}\n`;
}

function toCsv(report) {
  const rows = [
    [
      "term",
      "classification",
      "resultCount",
      "top1Citation",
      "top1SectionLabel",
      "top1ConclusionLike",
      "top3ConclusionLike",
      "top1ConclusionTermHit",
      "top3ConclusionTermHit",
      "searchableAnyDocCount",
      "searchableAnyChunkCount",
      "searchableConclusionDocCount",
      "searchableConclusionChunkCount",
      "activeAnyDocCount",
      "activeAnyChunkCount",
      "activeConclusionDocCount",
      "activeConclusionChunkCount",
      "recommendation",
      "error"
    ],
    ...report.rows.map((row) => [
      row.term,
      row.classification,
      String(row.resultCount),
      row.top1Citation || "",
      row.top1SectionLabel || "",
      row.top1ConclusionLike ? "1" : "0",
      row.top3ConclusionLike ? "1" : "0",
      row.top1ConclusionTermHit ? "1" : "0",
      row.top3ConclusionTermHit ? "1" : "0",
      String(row.searchableAnyDocCount),
      String(row.searchableAnyChunkCount),
      String(row.searchableConclusionDocCount),
      String(row.searchableConclusionChunkCount),
      String(row.activeAnyDocCount),
      String(row.activeAnyChunkCount),
      String(row.activeConclusionDocCount),
      String(row.activeConclusionChunkCount),
      row.recommendation,
      row.error || ""
    ])
  ];
  return rows.map((row) => row.map(csvEscape).join(",")).join("\n");
}

async function writeReports(report) {
  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  const csvPath = path.join(reportsDir, csvName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  await fs.writeFile(markdownPath, toMarkdown(report));
  await fs.writeFile(csvPath, `${toCsv(report)}\n`);
  return { jsonPath, markdownPath, csvPath };
}

async function main() {
  const dbPath = await detectDatabasePath().catch(() => "");
  const rows = [];

  for (const term of terms) {
    let counts = {
      searchableAnyDocCount: 0,
      searchableAnyChunkCount: 0,
      searchableConclusionDocCount: 0,
      searchableConclusionChunkCount: 0,
      activeAnyDocCount: 0,
      activeAnyChunkCount: 0,
      activeConclusionDocCount: 0,
      activeConclusionChunkCount: 0
    };
    let error = null;
    let resultCount = 0;
    let top1Citation = null;
    let top1SectionLabel = null;
    let top1ConclusionLike = false;
    let top3ConclusionLike = false;
    let top1ConclusionTermHit = false;
    let top3ConclusionTermHit = false;
    let topResults = [];

    try {
      if (dbPath) counts = await fetchCorpusCounts(dbPath, term);
      const response = await fetchJson(
        `${apiBase}/search`,
        {
          query: term,
          limit: resultLimit,
          snippetMaxLength: 320,
          corpusMode,
          filters: { approvedOnly: true }
        },
        term
      );
      const results = Array.isArray(response?.results) ? response.results : [];
      const top1 = results[0] || null;
      const top3 = results.slice(0, 3);
      resultCount = results.length;
      top1Citation = top1?.citation || null;
      top1SectionLabel = top1?.sectionLabel || null;
      top1ConclusionLike = Boolean(top1 && resultHasConclusionLike(top1));
      top3ConclusionLike = top3.some((result) => resultHasConclusionLike(result));
      top1ConclusionTermHit = Boolean(top1 && resultHasConclusionTermHit(top1, term));
      top3ConclusionTermHit = top3.some((result) => resultHasConclusionTermHit(result, term));
      topResults = top3.map((result, index) => ({
        rank: index + 1,
        citation: result?.citation || null,
        title: result?.title || null,
        sectionLabel: result?.sectionLabel || null,
        chunkType: result?.chunkType || null,
        score: typeof result?.score === "number" ? Number(result.score.toFixed(6)) : null,
        resultHasConclusionLike: resultHasConclusionLike(result),
        resultHasConclusionTermHit: resultHasConclusionTermHit(result, term),
        resultMentionsTerm: resultMentionsTerm(result, term)
      }));
    } catch (caught) {
      error = caught instanceof Error ? caught.message : String(caught);
    }

    const row = {
      term,
      resultCount,
      returnedAny: resultCount > 0,
      top1Citation,
      top1SectionLabel,
      top1ConclusionLike,
      top3ConclusionLike,
      top1ConclusionTermHit,
      top3ConclusionTermHit,
      topResults,
      error,
      ...counts
    };
    row.classification = classifyTerm(row);
    row.recommendation = buildRecommendation(row.classification);
    rows.push(row);

    if (pauseBetweenQueriesMs > 0) await sleep(pauseBetweenQueriesMs);
  }

  const summary = {
    termCount: rows.length,
    returnedAnyCount: rows.filter((row) => row.returnedAny).length,
    top1ConclusionLikeCount: rows.filter((row) => row.top1ConclusionLike).length,
    top3ConclusionLikeCount: rows.filter((row) => row.top3ConclusionLike).length,
    top1ConclusionTermHitCount: rows.filter((row) => row.top1ConclusionTermHit).length,
    top3ConclusionTermHitCount: rows.filter((row) => row.top3ConclusionTermHit).length,
    strongCount: rows.filter((row) => row.classification === "strong").length,
    goodButNotTop1Count: rows.filter((row) => row.classification === "good_but_not_top1").length,
    rankingGapCount: rows.filter((row) => row.classification === "ranking_gap").length,
    searchGapCount: rows.filter((row) => row.classification === "search_gap").length,
    activationGapCount: rows.filter((row) => row.classification === "activation_gap").length,
    conclusionGapCount: rows.filter((row) => row.classification === "conclusion_gap").length,
    corpusGapCount: rows.filter((row) => row.classification === "corpus_gap").length,
    requestErrorCount: rows.filter((row) => row.classification === "request_error").length
  };

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    corpusMode,
    resultLimit,
    dbPath: dbPath || null,
    summary,
    rows
  };

  const paths = await writeReports(report);
  console.log(JSON.stringify(summary, null, 2));
  console.log(`Conclusions term QA JSON report written to ${paths.jsonPath}`);
  console.log(`Conclusions term QA Markdown report written to ${paths.markdownPath}`);
  console.log(`Conclusions term QA CSV report written to ${paths.csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
