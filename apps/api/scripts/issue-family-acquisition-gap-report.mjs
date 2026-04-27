import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.ISSUE_FAMILY_ACQUISITION_JSON_NAME || "issue-family-acquisition-gap-report.json";
const markdownName = process.env.ISSUE_FAMILY_ACQUISITION_MARKDOWN_NAME || "issue-family-acquisition-gap-report.md";
const requestTimeoutMs = Number(process.env.ISSUE_FAMILY_ACQUISITION_TIMEOUT_MS || "30000");
const sqliteTimeoutMs = Number(process.env.ISSUE_FAMILY_ACQUISITION_SQLITE_TIMEOUT_MS || "45000");
const retries = Math.max(1, Number(process.env.ISSUE_FAMILY_ACQUISITION_RETRIES || "2"));
const configuredDbPath = process.env.ISSUE_FAMILY_ACQUISITION_DB_PATH || "";

const families = [
  {
    id: "buyout_pressure",
    label: "Buyout Pressure",
    searchQuery: "tenant accepted a buyout after pressure from landlord",
    signalGroups: [["buyout"], ["pressure", "pressuring", "coercion", "coercive", "threatened", "threat"]],
    seedQueries: ["buyout pressure", "buyout coercion", "coercive buyout"],
    expectation: "Should surface trusted buyout-pressure decisions if the corpus contains them."
  },
  {
    id: "section8_unlawful_detainer",
    label: "Section 8 Unlawful Detainer",
    searchQuery: "section 8 tenant facing unlawful detainer",
    signalGroups: [["section 8", "voucher"], ["unlawful detainer", "eviction action", "eviction"]],
    seedQueries: ["section 8 unlawful detainer", "voucher eviction", "section 8 eviction action"],
    expectation: "Should surface voucher or Section 8 eviction decisions if the corpus contains them."
  }
];

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

function normalizedWholeWordSqlExpr(expr) {
  return `(' ' || lower(
    replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(coalesce(${expr}, ''), char(10), ' '), char(13), ' '), char(9), ' '), char(8217), ' '), '''', ' '), '.', ' '), ',', ' '), ';', ' '), ':', ' '), '(', ' '), ')', ' '), '-', ' ')
  ) || ' ')`;
}

function buildSignalClause(expr, signal) {
  const normalized = normalizeSqlLike(signal).replace(/'/g, "''");
  if (!normalized) return "0";
  return `instr(${normalizedWholeWordSqlExpr(expr)}, ' ${normalized} ') > 0`;
}

function buildGroupClause(expr, group) {
  const items = (Array.isArray(group) ? group : [group]).filter(Boolean);
  return `(${items.map((signal) => buildSignalClause(expr, signal)).join(" or ")})`;
}

function buildConclusionSectionClause(expr) {
  return `(
    lower(coalesce(${expr}, '')) like '%conclusions of law%'
    or lower(coalesce(${expr}, '')) in ('conclusions_of_law', 'authority_discussion', 'analysis_reasoning')
  )`;
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
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
      if (names.has("documents") && names.has("document_chunks") && names.has("retrieval_search_chunks")) return candidate.fullPath;
    } catch {
      // keep scanning
    }
  }

  throw new Error("Could not find a usable local D1 sqlite database for issue-family acquisition gap audit.");
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
      if (attempt < retries) await new Promise((resolve) => setTimeout(resolve, 400 * attempt));
    } finally {
      clearTimeout(timeout);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError || `Request failed for ${label}`));
}

function docExistsForAllGroups(selectAlias, groups) {
  return groups
    .map((group, index) => `max(case when ${buildGroupClause(`${selectAlias}.chunk_text`, group)} then 1 else 0 end) as g${index + 1}`)
    .join(",\n             ");
}

function havingAllGroups(groups) {
  return groups.map((_, index) => `g${index + 1} = 1`).join(" and ");
}

async function fetchFamilyCounts(dbPath, family) {
  const groups = family.signalGroups;
  const conclusionDc = buildConclusionSectionClause("dc.section_label");
  const conclusionRs = buildConclusionSectionClause("rs.section_label");

  const sql = `
    with searchable_docs as (
      select d.id as document_id,
             d.citation as citation,
             d.title as title,
             ${docExistsForAllGroups("dc", groups)}
      from document_chunks dc
      join documents d on d.id = dc.document_id
      where d.file_type = 'decision_docx'
        and d.searchable_at is not null
        and d.rejected_at is null
      group by d.id, d.citation, d.title
    ),
    searchable_conclusions as (
      select d.id as document_id,
             d.citation as citation,
             d.title as title,
             ${docExistsForAllGroups("dc", groups)}
      from document_chunks dc
      join documents d on d.id = dc.document_id
      where d.file_type = 'decision_docx'
        and d.searchable_at is not null
        and d.rejected_at is null
        and ${conclusionDc}
      group by d.id, d.citation, d.title
    ),
    active_docs as (
      select rs.document_id as document_id,
             rs.citation as citation,
             rs.title as title,
             ${docExistsForAllGroups("rs", groups)}
      from retrieval_search_chunks rs
      join documents d on d.id = rs.document_id
      where d.file_type = 'decision_docx'
        and d.searchable_at is not null
        and d.rejected_at is null
        and rs.active = 1
      group by rs.document_id, rs.citation, rs.title
    ),
    active_conclusions as (
      select rs.document_id as document_id,
             rs.citation as citation,
             rs.title as title,
             ${docExistsForAllGroups("rs", groups)}
      from retrieval_search_chunks rs
      join documents d on d.id = rs.document_id
      where d.file_type = 'decision_docx'
        and d.searchable_at is not null
        and d.rejected_at is null
        and rs.active = 1
        and ${conclusionRs}
      group by rs.document_id, rs.citation, rs.title
    )
    select
      (select count(*) from searchable_docs where ${havingAllGroups(groups)}) as searchableAnyDocCount,
      (select count(*) from searchable_conclusions where ${havingAllGroups(groups)}) as searchableConclusionDocCount,
      (select count(*) from active_docs where ${havingAllGroups(groups)}) as activeAnyDocCount,
      (select count(*) from active_conclusions where ${havingAllGroups(groups)}) as activeConclusionDocCount;
  `;

  const rows = await execSqlite(dbPath, sql);
  const row = rows[0] || {};
  return {
    searchableAnyDocCount: Number(row.searchableAnyDocCount || 0),
    searchableConclusionDocCount: Number(row.searchableConclusionDocCount || 0),
    activeAnyDocCount: Number(row.activeAnyDocCount || 0),
    activeConclusionDocCount: Number(row.activeConclusionDocCount || 0)
  };
}

async function fetchSampleDocs(dbPath, family) {
  const groups = family.signalGroups;
  const sql = `
    with searchable_docs as (
      select d.id as document_id,
             d.citation as citation,
             d.title as title,
             ${docExistsForAllGroups("dc", groups)}
      from document_chunks dc
      join documents d on d.id = dc.document_id
      where d.file_type = 'decision_docx'
        and d.searchable_at is not null
        and d.rejected_at is null
      group by d.id, d.citation, d.title
    )
    select citation, title
    from searchable_docs
    where ${havingAllGroups(groups)}
    order by citation desc
    limit 8;
  `;
  return execSqlite(dbPath, sql);
}

function classifyFamily(row) {
  if (row.error) return "request_error";
  if (row.searchableAnyDocCount === 0) return "corpus_gap";
  if (row.searchableConclusionDocCount === 0) return "conclusion_gap";
  if (row.activeConclusionDocCount === 0) return "activation_gap";
  if (row.searchResultCount === 0) return "search_gap";
  if (row.activeConclusionDocCount < 3) return "thin_active_coverage";
  return "working";
}

function recommendationForFamily(classification) {
  switch (classification) {
    case "corpus_gap":
      return "This looks like a true corpus acquisition problem. Search tuning will not close it alone.";
    case "conclusion_gap":
      return "The family exists in searchable docs, but not in conclusion-like sections. We need better authority-section coverage or different source decisions.";
    case "activation_gap":
      return "Searchable conclusion-like docs exist, but they have not been activated into retrieval yet.";
    case "search_gap":
      return "Active conclusion-like docs exist, but the live query still returns zero. Query/scoping/ranking needs another pass.";
    case "thin_active_coverage":
      return "This is no longer empty, but the active conclusion-like footprint is still thin and likely brittle.";
    case "working":
      return "This family has live results and some active conclusion-like support.";
    default:
      return "The family could not be evaluated cleanly in this run.";
  }
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Issue Family Acquisition Gap Report");
  lines.push("");
  for (const row of report.rows) {
    lines.push(`## ${row.label}`);
    lines.push("");
    lines.push(`- classification: ${row.classification}`);
    lines.push(`- searchResultCount: ${row.searchResultCount}`);
    lines.push(`- searchableAnyDocCount: ${row.searchableAnyDocCount}`);
    lines.push(`- searchableConclusionDocCount: ${row.searchableConclusionDocCount}`);
    lines.push(`- activeAnyDocCount: ${row.activeAnyDocCount}`);
    lines.push(`- activeConclusionDocCount: ${row.activeConclusionDocCount}`);
    lines.push(`- recommendation: ${row.recommendation}`);
    lines.push(`- seedQueries: ${row.seedQueries.join(" | ")}`);
    lines.push("");
    if (row.sampleDocs.length > 0) {
      lines.push("### Sample Docs");
      lines.push("");
      for (const doc of row.sampleDocs) {
        lines.push(`- ${doc.citation || "<no citation>"} | ${doc.title || "<untitled>"}`);
      }
      lines.push("");
    }
  }
  return `${lines.join("\n")}\n`;
}

async function writeReports(report) {
  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  await fs.writeFile(markdownPath, toMarkdown(report));
  return { jsonPath, markdownPath };
}

async function main() {
  const dbPath = await detectDatabasePath().catch(() => "");
  const rows = [];

  for (const family of families) {
    let counts = {
      searchableAnyDocCount: 0,
      searchableConclusionDocCount: 0,
      activeAnyDocCount: 0,
      activeConclusionDocCount: 0
    };
    let searchResultCount = 0;
    let topCitations = [];
    let sampleDocs = [];
    let error = null;

    try {
      if (dbPath) {
        counts = await fetchFamilyCounts(dbPath, family);
        sampleDocs = await fetchSampleDocs(dbPath, family);
      }
      const response = await fetchJson(
        `${apiBase}/search`,
        {
          query: family.searchQuery,
          limit: 5,
          snippetMaxLength: 320,
          corpusMode: "trusted_only",
          filters: { approvedOnly: true }
        },
        family.id
      );
      const results = Array.isArray(response?.results) ? response.results : [];
      searchResultCount = results.length;
      topCitations = results.slice(0, 5).map((result) => result?.citation || null).filter(Boolean);
    } catch (caught) {
      error = caught instanceof Error ? caught.message : String(caught);
    }

    const row = {
      id: family.id,
      label: family.label,
      searchQuery: family.searchQuery,
      expectation: family.expectation,
      seedQueries: family.seedQueries,
      searchResultCount,
      topCitations,
      sampleDocs,
      error,
      ...counts
    };
    row.classification = classifyFamily(row);
    row.recommendation = recommendationForFamily(row.classification);
    rows.push(row);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    dbPath: dbPath || null,
    rows
  };

  const paths = await writeReports(report);
  console.log(JSON.stringify(rows.map((row) => ({
    id: row.id,
    classification: row.classification,
    searchResultCount: row.searchResultCount,
    searchableAnyDocCount: row.searchableAnyDocCount,
    searchableConclusionDocCount: row.searchableConclusionDocCount,
    activeConclusionDocCount: row.activeConclusionDocCount
  })), null, 2));
  console.log(`Issue-family acquisition JSON report written to ${paths.jsonPath}`);
  console.log(`Issue-family acquisition Markdown report written to ${paths.markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
