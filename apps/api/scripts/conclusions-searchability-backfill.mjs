import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";

const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.CONCLUSIONS_SEARCHABILITY_JSON_NAME || "conclusions-searchability-report.json";
const markdownName = process.env.CONCLUSIONS_SEARCHABILITY_MARKDOWN_NAME || "conclusions-searchability-report.md";
const csvName = process.env.CONCLUSIONS_SEARCHABILITY_CSV_NAME || "conclusions-searchability-report.csv";
const apply = (process.env.CONCLUSIONS_SEARCHABILITY_APPLY || "0") === "1";
const limit = Number(process.env.CONCLUSIONS_SEARCHABILITY_LIMIT || "12");
const busyTimeoutMs = Number(process.env.CONCLUSIONS_SEARCHABILITY_BUSY_TIMEOUT_MS || "5000");
const curlTimeoutSeconds = Number(process.env.CONCLUSIONS_SEARCHABILITY_CURL_TIMEOUT_SECONDS || "20");
const minChunkChars = Number(process.env.CONCLUSIONS_SEARCHABILITY_MIN_CHUNK_CHARS || "120");
const requestRetries = Math.max(1, Number(process.env.CONCLUSIONS_SEARCHABILITY_REQUEST_RETRIES || "3"));

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeHeading(value) {
  return normalizeWhitespace(String(value || ""))
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function sqlQuote(value) {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

function stableHash(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex");
}

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

async function runSql(sql) {
  await execFileAsync("sqlite3", ["-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, label = url) {
  let lastError = null;
  for (let attempt = 1; attempt <= requestRetries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), curlTimeoutSeconds * 1000);
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: { accept: "application/json" },
        signal: controller.signal
      });
      const text = await response.text();
      if (!response.ok) {
        throw new Error(`Request failed (${response.status}) ${url}: ${text.slice(0, 400)}`);
      }
      try {
        return JSON.parse(text || "{}");
      } catch {
        throw new Error(`Expected JSON response from ${url}; received non-JSON.`);
      }
    } catch (error) {
      lastError = error;
      if (attempt < requestRetries) {
        console.warn(`[conclusions-searchability] ${label} attempt ${attempt}/${requestRetries} failed: ${error instanceof Error ? error.message : String(error)}`);
        await sleep(400 * attempt);
      }
    } finally {
      clearTimeout(timeout);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError || `Request failed for ${label}`));
}

async function assertApiHealthy() {
  await fetchJson(`${apiBase}/health`, "health");
}

async function fetchPreview(documentId) {
  return fetchJson(`${apiBase}/admin/retrieval/documents/${documentId}/chunks?includeText=1`, `preview:${documentId}`);
}

async function ensureSearchTables() {
  await runSql(`
    CREATE TABLE IF NOT EXISTS retrieval_search_rows (
      search_id TEXT PRIMARY KEY,
      batch_id TEXT NOT NULL,
      document_id TEXT NOT NULL,
      chunk_id TEXT NOT NULL,
      title TEXT NOT NULL,
      chunk_type TEXT NOT NULL,
      retrieval_priority TEXT NOT NULL,
      citation_anchor_start TEXT NOT NULL,
      citation_anchor_end TEXT NOT NULL,
      has_canonical_reference_alignment INTEGER NOT NULL DEFAULT 0,
      source_link TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS retrieval_search_chunks (
      chunk_id TEXT PRIMARY KEY,
      batch_id TEXT NOT NULL,
      document_id TEXT NOT NULL,
      title TEXT NOT NULL,
      citation TEXT NOT NULL,
      source_file_ref TEXT NOT NULL,
      source_link TEXT NOT NULL,
      section_label TEXT NOT NULL,
      paragraph_anchor TEXT NOT NULL,
      citation_anchor TEXT NOT NULL,
      chunk_text TEXT NOT NULL,
      chunk_type TEXT NOT NULL,
      retrieval_priority TEXT NOT NULL,
      has_canonical_reference_alignment INTEGER NOT NULL DEFAULT 0,
      active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL
    );
  `);
}

async function loadOverallCounts() {
  const sql = `
    WITH doc_stats AS (
      SELECT
        d.id AS documentId,
        COUNT(DISTINCT CASE WHEN lower(trim(s.heading)) IN ('conclusions of law', 'conclusion') THEN s.id END) AS conclusionSectionCount,
        COUNT(DISTINCT CASE WHEN rs.active = 1 AND lower(trim(rs.section_label)) IN ('conclusions of law', 'conclusion') THEN rs.chunk_id END) AS activeTrustedConclusionChunkCount,
        COUNT(DISTINCT CASE WHEN lower(trim(c.section_label)) IN ('conclusions of law', 'conclusion') THEN c.id END) AS legacyConclusionChunkCount
      FROM documents d
      LEFT JOIN document_sections s ON s.document_id = d.id
      LEFT JOIN retrieval_search_chunks rs ON rs.document_id = d.id
      LEFT JOIN document_chunks c ON c.document_id = d.id
      WHERE d.file_type = 'decision_docx'
        AND d.rejected_at IS NULL
        AND d.searchable_at IS NOT NULL
        AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'KNOWN-REF-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'PILOT-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'HISTORICAL-%')
      GROUP BY d.id
    )
    SELECT
      COUNT(*) AS searchableDecisionDocCount,
      SUM(CASE WHEN conclusionSectionCount > 0 THEN 1 ELSE 0 END) AS docsWithConclusionSections,
      SUM(CASE WHEN conclusionSectionCount > 0 AND activeTrustedConclusionChunkCount = 0 THEN 1 ELSE 0 END) AS docsMissingTrustedConclusionChunks,
      SUM(CASE WHEN conclusionSectionCount > 0 AND activeTrustedConclusionChunkCount > 0 THEN 1 ELSE 0 END) AS docsWithTrustedConclusionChunks,
      SUM(CASE WHEN conclusionSectionCount > 0 AND legacyConclusionChunkCount > 0 THEN 1 ELSE 0 END) AS docsWithLegacyConclusionChunks
    FROM doc_stats;
  `;
  return (await runSqlJson(sql))[0] || {};
}

async function loadCandidateDocs() {
  const sql = `
    WITH doc_stats AS (
      SELECT
        d.id AS documentId,
        d.title AS title,
        d.citation AS citation,
        d.searchable_at AS searchableAt,
        d.source_r2_key AS sourceFileRef,
        COUNT(DISTINCT CASE WHEN lower(trim(s.heading)) IN ('conclusions of law', 'conclusion') THEN s.id END) AS conclusionSectionCount,
        COUNT(DISTINCT CASE WHEN rs.active = 1 AND lower(trim(rs.section_label)) IN ('conclusions of law', 'conclusion') THEN rs.chunk_id END) AS activeTrustedConclusionChunkCount,
        COUNT(DISTINCT CASE WHEN lower(trim(c.section_label)) IN ('conclusions of law', 'conclusion') THEN c.id END) AS legacyConclusionChunkCount
      FROM documents d
      LEFT JOIN document_sections s ON s.document_id = d.id
      LEFT JOIN retrieval_search_chunks rs ON rs.document_id = d.id
      LEFT JOIN document_chunks c ON c.document_id = d.id
      WHERE d.file_type = 'decision_docx'
        AND d.rejected_at IS NULL
        AND d.searchable_at IS NOT NULL
        AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'KNOWN-REF-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'PILOT-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'HISTORICAL-%')
      GROUP BY d.id
    )
    SELECT *
    FROM doc_stats
    WHERE conclusionSectionCount > 0
      AND activeTrustedConclusionChunkCount = 0
    ORDER BY searchableAt DESC, legacyConclusionChunkCount DESC, title ASC
    LIMIT ${Math.max(1, limit)};
  `;
  return runSqlJson(sql);
}

function isConclusionChunk(chunk) {
  const sectionLabel = normalizeHeading(chunk?.sectionLabel);
  const sectionKey = normalizeHeading(String(chunk?.sectionCanonicalKey || "").replace(/_/g, " "));
  const headingPath = Array.isArray(chunk?.headingPath) ? chunk.headingPath.map(normalizeHeading) : [];
  return (
    sectionLabel === "conclusions of law" ||
    sectionLabel === "conclusion" ||
    sectionKey === "conclusions of law" ||
    sectionKey === "conclusion" ||
    headingPath.includes("conclusions of law") ||
    headingPath.includes("conclusion")
  );
}

function selectConclusionChunks(preview) {
  const chunks = Array.isArray(preview?.chunks) ? preview.chunks : [];
  const out = [];
  const seen = new Set();
  for (const chunk of chunks) {
    if (!chunk?.chunkId || seen.has(chunk.chunkId)) continue;
    if (!isConclusionChunk(chunk)) continue;
    const sourceText = normalizeWhitespace(chunk?.sourceText);
    if (!sourceText || sourceText.length < minChunkChars) continue;
    seen.add(chunk.chunkId);
    out.push({
      chunkId: chunk.chunkId,
      sectionLabel: normalizeWhitespace(chunk.sectionLabel || "Conclusions of Law"),
      chunkType: normalizeWhitespace(chunk.chunkType || "authority_discussion"),
      retrievalPriority: normalizeWhitespace(chunk.retrievalPriority || "high"),
      paragraphAnchorStart: normalizeWhitespace(chunk.paragraphAnchorStart || chunk.paragraphAnchor || ""),
      citationAnchorStart: normalizeWhitespace(chunk.citationAnchorStart || chunk.citationAnchor || ""),
      citationAnchorEnd: normalizeWhitespace(chunk.citationAnchorEnd || chunk.citationAnchor || chunk.citationAnchorStart || ""),
      sourceText,
      hasCanonicalReferenceAlignment: chunk.hasCanonicalReferenceAlignment ? 1 : 0,
      sourceLink: normalizeWhitespace(chunk?.sourceLink || preview?.document?.sourceLink || "")
    });
  }
  return out;
}

function buildSqlWrites({ document, chunks, batchId, now }) {
  const statements = [];
  for (const chunk of chunks) {
    const searchId = `conclusions_search_${chunk.chunkId}`;
    statements.push(`
      INSERT OR REPLACE INTO retrieval_search_rows
      (search_id, batch_id, document_id, chunk_id, title, chunk_type, retrieval_priority, citation_anchor_start, citation_anchor_end,
       has_canonical_reference_alignment, source_link, created_at)
      VALUES (
        ${sqlQuote(searchId)},
        ${sqlQuote(batchId)},
        ${sqlQuote(document.documentId)},
        ${sqlQuote(chunk.chunkId)},
        ${sqlQuote(document.title || "Untitled")},
        ${sqlQuote(chunk.chunkType)},
        ${sqlQuote(chunk.retrievalPriority)},
        ${sqlQuote(chunk.citationAnchorStart)},
        ${sqlQuote(chunk.citationAnchorEnd || chunk.citationAnchorStart)},
        ${chunk.hasCanonicalReferenceAlignment ? 1 : 0},
        ${sqlQuote(chunk.sourceLink || document.sourceLink || "")},
        ${sqlQuote(now)}
      );
    `);
    statements.push(`
      INSERT OR REPLACE INTO retrieval_search_chunks
      (chunk_id, batch_id, document_id, title, citation, source_file_ref, source_link, section_label, paragraph_anchor, citation_anchor,
       chunk_text, chunk_type, retrieval_priority, has_canonical_reference_alignment, active, created_at)
      VALUES (
        ${sqlQuote(chunk.chunkId)},
        ${sqlQuote(batchId)},
        ${sqlQuote(document.documentId)},
        ${sqlQuote(document.title || "Untitled")},
        ${sqlQuote(document.citation || "")},
        ${sqlQuote(document.sourceFileRef || "")},
        ${sqlQuote(chunk.sourceLink || document.sourceLink || "")},
        ${sqlQuote(chunk.sectionLabel || "Conclusions of Law")},
        ${sqlQuote(chunk.paragraphAnchorStart || "")},
        ${sqlQuote(chunk.citationAnchorStart || "")},
        ${sqlQuote(chunk.sourceText)},
        ${sqlQuote(chunk.chunkType)},
        ${sqlQuote(chunk.retrievalPriority)},
        ${chunk.hasCanonicalReferenceAlignment ? 1 : 0},
        1,
        ${sqlQuote(now)}
      );
    `);
  }
  return statements.join("\n");
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Conclusions Searchability Backfill");
  lines.push("");
  lines.push(`- apply: ${report.apply}`);
  lines.push(`- inspectedDocCount: ${report.inspectedDocCount}`);
  lines.push(`- fetchedDocCount: ${report.fetchedDocCount}`);
  lines.push(`- candidateDocCount: ${report.candidateDocCount}`);
  lines.push(`- candidateChunkCount: ${report.candidateChunkCount}`);
  lines.push(`- appliedDocCount: ${report.appliedDocCount}`);
  lines.push(`- appliedChunkCount: ${report.appliedChunkCount}`);
  lines.push(`- fetchFailureCount: ${report.fetchFailureCount}`);
  lines.push("");
  lines.push("## Corpus");
  lines.push("");
  lines.push(`- searchableDecisionDocCountBefore: ${report.corpusBefore.searchableDecisionDocCount || 0}`);
  lines.push(`- docsWithConclusionSectionsBefore: ${report.corpusBefore.docsWithConclusionSections || 0}`);
  lines.push(`- docsMissingTrustedConclusionChunksBefore: ${report.corpusBefore.docsMissingTrustedConclusionChunks || 0}`);
  lines.push(`- docsWithTrustedConclusionChunksBefore: ${report.corpusBefore.docsWithTrustedConclusionChunks || 0}`);
  lines.push(`- docsMissingTrustedConclusionChunksAfter: ${report.corpusAfter.docsMissingTrustedConclusionChunks || 0}`);
  lines.push(`- docsWithTrustedConclusionChunksAfter: ${report.corpusAfter.docsWithTrustedConclusionChunks || 0}`);
  lines.push("");
  lines.push("## Sample Docs");
  lines.push("");
  for (const row of report.documents.slice(0, 20)) {
    lines.push(`- ${row.documentId} | ${row.citation || "<no citation>"} | chunks=${row.conclusionChunkCount} | applied=${row.applied}`);
  }
  if (report.failures.length > 0) {
    lines.push("");
    lines.push("## Failures");
    lines.push("");
    for (const row of report.failures.slice(0, 20)) {
      lines.push(`- ${row.documentId}: ${row.reason}`);
    }
  }
  return `${lines.join("\n")}\n`;
}

function toCsv(report) {
  const rows = [
    ["documentId", "citation", "title", "conclusionChunkCount", "chunkTypes", "applied", "reason"],
    ...report.documents.map((row) => [
      row.documentId,
      row.citation || "",
      row.title || "",
      String(row.conclusionChunkCount || 0),
      (row.chunkTypes || []).join("|"),
      row.applied ? "1" : "0",
      row.reason || ""
    ]),
    ...report.failures.map((row) => [row.documentId, "", "", "0", "", "0", row.reason || ""])
  ];
  return rows
    .map((row) =>
      row
        .map((value) => {
          const raw = String(value ?? "");
          return /[",\n]/.test(raw) ? `"${raw.replace(/"/g, '""')}"` : raw;
        })
        .join(",")
    )
    .join("\n");
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
  await assertApiHealthy();
  await ensureSearchTables();

  const corpusBefore = await loadOverallCounts();
  const candidateDocs = await loadCandidateDocs();
  const batchId = `conclusions_searchability_${stableHash(`${new Date().toISOString()}|${candidateDocs.map((row) => row.documentId).join("|")}`).slice(0, 16)}`;
  const now = new Date().toISOString();

  const documents = [];
  const failures = [];
  const statements = [];
  let fetchedDocCount = 0;
  let candidateChunkCount = 0;

  for (const doc of candidateDocs) {
    try {
      const preview = await fetchPreview(doc.documentId);
      fetchedDocCount += 1;
      const conclusionChunks = selectConclusionChunks(preview);
      const chunkTypes = Array.from(new Set(conclusionChunks.map((chunk) => chunk.chunkType))).sort((a, b) => a.localeCompare(b));
      const appliedForDoc = apply && conclusionChunks.length > 0;

      if (conclusionChunks.length > 0) {
        candidateChunkCount += conclusionChunks.length;
        documents.push({
          documentId: doc.documentId,
          citation: doc.citation || "",
          title: doc.title || "",
          conclusionChunkCount: conclusionChunks.length,
          chunkTypes,
          applied: appliedForDoc,
          reason: ""
        });
        if (apply) {
          statements.push(
            buildSqlWrites({
              document: preview.document || {
                documentId: doc.documentId,
                title: doc.title || "",
                citation: doc.citation || "",
                sourceFileRef: doc.sourceFileRef || "",
                sourceLink: ""
              },
              chunks: conclusionChunks,
              batchId,
              now
            })
          );
        }
      } else {
        documents.push({
          documentId: doc.documentId,
          citation: doc.citation || "",
          title: doc.title || "",
          conclusionChunkCount: 0,
          chunkTypes: [],
          applied: false,
          reason: "no_retrieval_preview_conclusion_chunks"
        });
      }
    } catch (error) {
      failures.push({
        documentId: doc.documentId,
        reason: error instanceof Error ? error.message : String(error)
      });
    }
  }

  if (apply && statements.length > 0) {
    await runSql(`BEGIN;\n${statements.join("\n")}\nCOMMIT;`);
  }

  const corpusAfter = await loadOverallCounts();
  const report = {
    generatedAt: now,
    apiBase,
    dbPath,
    apply,
    limit,
    minChunkChars,
    inspectedDocCount: candidateDocs.length,
    fetchedDocCount,
    candidateDocCount: documents.filter((row) => row.conclusionChunkCount > 0).length,
    candidateChunkCount,
    appliedDocCount: documents.filter((row) => row.applied).length,
    appliedChunkCount: apply ? candidateChunkCount : 0,
    fetchFailureCount: failures.length,
    corpusBefore,
    corpusAfter,
    documents,
    failures
  };

  const paths = await writeReports(report);
  console.log(JSON.stringify({
    inspectedDocCount: report.inspectedDocCount,
    fetchedDocCount: report.fetchedDocCount,
    candidateDocCount: report.candidateDocCount,
    candidateChunkCount: report.candidateChunkCount,
    appliedDocCount: report.appliedDocCount,
    appliedChunkCount: report.appliedChunkCount,
    fetchFailureCount: report.fetchFailureCount,
    docsMissingTrustedConclusionChunksBefore: report.corpusBefore.docsMissingTrustedConclusionChunks || 0,
    docsMissingTrustedConclusionChunksAfter: report.corpusAfter.docsMissingTrustedConclusionChunks || 0
  }, null, 2));
  console.log(`Conclusions searchability JSON report written to ${paths.jsonPath}`);
  console.log(`Conclusions searchability Markdown report written to ${paths.markdownPath}`);
  console.log(`Conclusions searchability CSV report written to ${paths.csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
