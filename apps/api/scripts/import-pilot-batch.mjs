import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const pilotDir = process.env.PILOT_DIR;
const jurisdiction = process.env.PILOT_JURISDICTION || "Pilot Jurisdiction";
const limit = Number(process.env.BATCH_LIMIT || "75");
const offset = Number(process.env.BATCH_OFFSET || "0");
const recursive = process.env.PILOT_RECURSIVE === "1";
const dryRun = process.env.PILOT_DRY_RUN === "1";
const reportName = process.env.PILOT_REPORT_NAME || "pilot-import-report.json";
const pilotLabel = process.env.PILOT_LABEL || "pilot";
const skipVectorOnIngest = process.env.SKIP_VECTOR_ON_INGEST === "1";
const skipExistingCitations = process.env.PILOT_SKIP_EXISTING_CITATIONS !== "0";
const d1SqlitePath = process.env.PILOT_D1_SQLITE_PATH || "";

if (!pilotDir) {
  console.error("PILOT_DIR is required. Example: PILOT_DIR=./pilot-docs API_BASE_URL=http://127.0.0.1:8787 pnpm import:pilot");
  process.exit(1);
}

function guessDateFromName(name) {
  const match = name.match(/(20\d{2})[-_](\d{2})[-_](\d{2})/);
  if (!match) return undefined;
  return `${match[1]}-${match[2]}-${match[3]}`;
}

function guessCitationFromName(name) {
  return name
    .replace(/\.[^.]+$/, "")
    .replace(/[^A-Za-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80)
    .toUpperCase();
}

function findDefaultD1SqlitePath() {
  const root = path.resolve(process.cwd(), ".wrangler", "state", "v3", "d1", "miniflare-D1DatabaseObject");
  try {
    const entries = fsSync.readdirSync(root).filter((entry) => entry.endsWith(".sqlite")).sort();
    if (entries.length === 0) return null;
    return path.join(root, entries[0]);
  } catch {
    return null;
  }
}

function loadExistingDecisionCitations() {
  if (!skipExistingCitations) {
    return { citations: new Set(), sqlitePath: null, error: null };
  }

  const sqlitePath = d1SqlitePath || findDefaultD1SqlitePath();
  if (!sqlitePath) {
    return { citations: new Set(), sqlitePath: null, error: "d1_sqlite_not_found" };
  }

  try {
    const raw = execFileSync(
      "sqlite3",
      [sqlitePath, "select citation from documents where file_type='decision_docx';"],
      { encoding: "utf8" }
    );
    const citations = new Set(
      String(raw || "")
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean)
    );
    return { citations, sqlitePath, error: null };
  } catch (error) {
    return {
      citations: new Set(),
      sqlitePath,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

async function postJson(endpoint, payload) {
  const response = await fetch(`${apiBase}${endpoint}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });

  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }

  if (!response.ok) {
    return { ok: false, status: response.status, body };
  }
  return { ok: true, status: response.status, body };
}

async function postMultipartDecision(endpoint, payload) {
  try {
    const form = new FormData();
    form.set("jurisdiction", payload.jurisdiction);
    form.set("title", payload.title);
    form.set("citation", payload.citation);
    if (payload.decisionDate) {
      form.set("decisionDate", payload.decisionDate);
    }
    form.set("performVectorUpsert", payload.performVectorUpsert ? "true" : "false");
    form.set(
      "file",
      new Blob([payload.sourceFile.bytes], {
        type: payload.sourceFile.mimeType || "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
      }),
      payload.sourceFile.filename
    );

    const response = await fetch(`${apiBase}${endpoint}`, {
      method: "POST",
      body: form
    });

    const text = await response.text();
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      body = { raw: text };
    }

    if (!response.ok) {
      return { ok: false, status: response.status, body };
    }
    return { ok: true, status: response.status, body };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      body: {
        error: error instanceof Error ? error.message : String(error),
        kind: "network_failure"
      }
    };
  }
}

async function getJson(endpoint) {
  const response = await fetch(`${apiBase}${endpoint}`);
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  if (!response.ok) {
    return { ok: false, status: response.status, body };
  }
  return { ok: true, status: response.status, body };
}

async function main() {
  const files = await collectDocxFiles(pilotDir, recursive);
  const existing = loadExistingDecisionCitations();

  if (files.length === 0) {
    console.log("No .docx files found in PILOT_DIR.");
    return;
  }

  const bounded = files.slice(Math.max(0, offset), Math.max(0, offset) + Math.max(1, limit));
  if (dryRun) {
    console.log(`DRY RUN: ${bounded.length} files selected (offset=${offset}, limit=${limit}, recursive=${recursive ? "yes" : "no"})`);
    bounded.forEach((item) => console.log(` - ${item}`));
    return;
  }

  const report = [];
  const seenCitationsInRun = new Set();

  for (const relativePath of bounded) {
    const filename = path.basename(relativePath);
    const citation = guessCitationFromName(filename);
    if (filename.startsWith("~$")) {
      report.push({ filename, relativePath, skipped: true, reason: "office_lock_temp_file" });
      console.log(`SKIPPED ${filename} -> office lock/temp file`);
      continue;
    }
    if (seenCitationsInRun.has(citation)) {
      report.push({ filename, relativePath, skipped: true, reason: "duplicate_citation_in_batch", citation });
      console.log(`SKIPPED ${filename} -> duplicate citation in current batch (${citation})`);
      continue;
    }
    seenCitationsInRun.add(citation);
    if (existing.citations.has(citation)) {
      report.push({ filename, relativePath, skipped: true, reason: "existing_citation_in_d1", citation });
      console.log(`SKIPPED ${filename} -> already imported (${citation})`);
      continue;
    }
    const filePath = path.join(pilotDir, relativePath);
    const bytes = await fs.readFile(filePath);

    const payload = {
      jurisdiction,
      title: filename.replace(/\.[^.]+$/, ""),
      citation,
      decisionDate: guessDateFromName(filename),
      sourceFile: {
        filename,
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytes
      },
      performVectorUpsert: !skipVectorOnIngest
    };

    const response = await postMultipartDecision("/ingest/decision-upload", payload);
    report.push({ filename, relativePath, response });

    if (response.ok) {
      console.log(`INGESTED ${filename} -> ${response.body.documentId} (qc: ${JSON.stringify(response.body.qc)})`);
    } else {
      console.log(`FAILED ${filename} -> ${response.status} ${JSON.stringify(response.body)}`);
    }
  }

  const succeeded = report.filter((row) => row.response?.ok);
  const detailReport = [];
  for (const row of succeeded) {
    const documentId = row.response.body?.documentId;
    if (!documentId) continue;
    let detail;
    try {
      detail = await getJson(`/admin/ingestion/documents/${documentId}`);
    } catch (error) {
      detail = {
        ok: false,
        status: 0,
        body: {
          error: error instanceof Error ? error.message : String(error),
          kind: "network_failure"
        }
      };
    }
    detailReport.push({ documentId, detail });
  }

  const detailSuccess = detailReport.filter((row) => row.detail.ok).map((row) => row.detail.body);
  const stagedCount = detailSuccess.filter((row) => !row.searchableAt).length;
  const searchableCount = detailSuccess.filter((row) => Boolean(row.searchableAt)).length;
  const approvedCount = detailSuccess.filter((row) => Boolean(row.approvedAt)).length;
  const totalWarnings = detailSuccess.reduce((sum, row) => sum + (Array.isArray(row.extractionWarnings) ? row.extractionWarnings.length : 0), 0);
  const totalUnresolvedRefs = detailSuccess.reduce((sum, row) => sum + (typeof row.unresolvedReferenceCount === "number" ? row.unresolvedReferenceCount : 0), 0);
  const totalCriticalExceptions = detailSuccess.reduce(
    (sum, row) => sum + (Array.isArray(row.criticalExceptionReferences) ? row.criticalExceptionReferences.length : 0),
    0
  );
  const totalFilteredNoise = detailSuccess.reduce((sum, row) => sum + (typeof row.filteredNoiseCount === "number" ? row.filteredNoiseCount : 0), 0);
  const totalLowTaxonomy = detailSuccess.filter((row) => Boolean(row.lowConfidenceTaxonomy)).length;
  const avgExtractionConfidence =
    detailSuccess.length > 0
      ? Number((detailSuccess.reduce((sum, row) => sum + Number(row.extractionConfidence || 0), 0) / detailSuccess.length).toFixed(3))
      : 0;

  const outputPath = path.resolve(process.cwd(), "reports", reportName);
  await fs.writeFile(
    outputPath,
    JSON.stringify(
      {
        apiBase,
        pilotLabel,
        pilotDir,
        importedAt: new Date().toISOString(),
        controls: { limit, offset, recursive, dryRun },
        ingestMode: {
          skipVectorOnIngest,
          skipExistingCitations,
          d1SqlitePath: existing.sqlitePath,
          existingCitationLoadError: existing.error
        },
        summary: {
          totalFound: files.length,
          attempted: bounded.length,
          succeeded: report.filter((row) => row.response?.ok).length,
          failed: report.filter((row) => row.response && !row.response.ok).length,
          skipped_existing_citation_count: report.filter((row) => row.skipped && row.reason === "existing_citation_in_d1").length,
          skipped_duplicate_in_batch_count: report.filter((row) => row.skipped && row.reason === "duplicate_citation_in_batch").length,
          skipped_temp_file_count: report.filter((row) => row.skipped && row.reason === "office_lock_temp_file").length,
          staged: stagedCount,
          searchable: searchableCount,
          approved: approvedCount,
          warning_count: totalWarnings,
          unresolved_reference_count: totalUnresolvedRefs,
          critical_exception_count: totalCriticalExceptions,
          filtered_noise_count: totalFilteredNoise,
          low_confidence_taxonomy_count: totalLowTaxonomy,
          avg_extraction_confidence: avgExtractionConfidence
        },
        review_summary: detailSuccess.map((row) => ({
          id: row.id,
          title: row.title,
          status: row.rejectedAt ? "rejected" : row.approvedAt ? "approved" : row.searchableAt ? "searchable" : "staged",
          warning_count: Array.isArray(row.extractionWarnings) ? row.extractionWarnings.length : 0,
          unresolved_reference_count: typeof row.unresolvedReferenceCount === "number" ? row.unresolvedReferenceCount : 0,
          critical_exception_references: Array.isArray(row.criticalExceptionReferences) ? row.criticalExceptionReferences : [],
          filtered_noise_count: typeof row.filteredNoiseCount === "number" ? row.filteredNoiseCount : 0,
          low_confidence_taxonomy: Boolean(row.lowConfidenceTaxonomy),
          extraction_confidence: Number(row.extractionConfidence || 0)
        })),
        report
      },
      null,
      2
    )
  );
  console.log(`\nReport written to ${outputPath}`);
}

async function collectDocxFiles(baseDir, walkRecursive) {
  const out = [];
  async function walk(currentDir, prefix = "") {
    const entries = await fs.readdir(currentDir, { withFileTypes: true });
    for (const entry of entries) {
      const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isFile() && /\.docx$/i.test(entry.name)) {
        out.push(relativePath);
      }
      if (entry.isDirectory() && walkRecursive) {
        await walk(fullPath, relativePath);
      }
    }
  }
  await walk(baseDir, "");
  return out.sort((a, b) => a.localeCompare(b));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
