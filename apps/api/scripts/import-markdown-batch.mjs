import fs from "node:fs/promises";
import fsSync from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const markdownDir = process.env.MARKDOWN_DIR;
const jurisdiction = process.env.MARKDOWN_JURISDICTION || process.env.PILOT_JURISDICTION || "San Francisco Rent Board";
const limit = Number(process.env.MARKDOWN_LIMIT || process.env.BATCH_LIMIT || "75");
const offset = Number(process.env.MARKDOWN_OFFSET || process.env.BATCH_OFFSET || "0");
const recursive = process.env.MARKDOWN_RECURSIVE !== "0";
const dryRun = process.env.MARKDOWN_DRY_RUN === "1";
const reportName = process.env.MARKDOWN_REPORT_NAME || "markdown-import-report.json";
const importLabel = process.env.MARKDOWN_LABEL || "markdown";
const skipVectorOnIngest = process.env.SKIP_VECTOR_ON_INGEST === "1";
const skipExistingCitations = process.env.MARKDOWN_SKIP_EXISTING_CITATIONS !== "0";
const d1SqlitePath = process.env.MARKDOWN_D1_SQLITE_PATH || process.env.PILOT_D1_SQLITE_PATH || "";

if (!markdownDir) {
  console.error(
    "MARKDOWN_DIR is required. Example: MARKDOWN_DIR=../../import-batches/markdown-corpus API_BASE_URL=http://127.0.0.1:8787 pnpm import:markdown"
  );
  process.exit(1);
}

function stripExtension(name) {
  return String(name || "").replace(/\.[^.]+$/, "");
}

function guessCitationFromName(name) {
  return stripExtension(name)
    .replace(/[^A-Za-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80)
    .toUpperCase();
}

function isShortAliasTitle(title) {
  return /^[A-Z0-9]{1,6}~[A-Z0-9]$/i.test(String(title || ""));
}

function normalizeDocumentIdentityPart(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

function buildDocumentIdentity(citation, title) {
  return `${normalizeDocumentIdentityPart(citation)}::${normalizeDocumentIdentityPart(title)}`;
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

function loadExistingDecisionKeys() {
  if (!skipExistingCitations) {
    return { keys: new Set(), sqlitePath: null, error: null };
  }

  const sqlitePath = d1SqlitePath || findDefaultD1SqlitePath();
  if (!sqlitePath) {
    return { keys: new Set(), sqlitePath: null, error: "d1_sqlite_not_found" };
  }

  try {
    const raw = execFileSync(
      "sqlite3",
      [sqlitePath, "select coalesce(citation, '') || char(31) || coalesce(title, '') from documents where file_type='decision_docx';"],
      { encoding: "utf8" }
    );
    const keys = new Set(
      String(raw || "")
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => {
          const [citation = "", title = ""] = line.split("\u001f");
          return buildDocumentIdentity(citation, title);
        })
    );
    return { keys, sqlitePath, error: null };
  } catch (error) {
    return {
      keys: new Set(),
      sqlitePath,
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

function guessDateFromContent(markdown) {
  const direct = markdown.match(/\b(20\d{2}|19\d{2})-(\d{2})-(\d{2})\b/);
  if (direct) {
    return `${direct[1]}-${direct[2]}-${direct[3]}`;
  }

  const monthMatch = markdown.match(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(20\d{2}|19\d{2})\b/i
  );
  if (!monthMatch) {
    return undefined;
  }

  const months = {
    january: "01",
    february: "02",
    march: "03",
    april: "04",
    may: "05",
    june: "06",
    july: "07",
    august: "08",
    september: "09",
    october: "10",
    november: "11",
    december: "12"
  };

  const month = months[String(monthMatch[1] || "").toLowerCase()];
  const day = String(monthMatch[2] || "").padStart(2, "0");
  const year = monthMatch[3];
  return month && year ? `${year}-${month}-${day}` : undefined;
}

function extractCitationFromContent(markdown, fallbackName) {
  const match = markdown.match(
    /\b(?:CASE\s*(?:NO\.?|NUMBER|#)|DOCKET\s*(?:NO\.?|#)|APPEAL\s*(?:NO\.?|#))\s*[:#-]?\s*([A-Z0-9][A-Z0-9\-/]{2,30})\b/i
  );
  return {
    citation: (match?.[1] || guessCitationFromName(fallbackName)).replace(/\s+/g, "").toUpperCase(),
    fromContent: Boolean(match?.[1])
  };
}

function deriveDocumentLabelFromContent(markdown) {
  const header = String(markdown || "")
    .slice(0, 4000)
    .replace(/\s+/g, " ")
    .toUpperCase();

  const patterns = [
    { regex: /\bPOST[-\s]+HEARING ORDER\b/, label: "Post-hearing Order" },
    { regex: /\bREMAND DECISION\b/, label: "Remand Decision" },
    { regex: /\bMINUTE ORDER REVISED\b/, label: "Minute Order Revised" },
    { regex: /\bREVISED MINUTE ORDER\b/, label: "Minute Order Revised" },
    { regex: /\bDECISION REVISED\b/, label: "Decision Revised" },
    { regex: /\bMINUTE ORDER\b/, label: "Minute Order" },
    { regex: /\bDISMISSAL\b/, label: "Dismissal" },
    { regex: /\bDECISION\b/, label: "Decision" },
    { regex: /\bORDER\b/, label: "Order" }
  ];

  const match = patterns.find((entry) => entry.regex.test(header));
  return match?.label || null;
}

function hasLikelyArtifactContent(markdown) {
  const text = String(markdown || "");
  return /LaserWriter|endnote textcaptionHeading|paragraphcemterIdent/i.test(text);
}

function sanitizeMarkdownContent(markdown) {
  const text = String(markdown || "");
  const markers = ["LaserWriter", "endnote textcaptionHeading", "paragraphcemterIdent"];
  const positions = markers
    .map((marker) => text.indexOf(marker))
    .filter((position) => position >= 0)
    .sort((a, b) => a - b);

  if (positions.length === 0) {
    return { markdown: text, trimmedArtifactTail: false };
  }

  return {
    markdown: text.slice(0, positions[0]).trimEnd(),
    trimmedArtifactTail: true
  };
}

function getImportDecision(filename, markdown) {
  const baseTitle = stripExtension(filename);
  const shortAlias = isShortAliasTitle(baseTitle);
  const citationInfo = extractCitationFromContent(markdown, filename);
  const documentLabel = deriveDocumentLabelFromContent(markdown);

  if (/\bTABLE\s+\d+\b/i.test(baseTitle)) {
    return {
      skip: true,
      reason: "table_fragment_filename",
      citation: citationInfo.citation,
      title: baseTitle
    };
  }

  if (shortAlias && hasLikelyArtifactContent(markdown)) {
    return {
      skip: true,
      reason: "short_alias_artifact_content",
      citation: citationInfo.citation,
      title: baseTitle
    };
  }

  if (shortAlias && !citationInfo.fromContent) {
    return {
      skip: true,
      reason: "short_alias_missing_citation",
      citation: citationInfo.citation,
      title: baseTitle
    };
  }

  if (shortAlias && !documentLabel) {
    return {
      skip: true,
      reason: "short_alias_missing_document_type",
      citation: citationInfo.citation,
      title: baseTitle
    };
  }

  return {
    skip: false,
    citation: citationInfo.citation,
    title: shortAlias && documentLabel ? `${citationInfo.citation} ${documentLabel}` : baseTitle,
    titleRecoveredFromContent: shortAlias && Boolean(documentLabel)
  };
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
        type: payload.sourceFile.mimeType || "text/markdown; charset=utf-8"
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

async function collectMarkdownFiles(baseDir, walkRecursive) {
  const out = [];

  async function walk(currentDir, prefix = "") {
    const entries = await fs.readdir(currentDir, { withFileTypes: true });
    for (const entry of entries) {
      const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isFile() && /\.(md|markdown)$/i.test(entry.name)) {
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

async function main() {
  const files = await collectMarkdownFiles(markdownDir, recursive);
  const existing = loadExistingDecisionKeys();

  if (files.length === 0) {
    console.log("No markdown files found in MARKDOWN_DIR.");
    return;
  }

  const bounded = files.slice(Math.max(0, offset), Math.max(0, offset) + Math.max(1, limit));
  if (dryRun) {
    console.log(
      `DRY RUN: ${bounded.length} markdown files selected (offset=${offset}, limit=${limit}, recursive=${recursive ? "yes" : "no"})`
    );
    bounded.forEach((item) => console.log(` - ${item}`));
    return;
  }

  const report = [];
  const seenDocumentKeysInRun = new Set();

  for (const relativePath of bounded) {
    const filename = path.basename(relativePath);
    if (filename.startsWith("~$")) {
      report.push({ filename, relativePath, skipped: true, reason: "office_lock_temp_file" });
      console.log(`SKIPPED ${filename} -> office lock/temp file`);
      continue;
    }

    const filePath = path.join(markdownDir, relativePath);
    const bytes = await fs.readFile(filePath);
    const rawMarkdown = bytes.toString("utf8");
    const sanitized = sanitizeMarkdownContent(rawMarkdown);
    const markdown = sanitized.markdown;
    const importDecision = getImportDecision(filename, markdown);
    const { citation, title } = importDecision;

    if (importDecision.skip) {
      report.push({ filename, relativePath, skipped: true, reason: importDecision.reason, citation, title });
      console.log(`SKIPPED ${filename} -> ${importDecision.reason}`);
      continue;
    }

    const documentKey = buildDocumentIdentity(citation, title);

    if (seenDocumentKeysInRun.has(documentKey)) {
      report.push({ filename, relativePath, skipped: true, reason: "duplicate_document_in_batch", citation, title, documentKey });
      console.log(`SKIPPED ${filename} -> duplicate document in current batch (${citation} / ${title})`);
      continue;
    }
    seenDocumentKeysInRun.add(documentKey);

    if (existing.keys.has(documentKey)) {
      report.push({ filename, relativePath, skipped: true, reason: "existing_document_in_d1", citation, title, documentKey });
      console.log(`SKIPPED ${filename} -> already imported (${citation} / ${title})`);
      continue;
    }

    const payload = {
      jurisdiction,
      title,
      citation,
      decisionDate: guessDateFromContent(markdown),
      sourceFile: {
        filename,
        mimeType: "text/markdown; charset=utf-8",
        bytes: Buffer.from(markdown, "utf8")
      },
      performVectorUpsert: !skipVectorOnIngest
    };

    const response = await postMultipartDecision("/ingest/decision-upload", payload);
    report.push({
      filename,
      relativePath,
      citation,
      title,
      documentKey,
      titleRecoveredFromContent: Boolean(importDecision.titleRecoveredFromContent),
      trimmedArtifactTail: Boolean(sanitized.trimmedArtifactTail),
      response
    });

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
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(
    outputPath,
    JSON.stringify(
      {
        apiBase,
        importLabel,
        markdownDir,
        importedAt: new Date().toISOString(),
        controls: { limit, offset, recursive, dryRun },
        ingestMode: {
          skipVectorOnIngest,
          skipExistingCitations,
          d1SqlitePath: existing.sqlitePath,
          existingDocumentLoadError: existing.error
        },
        summary: {
          totalFound: files.length,
          attempted: bounded.length,
          succeeded: report.filter((row) => row.response?.ok).length,
          failed: report.filter((row) => row.response && !row.response.ok).length,
          skipped_existing_document_count: report.filter((row) => row.skipped && row.reason === "existing_document_in_d1").length,
          skipped_duplicate_in_batch_count: report.filter((row) => row.skipped && row.reason === "duplicate_document_in_batch").length,
          skipped_temp_file_count: report.filter((row) => row.skipped && row.reason === "office_lock_temp_file").length,
          skipped_table_fragment_count: report.filter((row) => row.skipped && row.reason === "table_fragment_filename").length,
          skipped_short_alias_artifact_count: report.filter((row) => row.skipped && row.reason === "short_alias_artifact_content").length,
          skipped_short_alias_missing_citation_count: report.filter((row) => row.skipped && row.reason === "short_alias_missing_citation").length,
          skipped_short_alias_missing_document_type_count: report.filter((row) => row.skipped && row.reason === "short_alias_missing_document_type").length,
          title_recovered_from_content_count: report.filter((row) => row.titleRecoveredFromContent).length,
          trimmed_artifact_tail_count: report.filter((row) => row.trimmedArtifactTail).length,
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
          citation: row.citation,
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

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
