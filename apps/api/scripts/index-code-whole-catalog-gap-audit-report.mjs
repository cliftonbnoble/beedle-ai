import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.INDEX_CODE_WHOLE_GAP_JSON_NAME || "index-code-whole-catalog-gap-audit-report.json";
const markdownName = process.env.INDEX_CODE_WHOLE_GAP_MARKDOWN_NAME || "index-code-whole-catalog-gap-audit-report.md";
const csvName = process.env.INDEX_CODE_WHOLE_GAP_CSV_NAME || "index-code-whole-catalog-gap-audit-report.csv";
const busyTimeoutMs = Number(process.env.INDEX_CODE_WHOLE_GAP_BUSY_TIMEOUT_MS || "5000");
const dbDir = path.resolve(process.cwd(), ".wrangler/state/v3/d1/miniflare-D1DatabaseObject");
const indexCatalogPath = path.resolve(process.cwd(), "../../packages/shared/src/index-codes.ts");

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeToken(input) {
  return String(input || "")
    .toLowerCase()
    .replace(/[\s_]+/g, "")
    .replace(/[^a-z0-9.()\-]/g, "");
}

function normalizeIndexCode(input) {
  return normalizeToken(input).replace(/^ic/, "").replace(/^[-]+/, "");
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

async function resolveDbPath() {
  const entries = await fs.readdir(dbDir, { withFileTypes: true });
  const sqliteNames = entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".sqlite") && !entry.name.includes(".bak-"))
    .map((entry) => entry.name)
    .sort();
  if (!sqliteNames.length) throw new Error(`No live sqlite DB found under ${dbDir}`);
  return path.join(dbDir, sqliteNames[0]);
}

async function loadIndexCatalog() {
  const raw = await fs.readFile(indexCatalogPath, "utf8");
  const start = raw.indexOf("[");
  const end = raw.lastIndexOf("] as const;");
  if (start < 0 || end < 0) {
    throw new Error(`Could not parse canonical index code catalog from ${indexCatalogPath}`);
  }
  return JSON.parse(raw.slice(start, end + 1));
}

async function runSqlJson(dbPath, sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 200 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

function buildCsv(report) {
  const header = [
    "code",
    "description",
    "local_catalog_present",
    "direct_linked_real_doc_count",
    "raw_json_mention_real_doc_count",
    "raw_json_missing_link_doc_count",
    "sample_gap_citations"
  ];
  const lines = [header.join(",")];
  for (const row of report.rows) {
    lines.push(
      [
        row.code,
        row.description,
        row.localCatalogPresent ? "1" : "0",
        row.directLinkedRealDocCount,
        row.rawJsonMentionRealDocCount,
        row.rawJsonMissingLinkDocCount,
        row.sampleGapCitations.join("; ")
      ]
        .map(csvEscape)
        .join(",")
    );
  }
  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Whole-Catalog Index Code Gap Audit",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Database: \`${report.dbPath}\``,
    "",
    "## Summary",
    "",
    `- shared canonical catalog count: \`${report.summary.sharedCatalogCount}\``,
    `- local legal_index_codes count: \`${report.summary.localCatalogCount}\``,
    `- shared codes missing from local catalog: \`${report.summary.missingFromLocalCatalogCount}\``,
    `- shared codes with any direct linked real docs: \`${report.summary.codesWithDirectLinkedDocsCount}\``,
    `- shared codes with any raw JSON mentions: \`${report.summary.codesWithRawJsonMentionsCount}\``,
    `- shared codes with raw JSON mentions but zero direct links: \`${report.summary.codesWithRawJsonButZeroLinksCount}\``,
    `- shared codes with any raw JSON/link gap: \`${report.summary.codesWithAnyRawJsonLinkGapCount}\``,
    "",
    "## Largest Raw JSON vs Link Gaps",
    "",
    "| Code | Description | Local Catalog | Direct Links | Raw JSON Mentions | Gap Docs | Sample Gap Citations |",
    "| --- | --- | --- | ---: | ---: | ---: | --- |"
  ];

  for (const row of report.topGapRows) {
    lines.push(
      `| ${row.code} | ${row.description.replace(/\|/g, "\\|")} | ${row.localCatalogPresent} | ${row.directLinkedRealDocCount} | ${row.rawJsonMentionRealDocCount} | ${row.rawJsonMissingLinkDocCount} | ${row.sampleGapCitations.join(", ") || "<none>"} |`
    );
  }

  lines.push("");
  lines.push("## Notes");
  lines.push("");
  lines.push("- `Raw JSON mentions` means the code appears in `documents.index_codes_json` for a real decision.");
  lines.push("- `Direct links` means the code is reachable through valid `document_reference_links` records for that decision.");
  lines.push("- A non-zero gap means the document metadata likely contains the code, but the normalized link layer is not catching up yet.");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const dbPath = await resolveDbPath();
  const sharedCatalog = await loadIndexCatalog();

  const localRows = await runSqlJson(dbPath, `SELECT code_identifier, normalized_code FROM legal_index_codes WHERE active = 1`);
  const linkRows = await runSqlJson(
    dbPath,
    `
      SELECT d.id as document_id, d.citation, l.normalized_value, l.canonical_value
      FROM document_reference_links l
      JOIN documents d ON d.id = l.document_id
      WHERE d.file_type = 'decision_docx'
        AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'KNOWN-REF-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'PILOT-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'HISTORICAL-%')
        AND l.reference_type = 'index_code'
        AND l.is_valid = 1
    `
  );
  const docRows = await runSqlJson(
    dbPath,
    `
      SELECT id, citation, index_codes_json
      FROM documents
      WHERE file_type = 'decision_docx'
        AND (citation IS NULL OR citation NOT LIKE 'BEE-%')
        AND (citation IS NULL OR citation NOT LIKE 'KNOWN-REF-%')
        AND (citation IS NULL OR citation NOT LIKE 'PILOT-%')
        AND (citation IS NULL OR citation NOT LIKE 'HISTORICAL-%')
        AND index_codes_json IS NOT NULL
        AND trim(index_codes_json) <> ''
        AND trim(index_codes_json) <> '[]'
    `
  );

  const sharedByNormalized = new Map(
    sharedCatalog.map((row) => [normalizeIndexCode(row.code), { code: String(row.code), description: normalizeWhitespace(row.description || "") }])
  );
  const localNormalized = new Set(localRows.map((row) => String(row.normalized_code)));

  const directDocSets = new Map();
  for (const row of linkRows) {
    const candidates = unique([row.canonical_value, row.normalized_value].map((value) => normalizeIndexCode(value)));
    for (const normalized of candidates) {
      if (!sharedByNormalized.has(normalized)) continue;
      const current = directDocSets.get(normalized) || new Map();
      current.set(String(row.document_id), String(row.citation || ""));
      directDocSets.set(normalized, current);
    }
  }

  const rawDocSets = new Map();
  for (const row of docRows) {
    let parsed = [];
    try {
      parsed = JSON.parse(row.index_codes_json || "[]");
    } catch {
      parsed = [];
    }
    if (!Array.isArray(parsed)) continue;
    for (const value of parsed) {
      const normalized = normalizeIndexCode(value);
      if (!sharedByNormalized.has(normalized)) continue;
      const current = rawDocSets.get(normalized) || new Map();
      current.set(String(row.id), String(row.citation || ""));
      rawDocSets.set(normalized, current);
    }
  }

  const rows = sharedCatalog.map((option) => {
    const normalizedCode = normalizeIndexCode(option.code);
    const rawDocs = rawDocSets.get(normalizedCode) || new Map();
    const linkedDocs = directDocSets.get(normalizedCode) || new Map();
    const gapCitations = [];
    for (const [docId, citation] of rawDocs.entries()) {
      if (!linkedDocs.has(docId)) gapCitations.push(citation);
      if (gapCitations.length >= 5) break;
    }
    return {
      code: String(option.code),
      description: normalizeWhitespace(option.description || ""),
      normalizedCode,
      localCatalogPresent: localNormalized.has(normalizedCode),
      directLinkedRealDocCount: linkedDocs.size,
      rawJsonMentionRealDocCount: rawDocs.size,
      rawJsonMissingLinkDocCount: Array.from(rawDocs.keys()).filter((docId) => !linkedDocs.has(docId)).length,
      sampleGapCitations: gapCitations
    };
  });

  const topGapRows = rows
    .slice()
    .sort((a, b) => {
      const gapDiff = b.rawJsonMissingLinkDocCount - a.rawJsonMissingLinkDocCount;
      if (gapDiff !== 0) return gapDiff;
      const rawDiff = b.rawJsonMentionRealDocCount - a.rawJsonMentionRealDocCount;
      if (rawDiff !== 0) return rawDiff;
      return a.code.localeCompare(b.code);
    })
    .slice(0, 60);

  const report = {
    generatedAt: new Date().toISOString(),
    dbPath,
    summary: {
      sharedCatalogCount: sharedCatalog.length,
      localCatalogCount: localRows.length,
      missingFromLocalCatalogCount: rows.filter((row) => !row.localCatalogPresent).length,
      codesWithDirectLinkedDocsCount: rows.filter((row) => row.directLinkedRealDocCount > 0).length,
      codesWithRawJsonMentionsCount: rows.filter((row) => row.rawJsonMentionRealDocCount > 0).length,
      codesWithRawJsonButZeroLinksCount: rows.filter(
        (row) => row.rawJsonMentionRealDocCount > 0 && row.directLinkedRealDocCount === 0
      ).length,
      codesWithAnyRawJsonLinkGapCount: rows.filter((row) => row.rawJsonMissingLinkDocCount > 0).length
    },
    topGapRows,
    rows
  };

  await Promise.all([
    fs.writeFile(path.join(reportsDir, jsonName), JSON.stringify(report, null, 2)),
    fs.writeFile(path.join(reportsDir, markdownName), formatMarkdown(report)),
    fs.writeFile(path.join(reportsDir, csvName), buildCsv(report))
  ]);

  console.log(
    JSON.stringify(
      {
        sharedCatalogCount: report.summary.sharedCatalogCount,
        localCatalogCount: report.summary.localCatalogCount,
        missingFromLocalCatalogCount: report.summary.missingFromLocalCatalogCount,
        codesWithRawJsonButZeroLinksCount: report.summary.codesWithRawJsonButZeroLinksCount,
        codesWithAnyRawJsonLinkGapCount: report.summary.codesWithAnyRawJsonLinkGapCount
      },
      null,
      2
    )
  );
  console.log(`Whole-catalog index code gap audit JSON report written to ${path.join(reportsDir, jsonName)}`);
  console.log(`Whole-catalog index code gap audit Markdown report written to ${path.join(reportsDir, markdownName)}`);
  console.log(`Whole-catalog index code gap audit CSV report written to ${path.join(reportsDir, csvName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
