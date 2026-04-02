import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.INDEX_CODE_GAP_AUDIT_JSON_NAME || "index-code-gap-audit-report.json";
const markdownName = process.env.INDEX_CODE_GAP_AUDIT_MARKDOWN_NAME || "index-code-gap-audit-report.md";
const csvName = process.env.INDEX_CODE_GAP_AUDIT_CSV_NAME || "index-code-gap-audit-report.csv";
const busyTimeoutMs = Number(process.env.INDEX_CODE_GAP_AUDIT_BUSY_TIMEOUT_MS || "5000");
const dbDir = path.resolve(process.cwd(), ".wrangler/state/v3/d1/miniflare-D1DatabaseObject");
const indexCatalogPath = path.resolve(process.cwd(), "../../packages/shared/src/index-codes.ts");
const selectedCodes = Array.from(
  new Set(
    String(process.env.INDEX_CODE_GAP_AUDIT_CODES || "G44,G45,G76")
      .split(",")
      .map((value) => value.trim().toUpperCase())
      .filter(Boolean)
  )
);

function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeCode(value) {
  return normalize(value).replace(/\s+/g, "");
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function sqlQuote(value) {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function isRealDecisionCitation(citation) {
  return !/^BEE-|^KNOWN-REF-|^PILOT-|^HISTORICAL-/i.test(String(citation || ""));
}

function extractDescriptionCore(description) {
  const text = normalizeWhitespace(description);
  if (!text) return "";
  const parts = text.split("--").map((part) => normalizeWhitespace(part)).filter(Boolean);
  return parts.length > 1 ? parts[parts.length - 1] : text;
}

function tokenizeSignalTerms(description) {
  const core = extractDescriptionCore(description)
    .toLowerCase()
    .replace(/[^a-z0-9\s/-]/g, " ")
    .replace(/[\s/-]+/g, " ")
    .trim();
  const stopwords = new Set(["the", "and", "for", "with", "lack", "lacking", "loss", "of"]);
  return Array.from(new Set(core.split(/\s+/).filter((token) => token.length >= 4 && !stopwords.has(token))));
}

function buildDescriptionPhrases(description) {
  const raw = normalizeWhitespace(description);
  const core = extractDescriptionCore(description);
  const phrases = new Set([raw, core].map((value) => normalizeWhitespace(value)).filter(Boolean));
  return Array.from(phrases);
}

function isDhsCode(option) {
  return /^DHS\s*--/i.test(normalizeWhitespace(option?.description || ""));
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
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

async function collectCodeAudit(dbPath, option) {
  const normalizedCode = normalizeCode(option.code);
  const localCatalogRows = await runSqlJson(
    dbPath,
    `
      SELECT code_identifier, normalized_code, family, label, description
      FROM legal_index_codes
      WHERE normalized_code = ${sqlQuote(normalizedCode)}
         OR upper(code_identifier) = ${sqlQuote(String(option.code).toUpperCase())}
    `
  );

  const directLinkedRows = await runSqlJson(
    dbPath,
    `
      SELECT COUNT(DISTINCT d.id) AS doc_count
      FROM documents d
      JOIN document_reference_links l ON l.document_id = d.id
      WHERE d.file_type = 'decision_docx'
        AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'KNOWN-REF-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'PILOT-%')
        AND (d.citation IS NULL OR d.citation NOT LIKE 'HISTORICAL-%')
        AND l.reference_type = 'index_code'
        AND l.is_valid = 1
        AND (
          lower(l.normalized_value) = ${sqlQuote(normalizedCode)}
          OR upper(coalesce(l.canonical_value, '')) = ${sqlQuote(String(option.code).toUpperCase())}
        )
    `
  );

  const familyLegacyCodes = isDhsCode(option) ? ["13"] : [];
  const familyDocRows =
    familyLegacyCodes.length > 0
      ? await runSqlJson(
          dbPath,
          `
            SELECT COUNT(DISTINCT d.id) AS doc_count
            FROM documents d
            JOIN document_reference_links l ON l.document_id = d.id
            WHERE d.file_type = 'decision_docx'
              AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
              AND (d.citation IS NULL OR d.citation NOT LIKE 'KNOWN-REF-%')
              AND (d.citation IS NULL OR d.citation NOT LIKE 'PILOT-%')
              AND (d.citation IS NULL OR d.citation NOT LIKE 'HISTORICAL-%')
              AND l.reference_type = 'index_code'
              AND l.is_valid = 1
              AND l.normalized_value IN (${familyLegacyCodes.map(sqlQuote).join(", ")})
          `
        )
      : [{ doc_count: 0 }];

  const phrases = buildDescriptionPhrases(option.description || "");
  const signalTerms = tokenizeSignalTerms(option.description || "");
  const phraseClauses = phrases
    .map((phrase) => normalizeWhitespace(phrase).toLowerCase())
    .filter(Boolean)
    .map((phrase) => `lower(r.chunk_text) LIKE ${sqlQuote(`%${phrase}%`)}`);
  const tokenClauses = signalTerms.map((token) => `lower(r.chunk_text) LIKE ${sqlQuote(`%${token}%`)}`);
  const phraseHitExpression = phraseClauses.length > 0 ? `CASE WHEN ${phraseClauses.join(" OR ")} THEN 1 ELSE 0 END` : "0";
  const tokenHitTerms = tokenClauses.map((clause) => `(CASE WHEN ${clause} THEN 1 ELSE 0 END)`);
  const tokenHitExpression = tokenHitTerms.length > 0 ? tokenHitTerms.join(" + ") : "0";
  const minimumTokenHits = signalTerms.length <= 2 ? 1 : Math.ceil(signalTerms.length / 2);
  const evidenceClauseParts = [];
  if (phraseClauses.length > 0) evidenceClauseParts.push(`(${phraseClauses.join(" OR ")})`);
  if (tokenClauses.length > 0) evidenceClauseParts.push(`(${tokenHitExpression}) >= ${minimumTokenHits}`);
  const evidenceClause = evidenceClauseParts.length > 0 ? evidenceClauseParts.join(" OR ") : "0";
  const familyClause =
    familyLegacyCodes.length > 0
      ? `EXISTS (
          SELECT 1 FROM document_reference_links l_family
          WHERE l_family.document_id = d.id
            AND l_family.reference_type = 'index_code'
            AND l_family.is_valid = 1
            AND l_family.normalized_value IN (${familyLegacyCodes.map(sqlQuote).join(", ")})
        )`
      : "0";

  const candidateRows =
    evidenceClause !== "0"
      ? await runSqlJson(
          dbPath,
          `
            SELECT
              d.citation,
              d.author_name,
              d.decision_date,
              d.index_codes_json,
              group_concat(DISTINCT coalesce(l.canonical_value, l.normalized_value)) AS linked_codes,
              COUNT(DISTINCT r.chunk_id) AS matching_chunks,
              MAX(${phraseHitExpression}) AS phrase_hit_count,
              MAX(${tokenHitExpression}) AS token_hit_count,
              MIN(r.section_label) AS sample_section,
              substr(MIN(r.chunk_text), 1, 220) AS sample_excerpt
            FROM documents d
            JOIN retrieval_search_chunks r ON r.document_id = d.id AND r.active = 1
            LEFT JOIN document_reference_links l
              ON l.document_id = d.id
             AND l.reference_type = 'index_code'
             AND l.is_valid = 1
            WHERE d.file_type = 'decision_docx'
              AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
              AND (d.citation IS NULL OR d.citation NOT LIKE 'KNOWN-REF-%')
              AND (d.citation IS NULL OR d.citation NOT LIKE 'PILOT-%')
              AND (d.citation IS NULL OR d.citation NOT LIKE 'HISTORICAL-%')
              AND (${evidenceClause})
              AND (${familyClause})
            GROUP BY d.id
            ORDER BY matching_chunks DESC, d.citation ASC
            LIMIT 20
          `
        )
      : [];

  return {
    code: option.code,
    description: option.description || "",
    descriptionCore: extractDescriptionCore(option.description || ""),
    localCatalogPresent: localCatalogRows.length > 0,
    localCatalogRows,
    directLinkedDocCount: Number(directLinkedRows[0]?.doc_count || 0),
    familyLegacyCodes,
    familyDocCount: Number(familyDocRows[0]?.doc_count || 0),
    signalTerms,
    candidateDocCount: candidateRows.length,
    candidates: candidateRows.map((row) => ({
      citation: row.citation,
      authorName: row.author_name || "",
      decisionDate: row.decision_date || "",
      indexCodesJson: row.index_codes_json || "",
      linkedCodes: row.linked_codes ? String(row.linked_codes).split(",").filter(Boolean) : [],
      matchingChunks: Number(row.matching_chunks || 0),
      phraseHitCount: Number(row.phrase_hit_count || 0),
      tokenHitCount: Number(row.token_hit_count || 0),
      sampleSection: row.sample_section || "",
      sampleExcerpt: row.sample_excerpt || ""
    }))
  };
}

function buildCsv(report) {
  const header = [
    "code",
    "description",
    "local_catalog_present",
    "direct_linked_doc_count",
    "family_legacy_codes",
    "family_doc_count",
    "candidate_doc_count",
    "sample_citation",
    "sample_linked_codes",
    "sample_phrase_hit_count",
    "sample_token_hit_count",
    "sample_excerpt"
  ];
  const lines = [header.join(",")];
  for (const row of report.rows) {
    if (!row.candidates.length) {
      lines.push(
        [
          row.code,
          row.description,
          row.localCatalogPresent ? "1" : "0",
          row.directLinkedDocCount,
          row.familyLegacyCodes.join("; "),
          row.familyDocCount,
          row.candidateDocCount,
          "",
          "",
          "",
          "",
          ""
        ]
          .map(csvEscape)
          .join(",")
      );
      continue;
    }
    for (const candidate of row.candidates) {
      lines.push(
        [
          row.code,
          row.description,
          row.localCatalogPresent ? "1" : "0",
          row.directLinkedDocCount,
          row.familyLegacyCodes.join("; "),
          row.familyDocCount,
          row.candidateDocCount,
          candidate.citation,
          candidate.linkedCodes.join("; "),
          candidate.phraseHitCount,
          candidate.tokenHitCount,
          candidate.sampleExcerpt
        ]
          .map(csvEscape)
          .join(",")
      );
    }
  }
  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Index Code Gap Audit Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Database: \`${report.dbPath}\``,
    `- Selected codes: \`${report.selectedCodes.join(", ")}\``,
    "",
    "## Summary",
    "",
    `- shared canonical catalog count: \`${report.summary.sharedCatalogCount}\``,
    `- local legal_index_codes count: \`${report.summary.localCatalogCount}\``,
    `- selected codes missing from local catalog: \`${report.summary.localCatalogMissingSelectedCount}\``,
    `- selected codes with zero direct linked docs: \`${report.summary.zeroDirectLinkedSelectedCount}\``,
    `- selected codes with phrase-evidence candidates in legacy family docs: \`${report.summary.codesWithLegacyPhraseCandidates}\``,
    "",
    "## Findings",
    "",
    "- If a code exists in the shared UI catalog but not in `legal_index_codes` and has zero direct linked docs, the UI can offer a valid-looking filter that the indexed document universe cannot satisfy directly.",
    "- DHS subcodes are especially vulnerable because many real decisions still carry only legacy family code `13` in document metadata.",
    "- The candidate docs below are not auto-remediations. They are review-first leads showing where the searchable text appears to support a more specific canonical DHS code.",
    ""
  ];

  for (const row of report.rows) {
    lines.push(`## ${row.code} — ${row.description}`);
    lines.push("");
    lines.push(`- local catalog present: \`${row.localCatalogPresent}\``);
    lines.push(`- direct linked real decision docs: \`${row.directLinkedDocCount}\``);
    lines.push(`- legacy family codes used for admission: \`${row.familyLegacyCodes.join(", ") || "<none>"}\``);
    lines.push(`- legacy family real decision docs: \`${row.familyDocCount}\``);
    lines.push(`- signal terms: \`${row.signalTerms.join(", ") || "<none>"}\``);
    lines.push(`- candidate docs with phrase evidence inside family docs: \`${row.candidateDocCount}\``);
    lines.push("");
    if (!row.candidates.length) {
      lines.push("- No phrase-evidence candidates found in the current batch.");
      lines.push("");
      continue;
    }
    lines.push("| Citation | Judge | Linked Codes | Matching Chunks | Phrase Hits | Token Hits | Section | Excerpt |");
    lines.push("| --- | --- | --- | ---: | ---: | ---: | --- | --- |");
    for (const candidate of row.candidates) {
      lines.push(
        `| ${candidate.citation} | ${candidate.authorName || ""} | ${candidate.linkedCodes.join(", ") || "<none>"} | ${candidate.matchingChunks} | ${candidate.phraseHitCount} | ${candidate.tokenHitCount} | ${candidate.sampleSection || ""} | ${candidate.sampleExcerpt.replace(/\|/g, "\\|")} |`
      );
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const dbPath = await resolveDbPath();
  const catalog = await loadIndexCatalog();
  const catalogByCode = new Map(catalog.map((option) => [String(option.code || "").toUpperCase(), option]));
  const selectedOptions = selectedCodes.map((code) => catalogByCode.get(code)).filter(Boolean);
  if (!selectedOptions.length) {
    throw new Error(`None of the requested codes were found in the shared index-code catalog: ${selectedCodes.join(", ")}`);
  }

  const localCatalogCountRows = await runSqlJson(dbPath, "SELECT COUNT(*) AS count FROM legal_index_codes");
  const rows = [];
  for (const option of selectedOptions) {
    rows.push(await collectCodeAudit(dbPath, option));
  }

  const report = {
    generatedAt: new Date().toISOString(),
    dbPath,
    selectedCodes,
    summary: {
      sharedCatalogCount: catalog.length,
      localCatalogCount: Number(localCatalogCountRows[0]?.count || 0),
      localCatalogMissingSelectedCount: rows.filter((row) => !row.localCatalogPresent).length,
      zeroDirectLinkedSelectedCount: rows.filter((row) => row.directLinkedDocCount === 0).length,
      codesWithLegacyPhraseCandidates: rows.filter((row) => row.candidateDocCount > 0).length
    },
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
        selectedCodeCount: report.rows.length,
        localCatalogMissingSelectedCount: report.summary.localCatalogMissingSelectedCount,
        zeroDirectLinkedSelectedCount: report.summary.zeroDirectLinkedSelectedCount,
        codesWithLegacyPhraseCandidates: report.summary.codesWithLegacyPhraseCandidates
      },
      null,
      2
    )
  );
  console.log(`Index code gap audit JSON report written to ${path.join(reportsDir, jsonName)}`);
  console.log(`Index code gap audit Markdown report written to ${path.join(reportsDir, markdownName)}`);
  console.log(`Index code gap audit CSV report written to ${path.join(reportsDir, csvName)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
