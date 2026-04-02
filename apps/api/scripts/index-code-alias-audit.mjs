import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.INDEX_CODE_ALIAS_AUDIT_JSON_NAME || "index-code-alias-audit-report.json";
const markdownName = process.env.INDEX_CODE_ALIAS_AUDIT_MARKDOWN_NAME || "index-code-alias-audit-report.md";
const csvName = process.env.INDEX_CODE_ALIAS_AUDIT_CSV_NAME || "index-code-alias-audit-report.csv";
const busyTimeoutMs = Number(process.env.INDEX_CODE_ALIAS_AUDIT_BUSY_TIMEOUT_MS || "5000");
const requestedCodes = (process.env.INDEX_CODE_ALIAS_AUDIT_CODES || "G22,G23,G24,G27,G28,G93")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

const catalogPath = path.resolve(process.cwd(), "../../packages/shared/src/index-codes.ts");

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

function normalizeCitation(input) {
  return normalizeToken(input)
    .replace(/^section/, "")
    .replace(/^sec/, "")
    .replace(/^rule/, "")
    .replace(/^ordinance/, "")
    .replace(/^part[0-9a-z.\-]+\-/, "");
}

function extractCatalogReferenceCitations(raw) {
  const text = String(raw || "");
  const matches = new Set();

  for (const match of text.matchAll(/\b\d+\.\d+[a-z]?(?:\([a-z0-9]+\))*[a-z]?\b/gi)) {
    matches.add(match[0]);
  }

  for (const match of text.matchAll(/§\s*([0-9]+(?:\.[0-9]+)?(?:\([a-z0-9]+\))*)/gi)) {
    matches.add(match[1]);
  }

  return Array.from(matches);
}

function extractBaseCitation(value) {
  const match = String(value || "").match(/^(\d+\.\d+[a-z]?)/i);
  return match ? match[1] : null;
}

function buildCitationVariants(values) {
  const variants = new Set();
  for (const value of values) {
    const trimmed = normalizeWhitespace(value);
    if (!trimmed) continue;
    variants.add(trimmed);
    const base = extractBaseCitation(trimmed);
    if (base) variants.add(base);
  }
  return Array.from(variants);
}

function buildDescriptionPhrases(description) {
  const text = normalizeWhitespace(description);
  if (!text) return [];
  const phrases = new Set([text]);
  for (const part of text.split("--").map((item) => item.trim()).filter(Boolean)) {
    if (part.length >= 12 && !/\[reserved\]/i.test(part)) phrases.add(part);
    for (const subPart of part.split(" - ").map((item) => item.trim()).filter(Boolean)) {
      if (subPart.length >= 12 && !/\[reserved\]/i.test(subPart)) phrases.add(subPart);
    }
  }
  return Array.from(phrases).slice(0, 8);
}

function parseJsonArray(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
  } catch {
    return [];
  }
}

function countBy(values) {
  const counts = new Map();
  for (const value of values) {
    const key = String(value || "<empty>");
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value));
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync("sqlite3", ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
  return JSON.parse(stdout || "[]");
}

async function loadCatalog() {
  const raw = await fs.readFile(catalogPath, "utf8");
  const start = raw.indexOf("[");
  const endMarker = raw.indexOf("] as const;");
  if (start < 0 || endMarker < 0 || endMarker <= start) {
    throw new Error(`Unable to parse canonical index catalog from ${catalogPath}`);
  }
  const json = raw.slice(start, endMarker + 1);
  const parsed = JSON.parse(json);
  return Array.isArray(parsed) ? parsed : [];
}

async function fetchReferenceMatchedDocs(referenceType, citations) {
  if (!citations.length) return [];
  const clauses = citations.map(
    (citation) =>
      `(l.normalized_value = '${normalizeCitation(citation).replace(/'/g, "''")}' OR lower(coalesce(l.canonical_value, '')) = lower('${citation.replace(/'/g, "''")}'))`
  );
  const sql = `
    SELECT DISTINCT d.id, d.citation, d.title, d.index_codes_json as indexCodesJson
    FROM document_reference_links l
    JOIN documents d ON d.id = l.document_id
    WHERE d.file_type = 'decision_docx'
      AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
      AND l.reference_type = '${referenceType}'
      AND l.is_valid = 1
      AND (${clauses.join(" OR ")})
  `;
  return runSqlJson(sql);
}

async function fetchPhraseMatchedDocs(phrases) {
  if (!phrases.length) return [];
  const clauses = phrases.map((phrase) => `instr(lower(c.chunk_text), lower('${phrase.replace(/'/g, "''")}')) > 0`);
  const sql = `
    SELECT DISTINCT d.id, d.citation, d.title, d.index_codes_json as indexCodesJson
    FROM document_chunks c
    JOIN documents d ON d.id = c.document_id
    WHERE d.file_type = 'decision_docx'
      AND (d.citation IS NULL OR d.citation NOT LIKE 'BEE-%')
      AND (${clauses.join(" OR ")})
  `;
  return runSqlJson(sql);
}

async function fetchDirectLegacyRows(code) {
  const escaped = code.replace(/'/g, "''");
  const sql = `
    SELECT code_identifier as codeIdentifier, family, label, description
    FROM legal_index_codes
    WHERE upper(coalesce(label, '')) LIKE '%${escaped.toUpperCase()}%'
       OR upper(coalesce(description, '')) LIKE '%${escaped.toUpperCase()}%'
    ORDER BY code_identifier
  `;
  return runSqlJson(sql);
}

function summarizeDocs(rows) {
  const merged = new Map();
  for (const row of rows) {
    if (!row?.id || merged.has(row.id)) continue;
    merged.set(row.id, row);
  }
  const docs = Array.from(merged.values());
  const aliasCounts = countBy(
    docs.flatMap((row) => {
      const codes = parseJsonArray(row.indexCodesJson);
      return codes.length > 0 ? codes : ["<empty>"];
    })
  );
  return {
    docCount: docs.length,
    aliasCounts,
    sampleDocs: docs.slice(0, 12).map((row) => ({
      citation: row.citation,
      title: row.title,
      indexCodes: parseJsonArray(row.indexCodesJson)
    }))
  };
}

function buildCsv(report) {
  const header = [
    "canonical_code",
    "description",
    "rules_citations",
    "ordinance_citations",
    "phrases",
    "source_lane",
    "doc_count",
    "alias_value",
    "alias_count"
  ];
  const lines = [header.join(",")];

  for (const row of report.codes) {
    for (const lane of row.lanes) {
      if (!lane.aliasCounts.length) {
        lines.push(
          [
            row.code,
            row.description,
            row.rulesCitations.join("; "),
            row.ordinanceCitations.join("; "),
            row.searchPhrases.join("; "),
            lane.name,
            lane.docCount,
            "",
            ""
          ]
            .map(csvEscape)
            .join(",")
        );
        continue;
      }

      for (const alias of lane.aliasCounts) {
        lines.push(
          [
            row.code,
            row.description,
            row.rulesCitations.join("; "),
            row.ordinanceCitations.join("; "),
            row.searchPhrases.join("; "),
            lane.name,
            lane.docCount,
            alias.value,
            alias.count
          ]
            .map(csvEscape)
            .join(",")
        );
      }
    }
  }

  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Index Code Alias Audit",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Database: \`${report.dbPath}\``,
    `- Target codes: \`${report.summary.codeCount}\``,
    "",
    "## Summary",
    ""
  ];

  for (const row of report.codes) {
    lines.push(`## ${row.code}`);
    lines.push("");
    lines.push(`- description: ${row.description}`);
    lines.push(`- rules citations: \`${row.rulesCitations.join(", ") || "<none>"}\``);
    lines.push(`- ordinance citations: \`${row.ordinanceCitations.join(", ") || "<none>"}\``);
    lines.push(`- search phrases: \`${row.searchPhrases.join(" | ") || "<none>"}\``);
    lines.push(`- direct legacy rows: \`${row.directLegacyRows.length}\``);
    if (row.directLegacyRows.length) {
      for (const legacy of row.directLegacyRows) {
        lines.push(`  - legacy=\`${legacy.codeIdentifier}\` | ${normalizeWhitespace(legacy.description || legacy.label || "")}`);
      }
    }
    lines.push("");
    for (const lane of row.lanes) {
      lines.push(`- ${lane.name}: \`${lane.docCount}\` docs`);
      for (const alias of lane.aliasCounts.slice(0, 6)) {
        lines.push(`  - alias \`${alias.value}\`: \`${alias.count}\``);
      }
    }
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const catalog = await loadCatalog();
  const byCode = new Map(catalog.map((row) => [normalizeIndexCode(row.code), row]));

  const codeRows = [];

  for (const requestedCode of requestedCodes) {
    const option = byCode.get(normalizeIndexCode(requestedCode));
    if (!option) {
      codeRows.push({
        code: requestedCode,
        description: "<missing from catalog>",
        rulesCitations: [],
        ordinanceCitations: [],
        searchPhrases: [],
        directLegacyRows: [],
        lanes: []
      });
      continue;
    }

    const rulesCitations = buildCitationVariants(extractCatalogReferenceCitations(option.rules || ""));
    const ordinanceCitations = buildCitationVariants(extractCatalogReferenceCitations(option.ordinance || ""));
    const searchPhrases = buildDescriptionPhrases(option.description || "");

    const [rulesDocs, ordinanceDocs, phraseDocs, directLegacyRows] = await Promise.all([
      fetchReferenceMatchedDocs("rules_section", rulesCitations),
      fetchReferenceMatchedDocs("ordinance_section", ordinanceCitations),
      fetchPhraseMatchedDocs(searchPhrases),
      fetchDirectLegacyRows(requestedCode)
    ]);

    codeRows.push({
      code: option.code,
      description: option.description || "",
      rulesCitations,
      ordinanceCitations,
      searchPhrases,
      directLegacyRows,
      lanes: [
        { name: "rules_reference_docs", ...summarizeDocs(rulesDocs) },
        { name: "ordinance_reference_docs", ...summarizeDocs(ordinanceDocs) },
        { name: "phrase_match_docs", ...summarizeDocs(phraseDocs) }
      ]
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    dbPath,
    summary: {
      codeCount: codeRows.length
    },
    codes: codeRows
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Index code alias-audit JSON report written to ${jsonPath}`);
  console.log(`Index code alias-audit Markdown report written to ${markdownPath}`);
  console.log(`Index code alias-audit CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
