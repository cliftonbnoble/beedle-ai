import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.INDEX_CODE_CANONICALIZATION_JSON_NAME || "index-code-canonicalization-report.json";
const markdownName = process.env.INDEX_CODE_CANONICALIZATION_MARKDOWN_NAME || "index-code-canonicalization-report.md";
const csvName = process.env.INDEX_CODE_CANONICALIZATION_CSV_NAME || "index-code-canonicalization-report.csv";
const apply = (process.env.INDEX_CODE_CANONICALIZATION_APPLY || "0") === "1";
const includeFixtures = (process.env.INDEX_CODE_CANONICALIZATION_INCLUDE_FIXTURES || "0") === "1";
const maxAppendCanonicalCount = Math.max(1, Number(process.env.INDEX_CODE_CANONICALIZATION_MAX_APPEND_CODES || "6"));
const busyTimeoutMs = Number(process.env.INDEX_CODE_CANONICALIZATION_BUSY_TIMEOUT_MS || "5000");

function id(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

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
    .replace(/^part[0-9a-z.\-]+\-/, "");
}

function normalizeBareRulesCitation(input) {
  return normalizeToken(input)
    .replace(/^section/, "")
    .replace(/^sec/, "")
    .replace(/^rule/, "")
    .replace(/^[ivxlcdm]+\-/i, "")
    .replace(/^part[0-9a-z.\-]+\-/, "");
}

const SAFE_37X_ORDINANCE_PREFIX_BASES = new Set(["37.1", "37.2", "37.8"]);

function normalizeOrdinanceCitationForLookup(input) {
  const normalized = normalizeCitation(input);
  if (!normalized.startsWith("ordinance37.")) return normalized;
  const withoutPrefix = normalized.replace(/^ordinance/, "");
  const base = withoutPrefix.replace(/\([a-z0-9]+\)/g, "");
  if (SAFE_37X_ORDINANCE_PREFIX_BASES.has(base)) {
    return withoutPrefix;
  }
  return normalized;
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

function unique(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function uniqueSorted(values) {
  return unique(values).sort((a, b) => a.localeCompare(b));
}

function sqlQuote(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function isFixtureCitation(citation) {
  return /^BEE-|^KNOWN-REF-|^PILOT-|^HISTORICAL-/i.test(String(citation || ""));
}

function isCanonicalStyleCode(code) {
  return /^[A-Z]/.test(String(code || "").trim());
}

function isLegacyNumericCode(code) {
  return /^[0-9]/.test(String(code || "").trim());
}

function countBy(values) {
  const out = new Map();
  for (const value of values) {
    const key = String(value || "<unknown>");
    out.set(key, (out.get(key) || 0) + 1);
  }
  return Array.from(out.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || a.key.localeCompare(b.key));
}

function extractYear(value) {
  const normalized = normalizeWhitespace(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized.slice(0, 4) : "<missing>";
}

async function runSqlJson(sql) {
  const { stdout } = await execFileAsync(
    "sqlite3",
    ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql],
    {
      cwd: process.cwd(),
      maxBuffer: 100 * 1024 * 1024
    }
  );
  return JSON.parse(stdout || "[]");
}

async function runSql(sql) {
  await execFileAsync("sqlite3", ["-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
}

function buildCrosswalkMaps(rows) {
  const ordinance = new Map();
  const rules = new Map();
  const rulesBare = new Map();

  for (const row of rows) {
    if (!row.indexCodeId) continue;
    const code = normalizeWhitespace(row.indexCodeId);
    const ordinanceKey = normalizeOrdinanceCitationForLookup(row.ordinanceCitation || "");
    const rulesKey = normalizeCitation(row.rulesCitation || "");
    const rulesBareKey = normalizeBareRulesCitation(row.rulesCitation || "");
    if (ordinanceKey) {
      const current = ordinance.get(ordinanceKey) || [];
      current.push(code);
      ordinance.set(ordinanceKey, current);
    }
    if (rulesKey) {
      const current = rules.get(rulesKey) || [];
      current.push(code);
      rules.set(rulesKey, current);
    }
    if (rulesBareKey) {
      const current = rulesBare.get(rulesBareKey) || [];
      current.push(code);
      rulesBare.set(rulesBareKey, current);
    }
  }

  return {
    ordinance,
    rules,
    rulesBare
  };
}

function inferCrosswalkCodes(validRuleLinks, validOrdinanceLinks, crosswalkMaps) {
  const out = new Set();

  for (const citation of validOrdinanceLinks) {
    const key = normalizeOrdinanceCitationForLookup(citation);
    for (const code of crosswalkMaps.ordinance.get(key) || []) out.add(code);
  }

  for (const citation of validRuleLinks) {
    const key = normalizeCitation(citation);
    const bareKey = normalizeBareRulesCitation(citation);
    for (const code of crosswalkMaps.rules.get(key) || []) out.add(code);
    for (const code of crosswalkMaps.rulesBare.get(bareKey) || []) out.add(code);
  }

  return uniqueSorted(Array.from(out));
}

function buildCsv(report) {
  const header = [
    "citation",
    "title",
    "author_name",
    "decision_date",
    "repair_type",
    "current_codes",
    "legacy_codes",
    "canonical_codes",
    "inferred_canonical_codes",
    "next_codes"
  ];
  const lines = [header.join(",")];
  for (const row of report.repairCandidates) {
    lines.push(
      [
        row.citation,
        row.title,
        row.authorName || "",
        row.decisionDate || "",
        row.repairType,
        row.currentCodes.join("; "),
        row.legacyCodes.join("; "),
        row.canonicalCodes.join("; "),
        row.inferredCrosswalkCodes.join("; "),
        row.nextIndexCodes.join("; ")
      ]
        .map((value) => {
          const text = String(value ?? "");
          return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
        })
        .join(",")
    );
  }
  return `${lines.join("\n")}\n`;
}

function formatMarkdown(report) {
  const lines = [
    "# Index Code Canonicalization Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- Include fixtures: \`${report.includeFixtures}\``,
    `- Database: \`${report.dbPath}\``,
    "",
    "## Summary",
    "",
    `- real decision docs: \`${report.summary.realDecisionDocs}\``,
    `- real legacy-only docs: \`${report.summary.realLegacyOnlyDocs}\``,
    `- real canonical-only docs: \`${report.summary.realCanonicalOnlyDocs}\``,
    `- real mixed docs: \`${report.summary.realMixedDocs}\``,
    `- real docs with no index links: \`${report.summary.realNoIndexCoverageDocs}\``,
    `- real legacy-only docs with inferred canonical additions: \`${report.summary.realLegacyOnlyCanonicalizableDocs}\``,
    `- real docs missing codes but inferable from crosswalk: \`${report.summary.realMissingCoverageInferableDocs}\``,
    `- conservative real repair candidates: \`${report.summary.realRepairCandidateCount}\``,
    `- applied updates: \`${report.summary.appliedUpdateCount}\``,
    "",
    "## Notes",
    ""
  ];

  for (const note of report.summaryNotes) {
    lines.push(`- ${note}`);
  }

  lines.push("");
  lines.push("## Repair Candidates");
  lines.push("");
  for (const row of report.repairCandidates.slice(0, 60)) {
    lines.push(
      `- \`${row.citation}\` | type=\`${row.repairType}\` | current=\`${row.currentCodes.join(", ") || "<none>"}\` | inferred=\`${row.inferredCrosswalkCodes.join(", ") || "<none>"}\` | next=\`${row.nextIndexCodes.join(", ") || "<none>"}\``
    );
  }

  lines.push("");
  lines.push("## Missing-Coverage By Judge");
  lines.push("");
  for (const row of report.missingCoverage.byJudge.slice(0, 12)) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }

  lines.push("");
  lines.push("## Legacy-Only By Judge");
  lines.push("");
  for (const row of report.legacyOnly.byJudge.slice(0, 12)) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [docs, links, crosswalk] = await Promise.all([
    runSqlJson(`
      SELECT
        d.id,
        d.citation,
        d.title,
        d.author_name AS authorName,
        d.decision_date AS decisionDate,
        d.index_codes_json AS indexCodesJson
      FROM documents d
      WHERE d.file_type = 'decision_docx'
      ORDER BY d.created_at DESC;
    `),
    runSqlJson(`
      SELECT
        l.document_id AS documentId,
        l.reference_type AS referenceType,
        l.raw_value AS rawValue,
        l.normalized_value AS normalizedValue,
        l.canonical_value AS canonicalValue,
        l.is_valid AS isValid
      FROM document_reference_links l
      JOIN documents d ON d.id = l.document_id
      WHERE d.file_type = 'decision_docx';
    `),
    runSqlJson(`
      SELECT
        index_code_id AS indexCodeId,
        ordinance_citation AS ordinanceCitation,
        rules_citation AS rulesCitation
      FROM legal_reference_crosswalk;
    `)
  ]);

  const crosswalkMaps = buildCrosswalkMaps(crosswalk);
  const linksByDocument = new Map();
  for (const row of links) {
    const current = linksByDocument.get(row.documentId) || [];
    current.push({
      referenceType: row.referenceType,
      rawValue: row.rawValue,
      normalizedValue: row.normalizedValue,
      canonicalValue: row.canonicalValue,
      isValid: Number(row.isValid) === 1
    });
    linksByDocument.set(row.documentId, current);
  }

  const inspected = docs.map((row) => {
    const fixture = isFixtureCitation(row.citation);
    const metadataCodes = uniqueSorted(parseJsonArray(row.indexCodesJson).map(normalizeWhitespace));
    const documentLinks = linksByDocument.get(row.id) || [];
    const validIndexLinks = uniqueSorted(
      documentLinks
        .filter((link) => link.referenceType === "index_code" && link.isValid && normalizeWhitespace(link.canonicalValue))
        .map((link) => normalizeWhitespace(link.canonicalValue))
    );
    const validRuleLinks = uniqueSorted(
      documentLinks
        .filter((link) => link.referenceType === "rules_section" && link.isValid && normalizeWhitespace(link.canonicalValue))
        .map((link) => normalizeWhitespace(link.canonicalValue))
    );
    const validOrdinanceLinks = uniqueSorted(
      documentLinks
        .filter((link) => link.referenceType === "ordinance_section" && link.isValid && normalizeWhitespace(link.canonicalValue))
        .map((link) => normalizeWhitespace(link.canonicalValue))
    );
    const currentCodes = uniqueSorted([...metadataCodes, ...validIndexLinks]);
    const canonicalCodes = currentCodes.filter(isCanonicalStyleCode);
    const legacyCodes = currentCodes.filter(isLegacyNumericCode);
    const inferredCrosswalkCodes = inferCrosswalkCodes(validRuleLinks, validOrdinanceLinks, crosswalkMaps).filter(isCanonicalStyleCode);
    const inferredCanonicalAdditions = inferredCrosswalkCodes.filter((code) => !canonicalCodes.includes(code));
    const noIndexCoverage = validIndexLinks.length === 0;
    const legacyOnly = legacyCodes.length > 0 && canonicalCodes.length === 0;
    const mixed = legacyCodes.length > 0 && canonicalCodes.length > 0;
    const canonicalOnly = canonicalCodes.length > 0 && legacyCodes.length === 0;

    let repairType = null;
    let nextIndexCodes = [];
    if (legacyOnly && inferredCanonicalAdditions.length > 0 && inferredCanonicalAdditions.length <= maxAppendCanonicalCount) {
      repairType = "append_canonical_from_crosswalk";
      nextIndexCodes = uniqueSorted([...legacyCodes, ...inferredCanonicalAdditions]);
    } else if (noIndexCoverage && inferredCrosswalkCodes.length === 1) {
      repairType = "materialize_missing_from_crosswalk";
      nextIndexCodes = inferredCrosswalkCodes;
    }

    return {
      id: row.id,
      citation: row.citation,
      title: row.title,
      authorName: row.authorName || null,
      decisionDate: row.decisionDate || null,
      fixture,
      metadataCodes,
      currentCodes,
      currentValidIndexLinks: validIndexLinks,
      validRuleLinks,
      validOrdinanceLinks,
      canonicalCodes,
      legacyCodes,
      noIndexCoverage,
      legacyOnly,
      mixed,
      canonicalOnly,
      inferredCrosswalkCodes,
      inferredCanonicalAdditions,
      repairType,
      nextIndexCodes
    };
  });

  const realDocs = inspected.filter((row) => !row.fixture);
  const legacyOnlyReal = realDocs.filter((row) => row.legacyOnly);
  const missingCoverageReal = realDocs.filter((row) => row.noIndexCoverage);
  const repairCandidates = inspected.filter((row) => row.repairType && (includeFixtures || !row.fixture));
  const realRepairCandidates = repairCandidates.filter((row) => !row.fixture);

  if (apply && repairCandidates.length > 0) {
    const statements = ["BEGIN IMMEDIATE;"];
    for (const row of repairCandidates) {
      const nextIndexCodesJson = JSON.stringify(uniqueSorted(row.nextIndexCodes));
      statements.push(
        `UPDATE documents
         SET index_codes_json = ${sqlQuote(nextIndexCodesJson)},
             qc_has_index_codes = ${row.nextIndexCodes.length > 0 ? 1 : 0},
             updated_at = datetime('now')
         WHERE id = ${sqlQuote(row.id)};`
      );
      statements.push(
        `DELETE FROM document_reference_links
         WHERE document_id = ${sqlQuote(row.id)}
           AND reference_type = 'index_code';`
      );
      statements.push(
        `DELETE FROM document_reference_issues
         WHERE document_id = ${sqlQuote(row.id)}
           AND reference_type = 'index_code';`
      );
      for (const code of uniqueSorted(row.nextIndexCodes)) {
        statements.push(
          `INSERT INTO document_reference_links (
             id, document_id, reference_type, raw_value, normalized_value, canonical_value, is_valid, created_at
           ) VALUES (
             ${sqlQuote(id("drl"))},
             ${sqlQuote(row.id)},
             'index_code',
             ${sqlQuote(code)},
             ${sqlQuote(normalizeIndexCode(code))},
             ${sqlQuote(code)},
             1,
             datetime('now')
           );`
        );
      }
    }
    statements.push("COMMIT;");
    await runSql(statements.join("\n"));
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    includeFixtures,
    dbPath,
    summary: {
      realDecisionDocs: realDocs.length,
      realLegacyOnlyDocs: legacyOnlyReal.length,
      realCanonicalOnlyDocs: realDocs.filter((row) => row.canonicalOnly).length,
      realMixedDocs: realDocs.filter((row) => row.mixed).length,
      realNoIndexCoverageDocs: missingCoverageReal.length,
      realLegacyOnlyCanonicalizableDocs: legacyOnlyReal.filter((row) => row.inferredCanonicalAdditions.length > 0).length,
      realLegacyOnlySingleCanonicalizableDocs: legacyOnlyReal.filter((row) => row.inferredCanonicalAdditions.length === 1).length,
      realMissingCoverageInferableDocs: missingCoverageReal.filter((row) => row.inferredCrosswalkCodes.length > 0).length,
      realRepairCandidateCount: realRepairCandidates.length,
      appliedUpdateCount: apply ? repairCandidates.length : 0
    },
    summaryNotes: [
      legacyOnlyReal.length > 0
        ? `Most indexed real decisions are still legacy-only (${legacyOnlyReal.length}), so canonicalization is the highest-leverage filter improvement.`
        : "Legacy-only index-code coverage is no longer a major issue.",
      realRepairCandidates.length > 0
        ? `There are ${realRepairCandidates.length} conservative real-document canonicalization repairs ready to apply.`
        : "No conservative real-document canonicalization repairs were detected from current crosswalk evidence.",
      missingCoverageReal.filter((row) => row.inferredCrosswalkCodes.length > 0).length > 0
        ? `Some currently uncoded real decisions can still be materialized from crosswalk evidence (${missingCoverageReal.filter((row) => row.inferredCrosswalkCodes.length > 0).length}).`
        : "Missing-code decisions are not strongly inferable from current crosswalk evidence."
    ],
    legacyOnly: {
      byJudge: countBy(legacyOnlyReal.map((row) => row.authorName || "<unknown>")),
      byYear: countBy(legacyOnlyReal.map((row) => extractYear(row.decisionDate)))
    },
    missingCoverage: {
      byJudge: countBy(missingCoverageReal.map((row) => row.authorName || "<unknown>")),
      byYear: countBy(missingCoverageReal.map((row) => extractYear(row.decisionDate)))
    },
    repairCandidates: repairCandidates.map((row) => ({
      id: row.id,
      citation: row.citation,
      title: row.title,
      authorName: row.authorName,
      decisionDate: row.decisionDate,
      repairType: row.repairType,
      currentCodes: row.currentCodes,
      legacyCodes: row.legacyCodes,
      canonicalCodes: row.canonicalCodes,
      inferredCrosswalkCodes: row.inferredCrosswalkCodes,
      nextIndexCodes: row.nextIndexCodes
    })),
    manualReviewCandidates: inspected
      .filter((row) => !row.repairType && ((row.legacyOnly && row.inferredCrosswalkCodes.length > maxAppendCanonicalCount) || row.noIndexCoverage))
      .slice(0, 200)
      .map((row) => ({
        id: row.id,
        citation: row.citation,
        title: row.title,
        authorName: row.authorName,
        decisionDate: row.decisionDate,
        currentCodes: row.currentCodes,
        inferredCrosswalkCodes: row.inferredCrosswalkCodes,
        validRuleLinks: row.validRuleLinks,
        validOrdinanceLinks: row.validOrdinanceLinks
      }))
  };

  const jsonPath = path.resolve(reportsDir, jsonName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  const csvPath = path.resolve(reportsDir, csvName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));
  await fs.writeFile(csvPath, buildCsv(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Index code canonicalization JSON report written to ${jsonPath}`);
  console.log(`Index code canonicalization Markdown report written to ${markdownPath}`);
  console.log(`Index code canonicalization CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
