import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const defaultDbPath =
  "/Users/cliftonnoble/Documents/Beedle AI App/apps/api/.wrangler/state/v3/d1/miniflare-D1DatabaseObject/b00cf84e30534f05a5617838947ad6ffbfd67b7ec4555224601f8bb33ff98a87.sqlite";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.INDEX_CODE_ENRICHMENT_REPORT_NAME || "index-code-enrichment-report.json";
const markdownName = process.env.INDEX_CODE_ENRICHMENT_MARKDOWN_NAME || "index-code-enrichment-report.md";
const apply = (process.env.INDEX_CODE_ENRICHMENT_APPLY || "0") === "1";
const includeFixtures = (process.env.INDEX_CODE_ENRICHMENT_INCLUDE_FIXTURES || "0") === "1";
const busyTimeoutMs = Number(process.env.INDEX_CODE_ENRICHMENT_BUSY_TIMEOUT_MS || "5000");

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
  return Array.from(new Set(values));
}

function uniqueSorted(values) {
  return unique(values).sort((a, b) => a.localeCompare(b));
}

function sqlQuote(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function isFixtureCitation(citation) {
  return /^BEE-/i.test(String(citation || ""));
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

function extractIndexWarnings(rawWarningsJson) {
  const warnings = parseJsonArray(rawWarningsJson).map((item) => normalizeWhitespace(item));
  return warnings.filter((warning) => /index code/i.test(warning));
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

function selectCanonicalIndexCodes(rawCodes, legalIndexByNormalized) {
  const validCodes = [];
  const reservedCodes = [];
  const unknownCodes = [];

  for (const raw of rawCodes) {
    const normalized = normalizeIndexCode(raw);
    const legal = legalIndexByNormalized.get(normalized);
    if (!legal) {
      unknownCodes.push(normalizeWhitespace(raw));
      continue;
    }
    if (legal.isReserved) {
      reservedCodes.push(legal.codeIdentifier);
      continue;
    }
    validCodes.push(legal.codeIdentifier);
  }

  return {
    validCodes: uniqueSorted(validCodes),
    reservedCodes: uniqueSorted(reservedCodes),
    unknownCodes: uniqueSorted(unknownCodes)
  };
}

function formatMarkdown(report) {
  const lines = [
    "# Index Code Enrichment Report",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Apply mode: \`${report.apply}\``,
    `- Include fixtures: \`${report.includeFixtures}\``,
    `- Database: \`${report.dbPath}\``,
    `- Real decision docs: \`${report.summary.realDecisionDocs}\``,
    `- Real docs with index metadata: \`${report.summary.realDocsWithIndexMetadata}\``,
    `- Real docs with valid index links: \`${report.summary.realDocsWithValidIndexLinks}\``,
    `- Real docs missing index coverage: \`${report.summary.realDocsMissingIndexCoverage}\``,
    `- Real metadata/link parity gaps: \`${report.summary.realMetadataLinkParityGapCount}\``,
    `- Real crosswalk inferable docs: \`${report.summary.realCrosswalkInferableCount}\``,
    `- Conservative real repair candidates: \`${report.summary.realRepairCandidateCount}\``,
    `- Fixture-only repair candidates: \`${report.summary.fixtureRepairCandidateCount}\``,
    `- Applied updates: \`${report.summary.appliedUpdateCount}\``,
    "",
    "## Summary",
    "",
    ...report.summaryNotes.map((line) => `- ${line}`),
    "",
    "## Missing Coverage By Judge",
    ""
  ];

  for (const row of report.missingCoverage.byJudge.slice(0, 12)) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }

  lines.push("");
  lines.push("## Missing Coverage By Year");
  lines.push("");
  for (const row of report.missingCoverage.byYear.slice(0, 12)) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }

  lines.push("");
  lines.push("## Conservative Repair Candidates");
  lines.push("");
  for (const row of report.repairCandidates.slice(0, 50)) {
    lines.push(
      `- \`${row.citation}\` | type=\`${row.repairType}\` | next=\`${(row.nextIndexCodes || []).join(", ") || "<none>"}\` | currentMetadata=\`${(row.metadataCodes || []).join(", ") || "<none>"}\` | currentLinks=\`${(row.currentValidIndexLinks || []).join(", ") || "<none>"}\``
    );
  }

  lines.push("");
  lines.push("## Manual Review Candidates");
  lines.push("");
  for (const row of report.manualReviewCandidates.slice(0, 50)) {
    lines.push(
      `- \`${row.citation}\` | title=\`${row.title}\` | judge=\`${row.authorName || "<unknown>"}\` | date=\`${row.decisionDate || "<missing>"}\` | rulesLinks=\`${row.validRuleLinkCount}\` | ordinanceLinks=\`${row.validOrdinanceLinkCount}\` | indexWarnings=\`${(row.indexWarnings || []).join(" | ") || "<none>"}\``
    );
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const [docs, links, legalIndexCodes, crosswalk] = await Promise.all([
    runSqlJson(`
      SELECT
        d.id,
        d.citation,
        d.title,
        d.author_name AS authorName,
        d.decision_date AS decisionDate,
        d.index_codes_json AS indexCodesJson,
        d.rules_sections_json AS rulesSectionsJson,
        d.ordinance_sections_json AS ordinanceSectionsJson,
        d.extraction_warnings_json AS extractionWarningsJson,
        d.qc_has_index_codes AS qcHasIndexCodes,
        d.created_at AS createdAt
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
        code_identifier AS codeIdentifier,
        normalized_code AS normalizedCode,
        is_reserved AS isReserved
      FROM legal_index_codes
      WHERE active = 1;
    `),
    runSqlJson(`
      SELECT
        index_code_id AS indexCodeId,
        ordinance_citation AS ordinanceCitation,
        rules_citation AS rulesCitation
      FROM legal_reference_crosswalk;
    `)
  ]);

  const legalIndexByNormalized = new Map(
    legalIndexCodes.map((row) => [String(row.normalizedCode), { codeIdentifier: String(row.codeIdentifier), isReserved: Number(row.isReserved) === 1 }])
  );
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
    const metadataCodes = uniqueSorted(parseJsonArray(row.indexCodesJson).map((item) => normalizeWhitespace(item)).filter(Boolean));
    const metadataSelection = selectCanonicalIndexCodes(metadataCodes, legalIndexByNormalized);
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
    const inferredCrosswalkCodes = inferCrosswalkCodes(validRuleLinks, validOrdinanceLinks, crosswalkMaps);
    const currentCoverage = validIndexLinks.length > 0;
    const metadataParityGap = metadataSelection.validCodes.length > 0 && validIndexLinks.length === 0;
    const indexWarnings = extractIndexWarnings(row.extractionWarningsJson);

    let repairType = null;
    let nextIndexCodes = [];
    if (validIndexLinks.length === 0 && metadataSelection.validCodes.length > 0) {
      repairType = "materialize_valid_metadata_codes";
      nextIndexCodes = metadataSelection.validCodes;
    } else if (metadataCodes.length === 0 && validIndexLinks.length > 0) {
      repairType = "sync_metadata_from_existing_links";
      nextIndexCodes = validIndexLinks;
    } else if (metadataCodes.length === 0 && validIndexLinks.length === 0 && inferredCrosswalkCodes.length === 1) {
      repairType = "single_crosswalk_inference";
      nextIndexCodes = inferredCrosswalkCodes;
    }

    return {
      id: row.id,
      citation: row.citation,
      title: row.title,
      authorName: row.authorName || null,
      decisionDate: row.decisionDate || null,
      fixture,
      qcHasIndexCodes: Number(row.qcHasIndexCodes) === 1,
      metadataCodes,
      validMetadataCodes: metadataSelection.validCodes,
      reservedMetadataCodes: metadataSelection.reservedCodes,
      unknownMetadataCodes: metadataSelection.unknownCodes,
      currentValidIndexLinks: validIndexLinks,
      currentCoverage,
      metadataParityGap,
      validRuleLinks,
      validOrdinanceLinks,
      validRuleLinkCount: validRuleLinks.length,
      validOrdinanceLinkCount: validOrdinanceLinks.length,
      inferredCrosswalkCodes,
      inferredCrosswalkCodeCount: inferredCrosswalkCodes.length,
      indexWarnings,
      repairType,
      nextIndexCodes
    };
  });

  const realDocs = inspected.filter((row) => !row.fixture);
  const missingCoverageReal = realDocs.filter((row) => !row.currentCoverage);
  const repairCandidates = inspected.filter(
    (row) => row.repairType && (includeFixtures || !row.fixture)
  );
  const realRepairCandidates = repairCandidates.filter((row) => !row.fixture);
  const fixtureRepairCandidates = repairCandidates.filter((row) => row.fixture);
  const manualReviewCandidates = missingCoverageReal
    .filter((row) => !row.repairType)
    .sort((a, b) => {
      const structuralDelta = b.validOrdinanceLinkCount + b.validRuleLinkCount - (a.validOrdinanceLinkCount + a.validRuleLinkCount);
      if (structuralDelta !== 0) return structuralDelta;
      return String(b.decisionDate || "").localeCompare(String(a.decisionDate || ""));
    });

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
      totalDecisionDocs: inspected.length,
      realDecisionDocs: realDocs.length,
      fixtureDecisionDocs: inspected.length - realDocs.length,
      realDocsWithIndexMetadata: realDocs.filter((row) => row.metadataCodes.length > 0).length,
      realDocsWithValidIndexLinks: realDocs.filter((row) => row.currentCoverage).length,
      realDocsMissingIndexCoverage: missingCoverageReal.length,
      realMetadataLinkParityGapCount: realDocs.filter((row) => row.metadataParityGap).length,
      realCrosswalkInferableCount: missingCoverageReal.filter((row) => row.inferredCrosswalkCodeCount > 0).length,
      realCrosswalkSingleInferableCount: missingCoverageReal.filter((row) => row.inferredCrosswalkCodeCount === 1).length,
      realMissingCoverageWithRulesLinks: missingCoverageReal.filter((row) => row.validRuleLinkCount > 0).length,
      realMissingCoverageWithOrdinanceLinks: missingCoverageReal.filter((row) => row.validOrdinanceLinkCount > 0).length,
      realRepairCandidateCount: realRepairCandidates.length,
      fixtureRepairCandidateCount: fixtureRepairCandidates.length,
      appliedUpdateCount: apply ? repairCandidates.length : 0
    },
    summaryNotes: [
      realDocs.filter((row) => row.metadataCodes.length > 0).length === realDocs.filter((row) => row.currentCoverage).length &&
      realDocs.filter((row) => row.metadataParityGap).length === 0
        ? "Real decisions currently show full parity between index-code metadata and valid index-code links."
        : "Some real decisions have index-code metadata that has not yet been materialized into valid search links.",
      missingCoverageReal.filter((row) => row.inferredCrosswalkCodeCount > 0).length === 0
        ? "Crosswalk inference does not currently recover missing index codes for real decisions from existing rules/ordinance links."
        : "Some missing index-code decisions can be inferred conservatively from existing rules/ordinance crosswalks.",
      realRepairCandidates.length === 0
        ? "There are no conservative real-document auto-repairs available from current local metadata alone."
        : `There are ${realRepairCandidates.length} conservative real-document auto-repair candidates ready to apply.`,
      fixtureRepairCandidates.length > 0
        ? `Fixture-only parity repairs remain available (${fixtureRepairCandidates.length}), but they do not improve real search quality.`
        : "No fixture-only repair candidates were detected."
    ],
    missingCoverage: {
      byJudge: countBy(missingCoverageReal.map((row) => row.authorName || "<unknown>")),
      byYear: countBy(missingCoverageReal.map((row) => extractYear(row.decisionDate)))
    },
    repairCandidates: repairCandidates.map((row) => ({
      id: row.id,
      citation: row.citation,
      title: row.title,
      fixture: row.fixture,
      repairType: row.repairType,
      nextIndexCodes: row.nextIndexCodes,
      metadataCodes: row.metadataCodes,
      currentValidIndexLinks: row.currentValidIndexLinks,
      inferredCrosswalkCodes: row.inferredCrosswalkCodes
    })),
    manualReviewCandidates: manualReviewCandidates.slice(0, 200).map((row) => ({
      id: row.id,
      citation: row.citation,
      title: row.title,
      authorName: row.authorName,
      decisionDate: row.decisionDate,
      validRuleLinkCount: row.validRuleLinkCount,
      validOrdinanceLinkCount: row.validOrdinanceLinkCount,
      indexWarnings: row.indexWarnings,
      qcHasIndexCodes: row.qcHasIndexCodes,
      inferredCrosswalkCodes: row.inferredCrosswalkCodes
    }))
  };

  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Index code enrichment JSON report written to ${jsonPath}`);
  console.log(`Index code enrichment Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
