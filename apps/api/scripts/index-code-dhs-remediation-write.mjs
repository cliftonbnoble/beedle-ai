import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";
import {
  defaultDbPath,
  normalizeCode,
  normalizeWhitespace,
  parseJsonArray,
  runSqlJson,
  sqlQuote,
  uniqueSorted,
  TARGET_CODES
} from "./lib/dhs-index-code-remediation.mjs";

const execFileAsync = promisify(execFile);

const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const reportsDir = path.resolve(process.cwd(), "reports");
const apply = (process.env.DHS_REMEDIATION_APPLY || "0") === "1";
const busyTimeoutMs = Number(process.env.DHS_REMEDIATION_BUSY_TIMEOUT_MS || "5000");
const allowlistPath = path.resolve(
  process.cwd(),
  process.env.DHS_REMEDIATION_ALLOWLIST || "./scripts/index-code-dhs-remediation-allowlist.json"
);
const reviewReportPath = path.resolve(
  process.cwd(),
  process.env.DHS_REMEDIATION_REVIEW_REPORT || "./reports/index-code-dhs-review-report.json"
);
const jsonName = process.env.DHS_REMEDIATION_REPORT_NAME || "index-code-dhs-remediation-write-report.json";
const markdownName = process.env.DHS_REMEDIATION_MARKDOWN_NAME || "index-code-dhs-remediation-write-report.md";
const csvName = process.env.DHS_REMEDIATION_CSV_NAME || "index-code-dhs-remediation-write-report.csv";

function id(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

async function runSql(sql) {
  await execFileAsync("sqlite3", ["-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql], {
    cwd: process.cwd(),
    maxBuffer: 100 * 1024 * 1024
  });
}

async function readJson(filePath, fallback = null) {
  try {
    const content = await fs.readFile(filePath, "utf8");
    return JSON.parse(content);
  } catch {
    return fallback;
  }
}

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function validateApprovedEntries(entries) {
  const allowedCodes = new Set(TARGET_CODES);
  return entries.map((entry) => {
    const approvedCodes = uniqueSorted((entry.approvedCodes || []).map((code) => String(code).toUpperCase())).filter((code) =>
      allowedCodes.has(code)
    );
    return {
      citation: normalizeWhitespace(entry.citation),
      approvedCodes,
      reviewReason: normalizeWhitespace(entry.reviewReason),
      reviewer: normalizeWhitespace(entry.reviewer)
    };
  });
}

function toCsv(report) {
  const rows = [
    ["citation", "approved_codes", "current_codes", "current_link_codes", "next_codes", "already_satisfied", "review_reason"],
    ...report.rows.map((row) => [
      row.citation,
      row.approvedCodes.join("; "),
      row.currentCodes.join("; "),
      row.currentLinkCodes.join("; "),
      row.nextCodes.join("; "),
      row.alreadySatisfied,
      row.reviewReason || ""
    ])
  ];
  return `${rows.map((row) => row.map(csvEscape).join(",")).join("\n")}\n`;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# DHS Remediation Write Report");
  lines.push("");
  lines.push(`- Generated: \`${report.generatedAt}\``);
  lines.push(`- Apply mode: \`${report.apply}\``);
  lines.push(`- allowlist approved entries: \`${report.summary.allowlistApprovedCount}\``);
  lines.push(`- matched docs: \`${report.summary.matchedDocCount}\``);
  lines.push(`- unmatched citations: \`${report.summary.unmatchedCitationCount}\``);
  lines.push(`- already satisfied: \`${report.summary.alreadySatisfiedCount}\``);
  lines.push(`- pending updates: \`${report.summary.pendingUpdateCount}\``);
  lines.push(`- applied updates: \`${report.summary.appliedUpdateCount}\``);
  lines.push("");
  lines.push("## By Code");
  lines.push("");
  for (const row of report.summary.byCode) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }
  if (report.summary.unmatchedCitations.length > 0) {
    lines.push("");
    lines.push("## Unmatched Citations");
    lines.push("");
    for (const citation of report.summary.unmatchedCitations) lines.push(`- \`${citation}\``);
  }
  lines.push("");
  lines.push("## Reviewed Rows");
  lines.push("");
  for (const row of report.rows) {
    lines.push(
      `- \`${row.citation}\` | approved=\`${row.approvedCodes.join(", ")}\` | current=\`${row.currentCodes.join(", ") || "<none>"}\` | next=\`${row.nextCodes.join(", ")}\` | alreadySatisfied=\`${row.alreadySatisfied}\``
    );
    if (row.reviewReason) lines.push(`  - review: ${row.reviewReason}`);
    if (row.auditEvidence.length > 0) {
      for (const evidence of row.auditEvidence.slice(0, 2)) {
        lines.push(`  - ${evidence.code} | ${evidence.sectionLabel || "<unknown>"} | ${evidence.excerpt}`);
      }
    }
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  const allowlist = (await readJson(allowlistPath, { approved: [] })) || { approved: [] };
  const reviewReport = (await readJson(reviewReportPath, { candidates: [] })) || { candidates: [] };
  const reviewByCitation = new Map(
    (reviewReport.candidates || []).map((row) => [normalizeWhitespace(row.citation), row.detections || []])
  );

  const approvedEntries = validateApprovedEntries(allowlist.approved || []).filter(
    (entry) => entry.citation && entry.approvedCodes.length > 0
  );

  const citations = uniqueSorted(approvedEntries.map((entry) => entry.citation));
  const unmatchedCitations = [];
  let docs = [];
  if (citations.length > 0) {
    docs = await runSqlJson(
      dbPath,
      busyTimeoutMs,
      `
        SELECT
          d.id,
          d.citation,
          d.title,
          d.author_name AS authorName,
          d.decision_date AS decisionDate,
          d.index_codes_json AS indexCodesJson
        FROM documents d
        WHERE d.file_type = 'decision_docx'
          AND d.citation IN (${citations.map(sqlQuote).join(", ")})
        ORDER BY d.decision_date DESC, d.citation ASC
      `
    );
  }

  const docIds = docs.map((row) => row.id).filter(Boolean);
  let links = [];
  if (docIds.length > 0) {
    links = await runSqlJson(
      dbPath,
      busyTimeoutMs,
      `
        SELECT
          document_id AS documentId,
          coalesce(canonical_value, raw_value, normalized_value) AS codeValue,
          normalized_value AS normalizedValue,
          is_valid AS isValid
        FROM document_reference_links
        WHERE reference_type = 'index_code'
          AND document_id IN (${docIds.map(sqlQuote).join(", ")})
      `
    );
  }

  const linkCodesByDocumentId = new Map();
  for (const row of links) {
    if (!row.documentId || Number(row.isValid || 0) !== 1) continue;
    const current = linkCodesByDocumentId.get(row.documentId) || [];
    current.push(normalizeWhitespace(row.codeValue || row.normalizedValue));
    linkCodesByDocumentId.set(row.documentId, current);
  }

  const docsByCitation = new Map(docs.map((row) => [normalizeWhitespace(row.citation), row]));

  const rows = approvedEntries
    .map((entry) => {
      const doc = docsByCitation.get(entry.citation);
      if (!doc) {
        unmatchedCitations.push(entry.citation);
        return null;
      }

      const currentCodes = uniqueSorted(parseJsonArray(doc.indexCodesJson).map(normalizeWhitespace));
      const currentLinkCodes = uniqueSorted((linkCodesByDocumentId.get(doc.id) || []).map(normalizeWhitespace));
      const nextCodes = uniqueSorted([...currentCodes, ...currentLinkCodes, ...entry.approvedCodes]);
      const currentNormalized = new Set([...currentCodes, ...currentLinkCodes].map(normalizeCode));
      const alreadySatisfied = entry.approvedCodes.every((code) => currentNormalized.has(normalizeCode(code)));

      return {
        id: doc.id,
        citation: doc.citation,
        title: doc.title,
        authorName: doc.authorName || "",
        decisionDate: doc.decisionDate || "",
        approvedCodes: entry.approvedCodes,
        currentCodes,
        currentLinkCodes,
        nextCodes,
        alreadySatisfied,
        reviewReason: entry.reviewReason,
        auditEvidence: reviewByCitation.get(entry.citation) || []
      };
    })
    .filter(Boolean);

  const pendingUpdates = rows.filter((row) => !row.alreadySatisfied);

  if (apply && pendingUpdates.length > 0) {
    const statements = ["BEGIN IMMEDIATE;"];
    for (const row of pendingUpdates) {
      const nextIndexCodesJson = JSON.stringify(row.nextCodes);
      statements.push(
        `UPDATE documents
         SET index_codes_json = ${sqlQuote(nextIndexCodesJson)},
             qc_has_index_codes = 1,
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
      for (const code of row.nextCodes) {
        statements.push(
          `INSERT INTO document_reference_links (
             id, document_id, reference_type, raw_value, normalized_value, canonical_value, is_valid, created_at
           ) VALUES (
             ${sqlQuote(id("ref"))},
             ${sqlQuote(row.id)},
             'index_code',
             ${sqlQuote(code)},
             ${sqlQuote(normalizeCode(code))},
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
    dbPath,
    allowlistPath,
    reviewReportPath,
    summary: {
      allowlistApprovedCount: approvedEntries.length,
      matchedDocCount: rows.length,
      unmatchedCitationCount: unmatchedCitations.length,
      unmatchedCitations,
      alreadySatisfiedCount: rows.filter((row) => row.alreadySatisfied).length,
      pendingUpdateCount: pendingUpdates.length,
      appliedUpdateCount: apply ? pendingUpdates.length : 0,
      byCode: countBy(approvedEntries.flatMap((entry) => entry.approvedCodes))
    },
    rows
  };

  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  const csvPath = path.join(reportsDir, csvName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  await fs.writeFile(markdownPath, toMarkdown(report), "utf8");
  await fs.writeFile(csvPath, toCsv(report), "utf8");
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`DHS remediation write JSON report written to ${jsonPath}`);
  console.log(`DHS remediation write Markdown report written to ${markdownPath}`);
  console.log(`DHS remediation write CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
