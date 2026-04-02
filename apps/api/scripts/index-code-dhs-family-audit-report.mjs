import fs from "node:fs/promises";
import path from "node:path";
import { defaultDbPath, scanDhsLegacyCandidates } from "./lib/dhs-index-code-remediation.mjs";

const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number(process.env.DHS_FAMILY_AUDIT_BUSY_TIMEOUT_MS || "5000");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.DHS_FAMILY_AUDIT_JSON_NAME || "index-code-dhs-family-audit-report.json";
const markdownName = process.env.DHS_FAMILY_AUDIT_MARKDOWN_NAME || "index-code-dhs-family-audit-report.md";
const csvName = process.env.DHS_FAMILY_AUDIT_CSV_NAME || "index-code-dhs-family-audit-report.csv";

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function toCsv(report) {
  const rows = [
    ["code", "description", "exact_coverage_doc_count", "candidate_doc_count"],
    ...report.targetCodes.map((row) => [row.code, row.description, row.exactCoverageDocCount, row.candidateDocCount])
  ];
  return `${rows.map((row) => row.map(csvEscape).join(",")).join("\n")}\n`;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# DHS Family Audit");
  lines.push("");
  lines.push(`- Generated: \`${report.generatedAt}\``);
  lines.push(`- Database: \`${report.dbPath}\``);
  lines.push(`- legacy 13 docs inspected: \`${report.summary.legacy13DocCount}\``);
  lines.push(`- docs with any candidate canonical code: \`${report.summary.candidateDocCount}\``);
  lines.push("");
  lines.push("## Coverage By Code");
  lines.push("");
  for (const row of report.targetCodes) {
    lines.push(
      `- \`${row.code}\` | exact=\`${row.exactCoverageDocCount}\` | candidates=\`${row.candidateDocCount}\` | ${row.description || ""}`
    );
  }
  lines.push("");
  lines.push("## Candidate Judges");
  lines.push("");
  for (const row of report.summary.byJudge.slice(0, 15)) {
    lines.push(`- \`${row.key}\`: \`${row.count}\``);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const report = await scanDhsLegacyCandidates({ dbPath, busyTimeoutMs });
  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  const csvPath = path.join(reportsDir, csvName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  await fs.writeFile(markdownPath, toMarkdown(report), "utf8");
  await fs.writeFile(csvPath, toCsv(report), "utf8");
  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`DHS family audit JSON report written to ${jsonPath}`);
  console.log(`DHS family audit Markdown report written to ${markdownPath}`);
  console.log(`DHS family audit CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
