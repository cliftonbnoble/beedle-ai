import fs from "node:fs/promises";
import path from "node:path";
import { defaultDbPath, scanDhsLegacyCandidates } from "./lib/dhs-index-code-remediation.mjs";

const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number(process.env.DHS_REVIEW_BUSY_TIMEOUT_MS || "5000");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.DHS_REVIEW_JSON_NAME || "index-code-dhs-review-report.json";
const markdownName = process.env.DHS_REVIEW_MARKDOWN_NAME || "index-code-dhs-review-report.md";
const csvName = process.env.DHS_REVIEW_CSV_NAME || "index-code-dhs-review-report.csv";

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function toCsv(report) {
  const rows = [
    [
      "citation",
      "title",
      "author_name",
      "decision_date",
      "current_codes",
      "current_link_codes",
      "detected_codes",
      "best_score",
      "top_code",
      "top_section",
      "top_excerpt"
    ],
    ...report.candidates.map((row) => [
      row.citation,
      row.title || "",
      row.authorName || "",
      row.decisionDate || "",
      row.currentCodes.join("; "),
      row.currentLinkCodes.join("; "),
      row.detectedCodes.join("; "),
      row.bestScore,
      row.detections[0]?.code || "",
      row.detections[0]?.sectionLabel || "",
      row.detections[0]?.excerpt || ""
    ])
  ];
  return `${rows.map((row) => row.map(csvEscape).join(",")).join("\n")}\n`;
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# DHS Review Report");
  lines.push("");
  lines.push(`- Generated: \`${report.generatedAt}\``);
  lines.push(`- Database: \`${report.dbPath}\``);
  lines.push(`- candidate docs: \`${report.summary.candidateDocCount}\``);
  lines.push("");
  lines.push("## By Code");
  lines.push("");
  for (const row of report.summary.byCode) {
    lines.push(`- \`${row.code}\` | exact=\`${row.exactCoverageDocCount}\` | candidates=\`${row.candidateDocCount}\``);
  }
  lines.push("");
  lines.push("## Candidate Slice");
  lines.push("");
  for (const row of report.candidates.slice(0, 80)) {
    lines.push(
      `- \`${row.citation}\` | judge=\`${row.authorName || "<unknown>"}\` | current=\`${row.currentCodes.join(", ") || "<none>"}\` | detected=\`${row.detectedCodes.join(", ")}\` | bestScore=\`${row.bestScore}\``
    );
    for (const detection of row.detections.slice(0, 3)) {
      lines.push(`  - ${detection.code} | score=${detection.score} | section=${detection.sectionLabel || "<unknown>"}`);
      lines.push(`  - evidence: ${detection.excerpt}`);
    }
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
  console.log(JSON.stringify({ candidateDocCount: report.summary.candidateDocCount, byCode: report.summary.byCode }, null, 2));
  console.log(`DHS review JSON report written to ${jsonPath}`);
  console.log(`DHS review Markdown report written to ${markdownPath}`);
  console.log(`DHS review CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
