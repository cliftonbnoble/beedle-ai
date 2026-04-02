import fs from "node:fs/promises";
import path from "node:path";

const reportsDir = path.resolve(process.cwd(), "reports");
const acquisitionPath = path.resolve(reportsDir, "provisional-topic-acquisition-list-report.json");
const healthPath = path.resolve(reportsDir, "retrieval-health-report.json");
const jsonPath = path.resolve(reportsDir, "provisional-topic-intake-worksheet.json");
const csvPath = path.resolve(reportsDir, "provisional-topic-intake-worksheet.csv");
const markdownPath = path.resolve(reportsDir, "provisional-topic-intake-worksheet.md");

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function buildRows(acquisitionReport, healthReport) {
  const zeroMap = new Map((healthReport.queries || []).map((row) => [row.id, Number(row.totalResults || 0) === 0]));
  const rows = [];
  for (const entry of acquisitionReport.entries || []) {
    let rank = 1;
    for (const prompt of entry.searchPrompts || []) {
      rows.push({
        topic: entry.topic,
        priority: entry.priority,
        currentZeroResults: zeroMap.get(entry.topic) ?? Boolean(entry.currentlyZeroResults),
        targetNewDecisions: entry.targetNewDecisions,
        acquisitionRank: rank,
        searchPrompt: prompt,
        expectedTerms: (entry.expectedTerms || []).join("; "),
        candidateSourceUrl: "",
        candidateTitle: "",
        candidateCitation: "",
        sourceType: "decision_docx",
        importStatus: "planned",
        notes: "",
        keepIfFound: "yes"
      });
      rank += 1;
    }
  }
  return rows;
}

function toCsv(rows) {
  const columns = [
    "topic",
    "priority",
    "currentZeroResults",
    "targetNewDecisions",
    "acquisitionRank",
    "searchPrompt",
    "expectedTerms",
    "candidateSourceUrl",
    "candidateTitle",
    "candidateCitation",
    "sourceType",
    "importStatus",
    "notes",
    "keepIfFound"
  ];
  return [
    columns.join(","),
    ...rows.map((row) => columns.map((column) => csvEscape(row[column])).join(","))
  ].join("\n");
}

function toMarkdown(rows, acquisitionReport, healthReport) {
  const lines = [
    "# Provisional Topic Intake Worksheet",
    "",
    `- Generated: \`${new Date().toISOString()}\``,
    `- Source acquisition report: \`${path.basename(acquisitionPath)}\``,
    `- Retrieval health report: \`${path.basename(healthPath)}\``,
    `- Worksheet rows: \`${rows.length}\``,
    "",
    "## Intake Rules",
    "",
    "- Keep the gated reprocess policy in place; do not reprocess blocked local docs from this worksheet.",
    "- Prefer real decision sources that explicitly mention the target issue family in the decision text or title context.",
    "- Use the retrieval health report before and after each import batch.",
    "- Preserve broad recall: add topic coverage without narrowing the ranker.",
    ""
  ];

  for (const entry of acquisitionReport.entries || []) {
    lines.push(`## ${entry.topic}`);
    lines.push("");
    lines.push(`- Current zero results: \`${Boolean((healthReport.queries || []).find((row) => row.id === entry.topic)?.totalResults === 0)}\``);
    lines.push(`- Target new decisions: \`${entry.targetNewDecisions}\``);
    lines.push(`- Search prompts: ${(entry.searchPrompts || []).map((value) => `\`${value}\``).join(", ")}`);
    lines.push(`- Expected terms: ${(entry.expectedTerms || []).map((value) => `\`${value}\``).join(", ")}`);
    lines.push("");
  }

  lines.push("## Batch Import Checklist");
  lines.push("");
  lines.push("1. Run `pnpm report:retrieval-health` and save the before snapshot.");
  lines.push("2. Fill candidate source URLs and titles in the worksheet CSV.");
  lines.push("3. Import only real decision sources for the current topic batch.");
  lines.push("4. Keep `write:provisional-topic-gated-reprocess` unchanged; do not use it to force blocked topic docs through.");
  lines.push("5. After import + chunking + vector backfill, rerun `pnpm report:retrieval-health`.");
  lines.push("6. Compare hit rate, avg results/query, and avg decision diversity against the before snapshot.");
  lines.push("7. Confirm topic queries moved forward without collapsing broad recall on `heat`, `notice`, and `repair`.");
  lines.push("");

  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });
  const [acquisitionReport, healthReport] = await Promise.all([readJson(acquisitionPath), readJson(healthPath)]);
  const rows = buildRows(acquisitionReport, healthReport);

  const report = {
    generatedAt: new Date().toISOString(),
    sourceAcquisitionPath: acquisitionPath,
    retrievalHealthPath: healthPath,
    summary: {
      rowCount: rows.length,
      topicCount: new Set(rows.map((row) => row.topic)).size
    },
    rows
  };

  await Promise.all([
    fs.writeFile(jsonPath, JSON.stringify(report, null, 2)),
    fs.writeFile(csvPath, toCsv(rows)),
    fs.writeFile(markdownPath, toMarkdown(rows, acquisitionReport, healthReport))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Provisional topic intake worksheet JSON written to ${jsonPath}`);
  console.log(`Provisional topic intake worksheet CSV written to ${csvPath}`);
  console.log(`Provisional topic intake worksheet Markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
