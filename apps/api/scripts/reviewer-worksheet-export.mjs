import fs from "node:fs/promises";
import path from "node:path";
import { buildReviewerWorksheetExport, formatReviewerWorksheetCsv, formatReviewerWorksheetMarkdown } from "./reviewer-worksheet-export-utils.mjs";

const actionQueueInput = process.env.REVIEWER_WORKSHEET_ACTION_QUEUE_INPUT || "./reports/reviewer-action-queue.json";
const evidenceInput = process.env.REVIEWER_WORKSHEET_EVIDENCE_INPUT || "./reports/reviewer-legal-evidence.json";
const simInput = process.env.REVIEWER_WORKSHEET_SIM_INPUT || "./reports/reviewer-decision-sim.json";
const jsonOutput = process.env.REVIEWER_WORKSHEET_JSON_NAME || "reviewer-worksheet.json";
const csvOutput = process.env.REVIEWER_WORKSHEET_CSV_NAME || "reviewer-worksheet.csv";
const markdownOutput = process.env.REVIEWER_WORKSHEET_MARKDOWN_NAME || "reviewer-worksheet.md";

async function readJsonIfExists(relativePath) {
  const abs = path.resolve(process.cwd(), relativePath);
  try {
    const raw = await fs.readFile(abs, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function main() {
  const actionQueue = await readJsonIfExists(actionQueueInput);
  if (!actionQueue || !Array.isArray(actionQueue.rows)) {
    throw new Error(`REVIEWER_WORKSHEET_ACTION_QUEUE_INPUT missing or invalid: ${path.resolve(process.cwd(), actionQueueInput)}`);
  }
  const evidence = await readJsonIfExists(evidenceInput);
  const sim = await readJsonIfExists(simInput);

  const worksheet = buildReviewerWorksheetExport(actionQueue, evidence, sim);
  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    input: {
      actionQueueInputPathUsed: path.resolve(process.cwd(), actionQueueInput),
      evidenceInputPathUsed: path.resolve(process.cwd(), evidenceInput),
      simInputPathUsed: path.resolve(process.cwd(), simInput)
    },
    ...worksheet
  };

  const reportsDir = path.resolve(process.cwd(), "reports");
  const jsonPath = path.resolve(reportsDir, jsonOutput);
  const csvPath = path.resolve(reportsDir, csvOutput);
  const markdownPath = path.resolve(reportsDir, markdownOutput);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(csvPath, formatReviewerWorksheetCsv(report));
  await fs.writeFile(markdownPath, formatReviewerWorksheetMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Worksheet JSON written to ${jsonPath}`);
  console.log(`Worksheet CSV written to ${csvPath}`);
  console.log(`Worksheet Markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
