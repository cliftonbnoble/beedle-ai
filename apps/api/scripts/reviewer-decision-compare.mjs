import fs from "node:fs/promises";
import path from "node:path";
import { buildReviewerDecisionCompare, formatReviewerDecisionCompareMarkdown } from "./reviewer-decision-compare-utils.mjs";

const validateInput = process.env.REVIEWER_DECISION_COMPARE_VALIDATE_INPUT || "./reports/reviewer-worksheet-validate.json";
const queueInput = process.env.REVIEWER_DECISION_COMPARE_QUEUE_INPUT || "./reports/reviewer-action-queue.json";
const simInput = process.env.REVIEWER_DECISION_COMPARE_SIM_INPUT || "./reports/reviewer-decision-sim.json";
const reportName = process.env.REVIEWER_DECISION_COMPARE_REPORT_NAME || "reviewer-decision-compare.json";
const markdownName = process.env.REVIEWER_DECISION_COMPARE_MARKDOWN_NAME || "reviewer-decision-compare.md";

async function readJson(relativePath) {
  const abs = path.resolve(process.cwd(), relativePath);
  const raw = await fs.readFile(abs, "utf8");
  return JSON.parse(raw);
}

async function main() {
  const validateReport = await readJson(validateInput);
  const queueReport = await readJson(queueInput);
  const simReport = await readJson(simInput);

  const compare = buildReviewerDecisionCompare(validateReport, queueReport, simReport);
  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    input: {
      validateInputPathUsed: path.resolve(process.cwd(), validateInput),
      queueInputPathUsed: path.resolve(process.cwd(), queueInput),
      simInputPathUsed: path.resolve(process.cwd(), simInput)
    },
    ...compare
  };

  const reportsDir = path.resolve(process.cwd(), "reports");
  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatReviewerDecisionCompareMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Comparison JSON written to ${jsonPath}`);
  console.log(`Comparison Markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
