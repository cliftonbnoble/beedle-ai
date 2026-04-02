import fs from "node:fs/promises";
import path from "node:path";
import {
  loadReviewerWorksheetInput,
  validateReviewerWorksheetRows,
  formatReviewerWorksheetValidationMarkdown
} from "./reviewer-worksheet-validate-utils.mjs";

const inputPath = process.env.REVIEWER_WORKSHEET_INPUT || "./reports/reviewer-worksheet.csv";
const reportName = process.env.REVIEWER_WORKSHEET_VALIDATE_REPORT_NAME || "reviewer-worksheet-validate.json";
const markdownName = process.env.REVIEWER_WORKSHEET_VALIDATE_MARKDOWN_NAME || "reviewer-worksheet-validate.md";
const strict = String(process.env.REVIEWER_WORKSHEET_STRICT || "1") !== "0";

async function main() {
  const absoluteInput = path.resolve(process.cwd(), inputPath);
  const loaded = await loadReviewerWorksheetInput(absoluteInput);
  const validated = validateReviewerWorksheetRows(loaded.rows, strict);

  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    input: {
      inputPath: absoluteInput,
      inputFormat: loaded.inputFormat,
      headers: loaded.headers,
      strictMode: strict
    },
    ...validated
  };

  const reportsDir = path.resolve(process.cwd(), "reports");
  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatReviewerWorksheetValidationMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Validation JSON written to ${jsonPath}`);
  console.log(`Validation Markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
