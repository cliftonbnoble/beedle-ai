import fs from "node:fs/promises";
import path from "node:path";
import { loadReviewerWorksheetInput } from "./reviewer-worksheet-validate-utils.mjs";
import { formatReviewerWorksheetCsv, formatReviewerWorksheetMarkdown } from "./reviewer-worksheet-export-utils.mjs";
import { buildReviewerDecisionAutofill, formatReviewerDecisionAutofillMarkdown } from "./reviewer-decision-autofill-utils.mjs";

const worksheetInput = process.env.REVIEWER_DECISION_AUTOFILL_WORKSHEET_INPUT || "./reports/reviewer-worksheet.csv";
const queueInput = process.env.REVIEWER_DECISION_AUTOFILL_QUEUE_INPUT || "./reports/reviewer-action-queue.json";
const simInput = process.env.REVIEWER_DECISION_AUTOFILL_SIM_INPUT || "./reports/reviewer-decision-sim.json";
const evidenceInput = process.env.REVIEWER_DECISION_AUTOFILL_EVIDENCE_INPUT || "./reports/reviewer-legal-evidence.json";
const splitInput = process.env.REVIEWER_DECISION_AUTOFILL_SPLIT_INPUT || "./reports/reviewer-split-packets.json";
const validateInput = process.env.REVIEWER_DECISION_AUTOFILL_VALIDATE_INPUT || "./reports/reviewer-worksheet-validate.json";

const autofillReportName = process.env.REVIEWER_DECISION_AUTOFILL_REPORT_NAME || "reviewer-decision-autofill.json";
const autofillMarkdownName = process.env.REVIEWER_DECISION_AUTOFILL_MARKDOWN_NAME || "reviewer-decision-autofill.md";

const prefilledCsvName = process.env.REVIEWER_WORKSHEET_PREFILLED_CSV_NAME || "reviewer-worksheet-prefilled.csv";
const prefilledJsonName = process.env.REVIEWER_WORKSHEET_PREFILLED_JSON_NAME || "reviewer-worksheet-prefilled.json";
const prefilledMarkdownName = process.env.REVIEWER_WORKSHEET_PREFILLED_MARKDOWN_NAME || "reviewer-worksheet-prefilled.md";

const exceptionsCsvName = process.env.REVIEWER_WORKSHEET_EXCEPTIONS_CSV_NAME || "reviewer-worksheet-exceptions.csv";
const exceptionsJsonName = process.env.REVIEWER_WORKSHEET_EXCEPTIONS_JSON_NAME || "reviewer-worksheet-exceptions.json";
const exceptionsMarkdownName = process.env.REVIEWER_WORKSHEET_EXCEPTIONS_MARKDOWN_NAME || "reviewer-worksheet-exceptions.md";

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
  const worksheetAbs = path.resolve(process.cwd(), worksheetInput);
  const loadedWorksheet = await loadReviewerWorksheetInput(worksheetAbs);

  const queue = await readJsonIfExists(queueInput);
  const sim = await readJsonIfExists(simInput);
  const evidence = await readJsonIfExists(evidenceInput);
  const split = await readJsonIfExists(splitInput);
  const validate = await readJsonIfExists(validateInput);

  const generatedAt = new Date().toISOString();
  const autofill = buildReviewerDecisionAutofill({
    worksheetRows: loadedWorksheet.rows,
    actionQueueSummary: queue?.summary || null,
    simReport: sim,
    evidenceReport: evidence,
    splitReport: split,
    validateReport: validate,
    generatedAt
  });

  const autofillReport = {
    generatedAt,
    readOnly: true,
    input: {
      worksheetInputPathUsed: worksheetAbs,
      queueInputPathUsed: path.resolve(process.cwd(), queueInput),
      simInputPathUsed: path.resolve(process.cwd(), simInput),
      evidenceInputPathUsed: path.resolve(process.cwd(), evidenceInput),
      splitInputPathUsed: path.resolve(process.cwd(), splitInput),
      validateInputPathUsed: path.resolve(process.cwd(), validateInput),
      worksheetInputFormat: loadedWorksheet.inputFormat
    },
    ...autofill
  };

  const reportsDir = path.resolve(process.cwd(), "reports");
  const autofillJsonPath = path.resolve(reportsDir, autofillReportName);
  const autofillMdPath = path.resolve(reportsDir, autofillMarkdownName);

  const prefilledJsonPath = path.resolve(reportsDir, prefilledJsonName);
  const prefilledCsvPath = path.resolve(reportsDir, prefilledCsvName);
  const prefilledMdPath = path.resolve(reportsDir, prefilledMarkdownName);

  const exceptionsJsonPath = path.resolve(reportsDir, exceptionsJsonName);
  const exceptionsCsvPath = path.resolve(reportsDir, exceptionsCsvName);
  const exceptionsMdPath = path.resolve(reportsDir, exceptionsMarkdownName);

  await fs.writeFile(autofillJsonPath, JSON.stringify(autofillReport, null, 2));
  await fs.writeFile(autofillMdPath, formatReviewerDecisionAutofillMarkdown(autofillReport));

  await fs.writeFile(prefilledJsonPath, JSON.stringify(autofill.prefilledWorksheet, null, 2));
  await fs.writeFile(prefilledCsvPath, formatReviewerWorksheetCsv(autofill.prefilledWorksheet));
  await fs.writeFile(prefilledMdPath, formatReviewerWorksheetMarkdown(autofill.prefilledWorksheet));

  await fs.writeFile(exceptionsJsonPath, JSON.stringify(autofill.exceptionsWorksheet, null, 2));
  await fs.writeFile(exceptionsCsvPath, formatReviewerWorksheetCsv(autofill.exceptionsWorksheet));
  await fs.writeFile(exceptionsMdPath, formatReviewerWorksheetMarkdown(autofill.exceptionsWorksheet));

  console.log(JSON.stringify(autofill.summary, null, 2));
  console.log(`Autofill report JSON written to ${autofillJsonPath}`);
  console.log(`Autofill report Markdown written to ${autofillMdPath}`);
  console.log(`Prefilled worksheet JSON written to ${prefilledJsonPath}`);
  console.log(`Prefilled worksheet CSV written to ${prefilledCsvPath}`);
  console.log(`Prefilled worksheet Markdown written to ${prefilledMdPath}`);
  console.log(`Exceptions worksheet JSON written to ${exceptionsJsonPath}`);
  console.log(`Exceptions worksheet CSV written to ${exceptionsCsvPath}`);
  console.log(`Exceptions worksheet Markdown written to ${exceptionsMdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
