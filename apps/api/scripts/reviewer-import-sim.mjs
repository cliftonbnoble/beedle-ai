import fs from "node:fs/promises";
import path from "node:path";
import { buildReviewerImportSimulation, formatReviewerImportSimulationMarkdown } from "./reviewer-import-sim-utils.mjs";

const inputPath = process.env.REVIEWER_IMPORT_SIM_INPUT || "./reports/reviewer-worksheet-prefilled-validate.json";
const reportName = process.env.REVIEWER_IMPORT_SIM_REPORT_NAME || "reviewer-import-sim.json";
const markdownName = process.env.REVIEWER_IMPORT_SIM_MARKDOWN_NAME || "reviewer-import-sim.md";

async function main() {
  const absoluteInput = path.resolve(process.cwd(), inputPath);
  const raw = await fs.readFile(absoluteInput, "utf8");
  const validateReport = JSON.parse(raw);

  const simulation = buildReviewerImportSimulation(validateReport);
  const report = {
    generatedAt: new Date().toISOString(),
    readOnly: true,
    input: {
      validateInputPathUsed: absoluteInput
    },
    ...simulation
  };

  const reportsDir = path.resolve(process.cwd(), "reports");
  const jsonPath = path.resolve(reportsDir, reportName);
  const markdownPath = path.resolve(reportsDir, markdownName);

  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, formatReviewerImportSimulationMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Import simulation JSON written to ${jsonPath}`);
  console.log(`Import simulation Markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
