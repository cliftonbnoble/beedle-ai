import fs from "node:fs/promises";
import path from "node:path";
import { defaultReportsBaseDir, ensureDir, readJson, writeJson, writeText } from "./lib/overnight-corpus-lift-utils.mjs";
import {
  aggregateRunSummaries,
  formatOvernightCorpusLiftSummaryMarkdown
} from "./lib/overnight-corpus-lift-summary-utils.mjs";

const outputBaseDir = path.resolve(process.cwd(), process.env.OVERNIGHT_CORPUS_LIFT_OUTPUT_BASE_DIR || defaultReportsBaseDir);
const lookbackHours = Math.max(1, Number.parseInt(process.env.OVERNIGHT_CORPUS_LIFT_SUMMARY_LOOKBACK_HOURS || "18", 10));
const outputDir = path.resolve(process.cwd(), process.env.OVERNIGHT_CORPUS_LIFT_SUMMARY_OUTPUT_DIR || outputBaseDir);
const jsonName = process.env.OVERNIGHT_CORPUS_LIFT_SUMMARY_REPORT_NAME || "overnight-corpus-lift-morning-summary.json";
const markdownName = process.env.OVERNIGHT_CORPUS_LIFT_SUMMARY_MARKDOWN_NAME || "overnight-corpus-lift-morning-summary.md";
const targetSearchable = Number.parseInt(process.env.OVERNIGHT_CORPUS_LIFT_TARGET_SEARCHABLE || "7000", 10);

function withinLookback(timestamp) {
  const time = Date.parse(timestamp || "");
  if (!Number.isFinite(time)) return false;
  const ageMs = Date.now() - time;
  return ageMs >= 0 && ageMs <= lookbackHours * 60 * 60 * 1000;
}

export async function main() {
  await ensureDir(outputDir);
  const entries = await fs.readdir(outputBaseDir, { withFileTypes: true }).catch(() => []);
  const runSummaries = [];

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const summaryPath = path.resolve(outputBaseDir, entry.name, "summary.json");
    try {
      const summary = await readJson(summaryPath);
      if (withinLookback(summary.generatedAt)) {
        runSummaries.push(summary);
      }
    } catch {
      // ignore non-run directories
    }
  }

  const summary = aggregateRunSummaries(runSummaries, { targetSearchable });
  const report = {
    generatedAt: new Date().toISOString(),
    outputBaseDir,
    lookbackHours,
    summary
  };

  const jsonPath = path.resolve(outputDir, jsonName);
  const markdownPath = path.resolve(outputDir, markdownName);
  await Promise.all([
    writeJson(jsonPath, report),
    writeText(markdownPath, formatOvernightCorpusLiftSummaryMarkdown(report))
  ]);

  console.log(JSON.stringify(summary, null, 2));
  console.log(`Overnight corpus lift morning summary JSON written to ${jsonPath}`);
  console.log(`Overnight corpus lift morning summary Markdown written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
