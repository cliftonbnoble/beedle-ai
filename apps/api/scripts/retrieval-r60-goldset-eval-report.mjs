import fs from "node:fs/promises";
import path from "node:path";
import { R60_GOLDSET_TASKS, runR60GoldsetEvaluation } from "./retrieval-r60-goldset-eval-utils.mjs";
import { assertPreflightOrThrow, runRuntimePreflight, shouldFailFast } from "./retrieval-runtime-preflight-utils.mjs";
import { benchmarkResponseToBody, callBenchmarkDebug } from "./retrieval-benchmark-contract-utils.mjs";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.RETRIEVAL_R60_REPORT_NAME || "retrieval-r60-goldset-eval-report.json";
const markdownName = process.env.RETRIEVAL_R60_MARKDOWN_NAME || "retrieval-r60-goldset-eval-report.md";
const tasksName = process.env.RETRIEVAL_R60_TASKS_NAME || "retrieval-r60-goldset-tasks.json";
const queryLimit = Number.parseInt(process.env.RETRIEVAL_R60_LIMIT || "10", 10);

function toMarkdown(report) {
  const lines = [];
  lines.push("# Retrieval R60 Gold-Set Evaluation Report");
  lines.push("");
  lines.push("## Summary");
  for (const [key, value] of Object.entries(report.summary || {})) lines.push(`- ${key}: ${value}`);
  lines.push(`- top1DecisionHitRate: ${report.top1DecisionHitRate}`);
  lines.push(`- top3DecisionHitRate: ${report.top3DecisionHitRate}`);
  lines.push(`- top5DecisionHitRate: ${report.top5DecisionHitRate}`);
  lines.push(`- sectionTypeHitRate: ${report.sectionTypeHitRate}`);
  lines.push(`- noisyChunkDominationRate: ${report.noisyChunkDominationRate}`);
  lines.push("");

  lines.push("## Intent Breakdown");
  for (const row of report.intentBreakdown || []) {
    lines.push(
      `- ${row.intent}: tasks=${row.tasks}, top1=${row.top1DecisionHitRate}, top3=${row.top3DecisionHitRate}, top5=${row.top5DecisionHitRate}, sectionType=${row.sectionTypeHitRate}`
    );
  }
  lines.push("");

  lines.push("## False Positive Chunk Types");
  for (const row of report.falsePositiveChunkTypeCounts || []) {
    lines.push(`- ${row.chunkType}: ${row.count}`);
  }
  if (!(report.falsePositiveChunkTypeCounts || []).length) lines.push("- none");
  lines.push("");

  lines.push("## Query Rows (first 10)");
  for (const row of (report.queryResults || []).slice(0, 10)) {
    lines.push(
      `- ${row.queryId} | intent=${row.intent} | top1=${row.top1DecisionHit} | top3=${row.top3DecisionHit} | top5=${row.top5DecisionHit} | sectionHit=${row.sectionTypeHit} | noisy=${row.noisyChunkDominated} | firstExpectedRank=${row.firstExpectedRank}`
    );
  }
  lines.push("");
  lines.push("- Dry-run only. No activation writes, rollback writes, gate mutation, or runtime ranking mutation.");
  return `${lines.join("\n")}\n`;
}

async function main() {
  await fs.mkdir(reportsDir, { recursive: true });

  if (shouldFailFast()) {
    const preflight = await runRuntimePreflight({ apiBaseUrl: apiBase });
    assertPreflightOrThrow(preflight, "retrieval-r60-goldset-eval-report");
  }

  const tasksPath = path.resolve(reportsDir, tasksName);
  await fs.writeFile(tasksPath, JSON.stringify(R60_GOLDSET_TASKS, null, 2));

  const report = await runR60GoldsetEvaluation({
    apiBase,
    reportsDir,
    tasks: R60_GOLDSET_TASKS,
    limit: queryLimit,
    fetchSearchDebug: async (payload) => {
      const contract = await callBenchmarkDebug({ apiBaseUrl: apiBase, payload });
      return benchmarkResponseToBody(contract);
    }
  });

  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);
  await Promise.all([fs.writeFile(jsonPath, JSON.stringify(report, null, 2)), fs.writeFile(mdPath, toMarkdown(report))]);

  console.log(
    JSON.stringify(
      {
        tasksEvaluated: report.summary.tasksEvaluated,
        top1DecisionHitRate: report.top1DecisionHitRate,
        top3DecisionHitRate: report.top3DecisionHitRate,
        top5DecisionHitRate: report.top5DecisionHitRate,
        sectionTypeHitRate: report.sectionTypeHitRate,
        noisyChunkDominationRate: report.noisyChunkDominationRate
      },
      null,
      2
    )
  );
  console.log(`R60 gold-set tasks written to ${tasksPath}`);
  console.log(`R60 gold-set evaluation JSON report written to ${jsonPath}`);
  console.log(`R60 gold-set evaluation Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
