import path from "node:path";
import {
  checkHealth,
  defaultDbPath,
  ensureDir,
  formatTimestamp,
  queryCorpusSnapshot,
  readJson,
  runNodeScript,
  writeJson,
  writeText
} from "./lib/overnight-corpus-lift-utils.mjs";

const apiBaseUrl = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.RETRIEVAL_CATCH_UP_BUSY_TIMEOUT_MS || "5000", 10);
const rounds = Math.max(1, Number.parseInt(process.env.RETRIEVAL_CATCH_UP_ROUNDS || "30", 10));
const sleepMs = Math.max(0, Number.parseInt(process.env.RETRIEVAL_CATCH_UP_SLEEP_MS || "60000", 10));
const stopAfterNoProgressRounds = Math.max(1, Number.parseInt(process.env.RETRIEVAL_CATCH_UP_STOP_AFTER_NO_PROGRESS || "5", 10));
const retrievalLimit = Math.max(1, Number.parseInt(process.env.RETRIEVAL_CATCH_UP_LIMIT || "200", 10));
const retrievalOrder = String(process.env.RETRIEVAL_CATCH_UP_ORDER || "coverage_rich_decision_like_searchable_asc");
const outputBaseDir = path.resolve(
  process.cwd(),
  process.env.RETRIEVAL_CATCH_UP_OUTPUT_DIR || "reports/retrieval-activation-catch-up"
);

function scriptPath(name) {
  return path.resolve(process.cwd(), "scripts", name);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatSummaryMarkdown(report) {
  const lines = [
    "# Retrieval Activation Catch-up Loop",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Run status: \`${report.runStatus}\``,
    `- Planned rounds: \`${report.plannedRounds}\``,
    `- Completed rounds: \`${report.completedRounds}\``,
    `- Retrieval limit: \`${report.retrievalLimit}\``,
    `- Retrieval order: \`${report.retrievalOrder}\``,
    ""
  ];

  lines.push("## Starting Snapshot");
  for (const [key, value] of Object.entries(report.startSnapshot || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Ending Snapshot");
  for (const [key, value] of Object.entries(report.endSnapshot || {})) {
    lines.push(`- ${key}: ${value}`);
  }
  lines.push("");

  lines.push("## Rounds");
  for (const row of report.rounds || []) {
    lines.push(
      `- round ${row.round}: offset=\`${row.offset}\` activatedDelta=\`${row.activatedDocumentDelta}\` trusted=\`${row.bundleTrustedDocumentCount}\` status=\`${row.stageStatus}\``
    );
  }
  if (!(report.rounds || []).length) lines.push("- none");
  lines.push("");

  if (report.error) {
    lines.push("## Error");
    lines.push(`- ${report.error}`);
    lines.push("");
  }

  return `${lines.join("\n")}\n`;
}

async function runRound(roundDir, roundNumber) {
  await ensureDir(roundDir);
  const beforeSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const totalGap = Number(beforeSnapshot.searchableButNotActiveCount || 0);
  const windowCount = Math.max(1, Math.ceil(Math.max(totalGap, 1) / retrievalLimit));
  const offset = ((roundNumber - 1) % windowCount) * retrievalLimit;

  const retrievalEnv = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    RETRIEVAL_ACTIVATION_GAP_LIMIT: String(retrievalLimit),
    RETRIEVAL_ACTIVATION_GAP_OFFSET: String(offset),
    RETRIEVAL_ACTIVATION_GAP_ORDER: retrievalOrder,
    RETRIEVAL_ACTIVATION_GAP_OUTPUT_DIR: path.resolve(roundDir, "retrieval-activation-gap")
  };
  const retrievalResult = await runNodeScript(scriptPath("retrieval-activation-gap-write.mjs"), {
    cwd: process.cwd(),
    env: retrievalEnv
  });
  if (!retrievalResult.ok) {
    throw new Error(`Round ${roundNumber}: retrieval activation gap failed:\n${retrievalResult.stderr || retrievalResult.stdout}`);
  }

  const report = await readJson(path.resolve(roundDir, "retrieval-activation-gap", "retrieval-activation-gap-report.json"));
  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const summary = {
    round: roundNumber,
    generatedAt: new Date().toISOString(),
    offset,
    totalGapBefore: totalGap,
    stageStatus: String(report.stageStatus || "unknown"),
    bundleTrustedDocumentCount: Number(report.summary?.bundleTrustedDocumentCount || 0),
    activatedDocumentDelta: Number(afterSnapshot.activeRetrievalDecisionCount || 0) - Number(beforeSnapshot.activeRetrievalDecisionCount || 0),
    beforeSnapshot,
    afterSnapshot
  };

  await Promise.all([
    writeJson(path.resolve(roundDir, "summary.json"), summary),
    writeText(
      path.resolve(roundDir, "summary.md"),
      [
        "# Retrieval Catch-up Round",
        "",
        `- round: ${summary.round}`,
        `- offset: ${summary.offset}`,
        `- stageStatus: ${summary.stageStatus}`,
        `- bundleTrustedDocumentCount: ${summary.bundleTrustedDocumentCount}`,
        `- activatedDocumentDelta: ${summary.activatedDocumentDelta}`,
        ""
      ].join("\n")
    )
  ]);

  return summary;
}

export async function main() {
  await ensureDir(outputBaseDir);

  const health = await checkHealth(apiBaseUrl);
  if (!health.ok) {
    throw new Error(`API health check failed for ${apiBaseUrl}: ${health.error}`);
  }

  const runDir = path.resolve(outputBaseDir, formatTimestamp(new Date()));
  await ensureDir(runDir);

  const startSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const roundReports = [];
  let runStatus = "completed";
  let error = null;
  let noProgressRounds = 0;

  for (let round = 1; round <= rounds; round += 1) {
    try {
      const report = await runRound(path.resolve(runDir, `round-${String(round).padStart(2, "0")}`), round);
      roundReports.push(report);
      if (report.activatedDocumentDelta > 0) {
        noProgressRounds = 0;
      } else {
        noProgressRounds += 1;
      }
      if (noProgressRounds >= stopAfterNoProgressRounds) {
        runStatus = "stopped_no_progress";
        break;
      }
      if (round < rounds && sleepMs > 0) {
        await sleep(sleepMs);
      }
    } catch (stageError) {
      runStatus = "failed";
      error = stageError instanceof Error ? stageError.message : String(stageError);
      break;
    }
  }

  const endSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const summary = {
    generatedAt: new Date().toISOString(),
    apiBase: apiBaseUrl,
    dbPath,
    runStatus,
    plannedRounds: rounds,
    completedRounds: roundReports.length,
    sleepMs,
    stopAfterNoProgressRounds,
    retrievalLimit,
    retrievalOrder,
    startSnapshot,
    endSnapshot,
    overallDelta: {
      activeRetrievalDelta: Number(endSnapshot.activeRetrievalDecisionCount || 0) - Number(startSnapshot.activeRetrievalDecisionCount || 0),
      searchableButNotActiveDelta: Number(endSnapshot.searchableButNotActiveCount || 0) - Number(startSnapshot.searchableButNotActiveCount || 0)
    },
    rounds: roundReports.map((row) => ({
      round: row.round,
      offset: row.offset,
      stageStatus: row.stageStatus,
      bundleTrustedDocumentCount: row.bundleTrustedDocumentCount,
      activatedDocumentDelta: row.activatedDocumentDelta
    })),
    error
  };

  await Promise.all([
    writeJson(path.resolve(runDir, "summary.json"), summary),
    writeText(path.resolve(runDir, "summary.md"), formatSummaryMarkdown(summary))
  ]);

  console.log(JSON.stringify({ runStatus: summary.runStatus, overallDelta: summary.overallDelta }, null, 2));
  console.log(`Retrieval catch-up summary JSON written to ${path.resolve(runDir, "summary.json")}`);
  console.log(`Retrieval catch-up summary Markdown written to ${path.resolve(runDir, "summary.md")}`);

  if (summary.runStatus === "failed") {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
