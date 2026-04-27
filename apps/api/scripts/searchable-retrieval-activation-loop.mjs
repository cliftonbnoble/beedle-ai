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
const busyTimeoutMs = Number.parseInt(process.env.SEARCHABLE_RETRIEVAL_LOOP_BUSY_TIMEOUT_MS || "5000", 10);
const rounds = Math.max(1, Number.parseInt(process.env.SEARCHABLE_RETRIEVAL_LOOP_ROUNDS || "50", 10));
const sleepMs = Math.max(0, Number.parseInt(process.env.SEARCHABLE_RETRIEVAL_LOOP_SLEEP_MS || "10000", 10));
const stopAfterNoProgressRounds = Math.max(
  1,
  Number.parseInt(process.env.SEARCHABLE_RETRIEVAL_LOOP_STOP_AFTER_NO_PROGRESS || "5", 10)
);
const activationLimit = Math.max(1, Number.parseInt(process.env.SEARCHABLE_RETRIEVAL_LOOP_LIMIT || "25", 10));
const activationOrder = String(
  process.env.SEARCHABLE_RETRIEVAL_LOOP_ORDER || "decision_like_searchable_asc"
);
const performVectorUpsert = (process.env.SEARCHABLE_RETRIEVAL_LOOP_PERFORM_VECTOR_UPSERT || "0") === "1";
const outputBaseDir = path.resolve(
  process.cwd(),
  process.env.SEARCHABLE_RETRIEVAL_LOOP_OUTPUT_DIR || "reports/searchable-retrieval-activation-loop"
);

function scriptPath(name) {
  return path.resolve(process.cwd(), "scripts", name);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatSummaryMarkdown(report) {
  const lines = [
    "# Searchable Retrieval Activation Loop",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- API base: \`${report.apiBase}\``,
    `- Run status: \`${report.runStatus}\``,
    `- Planned rounds: \`${report.plannedRounds}\``,
    `- Completed rounds: \`${report.completedRounds}\``,
    `- Activation limit: \`${report.activationLimit}\``,
    `- Activation order: \`${report.activationOrder}\``,
    `- Vector during activation: \`${report.performVectorUpsert}\``,
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
      `- round ${row.round}: status=\`${row.stageStatus}\` selected=\`${row.selectedDocumentCount}\` attempted=\`${row.documentsAttempted}\` activatedDelta=\`${row.activatedDocumentDelta}\``
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
  const env = {
    API_BASE_URL: apiBaseUrl,
    D1_DB_PATH: dbPath,
    SEARCHABLE_RETRIEVAL_ACTIVATION_LIMIT: String(activationLimit),
    SEARCHABLE_RETRIEVAL_ACTIVATION_OFFSET: "0",
    SEARCHABLE_RETRIEVAL_ACTIVATION_ORDER: activationOrder,
    SEARCHABLE_RETRIEVAL_ACTIVATION_PERFORM_VECTOR_UPSERT: performVectorUpsert ? "1" : "0",
    SEARCHABLE_RETRIEVAL_ACTIVATION_OUTPUT_DIR: path.resolve(roundDir, "searchable-retrieval-activation")
  };

  const run = await runNodeScript(scriptPath("searchable-retrieval-activation-batch.mjs"), {
    cwd: process.cwd(),
    env
  });
  if (!run.ok) {
    throw new Error(`Round ${roundNumber}: searchable activation failed:\n${run.stderr || run.stdout}`);
  }

  const report = await readJson(
    path.resolve(roundDir, "searchable-retrieval-activation", "searchable-retrieval-activation-report.json")
  );
  const afterSnapshot = await queryCorpusSnapshot({ dbPath, busyTimeoutMs });
  const summary = {
    round: roundNumber,
    generatedAt: new Date().toISOString(),
    stageStatus: String(report.stageStatus || "unknown"),
    selectedDocumentCount: Number(report.summary?.selectedDocumentCount || 0),
    documentsAttempted: Number(report.summary?.documentsAttempted || 0),
    chunksAttempted: Number(report.summary?.chunksAttempted || 0),
    activatedDocumentDelta:
      Number(afterSnapshot.activeRetrievalDecisionCount || 0) - Number(beforeSnapshot.activeRetrievalDecisionCount || 0),
    searchableButNotActiveDelta:
      Number(afterSnapshot.searchableButNotActiveCount || 0) - Number(beforeSnapshot.searchableButNotActiveCount || 0),
    beforeSnapshot,
    afterSnapshot
  };

  await Promise.all([
    writeJson(path.resolve(roundDir, "summary.json"), summary),
    writeText(
      path.resolve(roundDir, "summary.md"),
      [
        "# Searchable Activation Round",
        "",
        `- round: ${summary.round}`,
        `- stageStatus: ${summary.stageStatus}`,
        `- selectedDocumentCount: ${summary.selectedDocumentCount}`,
        `- documentsAttempted: ${summary.documentsAttempted}`,
        `- chunksAttempted: ${summary.chunksAttempted}`,
        `- activatedDocumentDelta: ${summary.activatedDocumentDelta}`,
        `- searchableButNotActiveDelta: ${summary.searchableButNotActiveDelta}`,
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
    activationLimit,
    activationOrder,
    performVectorUpsert,
    startSnapshot,
    endSnapshot,
    overallDelta: {
      activeRetrievalDelta: Number(endSnapshot.activeRetrievalDecisionCount || 0) - Number(startSnapshot.activeRetrievalDecisionCount || 0),
      searchableButNotActiveDelta:
        Number(endSnapshot.searchableButNotActiveCount || 0) - Number(startSnapshot.searchableButNotActiveCount || 0)
    },
    rounds: roundReports.map((row) => ({
      round: row.round,
      stageStatus: row.stageStatus,
      selectedDocumentCount: row.selectedDocumentCount,
      documentsAttempted: row.documentsAttempted,
      chunksAttempted: row.chunksAttempted,
      activatedDocumentDelta: row.activatedDocumentDelta
    })),
    error
  };

  await Promise.all([
    writeJson(path.resolve(runDir, "summary.json"), summary),
    writeText(path.resolve(runDir, "summary.md"), formatSummaryMarkdown(summary))
  ]);

  console.log(JSON.stringify({ runStatus: summary.runStatus, overallDelta: summary.overallDelta }, null, 2));
  console.log(`Searchable activation loop summary JSON written to ${path.resolve(runDir, "summary.json")}`);
  console.log(`Searchable activation loop summary Markdown written to ${path.resolve(runDir, "summary.md")}`);

  if (summary.runStatus === "failed") {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
