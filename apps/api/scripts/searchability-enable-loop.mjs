import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

const apiBaseUrl = process.env.API_BASE_URL ?? "http://127.0.0.1:8787";
const limit = Math.max(1, Number.parseInt(process.env.SEARCHABILITY_ENABLE_LIMIT ?? "25", 10));
const maxRounds = Math.max(1, Number.parseInt(process.env.SEARCHABILITY_ENABLE_MAX_ROUNDS ?? "200", 10));
const sleepMs = Math.max(0, Number.parseInt(process.env.SEARCHABILITY_ENABLE_SLEEP_MS ?? "10000", 10));
const realOnly = process.env.SEARCHABILITY_ENABLE_REAL_ONLY !== "0";
const allowedModes = new Set([
  "qcPassed",
  "missingIndexOnlyTextReady",
  "singleContextTextReady",
  "decisionLikeTextReady"
]);
const mode = allowedModes.has(process.env.SEARCHABILITY_ENABLE_MODE || "")
  ? process.env.SEARCHABILITY_ENABLE_MODE
  : "qcPassed";
const outputDir = path.resolve(
  process.cwd(),
  process.env.SEARCHABILITY_ENABLE_OUTPUT_DIR ?? "reports/searchability-enable-loop",
);

function nowTimestamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function writeJson(filePath, payload) {
  await writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text();
  let data = null;

  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { raw: text };
  }

  return { response, data, text };
}

async function checkHealth(filePath) {
  const { response, data, text } = await fetchJson(`${apiBaseUrl}/health`);
  await writeJson(filePath, {
    status: response.status,
    ok: response.ok,
    body: data ?? text,
  });

  return response.ok;
}

async function main() {
  await mkdir(outputDir, { recursive: true });

  console.log(
    JSON.stringify(
      {
        apiBaseUrl,
        limit,
        maxRounds,
        sleepMs,
        realOnly,
        mode,
        outputDir,
      },
      null,
      2,
    ),
  );

  for (let round = 1; round <= maxRounds; round += 1) {
    const ts = nowTimestamp();
    console.log(`\n=== ROUND ${round} @ ${ts} ===`);

    const healthBeforePath = path.join(outputDir, `${ts}-health-before.json`);
    if (!(await checkHealth(healthBeforePath))) {
      console.error(`API unhealthy before round ${round}. Stopping.`);
      return;
    }

    const candidatePath = path.join(outputDir, `${ts}-candidates.json`);
    const enablePath = path.join(outputDir, `${ts}-enable.json`);

    let candidateResult;
    try {
      candidateResult = await fetchJson(
        `${apiBaseUrl}/admin/ingestion/searchability/candidates?limit=${limit}&realOnly=${realOnly ? "1" : "0"}&mode=${encodeURIComponent(mode)}`,
      );
    } catch (error) {
      await writeJson(candidatePath, { error: String(error) });
      console.error(`Candidate fetch failed in round ${round}.`);
      return;
    }

    await writeJson(candidatePath, {
      status: candidateResult.response.status,
      ok: candidateResult.response.ok,
      body: candidateResult.data ?? candidateResult.text,
    });

    if (!candidateResult.response.ok) {
      console.error(`Candidate fetch returned ${candidateResult.response.status}. Stopping.`);
      return;
    }

    const count = Number(candidateResult.data?.summary?.candidateCount ?? 0);
    console.log(`Candidate count: ${count}`);

    if (count === 0) {
      console.log("No more eligible candidates. Stopping.");
      return;
    }

    let enableResult;
    try {
      enableResult = await fetchJson(`${apiBaseUrl}/admin/ingestion/searchability/enable`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          limit,
          realOnly,
          mode,
          dryRun: false,
        }),
      });
    } catch (error) {
      await writeJson(enablePath, { error: String(error) });
      console.error(`Enable request failed in round ${round}.`);
      return;
    }

    await writeJson(enablePath, {
      status: enableResult.response.status,
      ok: enableResult.response.ok,
      body: enableResult.data ?? enableResult.text,
    });

    console.log(`Enable status: ${enableResult.response.status}`);
    if (!enableResult.response.ok) {
      console.error(`Enable failed in round ${round}. See ${enablePath}`);
      return;
    }

    const enabledCount = Number(enableResult.data?.summary?.enabledCount ?? 0);
    console.log(`Enabled count: ${enabledCount}`);

    const healthAfterPath = path.join(outputDir, `${ts}-health-after.json`);
    if (!(await checkHealth(healthAfterPath))) {
      console.error(`API unhealthy after round ${round}. Stopping.`);
      return;
    }

    if (sleepMs > 0) {
      await sleep(sleepMs);
    }
  }

  console.log("Reached max rounds. Stopping.");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
