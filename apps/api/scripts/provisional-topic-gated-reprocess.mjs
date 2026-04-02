import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const writeEnabled = process.env.TOPIC_GATED_REPROCESS_WRITE === "1";
const reportPath = path.resolve(process.cwd(), "reports", "provisional-topic-candidate-report.json");
const outputPath = path.resolve(process.cwd(), "reports", "provisional-topic-gated-reprocess-report.json");

async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    body = { raw };
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function main() {
  const report = JSON.parse(await fs.readFile(reportPath, "utf8"));
  const batch = report.recommendedBatch || [];

  const result = {
    generatedAt: new Date().toISOString(),
    apiBase,
    writeEnabled,
    sourceReport: reportPath,
    summary: {
      recommendedBatchSize: batch.length,
      blockedTopicLikelyCount: (report.blockedButTopicLikely || []).length,
      attempted: 0,
      reprocessed: 0,
      skipped: 0
    },
    candidates: [],
    blockedTopicLikely: (report.blockedButTopicLikely || []).slice(0, 12).map((row) => ({
      id: row.id,
      title: row.title,
      strongestTopic: row.strongestTopic,
      blockers: row.heuristic?.blockers || []
    })),
    acquisitionNeeded: batch.length === 0
  };

  if (!writeEnabled) {
    result.summary.skipped = batch.length;
    result.candidates = batch.map((row) => ({
      id: row.id,
      title: row.title,
      action: "dry_run_only",
      strongestTopic: row.heuristic?.strongestTopic || "unknown",
      score: row.heuristic?.score ?? 0
    }));
  } else {
    for (const row of batch) {
      result.summary.attempted += 1;
      const payload = await fetchJson(`${apiBase}/admin/ingestion/documents/${row.id}/reprocess`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}"
      });
      result.summary.reprocessed += 1;
      result.candidates.push({
        id: row.id,
        title: row.title,
        action: "reprocessed",
        payload
      });
    }
  }

  await fs.writeFile(outputPath, JSON.stringify(result, null, 2));
  console.log(JSON.stringify(result.summary, null, 2));
  console.log(`Provisional topic gated reprocess report written to ${outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
