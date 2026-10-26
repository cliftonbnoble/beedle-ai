import { randomUUID } from "node:crypto";
import { completeMaintenanceJob, readMaintenanceJob } from "./lib/maintenance-job-client.mjs";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const reportsDir = path.resolve(process.cwd(), "reports");
const reportName = process.env.RETRIEVAL_VECTOR_BACKFILL_REPORT_NAME || "retrieval-vector-backfill-report.json";
const markdownName = process.env.RETRIEVAL_VECTOR_BACKFILL_MARKDOWN_NAME || "retrieval-vector-backfill-report.md";

function formatMarkdown(report) {
  return ["# Retrieval Vector Backfill Job", "", `- Job: ${report.jobId}`, `- Snapshot: ${report.snapshotId}`, `- State: ${report.state}`, `- Dry run: ${report.dryRun}`,
    ...Object.entries(report.counts).map(([key,value])=>`- ${key}: ${value}`),"", "Completed counts are verified stored vectors. Skipped items changed or exceeded the source-text limit; inspect and prepare a new job."] .join("\n");
}

async function main() {
  if(process.env.MAINTENANCE_JOB_ID) {
    const report=await completeMaintenanceJob(apiBase,await readMaintenanceJob(apiBase,process.env.MAINTENANCE_JOB_ID));
    await saveReport(report);return;
  }
  const jobId=`maintenance_${randomUUID()}`;
  console.log(`Planned durable job ID: ${jobId}; resume with MAINTENANCE_JOB_ID if acknowledgement is lost.`);
  const response = await fetch(`${apiBase}/admin/retrieval/vectors/backfill`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jobId,
      batchSize: Number.parseInt(process.env.RETRIEVAL_VECTOR_BACKFILL_BATCH_SIZE || "25", 10),
      limit: process.env.RETRIEVAL_VECTOR_BACKFILL_LIMIT ? Number.parseInt(process.env.RETRIEVAL_VECTOR_BACKFILL_LIMIT, 10) : undefined,
      offset: Number.parseInt(process.env.RETRIEVAL_VECTOR_BACKFILL_OFFSET || "0", 10),
      dryRun: (process.env.RETRIEVAL_VECTOR_BACKFILL_DRY_RUN || "0") === "1",
      includeDocumentChunks: (process.env.RETRIEVAL_VECTOR_BACKFILL_INCLUDE_DOCUMENT_CHUNKS || "1") === "1",
      includeTrustedChunks: (process.env.RETRIEVAL_VECTOR_BACKFILL_INCLUDE_TRUSTED_CHUNKS || "1") === "1"
    })
  });

  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new Error(`Expected JSON response from vector backfill endpoint, got: ${raw.slice(0, 300)}`);
  }

  if (!response.ok) {
    throw new Error(`Vector backfill failed (${response.status}): ${JSON.stringify(body)}`);
  }

  await saveReport(body);
  const completed=await completeMaintenanceJob(apiBase,body);
  await saveReport(completed);
}

async function saveReport(body) {
  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.resolve(reportsDir, reportName);
  const mdPath = path.resolve(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(body, null, 2));
  await fs.writeFile(mdPath, formatMarkdown(body));

  console.log(JSON.stringify(body.counts, null, 2));
  console.log(`Retrieval vector backfill JSON report written to ${jsonPath}`);
  console.log(`Retrieval vector backfill Markdown report written to ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
