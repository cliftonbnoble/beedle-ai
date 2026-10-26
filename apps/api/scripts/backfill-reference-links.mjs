import { randomUUID } from "node:crypto";
import { completeMaintenanceJob, readMaintenanceJob } from "./lib/maintenance-job-client.mjs";
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number(process.env.BACKFILL_LIMIT || "1000");
const offset = Number(process.env.BACKFILL_OFFSET || "0");

async function main() {
  if(process.env.MAINTENANCE_JOB_ID) {
    console.log(await completeMaintenanceJob(apiBase,await readMaintenanceJob(apiBase,process.env.MAINTENANCE_JOB_ID)));return;
  }
  const jobId=`maintenance_${randomUUID()}`;
  console.log(`Planned durable job ID: ${jobId}; resume with MAINTENANCE_JOB_ID if acknowledgement is lost.`);
  const response = await fetch(`${apiBase}/admin/references/backfill`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jobId, limit, offset })
  });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  if (!response.ok) {
    throw new Error(`Backfill failed: ${response.status} ${JSON.stringify(body)}`);
  }
  body=await completeMaintenanceJob(apiBase,body);
  console.log("Reference backfill complete:");
  console.log(JSON.stringify(body, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
