import fs from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';

const cwd = process.cwd();
const reportsDir = path.resolve(cwd, 'reports');
const jobsDir = path.join(reportsDir, 'vector-backfill-jobs');
const stamp = new Date().toISOString().replace(/[.:]/g, '-');
const jobId = process.env.RETRIEVAL_VECTOR_BACKFILL_JOB_ID || `vector-backfill-${stamp}`;
const reportName = process.env.RETRIEVAL_VECTOR_BACKFILL_REPORT_NAME || `${jobId}.json`;
const markdownName = process.env.RETRIEVAL_VECTOR_BACKFILL_MARKDOWN_NAME || `${jobId}.md`;
const logPath = path.join(jobsDir, `${jobId}.log`);
const metaPath = path.join(jobsDir, `${jobId}.json`);
const env = {
  ...process.env,
  RETRIEVAL_VECTOR_BACKFILL_REPORT_NAME: reportName,
  RETRIEVAL_VECTOR_BACKFILL_MARKDOWN_NAME: markdownName
};

await fs.mkdir(jobsDir, { recursive: true });
const logHandle = await fs.open(logPath, 'a');
const child = spawn('node', ['./scripts/retrieval-vector-backfill.mjs'], {
  cwd,
  env,
  detached: true,
  stdio: ['ignore', logHandle.fd, logHandle.fd]
});
child.unref();
await logHandle.close();

const metadata = {
  jobId,
  pid: child.pid,
  launchedAt: new Date().toISOString(),
  cwd,
  logPath,
  reportPath: path.join(reportsDir, reportName),
  markdownPath: path.join(reportsDir, markdownName),
  apiBaseUrl: env.API_BASE_URL || 'http://127.0.0.1:8787',
  batchSize: Number.parseInt(env.RETRIEVAL_VECTOR_BACKFILL_BATCH_SIZE || '25', 10),
  limit: env.RETRIEVAL_VECTOR_BACKFILL_LIMIT ? Number.parseInt(env.RETRIEVAL_VECTOR_BACKFILL_LIMIT, 10) : null
};
await fs.writeFile(metaPath, `${JSON.stringify(metadata, null, 2)}\n`);
console.log(JSON.stringify(metadata, null, 2));
