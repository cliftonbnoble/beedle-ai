import fs from 'node:fs/promises';
import path from 'node:path';

const cwd = process.cwd();
const jobsDir = path.resolve(cwd, 'reports', 'vector-backfill-jobs');
const reportsDir = path.resolve(cwd, 'reports');
const requested = process.env.RETRIEVAL_VECTOR_BACKFILL_JOB_ID || '';

async function readLatestJob() {
  let jsonFiles = [];
  try {
    const entries = await fs.readdir(jobsDir);
    jsonFiles = entries.filter((name) => name.endsWith('.json')).sort();
  } catch {}

  const target = requested ? `${requested}.json` : jsonFiles[jsonFiles.length - 1];
  if (!target) throw new Error('No vector backfill jobs found.');

  const candidatePaths = [
    path.join(jobsDir, target),
    path.join(reportsDir, target)
  ];

  for (const candidate of candidatePaths) {
    try {
      const payload = JSON.parse(await fs.readFile(candidate, 'utf8'));
      if (payload && payload.jobId) return payload;
      if (payload && (payload.discoveredChunkCount != null || payload.processedCount != null)) {
        return {
          jobId: target.replace(/\.json$/, ''),
          pid: null,
          launchedAt: null,
          reportPath: path.join(reportsDir, target),
          reportExists: true,
          reportSummary: payload.counts || payload.summary || payload,
          logPath: path.join(jobsDir, `${target.replace(/\.json$/, '')}.log`)
        };
      }
    } catch {}
  }

  throw new Error(`Vector backfill job metadata not ready for ${target.replace(/\.json$/, '')}.`);
}

function pidAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

const job = await readLatestJob();
let reportExists = false;
let reportSummary = null;
try {
  const raw = await fs.readFile(job.reportPath, 'utf8');
  const parsed = JSON.parse(raw);
  reportExists = true;
  reportSummary = parsed.counts || parsed.summary || null;
} catch {}

let logTail = '';
try {
  const rawLog = await fs.readFile(job.logPath, 'utf8');
  logTail = rawLog.split(/\r?\n/).filter(Boolean).slice(-20).join('\n');
} catch {}

console.log(JSON.stringify({
  jobId: job.jobId,
  pid: job.pid,
  running: pidAlive(job.pid),
  launchedAt: job.launchedAt,
  reportPath: job.reportPath,
  reportExists,
  reportSummary,
  logPath: job.logPath,
  logTail
}, null, 2));
