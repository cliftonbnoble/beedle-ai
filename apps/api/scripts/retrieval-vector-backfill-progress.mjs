import fs from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const apiDir = process.cwd();
const reportsDir = path.resolve(apiDir, "reports");
const backgroundDir = path.join(reportsDir, "background-jobs");
const explicitRunDir = process.env.VECTOR_BACKFILL_RUN_DIR || "";
const dbPath = process.env.D1_DB_PATH || path.join(apiDir, ".wrangler/state/v3/d1/miniflare-D1DatabaseObject/c1163e5c7f431b6e93caee023c4de17a42d58b1f179732141bd39cd6138e1bac.sqlite");

async function pathExists(candidate) {
  try {
    await fs.access(candidate);
    return true;
  } catch {
    return false;
  }
}

async function latestVectorRunDir() {
  if (explicitRunDir) return explicitRunDir;
  const entries = await fs.readdir(backgroundDir, { withFileTypes: true }).catch(() => []);
  const dirs = [];
  for (const entry of entries) {
    if (!entry.isDirectory() || !entry.name.startsWith("vector-")) continue;
    const full = path.join(backgroundDir, entry.name);
    const stat = await fs.stat(full).catch(() => null);
    if (stat) dirs.push({ full, mtimeMs: stat.mtimeMs });
  }
  dirs.sort((a, b) => b.mtimeMs - a.mtimeMs);
  return dirs[0]?.full || null;
}

async function readJson(candidate) {
  try {
    return JSON.parse(await fs.readFile(candidate, "utf8"));
  } catch {
    return null;
  }
}

async function tail(candidate, lineCount = 30) {
  try {
    const raw = await fs.readFile(candidate, "utf8");
    return raw.split(/\r?\n/).filter(Boolean).slice(-lineCount);
  } catch {
    return [];
  }
}

async function sqliteCounts() {
  if (!(await pathExists(dbPath))) return null;
  const query = `
    SELECT
      (SELECT COUNT(*) FROM retrieval_search_chunks WHERE active=1) AS activeSearchChunks,
      (SELECT COUNT(DISTINCT chunk_id) FROM retrieval_embedding_rows) AS distinctEmbeddingChunks,
      (SELECT COUNT(*) FROM retrieval_embedding_rows) AS embeddingRows,
      (SELECT COUNT(DISTINCT document_id) FROM retrieval_embedding_rows) AS embeddingDocs;
  `;
  try {
    const { stdout } = await execFileAsync("sqlite3", ["-json", `file:${dbPath}?mode=ro&immutable=1`, query], { maxBuffer: 1024 * 1024 });
    return JSON.parse(stdout)[0] || null;
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error) };
  }
}

function parseLatestRound(lines) {
  const roundLine = [...lines].reverse().find((line) => line.includes("Vector retry round"));
  const offsetLine = [...lines].reverse().find((line) => line.includes("running vector backfill") || line.includes("complete; next offset"));
  return { roundLine: roundLine || null, offsetLine: offsetLine || null };
}

const runDir = await latestVectorRunDir();
const state = runDir ? await readJson(path.join(runDir, "vector-backfill-state.json")) : null;
const watchdogTail = runDir ? await tail(path.join(runDir, "watchdog.log"), 50) : [];
const vectorTail = runDir ? await tail(path.join(runDir, "vector.log"), 40) : [];
const counts = await sqliteCounts();
const latest = parseLatestRound(watchdogTail);
const totalActiveChunks = counts?.activeSearchChunks || null;
const nextOffset = state?.nextOffset ?? null;
const includeDocumentChunks = state?.includeDocumentChunks == null ? true : state.includeDocumentChunks === "1";
const includeTrustedChunks = state?.includeTrustedChunks == null ? true : state.includeTrustedChunks === "1";
const sourceMultiplier = (includeDocumentChunks ? 1 : 0) + (includeTrustedChunks ? 1 : 0);
const estimatedSourceRows = totalActiveChunks ? totalActiveChunks * Math.max(1, sourceMultiplier) : null;
const progressPct = estimatedSourceRows && nextOffset != null ? (nextOffset / estimatedSourceRows) * 100 : null;

const report = {
  generatedAt: new Date().toISOString(),
  runDir,
  state,
  counts,
  estimate: {
    totalActiveChunks,
    estimatedSourceRows,
    nextOffset,
    progressPct: progressPct == null ? null : Number(progressPct.toFixed(3)),
    sourceMultiplier,
    includeDocumentChunks,
    includeTrustedChunks,
    note: "Estimated source rows are based on activeSearchChunks multiplied by enabled source sets. Trusted-only mode uses one source set."
  },
  latest,
  watchdogTail,
  vectorTail
};

console.log(JSON.stringify(report, null, 2));
