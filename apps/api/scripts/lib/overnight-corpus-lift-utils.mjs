import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";
import { defaultDbPath } from "./dhs-index-code-remediation.mjs";

const execFileAsync = promisify(execFile);

export const DEFAULT_TIMEZONE = "America/Los_Angeles";
export const DEFAULT_OVERNIGHT_START_HOUR = 22;
export const DEFAULT_OVERNIGHT_END_HOUR = 7;
export const defaultReportsBaseDir = path.resolve(process.cwd(), "reports/overnight-corpus-lift");

export { defaultDbPath };

export function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

export function normalizeText(value) {
  return normalizeWhitespace(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

export function normalizeToken(value) {
  return normalizeText(value).replace(/[^a-z0-9.()\-]/g, "");
}

export function normalizeCitation(value) {
  return normalizeToken(value)
    .replace(/^section/, "")
    .replace(/^sec/, "")
    .replace(/^rule/, "")
    .replace(/^part[0-9a-z.\-]+\-/, "");
}

export function normalizeBareRulesCitation(value) {
  return normalizeCitation(value).replace(/^[ivxlcdm]+\-/i, "");
}

export function normalizeOrdinanceCitation(value) {
  return normalizeCitation(value).replace(/^ordinance/, "");
}

export function extractRulesFamily(value) {
  const normalized = normalizeBareRulesCitation(value);
  if (!normalized) return "<none>";
  const match = normalized.match(/^\d+(?:\.\d+)?/);
  return match ? match[0] : normalized.slice(0, 12);
}

export function extractOrdinanceFamily(value) {
  const normalized = normalizeOrdinanceCitation(value);
  if (!normalized) return "<none>";
  const match = normalized.match(/^\d+(?:\.\d+)?/);
  return match ? match[0] : normalized.slice(0, 12);
}

export function unique(values) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

export function uniqueSorted(values) {
  return unique(values).sort((a, b) => String(a).localeCompare(String(b)));
}

export function countBy(values) {
  const counts = new Map();
  for (const value of values || []) {
    const key = normalizeWhitespace(value || "<unknown>") || "<unknown>";
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return Array.from(counts.entries())
    .map(([key, count]) => ({ key, count }))
    .sort((a, b) => b.count - a.count || a.key.localeCompare(b.key));
}

export function sqlQuote(value) {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

export function buildRealDecisionPredicate(alias = "d") {
  return `
    ${alias}.file_type = 'decision_docx'
    AND ${alias}.rejected_at IS NULL
    AND COALESCE(${alias}.title, '') NOT LIKE 'Harness %'
    AND COALESCE(${alias}.title, '') NOT LIKE 'Retrieval %'
    AND COALESCE(${alias}.title, '') NOT LIKE 'R5 %'
    AND COALESCE(${alias}.citation, '') NOT LIKE 'BEE-%'
    AND COALESCE(${alias}.citation, '') NOT LIKE 'KNOWN-REF-%'
    AND COALESCE(${alias}.citation, '') NOT LIKE 'PILOT-%'
    AND COALESCE(${alias}.citation, '') NOT LIKE 'HISTORICAL-%'
    AND lower(COALESCE(${alias}.source_r2_key, '')) NOT LIKE '%harness%'
    AND lower(COALESCE(${alias}.source_r2_key, '')) NOT LIKE '%fixture%'
  `;
}

export async function runSqlJson({ dbPath = defaultDbPath, busyTimeoutMs = 5000, sql }) {
  const { stdout } = await execFileAsync(
    "sqlite3",
    ["-json", "-cmd", `.timeout ${busyTimeoutMs}`, dbPath, sql],
    { cwd: process.cwd(), maxBuffer: 100 * 1024 * 1024 }
  );
  return JSON.parse(stdout || "[]");
}

export async function runSqlFirst({ dbPath = defaultDbPath, busyTimeoutMs = 5000, sql }) {
  const rows = await runSqlJson({ dbPath, busyTimeoutMs, sql });
  return rows[0] || null;
}

export async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

export async function writeJson(filePath, payload) {
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

export async function writeText(filePath, text) {
  await ensureDir(path.dirname(filePath));
  await fs.writeFile(filePath, String(text), "utf8");
}

export async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

export async function readJsonIfExists(filePath) {
  try {
    return await readJson(filePath);
  } catch {
    return null;
  }
}

export async function fetchJson(url, init) {
  const response = await fetch(url, init);
  const raw = await response.text();
  let body = null;
  try {
    body = raw ? JSON.parse(raw) : null;
  } catch {
    body = { raw };
  }
  if (!response.ok) {
    throw new Error(`Request failed (${response.status}) for ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

export async function checkHealth(apiBaseUrl) {
  try {
    const body = await fetchJson(`${apiBaseUrl}/health`);
    return { ok: true, body };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }
}

export async function runNodeScript(scriptPath, { cwd = process.cwd(), env = {} } = {}) {
  const mergedEnv = { ...process.env, ...env };
  try {
    const result = await execFileAsync("node", [scriptPath], {
      cwd,
      env: mergedEnv,
      maxBuffer: 100 * 1024 * 1024
    });
    return {
      ok: true,
      code: 0,
      stdout: result.stdout || "",
      stderr: result.stderr || ""
    };
  } catch (error) {
    return {
      ok: false,
      code: Number(error?.code ?? 1),
      stdout: String(error?.stdout || ""),
      stderr: String(error?.stderr || error?.message || error)
    };
  }
}

export function formatTimestamp(date = new Date()) {
  return date.toISOString().replace(/[:.]/g, "-");
}

export function getZonedDateParts(date = new Date(), timeZone = DEFAULT_TIMEZONE) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
  const parts = Object.fromEntries(formatter.formatToParts(date).map((part) => [part.type, part.value]));
  return {
    year: Number(parts.year),
    month: Number(parts.month),
    day: Number(parts.day),
    hour: Number(parts.hour),
    minute: Number(parts.minute),
    second: Number(parts.second),
    isoLocal: `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`
  };
}

export function isWithinOvernightWindow({
  date = new Date(),
  timeZone = DEFAULT_TIMEZONE,
  startHour = DEFAULT_OVERNIGHT_START_HOUR,
  endHour = DEFAULT_OVERNIGHT_END_HOUR
} = {}) {
  const { hour } = getZonedDateParts(date, timeZone);
  if (startHour === endHour) return true;
  if (startHour < endHour) return hour >= startHour && hour < endHour;
  return hour >= startHour || hour < endHour;
}

export async function queryCorpusSnapshot({ dbPath = defaultDbPath, busyTimeoutMs = 5000 } = {}) {
  const row = await runSqlFirst({
    dbPath,
    busyTimeoutMs,
    sql: `
      WITH active_docs AS (
        SELECT DISTINCT document_id FROM retrieval_search_chunks WHERE active = 1
      )
      SELECT
        (SELECT COUNT(*) FROM documents d WHERE d.file_type = 'decision_docx') AS totalDecisionDocs,
        (SELECT COUNT(*) FROM documents d WHERE ${buildRealDecisionPredicate("d")}) AS realDecisionDocs,
        (SELECT COUNT(*) FROM documents d WHERE d.file_type = 'decision_docx' AND d.searchable_at IS NOT NULL) AS searchableDecisionDocs,
        (SELECT COUNT(*) FROM documents d WHERE ${buildRealDecisionPredicate("d")} AND d.searchable_at IS NOT NULL) AS realSearchableDecisionDocs,
        (SELECT COUNT(*) FROM documents d WHERE ${buildRealDecisionPredicate("d")} AND d.qc_passed = 1 AND d.searchable_at IS NULL) AS qcPassedNotSearchableCount,
        (SELECT COUNT(*) FROM documents d LEFT JOIN active_docs a ON a.document_id = d.id WHERE ${buildRealDecisionPredicate("d")} AND d.searchable_at IS NOT NULL AND a.document_id IS NULL) AS searchableButNotActiveCount,
        (SELECT COUNT(*) FROM documents d JOIN active_docs a ON a.document_id = d.id WHERE ${buildRealDecisionPredicate("d")} AND d.searchable_at IS NOT NULL) AS activeRetrievalDecisionCount,
        (SELECT COUNT(*) FROM documents d WHERE ${buildRealDecisionPredicate("d")} AND d.qc_has_index_codes = 0 AND d.qc_has_rules_section = 1 AND d.qc_has_ordinance_section = 1 AND d.searchable_at IS NULL) AS missingIndexOnlyCount,
        (SELECT COUNT(*) FROM documents d WHERE ${buildRealDecisionPredicate("d")} AND d.qc_passed = 0 AND d.searchable_at IS NULL) AS qcFailedNotSearchableCount
    `
  });

  return {
    totalDecisionDocs: Number(row?.totalDecisionDocs || 0),
    realDecisionDocs: Number(row?.realDecisionDocs || 0),
    searchableDecisionDocs: Number(row?.searchableDecisionDocs || 0),
    realSearchableDecisionDocs: Number(row?.realSearchableDecisionDocs || 0),
    qcPassedNotSearchableCount: Number(row?.qcPassedNotSearchableCount || 0),
    searchableButNotActiveCount: Number(row?.searchableButNotActiveCount || 0),
    activeRetrievalDecisionCount: Number(row?.activeRetrievalDecisionCount || 0),
    missingIndexOnlyCount: Number(row?.missingIndexOnlyCount || 0),
    qcFailedNotSearchableCount: Number(row?.qcFailedNotSearchableCount || 0)
  };
}

export function computeProgressSummary(before, after, targetSearchable = 7000) {
  const searchableBefore = Number(before?.searchableDecisionDocs || 0);
  const searchableAfter = Number(after?.searchableDecisionDocs || 0);
  const delta = searchableAfter - searchableBefore;
  return {
    searchableDelta: delta,
    targetSearchable,
    distanceToTargetBefore: Math.max(0, targetSearchable - searchableBefore),
    distanceToTargetAfter: Math.max(0, targetSearchable - searchableAfter),
    movedTowardTarget: delta > 0
  };
}
