import path from "node:path";
import {
  buildMissingIndexAuditReport,
  formatMissingIndexAuditMarkdown
} from "./lib/missing-index-audit-utils.mjs";
import { defaultDbPath, ensureDir, writeJson, writeText } from "./lib/overnight-corpus-lift-utils.mjs";

const dbPath = process.env.D1_DB_PATH || defaultDbPath;
const busyTimeoutMs = Number.parseInt(process.env.MISSING_INDEX_AUDIT_BUSY_TIMEOUT_MS || "5000", 10);
const limit = Math.max(0, Number.parseInt(process.env.MISSING_INDEX_AUDIT_LIMIT || "0", 10));
const outputDir = path.resolve(process.cwd(), process.env.MISSING_INDEX_AUDIT_OUTPUT_DIR || "reports");
const jsonName = process.env.MISSING_INDEX_AUDIT_REPORT_NAME || "missing-index-code-full-audit-report.json";
const markdownName = process.env.MISSING_INDEX_AUDIT_MARKDOWN_NAME || "missing-index-code-full-audit-report.md";

export async function main() {
  const report = await buildMissingIndexAuditReport({ dbPath, busyTimeoutMs, limit });
  await ensureDir(outputDir);

  const jsonPath = path.resolve(outputDir, jsonName);
  const markdownPath = path.resolve(outputDir, markdownName);

  await Promise.all([
    writeJson(jsonPath, report),
    writeText(markdownPath, formatMissingIndexAuditMarkdown(report))
  ]);

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Missing index code full audit JSON report written to ${jsonPath}`);
  console.log(`Missing index code full audit Markdown report written to ${markdownPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exitCode = 1;
});
