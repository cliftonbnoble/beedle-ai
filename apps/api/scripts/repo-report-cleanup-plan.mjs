import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_EXTENSIONS = new Set([".json", ".md", ".csv", ".log", ".txt"]);

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes / 1024;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function parseExtensions(value) {
  if (!value) return DEFAULT_EXTENSIONS;
  return new Set(
    String(value)
      .split(",")
      .map((item) => item.trim().toLowerCase())
      .filter(Boolean)
      .map((item) => (item.startsWith(".") ? item : `.${item}`))
  );
}

export function buildReportCleanupPlan(entries, options = {}) {
  const nowMs = options.nowMs ?? Date.now();
  const retentionDays = Math.max(1, Number(options.retentionDays ?? 14));
  const cutoffMs = nowMs - retentionDays * 24 * 60 * 60 * 1000;
  const extensions = options.extensions ?? DEFAULT_EXTENSIONS;
  const candidates = entries
    .filter((entry) => entry.isFile)
    .filter((entry) => extensions.has(path.extname(entry.relativePath).toLowerCase()))
    .filter((entry) => entry.mtimeMs < cutoffMs)
    .sort((a, b) => a.mtimeMs - b.mtimeMs || a.relativePath.localeCompare(b.relativePath));

  const totalBytes = entries.filter((entry) => entry.isFile).reduce((sum, entry) => sum + entry.size, 0);
  const candidateBytes = candidates.reduce((sum, entry) => sum + entry.size, 0);

  return {
    generatedAt: new Date(nowMs).toISOString(),
    retentionDays,
    cutoff: new Date(cutoffMs).toISOString(),
    summary: {
      totalFileCount: entries.filter((entry) => entry.isFile).length,
      totalBytes,
      totalSize: formatBytes(totalBytes),
      candidateFileCount: candidates.length,
      candidateBytes,
      candidateSize: formatBytes(candidateBytes)
    },
    candidates: candidates.map((entry) => ({
      relativePath: entry.relativePath,
      size: entry.size,
      sizeLabel: formatBytes(entry.size),
      modifiedAt: new Date(entry.mtimeMs).toISOString()
    }))
  };
}

function formatMarkdown(report, applied) {
  const lines = [
    "# Repo Report Cleanup Plan",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Retention: \`${report.retentionDays}\` days`,
    `- Cutoff: \`${report.cutoff}\``,
    `- Mode: \`${applied ? "applied" : "dry-run"}\``,
    `- Total files: \`${report.summary.totalFileCount}\` / \`${report.summary.totalSize}\``,
    `- Cleanup candidates: \`${report.summary.candidateFileCount}\` / \`${report.summary.candidateSize}\``,
    "",
    "## Oldest Candidates",
    ""
  ];

  for (const row of report.candidates.slice(0, 100)) {
    lines.push(`- \`${row.relativePath}\` | ${row.sizeLabel} | modified ${row.modifiedAt}`);
  }
  if (report.candidates.length > 100) {
    lines.push(`- ... ${report.candidates.length - 100} more`);
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function listFiles(rootDir) {
  const entries = [];

  async function walk(dir) {
    let dirents;
    try {
      dirents = await fs.readdir(dir, { withFileTypes: true });
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw error;
    }

    for (const dirent of dirents) {
      const absolutePath = path.join(dir, dirent.name);
      const relativePath = path.relative(rootDir, absolutePath);
      if (dirent.isDirectory()) {
        await walk(absolutePath);
      } else if (dirent.isFile()) {
        const stat = await fs.stat(absolutePath);
        entries.push({
          absolutePath,
          relativePath,
          isFile: true,
          size: stat.size,
          mtimeMs: stat.mtimeMs
        });
      }
    }
  }

  await walk(rootDir);
  return entries;
}

async function main() {
  const reportsDir = path.resolve(process.cwd(), process.env.REPO_REPORT_CLEANUP_DIR || "reports");
  const retentionDays = Number(process.env.REPO_REPORT_CLEANUP_RETENTION_DAYS || "14");
  const extensions = parseExtensions(process.env.REPO_REPORT_CLEANUP_EXTENSIONS);
  const apply = process.env.REPO_REPORT_CLEANUP_APPLY === "1";
  const maxApply = Math.max(1, Number(process.env.REPO_REPORT_CLEANUP_MAX_APPLY || "2000"));
  const jsonName = process.env.REPO_REPORT_CLEANUP_JSON_NAME || "repo-report-cleanup-plan.json";
  const markdownName = process.env.REPO_REPORT_CLEANUP_MARKDOWN_NAME || "repo-report-cleanup-plan.md";

  await fs.mkdir(reportsDir, { recursive: true });
  const entries = await listFiles(reportsDir);
  const report = buildReportCleanupPlan(entries, { retentionDays, extensions });

  const deleteCandidates = apply ? report.candidates.slice(0, maxApply) : [];
  for (const candidate of deleteCandidates) {
    await fs.rm(path.join(reportsDir, candidate.relativePath), { force: true });
  }

  const output = {
    ...report,
    applied: apply,
    maxApply,
    deletedFileCount: deleteCandidates.length,
    deletedBytes: deleteCandidates.reduce((sum, row) => sum + row.size, 0),
    deletedSize: formatBytes(deleteCandidates.reduce((sum, row) => sum + row.size, 0))
  };

  await fs.writeFile(path.join(reportsDir, jsonName), JSON.stringify(output, null, 2));
  await fs.writeFile(path.join(reportsDir, markdownName), formatMarkdown(output, apply));

  console.log(JSON.stringify(output.summary, null, 2));
  console.log(`${apply ? "Applied" : "Dry-run"} report cleanup plan written to ${path.join(reportsDir, jsonName)}`);
  if (apply) console.log(`Deleted ${output.deletedFileCount} files (${output.deletedSize})`);
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
