import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_TARGET_PATTERN = /\b(?:node|bash)\s+(\.\/scripts\/[^\s"'`]+(?:\.mjs|\.sh|\.json))/g;
const SCRIPT_FILE_PATTERN = /\.(?:mjs|sh|json)$/;
const MUTATING_ALIAS_PATTERN = /^(?:write|run|backfill|launch|import|reprocess|rollout|remediate|normalize):/;

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

function extractScriptTargets(command) {
  return Array.from(command.matchAll(SCRIPT_TARGET_PATTERN), (match) => match[1]);
}

function aliasCategory(alias) {
  const [prefix] = alias.split(":");
  return alias.includes(":") ? prefix : "uncategorized";
}

export function buildScriptInventory({ packageJson, scriptFiles, reportStats }) {
  const packageScripts = packageJson.scripts ?? {};
  const aliasEntries = Object.entries(packageScripts).sort(([a], [b]) => a.localeCompare(b));
  const targetToAliases = new Map();
  const missingTargets = [];

  for (const [alias, command] of aliasEntries) {
    for (const target of extractScriptTargets(String(command))) {
      const scriptName = target.replace(/^\.\/scripts\//, "");
      const aliases = targetToAliases.get(scriptName) ?? [];
      aliases.push(alias);
      targetToAliases.set(scriptName, aliases);
      if (!scriptFiles.includes(scriptName)) {
        missingTargets.push({ alias, target });
      }
    }
  }

  const aliasedScriptFiles = new Set(targetToAliases.keys());
  const unaliasedScriptFiles = scriptFiles.filter((file) => SCRIPT_FILE_PATTERN.test(file) && !aliasedScriptFiles.has(file));
  const duplicateTargets = Array.from(targetToAliases.entries())
    .filter(([, aliases]) => aliases.length > 1)
    .map(([script, aliases]) => ({ script, aliases: aliases.sort() }))
    .sort((a, b) => a.script.localeCompare(b.script));

  const aliasesByCategory = {};
  for (const [alias] of aliasEntries) {
    const category = aliasCategory(alias);
    aliasesByCategory[category] = (aliasesByCategory[category] ?? 0) + 1;
  }

  return {
    generatedAt: new Date().toISOString(),
    summary: {
      packageAliasCount: aliasEntries.length,
      topLevelScriptFileCount: scriptFiles.length,
      aliasedScriptFileCount: aliasedScriptFiles.size,
      unaliasedScriptFileCount: unaliasedScriptFiles.length,
      duplicateTargetCount: duplicateTargets.length,
      missingTargetCount: missingTargets.length,
      reportFileCount: reportStats.fileCount,
      reportTotalBytes: reportStats.totalBytes,
      reportTotalSize: formatBytes(reportStats.totalBytes)
    },
    aliasesByCategory,
    mutatingAliases: aliasEntries
      .filter(([alias]) => MUTATING_ALIAS_PATTERN.test(alias))
      .map(([alias, command]) => ({ alias, command })),
    duplicateTargets,
    missingTargets,
    unaliasedScriptFiles
  };
}

function toMarkdown(report) {
  const lines = [
    "# Repo Script Inventory",
    "",
    `- Generated: \`${report.generatedAt}\``,
    `- Package aliases: \`${report.summary.packageAliasCount}\``,
    `- Top-level script files: \`${report.summary.topLevelScriptFileCount}\``,
    `- Aliased script files: \`${report.summary.aliasedScriptFileCount}\``,
    `- Unaliased script files: \`${report.summary.unaliasedScriptFileCount}\``,
    `- Duplicate target mappings: \`${report.summary.duplicateTargetCount}\``,
    `- Missing script targets: \`${report.summary.missingTargetCount}\``,
    `- Local reports: \`${report.summary.reportFileCount}\` files / \`${report.summary.reportTotalSize}\``,
    "",
    "## Alias Categories",
    ""
  ];

  for (const [category, count] of Object.entries(report.aliasesByCategory).sort(([a], [b]) => a.localeCompare(b))) {
    lines.push(`- \`${category}\`: ${count}`);
  }

  lines.push("", "## Duplicate Targets", "");
  if (report.duplicateTargets.length === 0) {
    lines.push("- none");
  } else {
    for (const row of report.duplicateTargets) {
      lines.push(`- \`${row.script}\`: ${row.aliases.map((alias) => `\`${alias}\``).join(", ")}`);
    }
  }

  lines.push("", "## Missing Targets", "");
  if (report.missingTargets.length === 0) {
    lines.push("- none");
  } else {
    for (const row of report.missingTargets) {
      lines.push(`- \`${row.alias}\` -> \`${row.target}\``);
    }
  }

  lines.push("", "## Unaliased Top-Level Script Files", "");
  for (const file of report.unaliasedScriptFiles.slice(0, 100)) {
    lines.push(`- \`${file}\``);
  }
  if (report.unaliasedScriptFiles.length > 100) {
    lines.push(`- ... ${report.unaliasedScriptFiles.length - 100} more`);
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

async function listTopLevelScriptFiles(scriptsDir) {
  const entries = await fs.readdir(scriptsDir, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile() && SCRIPT_FILE_PATTERN.test(entry.name))
    .map((entry) => entry.name)
    .sort();
}

async function getReportStats(reportsDir) {
  let fileCount = 0;
  let totalBytes = 0;

  async function walk(dir) {
    let entries;
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw error;
    }

    for (const entry of entries) {
      const entryPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(entryPath);
      } else if (entry.isFile()) {
        const stat = await fs.stat(entryPath);
        fileCount += 1;
        totalBytes += stat.size;
      }
    }
  }

  await walk(reportsDir);
  return { fileCount, totalBytes };
}

async function main() {
  const cwd = process.cwd();
  const packageJson = JSON.parse(await fs.readFile(path.join(cwd, "package.json"), "utf8"));
  const scriptsDir = path.join(cwd, "scripts");
  const reportsDir = path.join(cwd, "reports");
  const jsonName = process.env.REPO_SCRIPT_INVENTORY_JSON_NAME || "repo-script-inventory.json";
  const markdownName = process.env.REPO_SCRIPT_INVENTORY_MARKDOWN_NAME || "repo-script-inventory.md";

  const scriptFiles = await listTopLevelScriptFiles(scriptsDir);
  const reportStats = await getReportStats(reportsDir);
  const report = buildScriptInventory({ packageJson, scriptFiles, reportStats });

  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  await fs.writeFile(jsonPath, JSON.stringify(report, null, 2));
  await fs.writeFile(markdownPath, toMarkdown(report));

  console.log(JSON.stringify(report.summary, null, 2));
  console.log(`Repo script inventory JSON report written to ${jsonPath}`);
  console.log(`Repo script inventory Markdown report written to ${markdownPath}`);
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
