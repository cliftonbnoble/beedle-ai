import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_TARGET_PATTERN = /\b(?:node|bash)\s+(\.\/scripts\/[^\s"'`]+(?:\.mjs|\.sh|\.json))/g;
const SCRIPT_FILE_PATTERN = /\.(?:mjs|sh|json)$/;
const MUTATING_ALIAS_PATTERN = /^(?:write|run|backfill|launch|import|reprocess|rollout|remediate|normalize):/;
const SUPPORT_SCRIPT_PATTERN = /(?:^|-)utils\.mjs$/;
const SUPPORT_CONFIG_PATTERN = /(?:^|[.-])(?:sample|tasks?|allowlist)\.[^.]+$|(?:^|-)tasks(?:[.-][^.]+)*\.json$/;

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

function isExpectedUnaliasedSupportFile(file) {
  return SUPPORT_SCRIPT_PATTERN.test(file) || SUPPORT_CONFIG_PATTERN.test(file);
}

function isExpectedApplyVariant(entries) {
  if (entries.length !== 2) return false;
  const aliases = entries.map((entry) => entry.alias);
  const hasReportAlias = aliases.some((alias) => alias.startsWith("report:"));
  const hasWriteAlias = aliases.some((alias) => alias.startsWith("write:"));
  const hasApplyCommand = entries.some((entry) => /\b[A-Z0-9_]*APPLY=1\b/.test(entry.command));
  return hasReportAlias && hasWriteAlias && hasApplyCommand;
}

function isExpectedProfileVariant(entries) {
  if (entries.length < 2) return false;
  if (isExpectedApplyVariant(entries)) return false;
  const categories = new Set(entries.map((entry) => aliasCategory(entry.alias)));
  const commands = new Set(entries.map((entry) => entry.command));
  if (categories.size !== 1 || commands.size <= 1) return false;
  const [category] = categories;
  if (!["report", "overnight"].includes(category)) return false;
  return entries.every((entry) => !MUTATING_ALIAS_PATTERN.test(entry.alias));
}

export function buildScriptInventory({ packageJson, scriptFiles, reportStats }) {
  const packageScripts = packageJson.scripts ?? {};
  const aliasEntries = Object.entries(packageScripts).sort(([a], [b]) => a.localeCompare(b));
  const targetToEntries = new Map();
  const missingTargets = [];

  for (const [alias, command] of aliasEntries) {
    for (const target of extractScriptTargets(String(command))) {
      const scriptName = target.replace(/^\.\/scripts\//, "");
      const entries = targetToEntries.get(scriptName) ?? [];
      entries.push({ alias, command: String(command) });
      targetToEntries.set(scriptName, entries);
      if (!scriptFiles.includes(scriptName)) {
        missingTargets.push({ alias, target });
      }
    }
  }

  const aliasedScriptFiles = new Set(targetToEntries.keys());
  const unaliasedScriptFiles = scriptFiles.filter((file) => SCRIPT_FILE_PATTERN.test(file) && !aliasedScriptFiles.has(file));
  const expectedUnaliasedSupportFiles = unaliasedScriptFiles.filter(isExpectedUnaliasedSupportFile);
  const actionableUnaliasedScriptFiles = unaliasedScriptFiles.filter((file) => !isExpectedUnaliasedSupportFile(file));
  const targetGroups = Array.from(targetToEntries.entries()).filter(([, entries]) => entries.length > 1);
  const duplicateTargets = targetGroups
    .flatMap(([script, entries]) => {
      const commandToAliases = new Map();
      for (const entry of entries) {
        const aliases = commandToAliases.get(entry.command) ?? [];
        aliases.push(entry.alias);
        commandToAliases.set(entry.command, aliases);
      }
      return Array.from(commandToAliases.entries())
        .filter(([, aliases]) => aliases.length > 1)
        .map(([command, aliases]) => ({ script, command, aliases: aliases.sort() }));
    })
    .sort((a, b) => a.script.localeCompare(b.script));
  const allCommandVariantTargets = targetGroups
    .map(([script, entries]) => ({
      script,
      aliases: entries.map((entry) => entry.alias).sort(),
      commandCount: new Set(entries.map((entry) => entry.command)).size,
      expectedApplyVariant: isExpectedApplyVariant(entries),
      expectedProfileVariant: isExpectedProfileVariant(entries)
    }))
    .filter((row) => row.commandCount > 1)
    .sort((a, b) => a.script.localeCompare(b.script));
  const expectedCommandVariantTargets = allCommandVariantTargets
    .filter((row) => row.expectedApplyVariant)
    .map(({ expectedApplyVariant, expectedProfileVariant, ...row }) => row);
  const expectedProfileVariantTargets = allCommandVariantTargets
    .filter((row) => row.expectedProfileVariant)
    .map(({ expectedApplyVariant, expectedProfileVariant, ...row }) => row);
  const commandVariantTargets = allCommandVariantTargets
    .filter((row) => !row.expectedApplyVariant && !row.expectedProfileVariant)
    .map(({ expectedApplyVariant, expectedProfileVariant, ...row }) => row);

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
      expectedUnaliasedSupportFileCount: expectedUnaliasedSupportFiles.length,
      actionableUnaliasedScriptFileCount: actionableUnaliasedScriptFiles.length,
      duplicateTargetCount: duplicateTargets.length,
      commandVariantTargetCount: commandVariantTargets.length,
      expectedCommandVariantTargetCount: expectedCommandVariantTargets.length,
      expectedProfileVariantTargetCount: expectedProfileVariantTargets.length,
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
    commandVariantTargets,
    expectedCommandVariantTargets,
    expectedProfileVariantTargets,
    missingTargets,
    actionableUnaliasedScriptFiles,
    expectedUnaliasedSupportFiles,
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
    `- Actionable unaliased script files: \`${report.summary.actionableUnaliasedScriptFileCount}\``,
    `- Expected unaliased support/config files: \`${report.summary.expectedUnaliasedSupportFileCount}\``,
    `- Exact duplicate target mappings: \`${report.summary.duplicateTargetCount}\``,
    `- Command-variant target mappings: \`${report.summary.commandVariantTargetCount}\``,
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

  lines.push("", "## Command-Variant Targets", "");
  if (report.commandVariantTargets.length === 0) {
    lines.push("- none");
  } else {
    for (const row of report.commandVariantTargets) {
      lines.push(`- \`${row.script}\`: ${row.aliases.map((alias) => `\`${alias}\``).join(", ")}`);
    }
  }

  lines.push("", "## Expected Apply Variants", "");
  if (report.expectedCommandVariantTargets.length === 0) {
    lines.push("- none");
  } else {
    for (const row of report.expectedCommandVariantTargets) {
      lines.push(`- \`${row.script}\`: ${row.aliases.map((alias) => `\`${alias}\``).join(", ")}`);
    }
  }

  lines.push("", "## Expected Profile Variants", "");
  if (report.expectedProfileVariantTargets.length === 0) {
    lines.push("- none");
  } else {
    for (const row of report.expectedProfileVariantTargets) {
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

  lines.push("", "## Actionable Unaliased Top-Level Script Files", "");
  if (report.actionableUnaliasedScriptFiles.length === 0) {
    lines.push("- none");
  }
  for (const file of report.actionableUnaliasedScriptFiles.slice(0, 100)) {
    lines.push(`- \`${file}\``);
  }
  if (report.actionableUnaliasedScriptFiles.length > 100) {
    lines.push(`- ... ${report.actionableUnaliasedScriptFiles.length - 100} more`);
  }

  lines.push("", "## Expected Unaliased Support/Config Files", "");
  if (report.expectedUnaliasedSupportFiles.length === 0) {
    lines.push("- none");
  }
  for (const file of report.expectedUnaliasedSupportFiles.slice(0, 100)) {
    lines.push(`- \`${file}\``);
  }
  if (report.expectedUnaliasedSupportFiles.length > 100) {
    lines.push(`- ... ${report.expectedUnaliasedSupportFiles.length - 100} more`);
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
