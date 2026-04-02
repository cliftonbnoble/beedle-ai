import fs from "node:fs/promises";
import path from "node:path";

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.OWNER_MOVE_IN_RUNTIME_PROBE_JSON_NAME || "owner-move-in-runtime-probe-report.json";
const markdownName = process.env.OWNER_MOVE_IN_RUNTIME_PROBE_MARKDOWN_NAME || "owner-move-in-runtime-probe-report.md";
const csvName = process.env.OWNER_MOVE_IN_RUNTIME_PROBE_CSV_NAME || "owner-move-in-runtime-probe-report.csv";
const attempts = Math.max(1, Number(process.env.OWNER_MOVE_IN_RUNTIME_PROBE_ATTEMPTS || "5"));
const timeoutMs = Number(process.env.OWNER_MOVE_IN_RUNTIME_PROBE_TIMEOUT_MS || "60000");
const pauseMs = Number(process.env.OWNER_MOVE_IN_RUNTIME_PROBE_PAUSE_MS || "1500");

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchAttempt(attempt) {
  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`${apiBase}/search`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "application/json"
      },
      body: JSON.stringify({
        query: "owner move in",
        limit: 12,
        offset: 0,
        snippetMaxLength: 320,
        corpusMode: "trusted_plus_provisional",
        filters: {
          approvedOnly: false,
          judgeNames: ["René Juárez"],
          fromDate: "2022-01-01",
          toDate: "2025-12-31"
        }
      }),
      signal: controller.signal
    });
    const text = await response.text();
    const durationMs = Date.now() - startedAt;
    if (!response.ok) {
      return {
        attempt,
        status: "http_error",
        durationMs,
        resultCount: 0,
        top1Citation: null,
        error: text.slice(0, 500)
      };
    }
    const parsed = JSON.parse(text || "{}");
    const results = Array.isArray(parsed.results) ? parsed.results : [];
    return {
      attempt,
      status: "returned",
      durationMs,
      resultCount: results.length,
      top1Citation: results[0]?.citation || null,
      error: null
    };
  } catch (error) {
    return {
      attempt,
      status: /aborted|timeout/i.test(String(error instanceof Error ? error.message : error)) ? "aborted" : "error",
      durationMs: Date.now() - startedAt,
      resultCount: 0,
      top1Citation: null,
      error: error instanceof Error ? error.message : String(error)
    };
  } finally {
    clearTimeout(timeout);
  }
}

function toMarkdown(report) {
  const lines = [
    "# Owner Move-In Runtime Probe",
    "",
    `- attempts: ${report.attempts}`,
    `- returnedCount: ${report.returnedCount}`,
    `- abortedCount: ${report.abortedCount}`,
    `- errorCount: ${report.errorCount}`,
    `- avgDurationMs: ${report.avgDurationMs}`,
    "",
    "## Attempts",
    ""
  ];
  for (const row of report.rows) {
    lines.push(
      `- attempt ${row.attempt} | status=${row.status} | durationMs=${row.durationMs} | resultCount=${row.resultCount} | top1=${row.top1Citation || "<none>"}${row.error ? ` | error=${row.error}` : ""}`
    );
  }
  return `${lines.join("\n")}\n`;
}

function toCsv(report) {
  const rows = [
    ["attempt", "status", "durationMs", "resultCount", "top1Citation", "error"],
    ...report.rows.map((row) => [row.attempt, row.status, row.durationMs, row.resultCount, row.top1Citation || "", row.error || ""])
  ];
  return rows.map((row) => row.map(csvEscape).join(",")).join("\n");
}

async function main() {
  const rows = [];
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    rows.push(await fetchAttempt(attempt));
    if (attempt < attempts && pauseMs > 0) await sleep(pauseMs);
  }

  const avgDurationMs = rows.length ? Math.round(rows.reduce((sum, row) => sum + row.durationMs, 0) / rows.length) : 0;
  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    attempts,
    returnedCount: rows.filter((row) => row.status === "returned").length,
    abortedCount: rows.filter((row) => row.status === "aborted").length,
    errorCount: rows.filter((row) => row.status !== "returned" && row.status !== "aborted").length,
    avgDurationMs,
    rows
  };

  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  const csvPath = path.join(reportsDir, csvName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  await fs.writeFile(markdownPath, toMarkdown(report));
  await fs.writeFile(csvPath, `${toCsv(report)}\n`);

  console.log(
    JSON.stringify(
      {
        attempts: report.attempts,
        returnedCount: report.returnedCount,
        abortedCount: report.abortedCount,
        errorCount: report.errorCount,
        avgDurationMs: report.avgDurationMs
      },
      null,
      2
    )
  );
  console.log(`Owner move-in runtime probe JSON report written to ${jsonPath}`);
  console.log(`Owner move-in runtime probe Markdown report written to ${markdownPath}`);
  console.log(`Owner move-in runtime probe CSV report written to ${csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
