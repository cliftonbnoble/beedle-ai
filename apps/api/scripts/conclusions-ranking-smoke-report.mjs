import fs from "node:fs/promises";
import path from "node:path";

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.CONCLUSIONS_RANKING_SMOKE_JSON_NAME || "conclusions-ranking-smoke-report.json";
const markdownName = process.env.CONCLUSIONS_RANKING_SMOKE_MARKDOWN_NAME || "conclusions-ranking-smoke-report.md";
const csvName = process.env.CONCLUSIONS_RANKING_SMOKE_CSV_NAME || "conclusions-ranking-smoke-report.csv";
const requestTimeoutMs = Number(process.env.CONCLUSIONS_RANKING_SMOKE_TIMEOUT_MS || "45000");
const retries = Math.max(1, Number(process.env.CONCLUSIONS_RANKING_SMOKE_RETRIES || "3"));
const pauseBetweenQueriesMs = Number(process.env.CONCLUSIONS_RANKING_SMOKE_PAUSE_MS || "750");

const tasks = [
  {
    id: "market_condition_rent_reduction",
    query: "the rent decrease constituted a new agreement setting a new base rent due to market conditions",
    expectedCitations: ["L230292-DECISION"]
  },
  {
    id: "burden_of_proof_allowable_increase",
    query: "the landlord has the burden of proving that an increase in excess of the allowable annual rent increase is justified",
    expectedCitations: ["L182249-DECISION", "L182247-DECISION", "L141961-DECISION", "L191853-DECISION"]
  },
  {
    id: "code_violation_certification_bar",
    query: "Costs for capital improvement work shall not be certified if the work was required to correct a code violation",
    expectedCitations: ["L182249-DECISION", "L182247-DECISION", "L141961-DECISION", "L191853-DECISION", "L110684-CORRECTED-DECISION"]
  },
  {
    id: "capital_improvement_good_faith_exception",
    query: "the landlord made timely good faith efforts within that 90 day period to commence and complete the work",
    expectedCitations: ["L182249-DECISION", "L182247-DECISION", "L141961-DECISION", "L191853-DECISION"]
  },
  {
    id: "capital_improvement_documentation_required",
    query: "an application for certification of capital improvement costs must be accompanied by supporting documentation including copies of invoices signed contracts and canceled checks",
    expectedCitations: ["L182249-DECISION", "L182247-DECISION", "L141961-DECISION", "L191853-DECISION"]
  },
  {
    id: "tenant_new_anniversary_date_market_conditions",
    query: "the tenant new anniversary date became october 1 2003 after the new agreement setting a new base rent",
    expectedCitations: ["L230292-DECISION"]
  }
];

function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

function isConclusionLikeSection(sectionLabel) {
  const raw = String(sectionLabel || "");
  const normalized = normalize(raw).replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return (
    /conclusions? of law/i.test(raw) ||
    normalized === "conclusions_of_law" ||
    normalized === "authority_discussion" ||
    normalized === "analysis_reasoning"
  );
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, payload, label) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          accept: "application/json"
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      });
      const text = await response.text();
      if (!response.ok) {
        throw new Error(`Request failed (${response.status}) ${url}: ${text.slice(0, 400)}`);
      }
      return JSON.parse(text || "{}");
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        console.warn(`[conclusions-ranking-smoke] ${label} attempt ${attempt}/${retries} failed: ${error instanceof Error ? error.message : String(error)}`);
        await sleep(400 * attempt);
      }
    } finally {
      clearTimeout(timeout);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError || `Request failed for ${label}`));
}

function toMarkdown(report) {
  const lines = [];
  lines.push("# Conclusions Ranking Smoke Report");
  lines.push("");
  lines.push(`- queryCount: ${report.queryCount}`);
  lines.push(`- returnedQueryCount: ${report.returnedQueryCount}`);
  lines.push(`- failedQueryCount: ${report.failedQueryCount}`);
  lines.push(`- top1ConclusionLikeCount: ${report.top1ConclusionLikeCount}`);
  lines.push(`- top5ConclusionLikeCount: ${report.top5ConclusionLikeCount}`);
  lines.push(`- expectedCitationTop5Count: ${report.expectedCitationTop5Count}`);
  lines.push("");
  lines.push("## Tasks");
  lines.push("");
  for (const row of report.rows) {
    lines.push(`- ${row.id} | returned=${row.resultCount > 0} | top1=${row.top1Citation || "<none>"} | top1Section=${row.top1SectionLabel || "<none>"} | top5ConclusionLike=${row.top5ConclusionLike} | expectedCitationFound=${row.expectedCitationFound}${row.error ? ` | error=${row.error}` : ""}`);
  }
  return `${lines.join("\n")}\n`;
}

function toCsv(report) {
  const rows = [
    ["id", "query", "top1Citation", "top1SectionLabel", "top1Score", "top5ConclusionLike", "expectedCitations", "expectedCitationFound", "error"],
    ...report.rows.map((row) => [
      row.id,
      row.query,
      row.top1Citation || "",
      row.top1SectionLabel || "",
      String(row.top1Score ?? ""),
      row.top5ConclusionLike ? "1" : "0",
      Array.isArray(row.expectedCitations) ? row.expectedCitations.join(" | ") : "",
      row.expectedCitationFound ? "1" : "0",
      row.error || ""
    ])
  ];
  return rows
    .map((row) =>
      row
        .map((value) => {
          const raw = String(value ?? "");
          return /[",\n]/.test(raw) ? `"${raw.replace(/"/g, '""')}"` : raw;
        })
        .join(",")
    )
    .join("\n");
}

async function writeReports(report) {
  await fs.mkdir(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, jsonName);
  const markdownPath = path.join(reportsDir, markdownName);
  const csvPath = path.join(reportsDir, csvName);
  await fs.writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  await fs.writeFile(markdownPath, toMarkdown(report));
  await fs.writeFile(csvPath, `${toCsv(report)}\n`);
  return { jsonPath, markdownPath, csvPath };
}

async function main() {
  const rows = [];

  for (const task of tasks) {
    try {
      const response = await fetchJson(
        `${apiBase}/search`,
        {
          query: task.query,
          limit: 8,
          snippetMaxLength: 320,
          corpusMode: "trusted_plus_provisional",
          filters: {
            approvedOnly: false
          }
        },
        task.id
      );

      const results = Array.isArray(response?.results) ? response.results : [];
      const top1 = results[0] || null;
      const top5 = results.slice(0, 5);
      const top5ConclusionLike = top5.some((row) => isConclusionLikeSection(row?.sectionLabel));
      const expectedCitationFound = Array.isArray(task.expectedCitations) && task.expectedCitations.length > 0
        ? top5.some((row) => task.expectedCitations.some((citation) => normalize(row?.citation) === normalize(citation)))
        : false;

      rows.push({
        id: task.id,
        query: task.query,
        expectedCitations: task.expectedCitations || [],
        resultCount: results.length,
        top1Citation: top1?.citation || null,
        top1SectionLabel: top1?.sectionLabel || null,
        top1Score: typeof top1?.score === "number" ? Number(top1.score.toFixed(6)) : null,
        top1ConclusionLike: Boolean(top1 && isConclusionLikeSection(top1.sectionLabel)),
        top5ConclusionLike,
        expectedCitationFound,
        error: null
      });
    } catch (error) {
      rows.push({
        id: task.id,
        query: task.query,
        expectedCitations: task.expectedCitations || [],
        resultCount: 0,
        top1Citation: null,
        top1SectionLabel: null,
        top1Score: null,
        top1ConclusionLike: false,
        top5ConclusionLike: false,
        expectedCitationFound: false,
        error: error instanceof Error ? error.message : String(error)
      });
    }

    if (pauseBetweenQueriesMs > 0) {
      await sleep(pauseBetweenQueriesMs);
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    queryCount: rows.length,
    returnedQueryCount: rows.filter((row) => row.resultCount > 0).length,
    failedQueryCount: rows.filter((row) => row.error).length,
    top1ConclusionLikeCount: rows.filter((row) => row.top1ConclusionLike).length,
    top5ConclusionLikeCount: rows.filter((row) => row.top5ConclusionLike).length,
    expectedCitationTop5Count: rows.filter((row) => row.expectedCitationFound).length,
    rows
  };

  const paths = await writeReports(report);
  console.log(JSON.stringify({
    queryCount: report.queryCount,
    returnedQueryCount: report.returnedQueryCount,
    failedQueryCount: report.failedQueryCount,
    top1ConclusionLikeCount: report.top1ConclusionLikeCount,
    top5ConclusionLikeCount: report.top5ConclusionLikeCount,
    expectedCitationTop5Count: report.expectedCitationTop5Count
  }, null, 2));
  console.log(`Conclusions ranking smoke JSON report written to ${paths.jsonPath}`);
  console.log(`Conclusions ranking smoke Markdown report written to ${paths.markdownPath}`);
  console.log(`Conclusions ranking smoke CSV report written to ${paths.csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
