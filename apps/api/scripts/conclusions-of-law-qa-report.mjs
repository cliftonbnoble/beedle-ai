import fs from "node:fs/promises";
import path from "node:path";

const apiBase = (process.env.API_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
const reportsDir = path.resolve(process.cwd(), "reports");
const jsonName = process.env.CONCLUSIONS_OF_LAW_QA_JSON_NAME || "conclusions-of-law-qa-report.json";
const markdownName = process.env.CONCLUSIONS_OF_LAW_QA_MARKDOWN_NAME || "conclusions-of-law-qa-report.md";
const csvName = process.env.CONCLUSIONS_OF_LAW_QA_CSV_NAME || "conclusions-of-law-qa-report.csv";
const limit = Number.parseInt(process.env.CONCLUSIONS_OF_LAW_QA_LIMIT || "5", 10);
const corpusMode = process.env.CONCLUSIONS_OF_LAW_QA_CORPUS_MODE || "trusted_only";
const requestTimeoutMs = Number(process.env.CONCLUSIONS_OF_LAW_QA_TIMEOUT_MS || "30000");
const retries = Math.max(1, Number(process.env.CONCLUSIONS_OF_LAW_QA_RETRIES || "2"));
const pauseBetweenQueriesMs = Number(process.env.CONCLUSIONS_OF_LAW_QA_PAUSE_MS || "300");

const tasks = [
  {
    id: "omi_follow_through",
    query: "owner move in notice but owner never moved in",
    expectation: "Should surface OMI follow-through conclusions of law or authority discussion."
  },
  {
    id: "market_condition_base_rent",
    query: "the rent decrease constituted a new agreement setting a new base rent due to market conditions",
    expectation: "Should surface conclusions of law on market-condition base-rent reasoning."
  },
  {
    id: "allowable_increase_burden",
    query: "the landlord has the burden of proving that an increase in excess of the allowable annual rent increase is justified",
    expectation: "Should surface conclusions of law on allowable annual increase burden of proof."
  },
  {
    id: "capital_improvement_code_violation",
    query: "Costs for capital improvement work shall not be certified if the work was required to correct a code violation",
    expectation: "Should surface conclusions of law on capital-improvement certification limits."
  },
  {
    id: "principal_place_of_residence",
    query: "principal place of residence tenant in occupancy",
    expectation: "Should surface OMI occupancy-standard conclusions of law."
  },
  {
    id: "buyout_pressure_natural_query",
    query: "tenant accepted a buyout after pressure from landlord",
    expectation: "Should surface buyout-pressure authority discussion or conclusions-style reasoning."
  },
  {
    id: "homeowners_exemption",
    query: "homeowner's exemption principal place of residence",
    expectation: "Should surface conclusions of law on homeowner's exemption and principal-residence reasoning."
  },
  {
    id: "lock_box",
    query: "lock box",
    expectation: "Should surface conclusions or authority discussion on lock-box access, keys, or related housing-service reasoning."
  },
  {
    id: "co_living",
    query: "co-living",
    expectation: "Should surface conclusions or authority discussion on separate tenancies, individual rooms, and shared/common-area reasoning."
  },
  {
    id: "camera_privacy",
    query: "camera privacy",
    expectation: "Should surface conclusions or authority discussion on camera or surveillance related privacy reasoning."
  },
  {
    id: "garage_space",
    query: "garage space",
    expectation: "Should surface conclusions or authority discussion on garage or parking-space housing-service reasoning."
  },
  {
    id: "common_areas",
    query: "common areas",
    expectation: "Should surface conclusions or authority discussion on common-area cleanliness, janitorial service, or related housing-service reasoning."
  },
  {
    id: "stairs",
    query: "stairs",
    expectation: "Should surface conclusions or authority discussion on stairs, handrails, or stair-safety housing-service reasoning."
  },
  {
    id: "porch",
    query: "porch",
    expectation: "Should surface conclusions or authority discussion on porch, landing, storage-room, or porch-door housing-service reasoning."
  },
  {
    id: "windows",
    query: "windows",
    expectation: "Should surface conclusions or authority discussion on inoperable, broken, leaking, or otherwise deficient window housing-service reasoning."
  },
  {
    id: "dog",
    query: "dog",
    expectation: "Should surface conclusions or authority discussion on dog-related pet policy, dog-free building, dog park, or related housing-service reasoning rather than incidental mentions."
  },
  {
    id: "college",
    query: "college",
    expectation: "Should surface conclusions or authority discussion on temporary absence for college, student housing, school breaks, or return-to-residence reasoning rather than bond passthrough material."
  },
  {
    id: "self_employed",
    query: "self employed",
    expectation: "Should surface conclusions or authority discussion on self-employment, 1099s, tax returns, or address-based residency proof rather than generic tax references."
  },
  {
    id: "adjudicated",
    query: "adjudicated",
    expectation: "Should surface conclusions or authority discussion on claims already decided, precluded, or properly adjudicated elsewhere rather than generic court references."
  },
  {
    id: "social_media",
    query: "social media",
    expectation: "Should surface conclusions or authority discussion on social-media evidence used for residency, occupancy, or roommate-search reasoning rather than unrelated habitability material."
  },
  {
    id: "caregiver",
    query: "caregiver",
    expectation: "Should surface conclusions or authority discussion on caregiver-related residency, return-to-unit, or principal-residence reasoning rather than generic caregiving references."
  },
  {
    id: "poop",
    query: "poop",
    expectation: "Should surface conclusions or authority discussion on feces, animal waste, sewage contamination, or related sanitation/housing-service reasoning rather than returning no results."
  },
  {
    id: "mold",
    query: "mold",
    expectation: "Should surface conclusions or authority discussion on mold, mildew, leaks, water intrusion, or related habitability/housing-service reasoning."
  },
  {
    id: "mildew",
    query: "mildew",
    expectation: "Should surface conclusions or authority discussion on mildew, mold, leaks, water intrusion, or related habitability/housing-service reasoning."
  },
  {
    id: "bed_bugs",
    query: "bed bugs",
    expectation: "Should surface conclusions or authority discussion on bed bugs, infestation, or related habitability/housing-service reasoning."
  },
  {
    id: "reasonable_accommodation",
    query: "reasonable accommodation",
    expectation: "Should surface conclusions or authority discussion on disability accommodation, service animals, or related housing/rent-board reasoning rather than unrelated employment-law accommodation material."
  },
  {
    id: "moot",
    query: "moot",
    expectation: "Should surface conclusions or authority discussion on claims rendered moot, null and void, rescinded, or administratively dismissed."
  },
  {
    id: "remote_work",
    query: "remote work",
    expectation: "Should surface conclusions or authority discussion on work-from-home interference from noise, utility loss, or habitability problems rather than generic work references."
  },
  {
    id: "divorce",
    query: "divorce",
    expectation: "Should surface conclusions or authority discussion on divorce, separation, or spouse-moved-out reasoning rather than generic spouse references."
  },
  {
    id: "package_security",
    query: "package security",
    expectation: "Should surface conclusions or authority discussion on package theft, delivery security, or related housing-service reasoning."
  },
  {
    id: "coin_operated",
    query: "coin-operated",
    expectation: "Should surface conclusions or authority discussion on coin-operated laundry housing-service reasoning."
  },
  {
    id: "intercom",
    query: "intercom",
    expectation: "Should surface conclusions or authority discussion on intercom, door-buzzer, or entry-system housing-service reasoning."
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

function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, payload, label) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
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
        console.warn(`[conclusions-of-law-qa] ${label} attempt ${attempt}/${retries} failed: ${error instanceof Error ? error.message : String(error)}`);
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
  lines.push("# Conclusions of Law QA Report");
  lines.push("");
  lines.push(`- queryCount: ${report.queryCount}`);
  lines.push(`- returnedQueryCount: ${report.returnedQueryCount}`);
  lines.push(`- failedQueryCount: ${report.failedQueryCount}`);
  lines.push(`- top1ConclusionLikeCount: ${report.top1ConclusionLikeCount}`);
  lines.push(`- top3ConclusionLikeCount: ${report.top3ConclusionLikeCount}`);
  lines.push(`- allQueriesTop3ConclusionLike: ${report.allQueriesTop3ConclusionLike}`);
  lines.push("");
  lines.push("## Tasks");
  lines.push("");
  for (const row of report.rows) {
    lines.push(`- ${row.id} | returned=${row.resultCount > 0} | top1Section=${row.top1SectionLabel || "<none>"} | top1Citation=${row.top1Citation || "<none>"} | top3ConclusionLike=${row.top3ConclusionLike}${row.error ? ` | error=${row.error}` : ""}`);
  }
  return `${lines.join("\n")}\n`;
}

function toCsv(report) {
  const rows = [
    ["id", "query", "resultCount", "top1Citation", "top1SectionLabel", "top1ConclusionLike", "top3ConclusionLike", "top3Sections", "error"],
    ...report.rows.map((row) => [
      row.id,
      row.query,
      String(row.resultCount),
      row.top1Citation || "",
      row.top1SectionLabel || "",
      row.top1ConclusionLike ? "1" : "0",
      row.top3ConclusionLike ? "1" : "0",
      row.top3Sections.join(" | "),
      row.error || ""
    ])
  ];
  return rows.map((row) => row.map(csvEscape).join(",")).join("\n");
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

function buildReport(rows, partial = true) {
  return {
    generatedAt: new Date().toISOString(),
    apiBase,
    corpusMode,
    queryCount: partial ? tasks.length : rows.length,
    completedQueryCount: rows.length,
    partial,
    returnedQueryCount: rows.filter((row) => row.resultCount > 0).length,
    failedQueryCount: rows.filter((row) => row.error).length,
    top1ConclusionLikeCount: rows.filter((row) => row.top1ConclusionLike).length,
    top3ConclusionLikeCount: rows.filter((row) => row.top3ConclusionLike).length,
    allQueriesTop3ConclusionLike: rows.length === tasks.length && rows.every((row) => row.top3ConclusionLike || row.error),
    rows
  };
}

async function main() {
  const rows = [];

  for (const task of tasks) {
    try {
      const response = await fetchJson(
        `${apiBase}/search`,
        {
          query: task.query,
          limit,
          snippetMaxLength: 320,
          corpusMode,
          filters: {
            approvedOnly: true
          }
        },
        task.id
      );

      const results = Array.isArray(response?.results) ? response.results : [];
      const top1 = results[0] || null;
      const top3 = results.slice(0, 3);
      const top3Sections = top3.map((row) => row?.sectionLabel || "<none>");
      rows.push({
        id: task.id,
        query: task.query,
        expectation: task.expectation,
        resultCount: results.length,
        top1Citation: top1?.citation || null,
        top1SectionLabel: top1?.sectionLabel || null,
        top1ConclusionLike: Boolean(top1 && isConclusionLikeSection(top1.sectionLabel)),
        top3ConclusionLike: top3.some((row) => isConclusionLikeSection(row?.sectionLabel)),
        top3Sections,
        topResults: top3.map((row, index) => ({
          rank: index + 1,
          citation: row?.citation || null,
          title: row?.title || null,
          sectionLabel: row?.sectionLabel || null,
          chunkType: row?.chunkType || null,
          score: typeof row?.score === "number" ? Number(row.score.toFixed(6)) : null
        })),
        error: null
      });
    } catch (error) {
      rows.push({
        id: task.id,
        query: task.query,
        expectation: task.expectation,
        resultCount: 0,
        top1Citation: null,
        top1SectionLabel: null,
        top1ConclusionLike: false,
        top3ConclusionLike: false,
        top3Sections: [],
        topResults: [],
        error: error instanceof Error ? error.message : String(error)
      });
    }

    await writeReports(buildReport(rows, true));
    if (pauseBetweenQueriesMs > 0) await sleep(pauseBetweenQueriesMs);
  }

  const report = buildReport(rows, false);

  const paths = await writeReports(report);
  console.log(JSON.stringify({
    queryCount: report.queryCount,
    returnedQueryCount: report.returnedQueryCount,
    failedQueryCount: report.failedQueryCount,
    top1ConclusionLikeCount: report.top1ConclusionLikeCount,
    top3ConclusionLikeCount: report.top3ConclusionLikeCount,
    allQueriesTop3ConclusionLike: report.allQueriesTop3ConclusionLike
  }, null, 2));
  console.log(`Conclusions-of-law QA JSON report written to ${paths.jsonPath}`);
  console.log(`Conclusions-of-law QA Markdown report written to ${paths.markdownPath}`);
  console.log(`Conclusions-of-law QA CSV report written to ${paths.csvPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
