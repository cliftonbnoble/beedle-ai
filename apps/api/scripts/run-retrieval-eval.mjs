import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const goldSetPath = process.env.GOLD_SET_PATH || path.resolve(process.cwd(), "eval", "gold-set.json");

async function postJson(pathname, payload) {
  const response = await fetch(`${apiBase}${pathname}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  return { status: response.status, body };
}

function pass(condition, message) {
  return { ok: Boolean(condition), message };
}

function checkSearchExpectation(item, result) {
  const checks = [];
  const expected = item.expected || {};
  const rows = result.body?.results || [];

  if (typeof expected.min_results === "number") {
    checks.push(pass(rows.length >= expected.min_results, `min_results >= ${expected.min_results} (got ${rows.length})`));
  }

  if (expected.expected_title_contains?.length) {
    const titleHit = rows.some((row) =>
      expected.expected_title_contains.every((needle) => String(row.title || "").toLowerCase().includes(String(needle).toLowerCase()))
    );
    checks.push(pass(titleHit, `title contains tokens: ${expected.expected_title_contains.join(", ")}`));
  }

  if (expected.expected_section_contains?.length) {
    const sectionHit = rows.some((row) =>
      expected.expected_section_contains.some((needle) => String(row.sectionLabel || "").toLowerCase().includes(String(needle).toLowerCase()))
    );
    checks.push(pass(sectionHit, `section contains one of: ${expected.expected_section_contains.join(", ")}`));
  }

  if (expected.expected_phrase_in_snippet) {
    const phrase = String(expected.expected_phrase_in_snippet).toLowerCase();
    const phraseHit = rows.some((row) => String(row.snippet || "").toLowerCase().includes(phrase));
    checks.push(pass(phraseHit, `snippet contains phrase: ${expected.expected_phrase_in_snippet}`));
  }

  return checks;
}

function checkCaseAssistantExpectation(item, result) {
  const checks = [];
  const expected = item.expected || {};

  const similar = result.body?.similar_cases || [];
  const law = result.body?.relevant_law || [];

  if (typeof expected.min_similar_cases === "number") {
    checks.push(pass(similar.length >= expected.min_similar_cases, `similar_cases >= ${expected.min_similar_cases} (got ${similar.length})`));
  }

  if (typeof expected.min_relevant_law === "number") {
    checks.push(pass(law.length >= expected.min_relevant_law, `relevant_law >= ${expected.min_relevant_law} (got ${law.length})`));
  }

  const citationLinked = (result.body?.citations || []).every((citation) =>
    [...similar, ...law].some((entry) => entry.citation_id === citation.id)
  );
  checks.push(pass(citationLinked, "all citation objects map to retrieved authorities"));

  return checks;
}

async function main() {
  const raw = await fs.readFile(goldSetPath, "utf8");
  const goldSet = JSON.parse(raw);

  const results = [];

  for (const item of goldSet) {
    if (item.mode === "search") {
      const response = await postJson("/admin/retrieval/debug", {
        query: item.query,
        limit: item.limit || 10,
        queryType: item.queryType || "keyword",
        filters: item.filters || { approvedOnly: false }
      });

      const checks = checkSearchExpectation(item, response);
      results.push({ id: item.id, mode: item.mode, status: response.status, checks });
      continue;
    }

    if (item.mode === "case_assistant") {
      const response = await postJson("/api/case-assistant", item.input);
      const checks = checkCaseAssistantExpectation(item, response);
      results.push({ id: item.id, mode: item.mode, status: response.status, checks });
    }
  }

  const summary = {
    totalItems: results.length,
    passedItems: results.filter((item) => item.status === 200 && item.checks.every((check) => check.ok)).length,
    failedItems: results.filter((item) => item.status !== 200 || item.checks.some((check) => !check.ok)).length
  };

  const report = {
    generatedAt: new Date().toISOString(),
    apiBase,
    goldSetPath,
    summary,
    results
  };

  const reportPath = path.resolve(process.cwd(), "reports", "retrieval-eval-report.json");
  await fs.writeFile(reportPath, JSON.stringify(report, null, 2));

  console.log("Retrieval evaluation summary:");
  console.log(JSON.stringify(summary, null, 2));
  console.log(`Detailed report: ${reportPath}`);

  if (summary.failedItems > 0 && String(process.env.EVAL_STRICT || "").toLowerCase() === "true") {
    process.exit(1);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
