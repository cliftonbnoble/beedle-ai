import { test, before } from "./live-test-helpers.mjs";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

// NS-13: graded relevance evaluation. Unlike the golden net (byte-identical top-N — a regression pin
// that also enshrines today's mistakes), this computes quality METRICS against judged ground truth:
//   P@5  — fraction of the top 5 with grade >= 1
//   MRR  — 1/rank of the first grade >= 1 result
// and asserts each query's metrics never drop below the committed baseline (improvements are welcome
// and re-baselined deliberately via UPDATE_SEARCH_EVAL_BASELINE=1). Latency is recorded per query and
// enforced only where the fixture sets latencyBudgetMs — budgets get set as the NS-2x perf fixes land.
const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const judgmentsPath = path.resolve(process.cwd(), "tests/fixtures/search-relevance-judgments.json");
const goldenPath = path.resolve(process.cwd(), "tests/fixtures/search-golden-ranking.json");
const baselinePath = path.resolve(process.cwd(), "tests/fixtures/search-relevance-baseline.json");
const updateBaseline = process.env.UPDATE_SEARCH_EVAL_BASELINE === "1";

async function fetchResults(request, attempt = 0) {
  const started = Date.now();
  const response = await fetch(`${apiBase}/admin/retrieval/debug`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(request)
  });
  if (response.status === 503 && attempt < 3) {
    await new Promise((resolve) => setTimeout(resolve, 1500));
    return fetchResults(request, attempt + 1);
  }
  assert.equal(response.status, 200, `search failed: ${response.status}`);
  const body = await response.json();
  return { citations: (body.results || []).map((row) => row.citation), latencyMs: Date.now() - started };
}

function resolveJudgments(entry, goldenById) {
  const raw = entry.judgments || {};
  if (!raw.__from_golden__) return raw;
  const golden = goldenById.get(raw.__from_golden__);
  assert.ok(golden, `golden id not found: ${raw.__from_golden__}`);
  const grade = raw.__grade_override__ ?? 2;
  const judgments = {};
  for (const citation of golden.expectedTopN || []) judgments[citation] = grade;
  return judgments;
}

function metrics(citations, judgments) {
  const top5 = citations.slice(0, 5);
  const relevant = top5.filter((citation) => (judgments[citation] || 0) >= 1).length;
  const p5 = top5.length ? relevant / 5 : 0;
  let mrr = 0;
  for (let index = 0; index < citations.length; index++) {
    if ((judgments[citations[index]] || 0) >= 1) {
      mrr = 1 / (index + 1);
      break;
    }
  }
  return { p5: Number(p5.toFixed(4)), mrr: Number(mrr.toFixed(4)) };
}

const fixture = JSON.parse(await fs.readFile(judgmentsPath, "utf8"));
const goldenFixture = JSON.parse(await fs.readFile(goldenPath, "utf8"));
const goldenById = new Map((goldenFixture.queries || goldenFixture).map((query) => [query.id, query]));
let baseline = null;
try {
  baseline = JSON.parse(await fs.readFile(baselinePath, "utf8"));
} catch {
  baseline = null;
}

const captured = {};

for (const entry of fixture.queries) {
  test(`relevance eval: ${entry.id} (${entry.class})`, async () => {
    const { citations, latencyMs } = await fetchResults(entry.request);

    if (entry.expectEmpty) {
      assert.equal(citations.length, 0, `${entry.id}: expected zero results, got ${citations.join(", ")}`);
      captured[entry.id] = { p5: 1, mrr: 1, latencyMs, top5: [] };
    } else if (entry.measureOnly) {
      captured[entry.id] = { p5: null, mrr: null, latencyMs, top5: citations.slice(0, 5) };
    } else {
      const judgments = resolveJudgments(entry, goldenById);
      const { p5, mrr } = metrics(citations, judgments);
      captured[entry.id] = { p5, mrr, latencyMs, top5: citations.slice(0, 5) };

      if (baseline && baseline[entry.id] && !updateBaseline) {
        const prior = baseline[entry.id];
        assert.ok(
          p5 >= prior.p5,
          `${entry.id}: P@5 regressed ${prior.p5} -> ${p5} (top5: ${citations.slice(0, 5).join(", ")})`
        );
        assert.ok(mrr >= prior.mrr, `${entry.id}: MRR regressed ${prior.mrr} -> ${mrr}`);
      }
    }

    if (entry.latencyBudgetMs && !updateBaseline) {
      assert.ok(
        latencyMs <= entry.latencyBudgetMs,
        `${entry.id}: latency ${latencyMs}ms exceeds budget ${entry.latencyBudgetMs}ms`
      );
    }
  });
}

test("relevance eval: scoreboard + baseline maintenance", async () => {
  const ids = Object.keys(captured);
  assert.ok(ids.length >= fixture.queries.length - 1, "most queries must have been captured");

  const scored = ids.filter((id) => captured[id].p5 !== null);
  const meanP5 = scored.reduce((sum, id) => sum + captured[id].p5, 0) / Math.max(1, scored.length);
  const meanMrr = scored.reduce((sum, id) => sum + captured[id].mrr, 0) / Math.max(1, scored.length);
  console.log(`\n  === relevance scoreboard (${scored.length} scored queries) ===`);
  console.log(`  mean P@5: ${meanP5.toFixed(3)}   mean MRR: ${meanMrr.toFixed(3)}`);
  for (const id of ids) {
    const row = captured[id];
    console.log(
      `  ${id.padEnd(28)} p5=${row.p5 === null ? "  — " : row.p5.toFixed(2)} mrr=${row.mrr === null ? "  — " : row.mrr.toFixed(2)} ${String(row.latencyMs).padStart(6)}ms`
    );
  }

  if (updateBaseline) {
    await fs.writeFile(baselinePath, JSON.stringify(captured, null, 2) + "\n");
    console.log(`  baseline written: ${baselinePath}`);
  } else {
    assert.ok(baseline, "no baseline committed — run with UPDATE_SEARCH_EVAL_BASELINE=1 to create it");
  }
});
