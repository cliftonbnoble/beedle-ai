import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const goldSetPath = process.env.DRAFT_GOLD_SET_PATH || path.resolve(process.cwd(), "eval", "draft-gold-set.json");

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

function evaluateItem(item, response) {
  const checks = [];
  const expected = item.expected || {};
  const body = response.body;
  const draft = body?.draft || {};
  const debug = body?.debug || {};

  const supportingAuthorities = draft.supporting_authorities || [];
  const paragraphSupport = draft.paragraph_support || [];
  const citations = draft.citations || [];
  const limitations = draft.limitations || [];

  if (typeof expected.min_supporting_authorities === "number") {
    checks.push(
      pass(
        supportingAuthorities.length >= expected.min_supporting_authorities,
        `supporting_authorities >= ${expected.min_supporting_authorities} (got ${supportingAuthorities.length})`
      )
    );
  }

  if (typeof expected.min_paragraph_support_items === "number") {
    checks.push(
      pass(paragraphSupport.length >= expected.min_paragraph_support_items, `paragraph_support >= ${expected.min_paragraph_support_items} (got ${paragraphSupport.length})`)
    );
  }

  if (typeof expected.min_supported_paragraph_ratio === "number") {
    const supported = paragraphSupport.filter((item) => item.support_level !== "unsupported").length;
    const ratio = paragraphSupport.length > 0 ? supported / paragraphSupport.length : 0;
    checks.push(pass(ratio >= expected.min_supported_paragraph_ratio, `supported paragraph ratio >= ${expected.min_supported_paragraph_ratio} (got ${ratio.toFixed(2)})`));
  }

  if (expected.confidence_one_of?.length) {
    checks.push(pass(expected.confidence_one_of.includes(draft.confidence), `confidence in [${expected.confidence_one_of.join(", ")}] (got ${draft.confidence})`));
  }

  if (typeof expected.min_limitations === "number") {
    checks.push(pass(limitations.length >= expected.min_limitations, `limitations >= ${expected.min_limitations} (got ${limitations.length})`));
  }

  if (expected.limitations_contains_any?.length) {
    const lower = limitations.map((line) => String(line).toLowerCase());
    const hit = expected.limitations_contains_any.some((needle) => lower.some((line) => line.includes(String(needle).toLowerCase())));
    checks.push(pass(hit, `limitations contain any: ${expected.limitations_contains_any.join(", ")}`));
  }

  const citationIds = new Set(citations.map((item) => item.id));
  const authorityCitationIds = new Set(supportingAuthorities.map((item) => item.citation_id));

  checks.push(pass(paragraphSupport.every((item) => item.citation_ids.every((id) => citationIds.has(id))), "all paragraph_support citation_ids resolve to citation objects"));
  checks.push(pass(citations.every((item) => authorityCitationIds.has(item.id)), "all citation objects map to supporting_authorities"));

  if (expected.no_fabricated_citations) {
    const authorityKeys = new Set(supportingAuthorities.map((item) => `${item.citation_id}|${item.citation_anchor}|${item.source_link}`));
    const fabricated = citations.some((item) => !authorityKeys.has(`${item.id}|${item.citation_anchor}|${item.source_link}`));
    checks.push(pass(!fabricated, "no fabricated citation objects"));
  }

  checks.push(pass(Array.isArray(debug.paragraph_support), "debug.paragraph_support is present"));
  checks.push(pass(Boolean(debug.confidence_signals), "debug.confidence_signals is present"));

  return checks;
}

async function main() {
  const raw = await fs.readFile(goldSetPath, "utf8");
  const goldSet = JSON.parse(raw);
  const results = [];

  for (const item of goldSet) {
    const response = await postJson("/admin/draft/debug", item.input);
    const checks = evaluateItem(item, response);
    results.push({
      id: item.id,
      status: response.status,
      checks
    });
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

  const reportPath = path.resolve(process.cwd(), "reports", "drafting-eval-report.json");
  await fs.writeFile(reportPath, JSON.stringify(report, null, 2));

  console.log("Drafting evaluation summary:");
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
