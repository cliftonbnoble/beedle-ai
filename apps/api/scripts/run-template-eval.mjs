import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const goldSetPath = process.env.TEMPLATE_GOLD_SET_PATH || path.resolve(process.cwd(), "eval", "template-gold-set.json");

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

async function main() {
  const raw = await fs.readFile(goldSetPath, "utf8");
  const goldSet = JSON.parse(raw);
  const results = [];

  for (const item of goldSet) {
    const response = await postJson("/api/draft/template", item.input);
    const body = response.body;
    const checks = [];
    const expected = item.expected || {};

    if (typeof expected.section_count === "number") {
      checks.push(pass((body.template_sections || []).length === expected.section_count, `section_count == ${expected.section_count}`));
    }
    if (typeof expected.min_sections_with_prompts === "number") {
      const count = (body.template_sections || []).filter((section) => (section.drafting_prompts || []).length > 0).length;
      checks.push(pass(count >= expected.min_sections_with_prompts, `sections_with_prompts >= ${expected.min_sections_with_prompts} (got ${count})`));
    }
    if (typeof expected.min_prompts_per_section === "number") {
      const minPrompts = Math.min(...(body.template_sections || []).map((section) => (section.drafting_prompts || []).length));
      checks.push(pass(minPrompts >= expected.min_prompts_per_section, `min_prompts_per_section >= ${expected.min_prompts_per_section} (got ${minPrompts})`));
    }
    if (typeof expected.max_prompts_per_section === "number") {
      const maxPrompts = Math.max(...(body.template_sections || []).map((section) => (section.drafting_prompts || []).length));
      checks.push(pass(maxPrompts <= expected.max_prompts_per_section, `max_prompts_per_section <= ${expected.max_prompts_per_section} (got ${maxPrompts})`));
    }
    if (typeof expected.citation_count === "number") {
      checks.push(pass((body.citations || []).length === expected.citation_count, `citation_count == ${expected.citation_count}`));
    }
    if (expected.requires_completeness_note) {
      checks.push(pass(Boolean(body.confidence_or_completeness_note), "confidence_or_completeness_note is present"));
    }

    const authorityKeys = new Set((body.supporting_authorities || []).map((row) => `${row.citation_id}|${row.citation_anchor}|${row.source_link}`));
    const noFabricated = (body.citations || []).every((citation) => authorityKeys.has(`${citation.id}|${citation.citation_anchor}|${citation.source_link}`));
    checks.push(pass(noFabricated, "no fabricated citations"));

    results.push({ id: item.id, status: response.status, checks });
  }

  const summary = {
    totalItems: results.length,
    passedItems: results.filter((item) => item.status === 200 && item.checks.every((check) => check.ok)).length,
    failedItems: results.filter((item) => item.status !== 200 || item.checks.some((check) => !check.ok)).length
  };

  const report = { generatedAt: new Date().toISOString(), apiBase, goldSetPath, summary, results };
  const reportPath = path.resolve(process.cwd(), "reports", "template-eval-report.json");
  await fs.writeFile(reportPath, JSON.stringify(report, null, 2));
  console.log("Template evaluation summary:");
  console.log(JSON.stringify(summary, null, 2));
  console.log(`Detailed report: ${reportPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
