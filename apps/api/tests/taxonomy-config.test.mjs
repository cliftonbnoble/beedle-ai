import { test, before } from "node:test";
import assert from "node:assert/strict";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";

async function fetchJson(pathname, init) {
  const response = await fetch(`${apiBase}${pathname}`, init);
  const raw = await response.text();
  let body;
  try {
    body = JSON.parse(raw);
  } catch {
    body = { raw };
  }
  return { status: response.status, body };
}

before(async () => {
  const health = await fetch(`${apiBase}/health`);
  assert.equal(health.status, 200, `Health check failed for ${apiBase}`);
});

test("taxonomy config inspection returns validated active config", async () => {
  const response = await fetchJson("/admin/config/taxonomy");
  assert.equal(response.status, 200);
  assert.ok(response.body.config);
  assert.ok(Array.isArray(response.body.config.case_types));
  assert.ok(Array.isArray(response.body.config.canonical_sections));
  assert.ok(response.body.stats.case_type_count >= 3);
  assert.ok(response.body.stats.canonical_section_count >= 5);
});

test("taxonomy resolve handles id, alias, and fallback predictably", async () => {
  const byId = await fetchJson("/admin/config/taxonomy/resolve", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ case_type: "zoning_variance" })
  });
  assert.equal(byId.status, 200);
  assert.equal(byId.body.match_type, "id");
  assert.equal(byId.body.resolved_case_type_id, "zoning_variance");

  const byAlias = await fetchJson("/admin/config/taxonomy/resolve", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ case_type: "variance" })
  });
  assert.equal(byAlias.status, 200);
  assert.equal(byAlias.body.match_type, "alias");
  assert.equal(byAlias.body.resolved_case_type_id, "zoning_variance");

  const fallback = await fetchJson("/admin/config/taxonomy/resolve", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ case_type: "unknown_type_123" })
  });
  assert.equal(fallback.status, 200);
  assert.equal(fallback.body.match_type, "fallback");
  assert.equal(fallback.body.resolved_case_type_id, "general");
  assert.ok(Array.isArray(fallback.body.warnings));
  assert.ok(fallback.body.warnings.length > 0);
});

test("taxonomy validate rejects incomplete config safely", async () => {
  const response = await fetchJson("/admin/config/taxonomy/validate", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      version: "broken",
      case_types: []
    })
  });
  assert.equal(response.status, 400);
  assert.equal(response.body.ok, false);
  assert.ok(response.body.error);
});

test("template endpoint keeps backward compatibility and fallback behavior", async () => {
  const known = await fetchJson("/api/draft/template", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ case_type: "zoning_variance", template_mode: "guided_scaffold" })
  });
  assert.equal(known.status, 200);
  assert.equal(known.body.case_type, "zoning_variance");
  assert.equal(known.body.template_sections.length, 5);

  const unknown = await fetchJson("/api/draft/template", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ case_type: "totally_unknown_case_type", template_mode: "guided_scaffold" })
  });
  assert.equal(unknown.status, 200);
  assert.equal(unknown.body.case_type, "general");
  assert.ok(unknown.body.guidance_notes.some((line) => String(line).toLowerCase().includes("falling back")));
});

test("template generation remains stable across configured case types", async () => {
  const config = await fetchJson("/admin/config/taxonomy");
  assert.equal(config.status, 200);

  for (const entry of config.body.config.case_types) {
    const response = await fetchJson("/api/draft/template", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ case_type: entry.id, template_mode: "guided_scaffold" })
    });
    assert.equal(response.status, 200, `template failed for case type ${entry.id}`);
    assert.equal(response.body.template_sections.length, 5, `unexpected section count for ${entry.id}`);
  }
});
