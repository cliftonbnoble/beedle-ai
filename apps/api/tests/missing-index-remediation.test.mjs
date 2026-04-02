import { before, test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");
const enabled = process.env.ALLOW_REFERENCE_REBUILD_TEST === "1";

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
  if (!enabled) return;
  const health = await fetch(`${apiBase}/health`);
  assert.equal(health.status, 200, `Health check failed for ${apiBase}`);

  const rebuild = await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [
        {
          code_identifier: "IC-104",
          linked_ordinance_sections: ["37.3(a)(1)"],
          linked_rules_sections: ["Rule 1.11"]
        }
      ],
      ordinance_sections: [{ section_number: "37.3", subsection_path: "(a)(1)", body_text: "Ordinance support" }],
      rules_sections: [{ part: "Part I", section_number: "1.11", body_text: "Rule support" }],
      crosswalk: [{ index_code: "IC-104", ordinance_section: "37.3(a)(1)", rules_section: "Rule 1.11", source: "test" }]
    })
  });
  assert.equal(rebuild.status, 200);
});

test(
  "missing index code can be inferred from validated rules/ordinance crosswalk links",
  { skip: !enabled },
  async () => {
  const bytes = await fs.readFile(path.join(fixturesDir, "decision_missing_index_but_linked_refs.docx.txt"));
  const ingested = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Missing Index Remediation ${Date.now()}`,
      citation: `BEE-MISSIDX-${Date.now()}`,
      sourceFile: {
        filename: "decision_missing_index_but_linked_refs.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: bytes.toString("base64")
      }
    })
  });
  assert.equal(ingested.status, 201);

  const detail = await fetchJson(`/admin/ingestion/documents/${ingested.body.documentId}`);
  assert.equal(detail.status, 200);
  assert.ok((detail.body.indexCodes || []).length > 0, "index code should be inferred when crosswalk evidence exists");
  assert.ok((detail.body.extractionWarnings || []).some((item) => String(item).includes("Index codes inferred from validated references")));
  assert.ok(!detail.body.failedQcRequirements.includes("missing_index_codes"));
  }
);
