import { before, test } from "node:test";
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

test("admin ingestion list supports broader-batch filter/sort controls", async () => {
  const response = await fetchJson(
    "/admin/ingestion/documents?status=all&sort=unresolvedReferenceDesc&unresolvedReferencesOnly=1&criticalExceptionsOnly=1&lowConfidenceTaxonomyOnly=1&filteredNoiseOnly=1&limit=50"
  );
  assert.equal(response.status, 200);
  assert.ok(Array.isArray(response.body.documents));
  assert.ok(response.body.summary);
  assert.ok(typeof response.body.summary.total === "number");
  assert.ok(typeof response.body.summary.withUnresolvedReferences === "number");
  assert.ok(typeof response.body.summary.withCriticalExceptions === "number");
  assert.ok(typeof response.body.summary.withFilteredNoise === "number");
  assert.ok(typeof response.body.summary.withLowConfidenceTaxonomy === "number");
  assert.ok(typeof response.body.summary.withMissingRulesDetection === "number");
  assert.ok(typeof response.body.summary.withMissingOrdinanceDetection === "number");
  assert.ok(typeof response.body.summary.realDocs === "number");
  assert.ok(typeof response.body.summary.realApprovalReady === "number");
  assert.ok(Array.isArray(response.body.summary.blockerBreakdown));
  for (const doc of response.body.documents || []) {
    assert.ok(typeof doc.unresolvedReferenceCount === "number");
    assert.ok(typeof doc.criticalExceptionCount === "number");
    assert.ok(typeof doc.filteredNoiseCount === "number");
    assert.ok(typeof doc.lowConfidenceTaxonomy === "boolean");
    assert.ok(typeof doc.missingRulesDetection === "boolean");
    assert.ok(typeof doc.missingOrdinanceDetection === "boolean");
    assert.ok(typeof doc.isLikelyFixture === "boolean");
    assert.ok(Array.isArray(doc.failedQcRequirements));
  }

  const blockerFiltered = await fetchJson("/admin/ingestion/documents?status=staged&blocker=metadata_not_confirmed&limit=30");
  assert.equal(blockerFiltered.status, 200);
  for (const doc of blockerFiltered.body.documents || []) {
    assert.ok((doc.approvalReadiness?.blockers || []).includes("metadata_not_confirmed"));
  }
});

test("taxonomy suggestion is surfaced in list and detail views", async () => {
  const list = await fetchJson("/admin/ingestion/documents?status=all&limit=20");
  assert.equal(list.status, 200);
  const first = (list.body.documents || [])[0];
  assert.ok(first, "expected at least one document in ingestion list");
  assert.ok(first.taxonomySuggestion);
  assert.ok("caseTypeId" in first.taxonomySuggestion);

  const detail = await fetchJson(`/admin/ingestion/documents/${first.id}`);
  assert.equal(detail.status, 200);
  assert.ok(detail.body.taxonomySuggestion);
  assert.ok(Array.isArray(detail.body.taxonomySuggestion.signals));
  assert.ok(Array.isArray(detail.body.criticalExceptionReferences));
  assert.ok(typeof detail.body.unresolvedReferenceCount === "number");

  const reprocessed = await fetchJson(`/admin/ingestion/documents/${first.id}/reprocess`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{}"
  });
  assert.equal(reprocessed.status, 200);
  assert.equal(reprocessed.body.id, first.id);
});
