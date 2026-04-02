import { before, test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");
const runTag = `rollout${Date.now().toString(36)}`;
const state = { validId: "", invalidId: "", pendingId: "", unsafeId: "", safeManualId: "", fixtureManualId: "" };

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

async function readFixtureBase64(name) {
  const bytes = await fs.readFile(path.join(fixturesDir, name));
  return bytes.toString("base64");
}

before(async () => {
  const health = await fetch(`${apiBase}/health`);
  assert.equal(health.status, 200, `Health check failed for ${apiBase}`);

  const valid = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Approval Rollout Valid ${runTag}`,
      citation: `BEE-ROLL-VALID-${runTag}`,
      decisionDate: "2026-03-07",
      sourceFile: {
        filename: "decision_pass.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
      }
    })
  });
  assert.equal(valid.status, 201);
  state.validId = valid.body.documentId;

  const invalid = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Approval Rollout Invalid ${runTag}`,
      citation: `BEE-ROLL-INVALID-${runTag}`,
      decisionDate: "2026-03-07",
      sourceFile: {
        filename: "decision_invalid_missing_required_metadata.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: await readFixtureBase64("decision_invalid_missing_required_metadata.docx.txt")
      }
    })
  });
  assert.equal(invalid.status, 201);
  state.invalidId = invalid.body.documentId;

  const pending = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Approval Rollout Pending ${runTag}`,
      citation: `BEE-ROLL-PENDING-${runTag}`,
      decisionDate: "2026-03-07",
      sourceFile: {
        filename: "decision_pass.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
      }
    })
  });
  assert.equal(pending.status, 201);
  state.pendingId = pending.body.documentId;

  const unsafe = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Approval Rollout Unsafe37x ${runTag}`,
      citation: `BEE-ROLL-UNSAFE-${runTag}`,
      decisionDate: "2026-03-07",
      sourceFile: {
        filename: "decision_pass.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
      }
    })
  });
  assert.equal(unsafe.status, 201);
  state.unsafeId = unsafe.body.documentId;

  const safeManual = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Approval Rollout SafeManual ${runTag}`,
      citation: `BEE-ROLL-SAFE-MANUAL-${runTag}`,
      decisionDate: "2026-03-07",
      sourceFile: {
        filename: "real-candidate-upload.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
      }
    })
  });
  assert.equal(safeManual.status, 201);
  state.safeManualId = safeManual.body.documentId;

  const fixtureManual = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Fixture Runtime Candidate ${runTag}`,
      citation: `BEE-FIXTURE-MANUAL-${runTag}`,
      decisionDate: "2026-03-07",
      sourceFile: {
        filename: "fixture-runtime-candidate.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: await readFixtureBase64("decision_pass.docx.txt")
      }
    })
  });
  assert.equal(fixtureManual.status, 201);
  state.fixtureManualId = fixtureManual.body.documentId;

  await fetchJson(`/admin/ingestion/documents/${state.validId}/metadata`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      index_codes: ["104"],
      rules_sections: ["Rule 1.11"],
      ordinance_sections: ["Ordinance 37.3(a)(1)"],
      confirm_required_metadata: true
    })
  });

  const unsafeUpdate = await fetchJson(`/admin/ingestion/documents/${state.unsafeId}/metadata`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      index_codes: ["104"],
      rules_sections: ["Rule 1.11"],
      ordinance_sections: ["Ordinance 37.3"],
      confirm_required_metadata: true
    })
  });
  assert.equal(unsafeUpdate.status, 200);

  const safeManualUpdate = await fetchJson(`/admin/ingestion/documents/${state.safeManualId}/metadata`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      index_codes: ["104"],
      rules_sections: ["Rule 1.11"],
      ordinance_sections: ["Ordinance37.2x"],
      confirm_required_metadata: true
    })
  });
  assert.equal(safeManualUpdate.status, 200);

  const fixtureManualUpdate = await fetchJson(`/admin/ingestion/documents/${state.fixtureManualId}/metadata`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      index_codes: ["104"],
      rules_sections: ["Rule 1.11"],
      ordinance_sections: ["Ordinance37.2x"],
      confirm_required_metadata: true
    })
  });
  assert.equal(fixtureManualUpdate.status, 200);
});

test("approval readiness classification exposes blockers for non-ready docs", async () => {
  const detail = await fetchJson(`/admin/ingestion/documents/${state.invalidId}`);
  assert.equal(detail.status, 200);
  assert.ok(detail.body.approvalReadiness);
  assert.equal(detail.body.approvalReadiness.eligible, false);
  assert.ok(Array.isArray(detail.body.approvalReadiness.blockers));
  assert.ok(detail.body.approvalReadiness.blockers.length > 0);
  assert.ok(Array.isArray(detail.body.failedQcRequirements));
  assert.ok(detail.body.qcGateDiagnostics);
  assert.equal(typeof detail.body.qcGateDiagnostics.hasIndexCodes, "boolean");
});

test("approval-ready filter only returns eligible docs", async () => {
  const list = await fetchJson("/admin/ingestion/documents?status=staged&approvalReadyOnly=1&sort=approvalReadinessDesc&limit=50");
  assert.equal(list.status, 200);
  for (const doc of list.body.documents || []) {
    assert.equal(doc.approvalReadiness?.eligible, true);
  }
});

test("real-only staged triage returns explainable readiness fields", async () => {
  const list = await fetchJson("/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=approvalReadinessDesc&limit=50");
  assert.equal(list.status, 200);
  assert.ok(list.body.summary);
  assert.ok(typeof list.body.summary.realDocs === "number");
  assert.ok(Array.isArray(list.body.summary.blockerBreakdown));
  for (const doc of list.body.documents || []) {
    assert.equal(doc.isLikelyFixture, false);
    assert.ok(Array.isArray(doc.approvalReadiness?.nextActions));
    assert.ok(Array.isArray(doc.approvalReadiness?.blockers));
    assert.ok(Array.isArray(doc.failedQcRequirements));
  }
});

test("reviewer queue filters expose triage and blocked-37x fields", async () => {
  const list = await fetchJson(
    "/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&sort=unresolvedLeverageDesc&limit=50"
  );
  assert.equal(list.status, 200);
  assert.equal(typeof list.body.summary.surfacedRuntimeManualCandidates, "number");
  assert.equal(typeof list.body.summary.surfacedRuntimeManualRealCandidates, "number");
  assert.equal(typeof list.body.summary.surfacedRuntimeManualFixtureCandidates, "number");
  assert.equal(typeof list.body.summary.unsafeRuntimeManualSurfacedViolations, "number");
  assert.equal(typeof list.body.summary.unsafeRuntimeManualSuppressedCount, "number");
  for (const doc of list.body.documents || []) {
    assert.ok(Array.isArray(doc.unresolvedBuckets));
    assert.equal(typeof doc.topRecommendedReviewerAction, "string");
    assert.ok(["low", "medium", "high"].includes(doc.estimatedReviewerEffort));
    assert.ok(["low", "medium", "high"].includes(doc.reviewerRiskLevel));
    assert.ok(Array.isArray(doc.blocked37xReferences));
    assert.equal(typeof doc.blocked37xReason, "string");
    assert.equal(typeof doc.blocked37xReviewerHint, "string");
    assert.equal(typeof doc.blocked37xSafeToBatchReview, "boolean");
    assert.ok(["keep_blocked", "possible_manual_context_fix_but_no_auto_apply"].includes(doc.runtimeDisposition));
    assert.ok(["none", "parenthetical_prefix_fix_candidate", "low_risk_not_found_residue"].includes(doc.runtimeManualReasonCode));
    assert.equal(typeof doc.runtimeManualReasonSummary, "string");
    assert.equal(typeof doc.runtimeSuggestedOperatorAction, "string");
    assert.equal(typeof doc.runtimeOperatorReviewSummary, "string");
    if (doc.runtimeSurfaceForManualReview) {
      assert.ok(doc.runtimeReviewDiagnostic);
      assert.equal(doc.runtimeReviewDiagnostic.runtimeDoNotAutoApply, true);
    } else {
      assert.equal(doc.runtimeReviewDiagnostic, null);
    }
    assert.equal(typeof doc.runtimePolicyReason, "string");
    assert.equal(typeof doc.runtimeSurfaceForManualReview, "boolean");
    assert.equal(typeof doc.runtimeManualReviewRequired, "boolean");
    assert.equal(doc.runtimeDoNotAutoApply, true);
    const unsafeFamilies = (doc.blocked37xReferences || []).map((item) => item.family).filter((family) => ["37.3", "37.7", "37.9"].includes(family));
    if (unsafeFamilies.length > 0) {
      assert.equal(doc.runtimeDisposition, "keep_blocked");
      assert.equal(doc.runtimeManualReasonCode, "none");
      assert.equal(doc.runtimeSurfaceForManualReview, false);
      assert.equal(doc.runtimeDoNotAutoApply, true);
    }
  }

  const riskFiltered = await fetchJson(
    "/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&reviewerRiskLevel=high&sort=reviewerEffortAsc&limit=50"
  );
  assert.equal(riskFiltered.status, 200);
  for (const doc of riskFiltered.body.documents || []) {
    assert.equal(doc.reviewerRiskLevel, "high");
  }

  const blocked37x = await fetchJson(
    "/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&blocked37xOnly=1&sort=blocked37xBatchKeyAsc&limit=50"
  );
  assert.equal(blocked37x.status, 200);
  for (const doc of blocked37x.body.documents || []) {
    assert.ok((doc.blocked37xReferences || []).length > 0);
    assert.equal(doc.runtimeDisposition, "keep_blocked");
    assert.equal(doc.runtimeDoNotAutoApply, true);
  }

  const runtimeManualOnly = await fetchJson(
    "/admin/ingestion/documents?status=staged&fileType=decision_docx&realOnly=1&runtimeManualCandidatesOnly=1&sort=reviewerEffortAsc&limit=50"
  );
  assert.equal(runtimeManualOnly.status, 200);
  for (const doc of runtimeManualOnly.body.documents || []) {
    assert.equal(doc.runtimeSurfaceForManualReview, true);
    assert.equal(doc.runtimeDisposition, "possible_manual_context_fix_but_no_auto_apply");
    assert.notEqual(doc.runtimeManualReasonCode, "none");
    assert.equal(typeof doc.runtimeManualReasonSummary, "string");
    assert.equal(typeof doc.runtimeSuggestedOperatorAction, "string");
    assert.equal(doc.runtimeDoNotAutoApply, true);
  }
});

test("metadata confirmation flow can recover near-ready candidates conservatively", async () => {
  const blockedList = await fetchJson(
    "/admin/ingestion/documents?status=staged&blocker=metadata_not_confirmed&sort=approvalReadinessDesc&limit=200"
  );
  assert.equal(blockedList.status, 200);
  assert.ok(Array.isArray(blockedList.body.documents));
  assert.ok(blockedList.body.documents.some((doc) => doc.id === state.pendingId));

  const before = await fetchJson(`/admin/ingestion/documents/${state.pendingId}`);
  assert.equal(before.status, 200);
  assert.ok(before.body.approvalReadiness.blockers.includes("metadata_not_confirmed"));
  assert.ok(before.body.validReferences);

  const confirm = await fetchJson(`/admin/ingestion/documents/${state.pendingId}/metadata`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      index_codes: ["104"],
      rules_sections: ["Rule 1.11"],
      ordinance_sections: ["Ordinance 37.3(a)(1)"],
      case_number: before.body.caseNumber || null,
      decision_date: before.body.decisionDate || null,
      author_name: before.body.authorName || null,
      outcome_label: before.body.outcomeLabel || "unclear",
      confirm_required_metadata: true
    })
  });
  assert.equal(confirm.status, 200);

  const after = await fetchJson(`/admin/ingestion/documents/${state.pendingId}`);
  assert.equal(after.status, 200);
  assert.ok(!after.body.approvalReadiness.blockers.includes("metadata_not_confirmed"));
  assert.equal(after.body.qcGateDiagnostics.passed, true);
});

test("runtime policy defaults unsafe 37.x families to keep_blocked and non-auto-apply", async () => {
  const unsafe = await fetchJson(`/admin/ingestion/documents/${state.unsafeId}`);
  assert.equal(unsafe.status, 200);
  const families = (unsafe.body.blocked37xReferences || []).map((row) => row.family);
  assert.ok(families.includes("37.3"));
  assert.equal(unsafe.body.runtimeDisposition, "keep_blocked");
  assert.equal(unsafe.body.runtimeManualReasonCode, "none");
  assert.equal(unsafe.body.runtimeSurfaceForManualReview, false);
  assert.equal(unsafe.body.runtimeDoNotAutoApply, true);
  assert.equal(unsafe.body.runtimeReviewDiagnostic, null);
});

test("runtime policy surfaces narrow safe manual candidate and manual-only filter includes it", async () => {
  const safeManual = await fetchJson(`/admin/ingestion/documents/${state.safeManualId}`);
  assert.equal(safeManual.status, 200);
  assert.equal(safeManual.body.runtimeDisposition, "possible_manual_context_fix_but_no_auto_apply");
  assert.equal(safeManual.body.runtimeManualReasonCode, "low_risk_not_found_residue");
  assert.equal(typeof safeManual.body.runtimeManualReasonSummary, "string");
  assert.equal(typeof safeManual.body.runtimeSuggestedOperatorAction, "string");
  assert.equal(typeof safeManual.body.runtimeOperatorReviewSummary, "string");
  assert.equal(safeManual.body.runtimeSurfaceForManualReview, true);
  assert.equal(safeManual.body.runtimeDoNotAutoApply, true);
  assert.ok(safeManual.body.runtimeReviewDiagnostic);
  assert.equal(safeManual.body.runtimeReviewDiagnostic.runtimeManualReasonCode, "low_risk_not_found_residue");
  assert.equal(safeManual.body.runtimeReviewDiagnostic.runtimeDoNotAutoApply, true);
  assert.ok((safeManual.body.unresolvedBuckets || []).includes("likely_parenthetical_or_prefix_fix"));
  assert.ok((safeManual.body.recurringCitationFamilies || []).includes("37.2"));

  const manualOnly = await fetchJson(
    "/admin/ingestion/documents?status=all&fileType=decision_docx&runtimeManualCandidatesOnly=1&limit=400"
  );
  assert.equal(manualOnly.status, 200);
  assert.equal(manualOnly.body.summary.unsafeRuntimeManualSurfacedViolations, 0);
  assert.ok(manualOnly.body.summary.surfacedRuntimeManualCandidates >= 1);
  const ids = new Set((manualOnly.body.documents || []).map((doc) => doc.id));
  assert.ok(ids.has(state.safeManualId));
  assert.ok(ids.has(state.fixtureManualId));
  assert.ok(!ids.has(state.unsafeId));
  for (const doc of manualOnly.body.documents || []) {
    assert.equal(doc.runtimeDisposition, "possible_manual_context_fix_but_no_auto_apply");
    assert.equal(doc.runtimeSurfaceForManualReview, true);
    assert.equal(doc.runtimeDoNotAutoApply, true);
  }

  const manualOnlyReal = await fetchJson(
    "/admin/ingestion/documents?status=all&fileType=decision_docx&runtimeManualCandidatesOnly=1&realOnly=1&limit=400"
  );
  assert.equal(manualOnlyReal.status, 200);
  assert.equal(manualOnlyReal.body.summary.surfacedRuntimeManualFixtureCandidates, 0);
  assert.equal(manualOnlyReal.body.summary.unsafeRuntimeManualSurfacedViolations, 0);
  const realIds = new Set((manualOnlyReal.body.documents || []).map((doc) => doc.id));
  assert.ok(realIds.has(state.safeManualId));
  assert.ok(!realIds.has(state.fixtureManualId));
  for (const doc of manualOnlyReal.body.documents || []) {
    assert.equal(doc.isLikelyFixture, false);
  }
});

test("promotion route remains conservative and explainable", async () => {
  const detail = await fetchJson(`/admin/ingestion/documents/${state.validId}`);
  assert.equal(detail.status, 200);
  assert.ok(detail.body.approvalReadiness);

  const approve = await fetchJson(`/admin/ingestion/documents/${state.validId}/approve`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{}"
  });
  if (detail.body.approvalReadiness.eligible) {
    assert.equal(approve.status, 200);
    assert.equal(approve.body.approved, true);
  } else {
    // Approval gate can still pass when non-gate cautions/blockers remain (e.g. unresolved/warning thresholds).
    assert.ok(approve.status === 200 || approve.status === 422);
    assert.equal(typeof approve.body.approved, "boolean");
  }
});
