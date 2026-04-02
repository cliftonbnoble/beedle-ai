import { before, test } from "node:test";
import assert from "node:assert/strict";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const runTag = `ref${Date.now().toString(36)}`;

const state = {
  knownDecisionId: "",
  unknownDecisionId: ""
};

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

function textFixtureBase64(text) {
  return Buffer.from(text, "utf8").toString("base64");
}

before(async () => {
  const health = await fetch(`${apiBase}/health`);
  assert.equal(health.status, 200, `Health check failed for ${apiBase}`);

  const rebuild = await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: {
        index_codes: "test:index",
        ordinance: "test:ordinance",
        rules: "test:rules"
      },
      index_codes: [
        {
          code_identifier: "IC-104",
          family: "Test Family",
          label: "Variance",
          description: "Variance case type",
          reserved: false,
          legacy_pre_1002: false,
          linked_ordinance_sections: ["77-19"],
          linked_rules_sections: ["Rule 3.1"],
          source_page_anchor: "index#p1"
        }
      ],
      ordinance_sections: [
        {
          section_number: "77-19",
          subsection_path: null,
          heading: "Lot Coverage",
          body_text: "Ordinance body",
          page_anchor: "ord#p1"
        }
      ],
      rules_sections: [
        {
          part: "Part 3",
          section_number: "3.1",
          heading: "Notice",
          body_text: "Rules body",
          page_anchor: "rules#p1"
        }
      ],
      crosswalk: [{ index_code: "IC-104", ordinance_section: "77-19", rules_section: "Rule 3.1", source: "test" }]
      ,
      coverage_report: {
        ordinance: {
          parser_used: "pdf",
          expected_section_count: 1,
          parsed_section_count: 1,
          duplicate_collisions_avoided: 0,
          low_confidence_sections: 0
        },
        rules: {
          parser_used: "pdf",
          expected_section_count: 1,
          parsed_section_count: 1,
          duplicate_collisions_avoided: 0,
          low_confidence_sections: 0
        },
        crosswalk: {
          total_candidates: 2,
          resolved_links: 1,
          unresolved_links: 1
        }
      }
    })
  });
  assert.equal(rebuild.status, 200, "reference rebuild should succeed");
});

test("normalized references inspect endpoint returns structured layers", async () => {
  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: {
        index_codes: "test:index",
        ordinance: "test:ordinance",
        rules: "test:rules"
      },
      index_codes: [
        {
          code_identifier: "IC-104",
          linked_ordinance_sections: ["37.2(g)", "88-88"],
          linked_rules_sections: ["10.10(c)(3)", "Rule 999.1"]
        }
      ],
      ordinance_sections: [{ section_number: "37.2", subsection_path: "(g)", body_text: "Ordinance body" }],
      rules_sections: [{ part: "Part 10", section_number: "10.10(c)(3)", body_text: "Rules body" }],
      crosswalk: [
        { index_code: "IC-104", ordinance_section: "37.2(g)", rules_section: "10.10(c)(3)", source: "resolved" },
        { index_code: "IC-104", ordinance_section: "88-88", source: "unresolved" }
      ],
      coverage_report: {
        ordinance: {
          parser_used: "layout_text",
          expected_section_count: 1,
          parsed_section_count: 999,
          duplicate_collisions_avoided: 0,
          low_confidence_sections: 0
        },
        rules: {
          parser_used: "layout_text",
          expected_section_count: 1,
          parsed_section_count: 999,
          duplicate_collisions_avoided: 0,
          low_confidence_sections: 0
        },
        crosswalk: { total_candidates: 2, resolved_links: 1, unresolved_links: 1 }
      }
    })
  });

  const inspect = await fetchJson("/admin/references");
  assert.equal(inspect.status, 200);
  assert.ok(inspect.body.summary.index_code_count >= 1);
  assert.ok(inspect.body.summary.ordinance_section_count >= 1);
  assert.ok(inspect.body.summary.rules_section_count >= 1);
  assert.ok(inspect.body.summary.crosswalk_count >= 2, "crosswalk candidates should be retained for unresolved inspection");
  assert.ok(Array.isArray(inspect.body.unresolved_crosswalks));
  assert.ok(inspect.body.unresolved_crosswalks.length >= 1);
  assert.equal(inspect.body.readiness_status.counts_consistent, false, "mismatch should be surfaced in readiness status");
  assert.equal(inspect.body.coverage_report.ordinance.parser_used, "layout_text");
});

test("critical citation verification helper reports resolved/unresolved states", async () => {
  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-104", linked_ordinance_sections: ["37.2(g)", "37.3(a)(1)", "37.15"], linked_rules_sections: ["Rule 1.11", "Rule 6.13", "Rule 10.10(c)(3)", "Rule 13.14"] }],
      ordinance_sections: [
        { section_number: "37.2", subsection_path: "(g)", body_text: "text" },
        { section_number: "37.3", subsection_path: "(a)(1)", body_text: "text" },
        { section_number: "37.15", subsection_path: null, body_text: "text" }
      ],
      rules_sections: [
        { part: "Part 1", section_number: "1.11", body_text: "text" },
        { part: "Part 6", section_number: "6.13", body_text: "text" },
        { part: "Part 10", section_number: "10.10(c)(3)", body_text: "text" },
        { part: "Part 13", section_number: "13.14", body_text: "text" }
      ],
      crosswalk: [
        { index_code: "IC-104", ordinance_section: "37.2(g)", source: "resolved" },
        { index_code: "IC-104", rules_section: "Rule 10.10(c)(3)", source: "resolved" }
      ],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 3, parsed_section_count: 3, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 4, parsed_section_count: 4, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 2, resolved_links: 2, unresolved_links: 0 }
      }
    })
  });
  const verify = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      citations: ["37.2(g)", "10.10(c)(3)", "13.14", "99.99"]
    })
  });
  assert.equal(verify.status, 200);
  const checks = verify.body.checks || [];
  assert.ok(checks.some((row) => row.citation === "37.2(g)" && row.status === "resolved"));
  assert.ok(checks.some((row) => row.citation === "10.10(c)(3)" && row.status === "resolved"));
  assert.ok(checks.some((row) => row.citation === "1.11" && row.status === "resolved"));
  assert.ok(checks.some((row) => row.citation === "6.13" && row.status === "resolved"));
  assert.ok(checks.some((row) => row.citation === "13.14" && row.status === "resolved"));
  assert.ok(checks.some((row) => row.citation === "99.99" && row.status === "unresolved"));
});

test("critical citation verification handles long malformed input safely", async () => {
  const longCitation = `37.2(${("abcdef").repeat(2000)})`;
  const verify = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations: [longCitation] })
  });
  assert.equal(verify.status, 200);
  assert.equal(Array.isArray(verify.body.checks), true);
  assert.equal(verify.body.checks[0]?.citation, longCitation);
});

test("decision ingest flags unknown malformed references for QC review", async () => {
  const ingest = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Unknown Reference Decision ${runTag}`,
      citation: `UNK-REF-${runTag}`,
      sourceFile: {
        filename: "unknown-reference.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: textFixtureBase64(
          "Index Codes\nIC-999\n\nRules\nRule 99.9\n\nOrdinance\nOrdinance 999-99\n\nAnalysis\nThe applicant seeks variance relief."
        )
      }
    })
  });
  assert.equal(ingest.status, 201);
  state.unknownDecisionId = ingest.body.documentId;

  const detail = await fetchJson(`/admin/ingestion/documents/${state.unknownDecisionId}`);
  assert.equal(detail.status, 200);
  assert.ok(Array.isArray(detail.body.referenceIssues));
  assert.ok(detail.body.referenceIssues.length >= 3);
});

test("search filters resolve through normalized reference links", async () => {
  const ingest = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Known Reference Decision ${runTag}`,
      citation: `KNOWN-REF-${runTag}`,
      sourceFile: {
        filename: "known-reference.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: textFixtureBase64(
          "Index Codes\nIC-104\n\nRules\nRule 3.1\n\nOrdinance\nOrdinance 77-19\n\nAnalysis\nVariance relief is discussed."
        )
      }
    })
  });
  assert.equal(ingest.status, 201);
  state.knownDecisionId = ingest.body.documentId;

  const confirm = await fetchJson(`/admin/ingestion/documents/${state.knownDecisionId}/metadata`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      index_codes: ["IC-104"],
      rules_sections: ["Rule 3.1"],
      ordinance_sections: ["Ordinance 77-19"],
      confirm_required_metadata: true
    })
  });
  assert.equal(confirm.status, 200);

  const approve = await fetchJson(`/decisions/${state.knownDecisionId}/approve`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({})
  });
  assert.equal(approve.status, 200);
  assert.equal(approve.body.approved, true);

  const search = await fetchJson("/search", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      query: "variance",
      limit: 20,
      filters: {
        approvedOnly: true,
        indexCode: "IC-104"
      }
    })
  });
  assert.equal(search.status, 200);
  assert.ok(search.body.results.some((row) => row.documentId === state.knownDecisionId), "expected known decision in filtered results");
});

test("rebuild dedupes normalized citation collisions deterministically", async () => {
  const rebuild = await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-222", linked_ordinance_sections: ["37.15"], linked_rules_sections: ["6.13"] }],
      ordinance_sections: [
        { section_number: "37.15", subsection_path: null, heading: "Short", body_text: "Short body.", page_anchor: "p1" },
        {
          section_number: "37.15",
          subsection_path: null,
          heading: "Longer Canonical Heading",
          body_text: "Longer ordinance body text with substantially more detail for deterministic selection.",
          page_anchor: "p2"
        }
      ],
      rules_sections: [
        { part: "Part 6", section_number: "6.13", heading: "First", body_text: "First rules body.", page_anchor: "r1" },
        {
          part: "Part 6",
          section_number: "6.13",
          heading: "Second",
          body_text: "Second rules body with additional clarifying content and details.",
          page_anchor: "r2"
        }
      ],
      crosswalk: [{ index_code: "IC-222", ordinance_section: "37.15", rules_section: "6.13", source: "test_collision" }],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 2, parsed_section_count: 2, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 2, parsed_section_count: 2, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 1, resolved_links: 1, unresolved_links: 0 }
      }
    })
  });
  assert.equal(rebuild.status, 200);
  assert.equal(rebuild.body.counts.ordinance_sections, 1);
  assert.equal(rebuild.body.counts.rules_sections, 1);
  assert.ok(rebuild.body.collision_diagnostics.ordinance.duplicate_normalized_citations_encountered >= 1);
  assert.ok(rebuild.body.collision_diagnostics.rules.duplicate_normalized_citations_encountered >= 1);
});

test("failed rebuild leaves previously committed reference state intact", async () => {
  const before = await fetchJson("/admin/references");
  assert.equal(before.status, 200);
  const beforeOrdinanceCount = before.body.summary.ordinance_section_count;
  const beforeRulesCount = before.body.summary.rules_section_count;

  const failing = await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "broken", ordinance: "broken", rules: "broken" },
      index_codes: [{ code_identifier: "IC-333", linked_ordinance_sections: [], linked_rules_sections: [] }],
      ordinance_sections: [{ section_number: "37.2", subsection_path: null, heading: "Broken", body_text: null }],
      rules_sections: [{ part: "Part 1", section_number: "1.11", heading: "Rule", body_text: "Body" }],
      crosswalk: [],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 0, resolved_links: 0, unresolved_links: 0 }
      }
    })
  });

  assert.equal(failing.status, 400);

  const after = await fetchJson("/admin/references");
  assert.equal(after.status, 200);
  assert.equal(after.body.summary.ordinance_section_count, beforeOrdinanceCount);
  assert.equal(after.body.summary.rules_section_count, beforeRulesCount);
});

test("counts consistency allows parsed-to-committed deltas explained by duplicate merges", async () => {
  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-444", linked_ordinance_sections: ["37.15"], linked_rules_sections: ["Rule 6.13"] }],
      ordinance_sections: [{ section_number: "37.15", body_text: "Body text" }],
      rules_sections: [{ part: "Part 6", section_number: "6.13", body_text: "Body text" }],
      crosswalk: [{ index_code: "IC-444", ordinance_section: "37.15", rules_section: "Rule 6.13", source: "resolved" }],
      coverage_report: {
        ordinance: {
          parser_used: "true_text",
          expected_section_count: 2,
          parsed_section_count: 2,
          duplicate_collisions_avoided: 1,
          low_confidence_sections: 0,
          duplicates_dropped: 1
        },
        rules: {
          parser_used: "true_text",
          expected_section_count: 2,
          parsed_section_count: 2,
          duplicate_collisions_avoided: 1,
          low_confidence_sections: 0,
          duplicates_dropped: 1
        },
        crosswalk: { total_candidates: 1, resolved_links: 1, unresolved_links: 0 }
      }
    })
  });

  const inspect = await fetchJson("/admin/references");
  assert.equal(inspect.status, 200);
  assert.equal(inspect.body.readiness_status.counts_consistent, true);
});

test("crosswalk resolves exact rule citations including 6.14 and 8.12", async () => {
  const rebuild = await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [
        {
          code_identifier: "IC-614",
          linked_ordinance_sections: ["37.2(g)"],
          linked_rules_sections: ["Rule 6.14", "Rule 8.12"]
        }
      ],
      ordinance_sections: [{ section_number: "37.2", subsection_path: "(g)", body_text: "text" }],
      rules_sections: [
        { part: "Part 6", section_number: "6.14", body_text: "text" },
        { part: "Part 8", section_number: "8.12", body_text: "text" }
      ],
      crosswalk: [
        { index_code: "IC-614", ordinance_section: "37.2(g)", source: "resolved" },
        { index_code: "IC-614", rules_section: "Rule 6.14", source: "resolved" },
        { index_code: "IC-614", rules_section: "Rule 8.12", source: "resolved" }
      ],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 2, parsed_section_count: 2, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 3, resolved_links: 3, unresolved_links: 0 }
      }
    })
  });
  assert.equal(rebuild.status, 200);

  const inspect = await fetchJson("/admin/references");
  assert.equal(inspect.status, 200);
  assert.equal(inspect.body.unresolved_crosswalks.length, 0);
  assert.ok(inspect.body.summary.crosswalk_count >= 3);

  const verify = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations: ["6.14", "8.12"] })
  });
  assert.equal(verify.status, 200);
  assert.ok(verify.body.checks.some((row) => row.citation === "6.14" && row.status === "resolved"));
  assert.ok(verify.body.checks.some((row) => row.citation === "8.12" && row.status === "resolved"));
});

test("rules citation inventory endpoint returns exact and prefix-filtered rows", async () => {
  const byExact = await fetchJson("/admin/references/rules?citation=6.14&limit=10");
  assert.equal(byExact.status, 200);
  assert.ok(byExact.body.total >= 1);
  assert.ok(byExact.body.rows.some((row) => row.normalized_citation === "6.14"));

  const byPrefix = await fetchJson("/admin/references/rules?prefix=10.10&limit=20");
  assert.equal(byPrefix.status, 200);
  assert.ok(byPrefix.body.total >= 1);
  assert.ok(byPrefix.body.rows.every((row) => row.normalized_citation.startsWith("10.10")));
});

test("part-prefixed rules citations resolve via bare canonical lookup", async () => {
  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-ROMAN", linked_ordinance_sections: [], linked_rules_sections: ["Rule 1.11"] }],
      ordinance_sections: [],
      rules_sections: [{ part: "I", section_number: "1.11", body_text: "Anniversary Date" }],
      crosswalk: [{ index_code: "IC-ROMAN", rules_section: "Rule 1.11", source: "roman_test" }],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 0, parsed_section_count: 0, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 1, resolved_links: 1, unresolved_links: 0 }
      }
    })
  });

  const inventory = await fetchJson("/admin/references/rules?bare=1.11&limit=5");
  assert.equal(inventory.status, 200);
  assert.ok(inventory.body.rows.some((row) => row.display_citation === "I-1.11" && row.canonical_bare_citation === "1.11"));

  const verify = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations: ["1.11"] })
  });
  assert.equal(verify.status, 200);
  assert.ok(verify.body.checks.some((row) => row.citation === "1.11" && row.status === "resolved"));
});

test("ordinance subsection requires exact match for 37.3(a)(1)", async () => {
  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-373", linked_ordinance_sections: ["37.3(a)(1)"], linked_rules_sections: [] }],
      ordinance_sections: [{ section_number: "37.3", subsection_path: "(a)", body_text: "parent only" }],
      rules_sections: [],
      crosswalk: [{ index_code: "IC-373", ordinance_section: "37.3(a)(1)", source: "test" }],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 0, parsed_section_count: 0, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 1, resolved_links: 0, unresolved_links: 1 }
      }
    })
  });
  const verifyMissing = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations: ["37.3(a)(1)"] })
  });
  assert.equal(verifyMissing.status, 200);
  assert.ok(verifyMissing.body.checks.some((row) => row.citation === "37.3(a)(1)" && row.status === "unresolved"));

  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-373", linked_ordinance_sections: ["37.3(a)(1)"], linked_rules_sections: [] }],
      ordinance_sections: [{ section_number: "37.3", subsection_path: "(a)(1)", body_text: "exact subsection" }],
      rules_sections: [],
      crosswalk: [{ index_code: "IC-373", ordinance_section: "37.3(a)(1)", source: "test" }],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 0, parsed_section_count: 0, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 1, resolved_links: 1, unresolved_links: 0 }
      }
    })
  });
  const verifyExact = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations: ["37.3(a)(1)"] })
  });
  assert.equal(verifyExact.status, 200);
  assert.ok(verifyExact.body.checks.some((row) => row.citation === "37.3(a)(1)" && row.status === "resolved"));
});

test("safe 37.x ordinance-prefixed normalization resolves only approved family", async () => {
  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-104", linked_ordinance_sections: ["37.1", "37.2", "37.8"], linked_rules_sections: ["Rule 6.13"] }],
      ordinance_sections: [
        { section_number: "37.1", body_text: "text" },
        { section_number: "37.2", body_text: "text" },
        { section_number: "37.8", body_text: "text" }
      ],
      rules_sections: [{ part: "VI", section_number: "6.13", body_text: "text" }],
      crosswalk: [],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 3, parsed_section_count: 3, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 0, resolved_links: 0, unresolved_links: 0 }
      }
    })
  });

  const ingest = await fetchJson("/ingest/decision", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      jurisdiction: "City of Beedle",
      title: `Safe 37x Ordinance Mapping ${runTag}`,
      citation: `SAFE-37X-${runTag}`,
      sourceFile: {
        filename: "safe-37x.docx",
        mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        bytesBase64: textFixtureBase64(
          "Index Codes\nIC-104\n\nRules\nRule 37.8\n\nOrdinance\nOrdinance 37.2\nOrdinance 37.8\nOrdinance 37.3\nOrdinance 37.7\nOrdinance 37.9\n\nFindings\nService reduction issues are discussed."
        )
      }
    })
  });
  assert.equal(ingest.status, 201);
  const docId = ingest.body.documentId;

  const detail = await fetchJson(`/admin/ingestion/documents/${docId}`);
  assert.equal(detail.status, 200);
  const issues = detail.body.referenceIssues || [];
  const validOrd = detail.body.validReferences?.ordinanceSections || [];

  assert.ok(validOrd.includes("37.2"), "Ordinance 37.2 should resolve safely");
  assert.ok(validOrd.includes("37.8"), "Ordinance 37.8 should resolve safely");
  assert.ok(!issues.some((row) => row.referenceType === "ordinance_section" && row.normalizedValue === "37.2"));
  assert.ok(!issues.some((row) => row.referenceType === "ordinance_section" && row.normalizedValue === "37.8"));

  assert.ok(issues.some((row) => row.referenceType === "ordinance_section" && row.normalizedValue === "ordinance37.3"));
  assert.ok(issues.some((row) => row.referenceType === "ordinance_section" && row.normalizedValue === "ordinance37.7"));
  assert.ok(issues.some((row) => row.referenceType === "ordinance_section" && row.normalizedValue === "ordinance37.9"));

  assert.ok(
    issues.some((row) => row.referenceType === "rules_section" && row.normalizedValue === "37.8"),
    "Rules-side 37.8 must remain unresolved; no cross-context auto-resolution"
  );
});

test("rules subsection requires exact match for 10.10(c)(3)", async () => {
  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-1010", linked_ordinance_sections: [], linked_rules_sections: ["Rule 10.10(c)(3)"] }],
      ordinance_sections: [],
      rules_sections: [{ part: "X", section_number: "10.10", body_text: "parent rule only" }],
      crosswalk: [{ index_code: "IC-1010", rules_section: "Rule 10.10(c)(3)", source: "test" }],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 0, parsed_section_count: 0, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 1, resolved_links: 0, unresolved_links: 1 }
      }
    })
  });

  const verifyParentOnly = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations: ["10.10(c)(3)"] })
  });
  assert.equal(verifyParentOnly.status, 200);
  assert.ok(
    verifyParentOnly.body.checks.some(
      (row) => row.citation === "10.10(c)(3)" && row.status === "unresolved" && row.diagnostic === "parent_or_related_only"
    )
  );

  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-1010", linked_ordinance_sections: [], linked_rules_sections: ["Rule 10.10(c)(3)"] }],
      ordinance_sections: [],
      rules_sections: [{ part: "X", section_number: "10.10(c)(3)", body_text: "exact subsection rule" }],
      crosswalk: [{ index_code: "IC-1010", rules_section: "Rule 10.10(c)(3)", source: "test" }],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 0, parsed_section_count: 0, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 1, resolved_links: 1, unresolved_links: 0 }
      }
    })
  });

  const verifyExact = await fetchJson("/admin/references/verify-citations", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations: ["10.10(c)(3)"] })
  });
  assert.equal(verifyExact.status, 200);
  assert.ok(
    verifyExact.body.checks.some(
      (row) => row.citation === "10.10(c)(3)" && row.status === "resolved" && row.diagnostic === "exact_match"
    )
  );
});

test("admin references exposes classified critical exceptions and readiness recommendation", async () => {
  await fetchJson("/admin/references/rebuild", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      source_trace: { index_codes: "test:index", ordinance: "test:ordinance", rules: "test:rules" },
      index_codes: [{ code_identifier: "IC-EX", linked_ordinance_sections: ["37.2(g)"], linked_rules_sections: ["Rule 10.10(c)(3)"] }],
      ordinance_sections: [{ section_number: "37.2", subsection_path: "(g)", body_text: "exact ordinance" }],
      rules_sections: [{ part: "X", section_number: "10.10", body_text: "parent rule only" }],
      crosswalk: [
        { index_code: "IC-EX", ordinance_section: "37.2(g)", source: "resolved" },
        { index_code: "IC-EX", rules_section: "Rule 10.10(c)(3)", source: "unresolved" }
      ],
      coverage_report: {
        ordinance: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        rules: { parser_used: "true_text", expected_section_count: 1, parsed_section_count: 1, duplicate_collisions_avoided: 0, low_confidence_sections: 0 },
        crosswalk: { total_candidates: 2, resolved_links: 1, unresolved_links: 1 }
      }
    })
  });
  const inspect = await fetchJson("/admin/references");
  assert.equal(inspect.status, 200);
  assert.ok(["blocked", "safe_for_limited_pilot_import", "safe_for_broader_import"].includes(inspect.body.readiness_status.readiness_recommendation));
  assert.ok(Array.isArray(inspect.body.critical_citation_exceptions));
  assert.ok(inspect.body.critical_citation_exceptions.some((row) => row.citation === "10.10(c)(3)"));
});
