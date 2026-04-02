import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
  ADJUDICATION_TEMPLATE_FIELDS,
  analyzeTitleComparison,
  fetchDocumentSnapshots,
  loadAdjudicationInput,
  parseCsvRows,
  simulateAdjudicationRow
} from "../scripts/adjudication-dry-run-utils.mjs";

const execFileAsync = promisify(execFile);

function formatTemplateJson(rows) {
  const shells = rows.map((row) => ({
    documentId: String(row.documentId || ""),
    title: String(row.title || ""),
    batchKey: String(row.batchKey || ""),
    reviewerDecision: "",
    reviewerNotes: "",
    citationActionType: "",
    citationOriginal: "",
    citationReplacement: "",
    confirmMetadata: "",
    escalate: "",
    doNotApprove: "",
    reviewedBy: "",
    reviewedAt: ""
  }));
  return { generatedAt: new Date().toISOString(), filters: { realOnly: true }, rows: shells };
}

function formatTemplateCsv(rows) {
  const header = ADJUDICATION_TEMPLATE_FIELDS.join(",");
  const body = rows
    .map((row) =>
      [
        row.documentId || "",
        row.title || "",
        row.batchKey || "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        ""
      ].join(",")
    )
    .join("\n");
  return `${header}\n${body}\n`;
}

test("CSV parsing works for adjudication template rows", () => {
  const csv = [
    "documentId,title,batchKey,reviewerDecision,citationActionType,citationOriginal,citationReplacement,confirmMetadata,escalate,doNotApprove",
    "doc_1,Doc One,batch-1,confirm_metadata,,,,true,false,false"
  ].join("\n");
  const parsed = parseCsvRows(csv);
  assert.equal(parsed.rows.length, 1);
  assert.equal(parsed.rows[0].documentId, "doc_1");
  assert.equal(parsed.rows[0].reviewerDecision, "confirm_metadata");
});

test("JSON row simulation supports mixed valid/invalid rows", () => {
  const doc = {
    id: "doc_1",
    title: "Doc One",
    unresolvedBuckets: [],
    metadataConfirmationWouldUnlock: true,
    validReferences: { indexCodes: ["13"], rulesSections: ["1.11"], ordinanceSections: ["37.2"] }
  };
  const valid = simulateAdjudicationRow(
    {
      documentId: "doc_1",
      batchKey: "b1",
      reviewerDecision: "confirm_metadata",
      citationActionType: "",
      confirmMetadata: "true",
      escalate: "false",
      doNotApprove: "false"
    },
    doc,
    true
  );
  const invalid = simulateAdjudicationRow(
    {
      documentId: "",
      batchKey: "",
      reviewerDecision: "unknown",
      citationActionType: "bad",
      confirmMetadata: "maybe",
      escalate: "no",
      doNotApprove: "no"
    },
    null,
    true
  );
  assert.equal(valid.outcome, "metadata_confirmation_candidate");
  assert.equal(valid.rowState, "reviewed_confirm_metadata");
  assert.equal(invalid.outcome, "invalid_row");
  assert.equal(invalid.rowState, "invalid_unknown_decision");
});

test("JSON adjudication input parsing works with wrapped rows", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "adj-dry-run-"));
  const file = path.join(dir, "reviewer-adjudication-template.json");
  await fs.writeFile(file, JSON.stringify({ rows: [{ documentId: "doc_1", title: "Doc One", batchKey: "b1" }] }), "utf8");
  const loaded = await loadAdjudicationInput(file);
  assert.equal(loaded.inputFormat, "json");
  assert.equal(loaded.rows.length, 1);
  assert.equal(loaded.rows[0].documentId, "doc_1");
});

test("real export shaped JSON round-trips with non-zero rows", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "adj-dry-run-json-"));
  const file = path.join(dir, "reviewer-adjudication-template.json");
  await fs.writeFile(
    file,
    JSON.stringify({
      generatedAt: new Date().toISOString(),
      filters: { realOnly: true },
      rows: [
        {
          documentId: "doc_1",
          title: "Doc One",
          batchKey: "b1",
          reviewerDecision: "",
          reviewerNotes: "",
          citationActionType: "",
          citationOriginal: "",
          citationReplacement: "",
          confirmMetadata: "",
          escalate: "",
          doNotApprove: "",
          reviewedBy: "",
          reviewedAt: ""
        }
      ]
    }),
    "utf8"
  );
  const loaded = await loadAdjudicationInput(file);
  assert.equal(loaded.rows.length, 1);
  assert.equal(loaded.diagnostics.rowExtractionPathUsed, "rows");
});

test("real export shaped CSV round-trips with non-zero rows", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "adj-dry-run-csv-"));
  const file = path.join(dir, "reviewer-adjudication-template.csv");
  const header = ADJUDICATION_TEMPLATE_FIELDS.join(",");
  const line = ["doc_1", "Doc One", "b1", "", "", "", "", "", "", "", "", "", ""].join(",");
  await fs.writeFile(file, `${header}\n${line}\n`, "utf8");
  const loaded = await loadAdjudicationInput(file);
  assert.equal(loaded.rows.length, 1);
  assert.equal(loaded.diagnostics.rowExtractionPathUsed, "csv_rows");
});

test("header-only template is explicitly surfaced", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "adj-dry-run-header-"));
  const file = path.join(dir, "reviewer-adjudication-template.csv");
  await fs.writeFile(file, `${ADJUDICATION_TEMPLATE_FIELDS.join(",")}\n`, "utf8");
  const loaded = await loadAdjudicationInput(file);
  assert.equal(loaded.rows.length, 0);
  assert.ok(loaded.diagnostics.parseWarnings.includes("template_has_headers_only"));
});

test("file round-trip with real adjudication formatter output (csv/json)", async () => {
  const templateRows = [
    {
      documentId: "doc_1",
      title: "Doc One",
      batchKey: "bk-1"
    }
  ];
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "adj-dry-run-roundtrip-"));
  const csvFile = path.join(dir, "reviewer-adjudication-template.csv");
  const jsonFile = path.join(dir, "reviewer-adjudication-template.json");
  await fs.writeFile(csvFile, formatTemplateCsv(templateRows), "utf8");
  await fs.writeFile(jsonFile, JSON.stringify(formatTemplateJson(templateRows), null, 2), "utf8");
  const csvLoaded = await loadAdjudicationInput(csvFile);
  const jsonLoaded = await loadAdjudicationInput(jsonFile);
  assert.ok(csvLoaded.rows.length > 0);
  assert.ok(jsonLoaded.rows.length > 0);
  assert.equal(csvLoaded.rows[0].documentId, "doc_1");
  assert.equal(jsonLoaded.rows[0].documentId, "doc_1");
});

test("export error payload shape is surfaced explicitly", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "adj-dry-run-error-shape-"));
  const file = path.join(dir, "reviewer-adjudication-template.json");
  await fs.writeFile(file, JSON.stringify({ error: "D1_ERROR: too many SQL variables" }), "utf8");
  const loaded = await loadAdjudicationInput(file);
  assert.equal(loaded.rows.length, 0);
  assert.ok(loaded.diagnostics.parseWarnings.includes("export_error_payload"));
  assert.equal(loaded.diagnostics.rowExtractionPathUsed, "error_payload");
});

test("conflicting flags are detected", () => {
  const doc = { id: "doc_1", title: "Doc One", unresolvedBuckets: [], metadataConfirmationWouldUnlock: false, validReferences: { indexCodes: [], rulesSections: [], ordinanceSections: [] } };
  const row = {
    documentId: "doc_1",
    title: "Doc One",
    batchKey: "b1",
    reviewerDecision: "confirm_metadata",
    citationActionType: "",
    confirmMetadata: "true",
    escalate: "true",
    doNotApprove: "true"
  };
  const simulated = simulateAdjudicationRow(row, doc, true);
  assert.equal(simulated.outcome, "conflicting_flags");
  assert.equal(simulated.rowState, "invalid_conflicting_flags");
});

test("title mismatch is flagged and not remapped", () => {
  const doc = { id: "doc_1", title: "Doc One", unresolvedBuckets: [], metadataConfirmationWouldUnlock: false, validReferences: { indexCodes: [], rulesSections: [], ordinanceSections: [] } };
  const row = {
    documentId: "doc_1",
    title: "Different Title",
    batchKey: "b1",
    reviewerDecision: "no_action",
    citationActionType: "",
    confirmMetadata: "false",
    escalate: "false",
    doNotApprove: "false"
  };
  const simulated = simulateAdjudicationRow(row, doc, true);
  assert.equal(simulated.outcome, "title_mismatch");
  assert.equal(simulated.rowState, "invalid_title_mismatch");
});

test("blocked 37.3/37.7/37.9 remains blocked in simulation", () => {
  const doc = {
    id: "doc_1",
    title: "Doc One",
    unresolvedBuckets: ["unsafe_37x_structural_block"],
    metadataConfirmationWouldUnlock: true,
    validReferences: { indexCodes: ["13"], rulesSections: ["1.11"], ordinanceSections: ["37.2"] }
  };
  const row = {
    documentId: "doc_1",
    title: "Doc One",
    batchKey: "b1",
    reviewerDecision: "citation_fix",
    citationActionType: "replace_citation",
    citationOriginal: "Ordinance 37.3",
    citationReplacement: "37.3",
    confirmMetadata: "false",
    escalate: "false",
    doNotApprove: "false"
  };
  const simulated = simulateAdjudicationRow(row, doc, true);
  assert.equal(simulated.outcome, "blocked_structural");
  assert.equal(simulated.rowState, "reviewed_citation_fix_blocked");
});

test("safe supported normalization candidate is recognized", () => {
  const doc = {
    id: "doc_1",
    title: "Doc One",
    unresolvedBuckets: [],
    metadataConfirmationWouldUnlock: false,
    validReferences: { indexCodes: ["13"], rulesSections: ["1.11"], ordinanceSections: ["37.2"] }
  };
  const row = {
    documentId: "doc_1",
    title: "Doc One",
    batchKey: "b1",
    reviewerDecision: "citation_fix",
    citationActionType: "normalize_format",
    citationOriginal: "Ordinance 37.2",
    citationReplacement: "37.2",
    confirmMetadata: "false",
    escalate: "false",
    doNotApprove: "false"
  };
  const simulated = simulateAdjudicationRow(row, doc, true);
  assert.equal(simulated.outcome, "one_safe_manual_fix_candidate");
  assert.equal(simulated.rowState, "reviewed_citation_fix_supported");
});

test("blank template row is valid-but-unreviewed", () => {
  const doc = {
    id: "doc_1",
    title: "Doc One",
    unresolvedBuckets: [],
    metadataConfirmationWouldUnlock: false,
    validReferences: { indexCodes: ["13"], rulesSections: ["1.11"], ordinanceSections: ["37.2"] }
  };
  const row = {
    documentId: "doc_1",
    title: "Doc One",
    batchKey: "b1",
    reviewerDecision: "",
    reviewerNotes: "",
    citationActionType: "",
    citationOriginal: "",
    citationReplacement: "",
    confirmMetadata: "",
    escalate: "",
    doNotApprove: ""
  };
  const simulated = simulateAdjudicationRow(row, doc, true);
  assert.equal(simulated.rowState, "blank_template_row");
  assert.equal(simulated.safeToApplyInFuture, false);
});

test("blank template row with missing batchKey is non-fatal warning classification", () => {
  const doc = {
    id: "doc_1",
    title: "Doc One",
    unresolvedBuckets: [],
    metadataConfirmationWouldUnlock: false,
    validReferences: { indexCodes: ["13"], rulesSections: ["1.11"], ordinanceSections: ["37.2"] }
  };
  const row = {
    documentId: "doc_1",
    title: "Doc One",
    batchKey: "",
    reviewerDecision: "",
    citationActionType: "",
    citationOriginal: "",
    citationReplacement: "",
    confirmMetadata: "",
    escalate: "",
    doNotApprove: ""
  };
  const simulated = simulateAdjudicationRow(row, doc, true);
  assert.equal(simulated.rowState, "blank_template_row");
  assert.ok(simulated.reasons.includes("missing_batchKey_on_blank_template_row"));
});

test("reviewed row with missing batchKey remains invalid", () => {
  const doc = {
    id: "doc_1",
    title: "Doc One",
    unresolvedBuckets: [],
    metadataConfirmationWouldUnlock: true,
    validReferences: { indexCodes: ["13"], rulesSections: ["1.11"], ordinanceSections: ["37.2"] }
  };
  const row = {
    documentId: "doc_1",
    title: "Doc One",
    batchKey: "",
    reviewerDecision: "confirm_metadata",
    confirmMetadata: "true"
  };
  const simulated = simulateAdjudicationRow(row, doc, true);
  assert.equal(simulated.rowState, "invalid_missing_required_fields");
  assert.ok(simulated.reasons.includes("missing_batchKey_on_reviewed_row"));
});

test("partially reviewed rows classify supported and blocked outcomes", () => {
  const safeDoc = {
    id: "doc_safe",
    title: "Safe Doc",
    unresolvedBuckets: [],
    metadataConfirmationWouldUnlock: true,
    validReferences: { indexCodes: ["13"], rulesSections: ["1.11"], ordinanceSections: ["37.2"] }
  };
  const blockedDoc = {
    id: "doc_blocked",
    title: "Blocked Doc",
    unresolvedBuckets: ["unsafe_37x_structural_block", "cross_context_ambiguous"],
    metadataConfirmationWouldUnlock: false,
    validReferences: { indexCodes: [], rulesSections: [], ordinanceSections: [] }
  };
  const reviewedNoAction = simulateAdjudicationRow(
    { documentId: "doc_safe", title: "Safe Doc", batchKey: "b1", reviewerDecision: "no_action" },
    safeDoc,
    true
  );
  const reviewedConfirm = simulateAdjudicationRow(
    { documentId: "doc_safe", title: "Safe Doc", batchKey: "b1", reviewerDecision: "confirm_metadata", confirmMetadata: "true" },
    safeDoc,
    true
  );
  const reviewedSupportedFix = simulateAdjudicationRow(
    {
      documentId: "doc_safe",
      title: "Safe Doc",
      batchKey: "b1",
      reviewerDecision: "citation_fix",
      citationActionType: "normalize_format",
      citationOriginal: "Ordinance 37.2",
      citationReplacement: "37.2"
    },
    safeDoc,
    true
  );
  const reviewedBlockedFix = simulateAdjudicationRow(
    {
      documentId: "doc_blocked",
      title: "Blocked Doc",
      batchKey: "b1",
      reviewerDecision: "citation_fix",
      citationActionType: "replace_citation",
      citationOriginal: "Ordinance 37.3",
      citationReplacement: "37.3"
    },
    blockedDoc,
    true
  );
  const conflicting = simulateAdjudicationRow(
    {
      documentId: "doc_safe",
      title: "Safe Doc",
      batchKey: "b1",
      reviewerDecision: "confirm_metadata",
      confirmMetadata: "true",
      escalate: "true",
      doNotApprove: "true"
    },
    safeDoc,
    true
  );
  assert.equal(reviewedNoAction.rowState, "reviewed_no_action");
  assert.equal(reviewedConfirm.rowState, "reviewed_confirm_metadata");
  assert.equal(reviewedSupportedFix.rowState, "reviewed_citation_fix_supported");
  assert.equal(reviewedBlockedFix.rowState, "reviewed_citation_fix_blocked");
  assert.equal(conflicting.rowState, "invalid_conflicting_flags");
});

test("document snapshot loader is read-only GET behavior", async () => {
  const calls = [];
  const fetcher = async (endpoint, init) => {
    calls.push({ endpoint, init });
    return { status: 200, body: { id: endpoint.split("/").pop(), title: "Doc" } };
  };
  const map = await fetchDocumentSnapshots(fetcher, ["doc_1", "doc_2"]);
  assert.equal(map.size, 2);
  assert.ok(calls.every((call) => call.init === undefined || call.init?.method === undefined || call.init?.method === "GET"));
});

test("dry-run supports markdown report output", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "adj-dry-run-md-"));
  const inputFile = path.join(dir, "reviewer-adjudication-template.json");
  const reportName = "adjudication-dry-run-report.md";
  await fs.writeFile(inputFile, JSON.stringify([{ documentId: "", title: "Doc Missing", batchKey: "b1", reviewerDecision: "no_action" }]), "utf8");
  await execFileAsync("node", ["./scripts/adjudication-import-dry-run.mjs"], {
    cwd: path.resolve(process.cwd()),
    env: {
      ...process.env,
      API_BASE_URL: "http://127.0.0.1:8787",
      ADJUDICATION_INPUT: inputFile,
      ADJUDICATION_REPORT_NAME: reportName,
      ADJUDICATION_REPORT_FORMAT: "markdown",
      ADJUDICATION_STRICT: "1"
    }
  });
  const reportPath = path.join(process.cwd(), "reports", reportName);
  const markdown = await fs.readFile(reportPath, "utf8");
  assert.ok(markdown.includes("# Adjudication Dry-Run Summary"));
  assert.ok(markdown.includes("## Summary"));
  assert.ok(markdown.includes("## Invalid Rows"));
  assert.ok(markdown.includes("## Blank Template Rows"));
});

test("title diagnostics classify punctuation/case/whitespace/spacing/semantic differences", () => {
  assert.equal(analyzeTitleComparison("Doc Title ", "Doc Title").mismatchReason, "trailing_whitespace");
  assert.equal(analyzeTitleComparison("Doc: Title", "Doc Title").mismatchReason, "punctuation_only_difference");
  assert.equal(analyzeTitleComparison("DOC TITLE", "Doc Title").mismatchReason, "case_only_difference");
  assert.equal(analyzeTitleComparison("Doc   Title", "Doc Title").mismatchReason, "duplicate_spacing");
  assert.equal(analyzeTitleComparison("Tenant Petition Order", "Landlord Petition Order").mismatchReason, "true_semantic_mismatch");
});

test("canonical title hints appear only when enabled", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "adj-dry-run-hints-"));
  const inputFile = path.join(dir, "reviewer-adjudication-template.json");
  const reportNameOff = "adjudication-dry-run-hints-off.json";
  const reportNameOn = "adjudication-dry-run-hints-on.json";
  await fs.writeFile(inputFile, JSON.stringify([{ documentId: "", title: "Doc Missing", batchKey: "b1", reviewerDecision: "no_action" }]), "utf8");

  await execFileAsync("node", ["./scripts/adjudication-import-dry-run.mjs"], {
    cwd: path.resolve(process.cwd()),
    env: {
      ...process.env,
      API_BASE_URL: "http://127.0.0.1:8787",
      ADJUDICATION_INPUT: inputFile,
      ADJUDICATION_REPORT_NAME: reportNameOff,
      ADJUDICATION_STRICT: "1"
    }
  });
  await execFileAsync("node", ["./scripts/adjudication-import-dry-run.mjs"], {
    cwd: path.resolve(process.cwd()),
    env: {
      ...process.env,
      API_BASE_URL: "http://127.0.0.1:8787",
      ADJUDICATION_INPUT: inputFile,
      ADJUDICATION_REPORT_NAME: reportNameOn,
      ADJUDICATION_STRICT: "1",
      ADJUDICATION_INCLUDE_CANONICAL_TITLE_HINTS: "1"
    }
  });
  const offReport = JSON.parse(await fs.readFile(path.join(process.cwd(), "reports", reportNameOff), "utf8"));
  const onReport = JSON.parse(await fs.readFile(path.join(process.cwd(), "reports", reportNameOn), "utf8"));
  assert.equal(offReport.includeCanonicalTitleHints, false);
  assert.equal(onReport.includeCanonicalTitleHints, true);
});
