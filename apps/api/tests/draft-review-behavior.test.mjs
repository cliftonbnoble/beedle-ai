import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { loadTs } from "./helpers/load-ts.mjs";
import { draftReviewFixture } from "./helpers/draft-review-fixture.mjs";
const { exportDraft } = loadTs("src/services/draft-export.ts");
const { captureDraftProvenance } = loadTs("src/services/draft-provenance.ts");

test("Markdown, text, and HTML packages preserve review status, evidence, bibliography, and provenance", async () => {
  const draft = await draftReviewFixture();
  for (const format of ["markdown", "text", "html"]) {
    const result = await exportDraft({ kind: "conclusions", format, conclusions: draft });
    for (const text of ["HUMAN REVIEW REQUIRED", "Jurisdiction and effective edition unverified", "Fixture provider unavailable", "2026-10-26T16:00:00.000Z", "possible_negation_conflict", "unsupported", "rule-test-v1", "revision=v1", "did not retain possession", "excerpt_sha256", "retrieval-only-2"]) {
      // The hash label is intentionally human-readable in document exports.
      assert.ok(result.content.includes(text === "excerpt_sha256" ? draft.provenance.sources[0].excerpt_sha256 : text), `${format} lost ${text}`);
    }
    assert.equal(result.metadata.citation_count, 1);
  }
});

test("source/model markup cannot escape HTML or Markdown evidence containers", async () => {
  const draft = await draftReviewFixture();
  draft.draft_text = "```\n<script>alert(1)</script>\n```";
  draft.citations[0].source_link = "javascript:alert(1)";
  const html = await exportDraft({ kind: "conclusions", format: "html", conclusions: draft });
  assert.doesNotMatch(html.content, /<script>|href="javascript:/);
  assert.match(html.content, /&lt;script&gt;/);
  const markdown = await exportDraft({ kind: "conclusions", format: "markdown", conclusions: draft });
  assert.ok(markdown.content.includes("````text\n```\n<script>"));
});

test("captured excerpt hashes and observed extraction metadata retain honest version status", async () => {
  const draft = await draftReviewFixture();
  const source = { ...draft.supporting_authorities[0], document_id: "decision", source_kind: "similar_decision", source_revision: undefined };
  const env = { DB: { prepare() { return { bind() { return this; }, all: async () => ({ results: [{ id: "decision", revision: "r2", metadata: JSON.stringify({ extraction: { parser: "pdfjs/test" } }) }] }) }; } } };
  const result = await captureDraftProvenance(env, [source]);
  assert.equal(result.sources[0].excerpt_sha256, createHash("sha256").update(source.snippet).digest("hex"));
  assert.equal(result.sources[0].revision_status, "observed_after_retrieval");
  assert.equal(result.sources[0].extraction_version, "pdfjs/test");
  assert.equal(result.corpus_version, null);
});
