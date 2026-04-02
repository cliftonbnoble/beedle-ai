import { test } from "node:test";
import assert from "node:assert/strict";
import { chooseParserPath, choosePreferredSource, citationMatch, normalizeSectionRef } from "../scripts/reference-normalization-utils.mjs";

test("fallback parser selection chooses text export when pdf coverage is below threshold", () => {
  const selected = chooseParserPath({ pdfCount: 4, textCount: 35, minThreshold: 15 });
  assert.equal(selected, "text_export");
});

test("fallback parser selection keeps pdf path when text export is absent", () => {
  const selected = chooseParserPath({ pdfCount: 18, textCount: undefined, minThreshold: 15 });
  assert.equal(selected, "pdf");
});

test("citation normalization preserves subsection identity", () => {
  assert.equal(normalizeSectionRef("37.3(a)(1)"), "37.3(a)(1)");
  assert.equal(normalizeSectionRef("Rule 10.10(c)(3)"), "10.10(c)(3)");
  assert.equal(normalizeSectionRef("Section 6.13"), "6.13");
});

test("source priority prefers true text, then layout, then pdf with deterministic fallback", () => {
  const selected = choosePreferredSource({
    trueText: { source_path: "true.txt", section_count: 2 },
    layoutText: { source_path: "layout.txt", section_count: 20 },
    pdf: { source_path: "doc.pdf", section_count: 5 },
    minThreshold: 15
  });
  assert.equal(selected.source_type, "layout_text");
});

test("citation matching is subsection-aware and avoids malformed supersets", () => {
  assert.equal(citationMatch("37.15", "37.15"), true);
  assert.equal(citationMatch("37.15", "37.15(a)"), true);
  assert.equal(citationMatch("37.3(a)(1)", "37.3"), false);
  assert.equal(citationMatch("37.15", "37.1(5)"), false);
  assert.equal(citationMatch("37.15", "37.150"), false);
  assert.equal(citationMatch("10.10(c)(3)", "10.10(c)(3)"), true);
});
