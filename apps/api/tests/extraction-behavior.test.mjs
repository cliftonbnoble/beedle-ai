import test from "node:test";
import assert from "node:assert/strict";
import { zipSync, strToU8 } from "fflate";
import { loadTs } from "./helpers/load-ts.mjs";
import { pdfFixture } from "./helpers/pdf-fixture.mjs";

const { parseDocument, parseMarkdownDocument } = loadTs("src/services/parser.ts");
const { ExtractionReviewRequired } = loadTs("src/services/text-extraction.ts");
const { ingestDocumentBytes } = loadTs("src/services/ingest.ts");
const docx = (xml) => zipSync({ "[Content_Types].xml": strToU8("<Types/>"), "word/document.xml": strToU8(xml) });

test("compressed PDF streams and font mapping produce text plus page coordinates", async () => {
  const result = await parseDocument(pdfFixture({ pages: 2 }), "law_pdf");
  assert.match(result.plainText, /landlord did not prove the alleged breach/);
  assert.doesNotMatch(result.plainText, /endobj|FlateDecode/);
  assert.equal(result.extraction.status, "extracted");
  assert.equal(result.extraction.pages.length, 2);
  assert.equal(result.extraction.pages[0].items[0].y, 720);
  assert.match(result.extraction.parser, /pdfjs\//);
});

test("blank/scanned, corrupt, and oversized PDFs require review instead of binary fallbacks", async () => {
  for (const bytes of [pdfFixture({ blank: true }), pdfFixture({marginOnly:true}), strToU8("%PDF-1.7\n1 0 obj << /Type /Catalog >> endobj"), new Uint8Array(3 * 1024 * 1024 + 1), pdfFixture({ pages: 51 })]) {
    await assert.rejects(parseDocument(bytes, "law_pdf"), ExtractionReviewRequired);
  }
});

test("DOCX uses namespace-aware XML, preserves entities/table text, and rejects malformed XML", async () => {
  const xml = '<d:document xmlns:d="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><d:body><d:tbl><d:tr><d:tc><d:p><d:r><d:t>The tenant &amp; landlord disagree about possession of the dwelling.</d:t><d:tab/><d:t>Review the original evidence.</d:t></d:r></d:p></d:tc></d:tr></d:tbl></d:body></d:document>';
  const result = await parseDocument(docx(xml), "decision_docx");
  assert.match(result.plainText, /tenant & landlord/);
  assert.match(result.plainText, /dwelling\. Review/);
  for (const bad of [xml.replace("</d:p>", ""), '<!DOCTYPE test><d:document/>', '<document>Some arbitrary XML should never be accepted as a Word decision.</document>']) {
    await assert.rejects(parseDocument(docx(bad), "decision_docx"), ExtractionReviewRequired);
  }
  assert.throws(() => parseMarkdownDocument(strToU8("# Empty")), ExtractionReviewRequired);
});

test("failed extraction retains only an unpublished original and never queues vectors", async () => {
  const writes = [];
  const env = {
    R2_PUBLIC_BASE_URL: "https://example.invalid",
    DB: { prepare(sql) { return { bind(...args) { this.args = args; return this; }, async run() { writes.push({ sql, args: this.args }); } }; } },
    SOURCE_BUCKET: { async put() { writes.push({ stored: true }); }, async delete() { throw new Error("unexpected cleanup"); } },
    VECTOR_JOBS_QUEUE: { send() { throw new Error("must not queue"); } }
  };
  const result = await ingestDocumentBytes(env, { fileType: "law_pdf", jurisdiction: "Test", title: "Scanned law", citation: "Test law", sourceFile: { filename: "law.pdf", mimeType: "application/pdf" } }, pdfFixture({ blank: true }));
  assert.equal(result.searchable, false);
  assert.equal(result.extractionStatus, "needs_review");
  assert.equal(result.chunkCount, 0);
  assert.equal(result.vectorJobStatus, "not_requested");
  assert.equal(writes.length, 2);
  assert.match(writes[1].sql, /0, 0, 0, 0, 0, NULL, NULL/);
  assert.equal(JSON.parse(writes[1].args[8]).extraction.status, "needs_review");
});
