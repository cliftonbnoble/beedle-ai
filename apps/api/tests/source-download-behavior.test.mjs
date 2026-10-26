import test from "node:test";
import assert from "node:assert/strict";
import { zipSync, strToU8 } from "fflate";
import { loadTs } from "./helpers/load-ts.mjs";

const { detectSourceFormat, sourceContentTypes, sourceDownloadHeaders } = loadTs("src/services/source-format.ts");
const { handleGetSource } = loadTs("src/routes/source.ts");
const { storeSourceFile } = loadTs("src/services/storage.ts");

test("source formats are derived from bytes and explicit format, not an active-content MIME hint", () => {
  assert.equal(detectSourceFormat(strToU8("%PDF-1.7\n"), "law.pdf", "law_pdf"), "pdf");
  assert.equal(detectSourceFormat(strToU8("# Decision\nActual text"), "case.md", "decision_docx"), "markdown");
  const docx = zipSync({ "[Content_Types].xml": strToU8("<Types/>"), "word/document.xml": strToU8("<document/>") });
  assert.equal(detectSourceFormat(docx, "case.docx", "decision_docx"), "docx");
  for (const filename of ["case.docx", "case.pdf", "case.md"]) {
    assert.throws(() => detectSourceFormat(strToU8("<!doctype html><script>alert(1)</script>"), filename, filename.endsWith("pdf") ? "law_pdf" : "decision_docx"));
  }
  assert.throws(() => detectSourceFormat(strToU8("<svg onload='alert(1)'/>"), "case.md", "decision_docx"));
  assert.throws(() => detectSourceFormat(docx, "case.md", "decision_docx"));
  assert.throws(() => detectSourceFormat(zipSync({ "unrelated.txt": strToU8("text") }), "case.docx", "decision_docx"));
});

test("stored sources carry derived MIME and attachment metadata", async () => {
  let options;
  await storeSourceFile({ SOURCE_BUCKET: { put: async (_key, _bytes, value) => { options = value; } } }, "sources/a.pdf", strToU8("%PDF-1.7"), "pdf");
  assert.equal(options.httpMetadata.contentType, sourceContentTypes.pdf);
  assert.match(options.httpMetadata.contentDisposition, /^attachment;/);
  assert.equal(options.customMetadata.sourceFormat, "pdf");
});

function environment(object, paragraphs = []) {
  return {
    DB: { prepare: () => ({ bind() { return this; }, first: async () => ({ title: "Case", citation: "C1", sourceKey: "sources/untrusted.html" }), all: async () => ({ results: paragraphs }) }) },
    SOURCE_BUCKET: { get: async () => object }
  };
}

test("legacy active content downloads as an opaque attachment with execution disabled", async () => {
  const request = Object.assign(new Request("https://api.example/source/id"), { params: { documentId: "id" } });
  const response = await handleGetSource(request, environment({ body: "<script>alert(1)</script>", httpMetadata: { contentType: "text/html" } }));
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-type"), "application/octet-stream");
  assert.match(response.headers.get("content-disposition"), /^attachment;/);
  assert.equal(response.headers.get("x-content-type-options"), "nosniff");
  assert.match(response.headers.get("content-security-policy"), /sandbox/);
  assert.equal(await response.text(), "<script>alert(1)</script>");
});

test("validated sources and reconstructed Markdown remain downloadable", async () => {
  const request = Object.assign(new Request("https://api.example/source/id"), { params: { documentId: "id" } });
  const response = await handleGetSource(request, environment({ body: "%PDF-1.7", customMetadata: { contentValidation: "1", sourceFormat: "pdf" }, httpMetadata: { contentType: "text/html" } }));
  assert.equal(response.headers.get("content-type"), "application/pdf");
  const fallback = await handleGetSource(request, environment(null, [{ heading: "Findings", text: "Source text" }]));
  assert.equal(fallback.headers.get("x-beedle-source-fallback"), "r2-missing-db-text");
  assert.match(fallback.headers.get("content-disposition"), /^attachment;/);
  assert.match(await fallback.text(), /Source text/);
  assert.doesNotMatch(sourceDownloadHeaders('bad"\r\nheader.html').get("content-disposition"), /\r|\n/);
});
