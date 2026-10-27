import test from "node:test";
import assert from "node:assert/strict";
import { zipSync, strToU8, Zip, ZipDeflate } from "fflate";
import { loadTs } from "./helpers/load-ts.mjs";

const { readLimitedFormData } = loadTs("src/lib/multipart.ts");
const { RequestBodyTooLargeError, RequestValidationError, toErrorResponse } = loadTs("src/lib/http.ts");
const { readDocxDocumentXml, inspectDocxArchive, docxLimits } = loadTs("src/services/docx-archive.ts");
const { parseDocument } = loadTs("src/services/parser.ts");

function chunkedRequest(text, chunkSize = 7, extraHeaders = {}) {
  const bytes = strToU8(text);
  const state = { read: 0, cancelled: false };
  const stream = new ReadableStream({
    pull(controller) {
      if (state.read === bytes.length) { controller.close(); return; }
      controller.enqueue(bytes.subarray(state.read, state.read + chunkSize));
      state.read = Math.min(bytes.length, state.read + chunkSize);
    },
    cancel() { state.cancelled = true; }
  }, { highWaterMark: 0 });
  return { state, request: new Request("https://test.invalid/ingest", { method: "POST", duplex: "half", body: stream, headers: { "content-type": "multipart/form-data; boundary=AaB03x", ...extraHeaders } }) };
}
const part = (name, text) => `--AaB03x\r\nContent-Disposition: form-data; name="${name}"\r\n\r\n${text}\r\n`;

test("multipart limit cancels headerless and falsely small declared bodies before consuming the envelope", async () => {
  for (const headers of [{}, { "content-length": "1" }]) {
    const { state, request } = chunkedRequest(part("title", "x".repeat(2000)) + "--AaB03x--\r\n", 7, headers);
    await assert.rejects(readLimitedFormData(request, 128), (error) => {
      assert.equal(toErrorResponse(error).status, 413);
      return error instanceof RequestBodyTooLargeError;
    });
    assert.ok(state.read <= 140, String(state.read));
    assert.equal(state.cancelled, true);
  }
});

test("multipart boundaries split across chunks obey field-count and metadata limits", async () => {
  const { request } = chunkedRequest(part("title", "Case") + "--AaB03x--\r\n", 1);
  assert.equal((await readLimitedFormData(request, 1024)).get("title"), "Case");
  const many = chunkedRequest(Array.from({ length: 20 }, (_, i) => part(`f${i}`, "value")).join("") + "--AaB03x--\r\n");
  await assert.rejects(readLimitedFormData(many.request, 10000, 2), RequestBodyTooLargeError);
  assert.equal(many.state.cancelled, true);
  const metadata = chunkedRequest(part("title", "x".repeat(8193)) + "--AaB03x--\r\n", 1024);
  await assert.rejects(readLimitedFormData(metadata.request, 10000), RequestValidationError);
});

const xml = "<w:document><w:p><w:r><w:t>Actual decision text.</w:t></w:r></w:p></w:document>";
function archive(extra = {}, options) {
  return zipSync({ "[Content_Types].xml": strToU8("<Types/>"), "word/document.xml": strToU8(xml), ...extra }, options);
}

test("DOCX reads required XML, supports stored/deflated entries, and does not expand media", () => {
  for (const level of [0, 6]) assert.equal(readDocxDocumentXml(archive({}, { level })), xml);
  const bytes = archive({ "word/media/image.bin": Uint8Array.from({ length: 1000 }, (_, i) => i % 251) });
  const media = inspectDocxArchive(bytes).get("word/media/image.bin");
  bytes[media.offset] ^= 255; // corrupt unused media must never reach a decompressor
  assert.equal(readDocxDocumentXml(bytes), xml);
});

test("DOCX accepts ZIP data descriptors from a streaming archive writer", () => {
  const chunks = [];
  const zip = new Zip((error, data) => { if (error) throw error; chunks.push(data); });
  for (const [name, text] of [["[Content_Types].xml", "<Types/>"], ["word/document.xml", xml]]) {
    const file = new ZipDeflate(name);
    zip.add(file);
    const bytes = strToU8(text);
    file.push(bytes.subarray(0, 4));
    file.push(bytes.subarray(4), true);
  }
  zip.end();
  assert.equal(readDocxDocumentXml(new Uint8Array(Buffer.concat(chunks))), xml);
});

test("DOCX preflight rejects entry counts, aggregate/entry expansion, ratios, corrupt and unsafe archives", async () => {
  const bytes = archive();
  for (const limits of [
    { ...docxLimits, entries: 1 },
    { ...docxLimits, entryBytes: 40 },
    { ...docxLimits, totalBytes: 40 },
    { ...docxLimits, ratio: 0.5 }
  ]) assert.throws(() => readDocxDocumentXml(bytes, limits), RequestBodyTooLargeError);
  assert.throws(() => readDocxDocumentXml(bytes.subarray(0, bytes.length - 5)), RequestValidationError);
  assert.throws(() => readDocxDocumentXml(archive({ "../escape": strToU8("no") })), RequestValidationError);
  await assert.rejects(parseDocument(strToU8("not a zip file"), "decision_docx"), RequestValidationError);
});

test("forged decompressed sizes and checksum corruption fail instead of being decoded as text", () => {
  const bytes = archive();
  const view = new DataView(bytes.buffer);
  for (let at = 0; at < bytes.length - 46; at++) {
    if (view.getUint32(at, true) !== 0x02014b50) continue;
    const size = view.getUint16(at + 28, true);
    if (new TextDecoder().decode(bytes.subarray(at + 46, at + 46 + size)) !== "word/document.xml") continue;
    const local = view.getUint32(at + 42, true);
    view.setUint32(at + 24, 1, true);
    view.setUint32(local + 22, 1, true);
    break;
  }
  assert.throws(() => readDocxDocumentXml(bytes), RequestBodyTooLargeError);
  const corrupt = archive({}, { level: 0 });
  const entry = inspectDocxArchive(corrupt).get("word/document.xml");
  corrupt[entry.offset + 2] ^= 1;
  assert.throws(() => readDocxDocumentXml(corrupt), /checksum/);
});
