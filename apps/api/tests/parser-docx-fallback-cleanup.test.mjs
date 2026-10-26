import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

test("binary parsing has no text fallback and retains heading cleanup", async () => {
  const source = await readFile(new URL("../src/services/parser.ts", import.meta.url), "utf8");
  assert.match(source, /EMBEDDED_HEADING_SPLITS/);
  assert.match(source, /extractDocxText/);
  assert.match(source, /extractPdfText/);
  assert.doesNotMatch(source, /XML-scrubbed UTF-8 fallback|extractDocxParagraphsFromXml|extractPdfParagraphs/);
});
