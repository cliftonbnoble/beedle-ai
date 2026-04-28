import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

test("markdown ingest hooks remain wired in parser and ingest service", async () => {
  const parserPath = path.resolve(process.cwd(), "src/services/parser.ts");
  const ingestPath = path.resolve(process.cwd(), "src/services/ingest.ts");
  const parserSource = await fs.readFile(parserPath, "utf8");
  const ingestSource = await fs.readFile(ingestPath, "utf8");

  assert.match(parserSource, /extractMarkdownParagraphs/);
  assert.match(parserSource, /parseMarkdownDocument/);
  assert.match(parserSource, /isMarkdownTableDivider/);
  assert.match(ingestSource, /isMarkdownSourceFile/);
  assert.match(ingestSource, /parseMarkdownDocument\(bytes\)/);
});
