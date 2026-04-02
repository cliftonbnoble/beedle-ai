import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

test("docx fallback cleanup hooks remain in parser source", async () => {
  const parserPath = path.resolve(process.cwd(), "src/services/parser.ts");
  const source = await fs.readFile(parserPath, "utf8");

  assert.match(source, /EMBEDDED_HEADING_SPLITS/);
  assert.match(source, /extractDocxParagraphsFromXml/);
  assert.match(source, /XML-scrubbed UTF-8 fallback/);
  assert.match(source, /scrubDocxArtifacts/);
  assert.match(source, /Recovered .* embedded heading boundaries/);
  assert.match(source, /CAPTION_HEADING_PATTERNS/);
  assert.match(source, /isCaptionHeadingNoise/);
  assert.match(source, /Dropped pure DOCX XML artifact paragraph/);
});
