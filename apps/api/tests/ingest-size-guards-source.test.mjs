import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const ingestRoutePath = path.resolve(process.cwd(), "src/routes/ingest.ts");
const ingestServicePath = path.resolve(process.cwd(), "src/services/ingest.ts");
const parserPath = path.resolve(process.cwd(), "src/services/parser.ts");

test("ingest upload and parser paths have practical size guards", async () => {
  const route = await fs.readFile(ingestRoutePath, "utf8");
  const ingest = await fs.readFile(ingestServicePath, "utf8");
  const parser = await fs.readFile(parserPath, "utf8");

  assert.match(route, /const maxIngestUploadBytes = 15 \* 1024 \* 1024/);
  assert.match(route, /request\.headers\.get\("content-length"\)/);
  assert.match(route, /file\.size > maxIngestUploadBytes/);
  assert.match(route, /fileBuffer\.byteLength > maxIngestUploadBytes/);
  assert.match(route, /status: 413/);
  assert.match(ingest, /const maxIngestSourceBytes = 15 \* 1024 \* 1024/);
  assert.match(ingest, /raw\.length > maxIngestSourceBytes/);
  assert.match(parser, /const maxDocxDecompressedBytes = 40 \* 1024 \* 1024/);
  assert.match(parser, /Object\.values\(files\)\.reduce/);
  assert.match(parser, /decompressedBytes > maxDocxDecompressedBytes/);
});
