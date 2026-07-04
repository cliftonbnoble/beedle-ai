import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const read = (file) => fs.readFile(path.resolve(process.cwd(), file), "utf8");

// NS-22: bge-base-en-v1.5 is asymmetric (s2p). QUERIES must carry the instruction prefix; PASSAGES
// must embed raw — a prefixed passage or a raw query silently degrades retrieval quality with no
// error anywhere. These pins keep the two sides from drifting.
test("query-side embeds carry the bge instruction prefix; passage-side embeds stay raw (NS-22)", async () => {
  const embeddings = await read("src/services/embeddings.ts");
  assert.match(embeddings, /BGE_QUERY_INSTRUCTION = "Represent this sentence for searching relevant passages: "/);
  assert.match(embeddings, /options\?\.isQuery \? `\$\{BGE_QUERY_INSTRUCTION\}\$\{input\}` : input/);

  const searchFts = await read("src/services/search-fts.ts");
  assert.match(searchFts, /embed\(env, query, \{ isQuery: true \}\)/, "search query embeds must set isQuery");

  const ingest = await read("src/services/ingest.ts");
  assert.match(ingest, /embed\(env, chunk\.chunkText\)(?!, \{)/, "ingest passage embeds must stay raw");
  const backfill = await read("src/services/retrieval-vector-backfill.ts");
  assert.match(backfill, /embed\(env, row\.sourceText\)(?!, \{)/, "backfill passage embeds must stay raw");
  const activation = await read("src/services/retrieval-activation.ts");
  assert.match(activation, /embed\(env, embeddingRow\.sourceText\)(?!, \{)/, "activation passage embeds must stay raw");

  const probe = await read("src/services/retrieval-vector-probe.ts");
  assert.match(probe, /isQuery: Boolean\(parsed\.queryText\)/, "probe prefixes only explicit query text");
});

// NS-27: the namespace-less Vectorize retry may only fire when the error is actually about the
// namespace option — any other error must be recorded and surfaced (vectorErrored), never silently
// retried across namespaces or collapsed into "no semantic matches".
test("vector failures are visible and the namespace fallback is gated (NS-27)", async () => {
  const searchFts = await read("src/services/search-fts.ts");
  assert.match(searchFts, /if \(!\/namespace\/i\.test\(message\)\) \{\s*recordError\(error\);\s*return \[\];/);
  assert.match(searchFts, /vectorErrored: boolean;/);
  assert.match(searchFts, /\[search-vector\] query-channel error/);

  const shared = await read("../../packages/shared/src/index.ts");
  assert.match(shared, /vectorErrored: z\.boolean\(\)\.default\(false\)/);
  assert.match(shared, /vectorErrorMessage: z\.string\(\)\.default\(""\)/);
});
