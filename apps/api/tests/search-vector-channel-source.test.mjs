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

// NS-10/NS-21: vector recall depth. The vector-first check must precede the <=2-token skip (bare
// "harassment" is DESIGNED to lean on semantic recall); topK ceiling is Vectorize's 100 with a
// floor of 40 (metadata is never requested — the merge reads only id+score); vectorSearchLimit
// floors at 50 so page size cannot shrink semantic recall.
test("vector recall depth: skip-order, topK ceiling/floor, no metadata (NS-10/NS-21)", async () => {
  const scoring = await read("src/services/search-scoring.ts");
  const skipBody = scoring.slice(scoring.indexOf("export function shouldSkipVectorSearch"), scoring.indexOf("export function enhanceQueryWithIndexCodeContext"));
  const vectorFirstIdx = skipBody.indexOf("NORMALIZED_VECTOR_FIRST_ISSUE_TERMS.some");
  const twoTokenSkipIdx = skipBody.indexOf("if (tokenCount <= 2) return true;");
  assert.ok(vectorFirstIdx > -1 && twoTokenSkipIdx > -1 && vectorFirstIdx < twoTokenSkipIdx, "vector-first check must precede the <=2-token skip");

  const searchFts = await read("src/services/search-fts.ts");
  assert.match(searchFts, /const topK = Math\.min\(100, Math\.max\(limit \* 2, 40\)\)/);
  assert.doesNotMatch(searchFts, /returnMetadata/, "vector queries must not request metadata (caps topK)");

  const analysis = await read("src/services/search-query-analysis.ts");
  assert.match(analysis, /Math\.max\(hasCombinedStructuredFilters \? pageWindow \* 2 : pageWindow, 50\)/);
});

// NS-16: the vector fusion term uses the FIXED affine calibration (bge band 0.55-0.95 spread across
// [0,1]) — never per-result-set normalization (pagination-unstable) and never applied to the raw
// scores that guard thresholds read. Zero must stay zero so inert-vector environments are unchanged.
test("vector fusion uses fixed calibration; guards keep raw cosines (NS-16)", async () => {
  const scoring = await read("src/services/search-scoring.ts");
  assert.match(scoring, /function calibratedVectorFusionScore\(rawCosine: number\): number \{\s*if \(rawCosine <= 0\) return 0;\s*const calibrated = \(rawCosine - 0\.55\) \/ 0\.35;\s*return Math\.max\(0, Math\.min\(1, calibrated\)\);/);
  assert.match(scoring, /calibratedVectorFusionScore\(vectorScore\) \* 0\.23/);
  assert.doesNotMatch(scoring, /vectorScore \* 0\.23/, "the raw cosine must not feed the fusion weight directly");
  // Guard thresholds keep reading the raw score.
  assert.match(scoring, /diagnostics\.vectorScore < 0\.72/);
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
