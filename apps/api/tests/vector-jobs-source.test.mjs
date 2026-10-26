import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

test("ingestion queues durable vector jobs instead of embedding chunks in the request", async () => {
  const root = process.cwd();
  const [ingest, jobs, worker, config, migration] = await Promise.all([
    fs.readFile(path.resolve(root, "src/services/ingest.ts"), "utf8"),
    fs.readFile(path.resolve(root, "src/services/vector-jobs.ts"), "utf8"),
    fs.readFile(path.resolve(root, "src/index.ts"), "utf8"),
    fs.readFile(path.resolve(root, "wrangler.toml"), "utf8"),
    fs.readFile(path.resolve(root, "migrations/0011_document_vector_jobs.sql"), "utf8")
  ]);

  assert.match(ingest, /vectorOutboxStatement/);
  assert.match(ingest, /await enqueueVectorJob\(env, documentId\)/);
  assert.doesNotMatch(ingest, /await embed\(env, chunk\.chunkText\)/);
  assert.match(jobs, /const embeddingConcurrency = 4/);
  assert.match(jobs, /Promise.allSettled/);
  assert.match(jobs, /owner_token=\?/);
  assert.match(worker, /queue: async \(batch: MessageBatch<VectorJobMessage>, env: Env\)/);
  assert.match(worker, /consumeVectorJobBatch\(batch, env\)/);
  assert.doesNotMatch(worker, /message\.attempts/);
  assert.match(config, /binding = "VECTOR_JOBS_QUEUE"/);
  assert.match(config, /max_concurrency = 2/);
  assert.match(migration, /CREATE TABLE IF NOT EXISTS document_vector_jobs/);
});
