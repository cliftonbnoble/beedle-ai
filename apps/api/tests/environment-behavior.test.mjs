import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { loadTs } from './helpers/load-ts.mjs';
import { memoryD1 } from './helpers/d1-memory.mjs';
import { validateDeployment } from '../scripts/deploy-environment.mjs';
const { deploymentEnvironment } = loadTs('src/lib/deployment.ts');
const { ingestDocumentBytes } = loadTs('src/services/ingest.ts');
const { backfillRetrievalVectors } = loadTs('src/services/retrieval-vector-backfill.ts');
const { processDocumentVectorJob } = loadTs('src/services/vector-jobs.ts');
const config = JSON.parse(readFileSync(new URL('../wrangler.json', import.meta.url)));

test('default config has only local identities; production resources are explicitly selected and preserved', () => {
  assert.equal(config.vars.APP_ENV, 'local');
  assert.equal(config.ai, undefined); assert.equal(config.vectorize, undefined);
  assert.doesNotMatch(JSON.stringify({...config, env: undefined}), /remote|f7e4fa53|clifton23|openrouter/);
  assert.equal(config.workers_dev, false);
  assert.throws(() => validateDeployment(config), /explicitly/);
  assert.equal(validateDeployment(config, 'preview').name, 'beedle-api-preview');
  const unconfigured = structuredClone(config);
  unconfigured.env.preview.vars.DEPLOYMENT_READY = 'false';
  unconfigured.env.preview.d1_databases[0].database_id = 'REPLACE_WITH_PREVIEW_DATABASE_ID';
  assert.throws(() => validateDeployment(unconfigured, 'preview'), /Complete/);
  const production = validateDeployment(config, 'production');
  assert.equal(production.name, 'beedle-api');
  assert.equal(production.d1_databases[0].database_id, 'f7e4fa53-4d05-474f-bf2c-d08a9f6139d5');
  assert.equal(production.r2_buckets[0].bucket_name, 'beedle-sources');
  assert.equal(production.vectorize[0].index_name, 'beedle-docs');
  assert.equal(production.vars.VECTOR_NAMESPACE, 'beedle-docs');
  assert.equal(production.queues.consumers[0].queue, 'beedle-vector-jobs');
});
test('preview deployment rejects shared database, queue, vector and rate-limit identities', () => {
  const ready = structuredClone(config);
  ready.env.preview.vars.DEPLOYMENT_READY = 'true';
  ready.env.preview.vars.CORS_ALLOWED_ORIGINS = 'https://preview.example.com';
  ready.env.preview.vars.SOURCE_PROXY_BASE_URL = 'https://preview-api.example.com';
  ready.env.preview.d1_databases[0].database_id = '00000000-0000-0000-0000-000000000002';
  assert.equal(validateDeployment(ready, 'preview').name, 'beedle-api-preview');
  for (const key of ['d1_databases', 'r2_buckets', 'vectorize', 'queues', 'ratelimits']) {
    const bad = structuredClone(ready); bad.env.preview[key] = bad.env.production[key];
    assert.throws(() => validateDeployment(bad, 'preview'), /independent/);
  }
});
test('missing, unconfigured or mixed runtime selection fails closed', () => {
  for (const env of [{}, {APP_ENV:'preview', DEPLOYMENT_READY:'false'}, {APP_ENV:'local', DEPLOYMENT_READY:'true', VECTOR_NAMESPACE:'beedle-docs'}]) {
    assert.throws(() => deploymentEnvironment(env), /environment/);
  }
});
test('local ingestion, queue processing and backfill cannot invoke accidentally supplied remote capabilities', async () => {
  const {DB, sqlite} = memoryD1(); const objects = new Map(); let remoteCalls = 0; let queueSends = 0;
  const remote = async () => { remoteCalls++; throw new Error('production capability invoked'); };
  const input = {...config.vars, DB, LLM_API_KEY:'synthetic-secret', AI:{run:remote}, VECTOR_INDEX:{upsert:remote,query:remote},
    VECTOR_JOBS_QUEUE:{async send(){queueSends++;}}, SOURCE_BUCKET:{
      async put(key,bytes){objects.set(key,new Uint8Array(bytes));},
      async get(key){const bytes=objects.get(key); return bytes?{size:bytes.length,arrayBuffer:async()=>bytes.slice().buffer}:null;},
      async delete(key){objects.delete(key);}
    }};
  try {
    const env=deploymentEnvironment(input);
    assert.equal(env.LLM_API_KEY,undefined);assert.equal(env.AI,undefined);assert.equal(env.VECTOR_INDEX,undefined);
    const result=await ingestDocumentBytes(env,{title:'Local isolation',citation:'TEST',jurisdiction:'Test',performVectorUpsert:true,fileType:'decision_docx',sourceFile:{filename:'local.md',mimeType:'text/markdown'}},new TextEncoder().encode('# Findings of Fact\n\nThe tenant presented evidence that the landlord received notice of repairs. '.repeat(15)));
    assert.ok(objects.size>0);assert.ok(result.chunkCount>0);
    await processDocumentVectorJob(env,result.documentId);
    sqlite.prepare('UPDATE documents SET searchable_at=? WHERE id=?').run(new Date().toISOString(),result.documentId);
    const backfill=await backfillRetrievalVectors(env,{limit:5});
    const {stepMaintenanceJob}=loadTs('src/services/maintenance-jobs.ts');
    const advanced=await stepMaintenanceJob(env,backfill.jobId);
    assert.equal(advanced.counts.completed,0);assert.equal(advanced.state,'retryable');
    assert.equal(remoteCalls,0);assert.ok(queueSends<=1);
    assert.ok(input.AI); // sanitization does not mutate the original bindings
  } finally {sqlite.close();}
});

test('deployment selection agrees with runtime namespace and compares canonical URL origins',()=>{
  const ready=structuredClone(config); const preview=ready.env.preview;
  preview.vars.DEPLOYMENT_READY='true'; preview.d1_databases[0].database_id='00000000-0000-0000-0000-000000000002';
  preview.vars.CORS_ALLOWED_ORIGINS='https://preview.example.com';preview.vars.SOURCE_PROXY_BASE_URL='https://preview-api.example.com';
  preview.vars.VECTOR_NAMESPACE='unconfigured-preview-namespace';assert.throws(()=>validateDeployment(ready,'preview'));
  preview.vars.VECTOR_NAMESPACE='beedle-docs-preview';
  for(const sourceUrl of [config.env.production.vars.SOURCE_PROXY_BASE_URL+'/', config.env.production.vars.SOURCE_PROXY_BASE_URL+'.']){
    preview.vars.SOURCE_PROXY_BASE_URL=sourceUrl;assert.throws(()=>validateDeployment(ready,'preview'),/independent/);
  }
  preview.vars.SOURCE_PROXY_BASE_URL='https://preview-api.example.com';
  preview.vars.CORS_ALLOWED_ORIGINS=config.env.production.vars.CORS_ALLOWED_ORIGINS+'/';assert.throws(()=>validateDeployment(ready,'preview'),/independent/);
});
