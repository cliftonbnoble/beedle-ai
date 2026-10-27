import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from './helpers/load-ts.mjs';
import { memoryD1 } from './helpers/d1-memory.mjs';
const { RequestTelemetry, telemetryEnvironment, operationalReason, observedTask } = loadTs('src/lib/telemetry.ts');
const { default: worker } = loadTs('src/index.ts');
const { boundedCompletion } = loadTs('src/lib/provider-operation.ts');
const { inspectOperationalHealth } = loadTs('src/services/operational-health.ts');
const { assistantGenerationStatus } = loadTs('../../packages/shared/src/index.ts');
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
function span(records = [], kind = 'http') { return new RequestTelemetry(kind, '/search', 'preview', 1, null, record => records.push(record)); }
const envDefaults = { APP_ENV: 'preview', DEPLOYMENT_READY: 'true', VECTOR_NAMESPACE: 'beedle-docs-preview', AI_EMBEDDING_MODEL: 'fixture', AI_EMBEDDING_DIMENSIONS: '2', TELEMETRY_SUCCESS_SAMPLE_RATE: '1', CORS_ALLOWED_ORIGINS: 'https://web.example', SEARCH_RATE_LIMIT: { async limit() { return { success: true }; } }, REVIEW_READ_RATE_LIMIT: { async limit() { return { success: true }; } } };
test('binding instrumentation preserves D1 transaction/first/raw semantics and only records aggregate metadata', async () => {
    const { DB, sqlite } = memoryD1();
    const events = [], trace = span(events);
    try {
        const env = telemetryEnvironment({ ...envDefaults, DB }, trace);
        const secret = 'PRIVATE-LANDLORD-FACT-NEVER-LOG';
        await env.DB.prepare('INSERT INTO compute_budget_counters(scope,category,minute_bucket,day_bucket,minute_used,day_used,updated_at) VALUES(?,?,0,0,1,1,?)').bind(secret, 'llm', secret).run();
        assert.equal(await env.DB.prepare('SELECT scope FROM compute_budget_counters').first('scope'), secret);
        assert.equal((await env.DB.prepare('SELECT scope FROM compute_budget_counters').raw())[0][0], secret);
        const result = await env.DB.batch([env.DB.prepare('SELECT scope FROM compute_budget_counters'), env.DB.prepare('UPDATE compute_budget_counters SET minute_used=2 RETURNING minute_used')]);
        assert.equal(result[0].results[0].scope, secret);
        await assert.rejects(env.DB.batch([env.DB.prepare('UPDATE compute_budget_counters SET minute_used=3'), env.DB.prepare('INSERT INTO absent VALUES(1)')]));
        assert.equal(await env.DB.prepare('SELECT minute_used FROM compute_budget_counters').first('minute_used'), 2);
        trace.stage('lexical_search', { ms: 4, query: secret, source: secret });
        trace.tag('pipeline', secret);
        trace.finish(200);
        assert.equal(events.length, 1);
        assert.equal(events[0].counters.db_statements, 8);
        assert.ok(events[0].counters.db_meta_missing > 0);
        assert.equal(events[0].operations.d1.failed, 1);
        assert.ok(!JSON.stringify(events).includes(secret));
        assert.equal(events[0].stages.lexical_search, 4);
    }
    finally {
        sqlite.close();
    }
});
test('R2 and provider bindings keep their receivers and stream bodies while child spans unwrap parent wrappers', async () => {
    const rows = [], parent = span(rows), child = span(rows, 'background');
    let bindingCalls = 0;
    const body = new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array([1, 2])); controller.close(); } }), object = { body, size: 2 };
    const bucket = { async get(key) { assert.equal(this, bucket); assert.equal(key, 'PRIVATE-SOURCE'); bindingCalls++; return object; } };
    const ai = { async run() { assert.equal(this, ai); return { data: [[.1, .2]] }; } }, vector = { async query() { assert.equal(this, vector); return { matches: [] }; } };
    const first = telemetryEnvironment({ ...envDefaults, SOURCE_BUCKET: bucket, AI: ai, VECTOR_INDEX: vector }, parent), second = telemetryEnvironment(first, child);
    assert.equal(await second.SOURCE_BUCKET.get('PRIVATE-SOURCE'), object);
    assert.equal(object.body, body);
    assert.equal(body.locked, false);
    await second.AI.run('private-model', { text: ['PRIVATE-FACT'] });
    await second.VECTOR_INDEX.query([.1, .2], { namespace: 'PRIVATE-NAMESPACE' });
    parent.finish(200);
    child.finish(200);
    assert.equal(bindingCalls, 1);
    assert.equal(rows[0].operations.r2_get, undefined);
    assert.equal(rows[1].operations.r2_get.calls, 1);
    assert.equal(rows[1].operations.ai_run.calls, 1);
    assert.ok(!JSON.stringify(rows).includes('PRIVATE'));
});
test('ingress timing includes pre-handler readiness and never accepts client identifiers or URL facts as labels', async () => {
    const records = [], original = console.info;
    console.info = record => records.push(record);
    try {
        const response = await worker.fetch(new Request('https://api.example/admin/operations/health?question=PRIVATE-QUERY', { headers: { origin: 'https://web.example', 'x-request-id': 'PRIVATE-IDENTITY' } }), { ...envDefaults, DB: { prepare() { return { async first() { await wait(35); throw new Error('PRIVATE-UPLOADED-TEXT'); } }; } } }, {});
        assert.equal(response.status, 503);
        assert.match(response.headers.get('x-request-id'), /^[0-9a-f-]{36}$/);
        assert.notEqual(response.headers.get('x-request-id'), 'PRIVATE-IDENTITY');
        assert.ok(Number(response.headers.get('server-timing').match(/dur=(\d+(?:\.\d+)?)/)[1]) >= 30);
        assert.match(response.headers.get('access-control-expose-headers'), /x-request-id/);
        const event = records.find(row => row.event === 'operation_complete');
        assert.equal(event.requestId, response.headers.get('x-request-id'));
        assert.equal(event.route, '/admin/operations/health');
        assert.ok(event.durationMs >= 30);
        assert.ok(!JSON.stringify(records).includes('PRIVATE'));
    }
    finally {
        console.info = original;
    }
});
test('bounded provider errors distinguish 429, malformed bodies and network failures without retaining payloads', async () => {
    const original = globalThis.fetch, events = [];
    try {
        const trace = span(events);
        for (const [response, reason] of [[new Response('PRIVATE-UPSTREAM-ERROR', { status: 429 }), 'provider_rate_limited'], [new Response('PRIVATE-MALFORMED'), 'invalid_response']]) {
            globalThis.fetch = async () => response;
            await assert.rejects(boundedCompletion('https://provider.example', {}, { telemetry: trace }), error => error.reason === reason);
        }
        globalThis.fetch = async () => { throw new Error('PRIVATE-SECRET-URL'); };
        await assert.rejects(boundedCompletion('https://provider.example', {}, { telemetry: trace }), error => error.reason === 'network_failure');
        trace.finish(200);
        assert.equal(events[0].operations.llm.failed, 3);assert.deepEqual(events[0].providerStatuses,{'200':1,'429':1});
        assert.deepEqual(events[0].tags.llm_result, ['provider_rate_limited', 'invalid_response', 'network_failure']);
        assert.ok(!JSON.stringify(events).includes('PRIVATE'));
        assert.equal(operationalReason(new Error('PRIVATE-SOURCE')), 'operation_failed');
    }
    finally {
        globalThis.fetch = original;
    }
});
test('durable-job correlation is stable across deliveries and excludes identifiers and arbitrary error strings', async () => {
    const first = [], second = [];
    const a = span(first, 'queue'), b = span(second, 'queue');
    await a.job('PRIVATE-CASE-ID', 'PRIVATE-GENERATION', 'retryable', 1, 0);
    await b.job('PRIVATE-CASE-ID', 'PRIVATE-GENERATION', 'completed', 2, 4);
    a.finish(200);
    b.finish(200);
    assert.equal(first[0].jobId, second[0].jobId);
    assert.notEqual(a.id, b.id);
    assert.equal(first[0].requestId, a.id);
    assert.equal(second[0].requestId, b.id);
    assert.ok(!JSON.stringify([...first, ...second]).includes('PRIVATE'));
    const failed=[],trace=span(failed),{enqueueVectorJob}=loadTs('src/services/vector-jobs.ts');
    await assert.rejects(enqueueVectorJob({TELEMETRY:trace,DB:{prepare(){return {bind(){return this;},async first(){return {generation_id:'PRIVATE-GENERATION'};}};}},VECTOR_JOBS_QUEUE:{async send(){throw new Error('PRIVATE-QUEUE-ERROR');}}},'PRIVATE-CASE-ID'));
    assert.equal(failed[0].jobId,first[0].jobId);assert.equal(failed[0].outcome,'dispatch_deferred');assert.ok(!JSON.stringify(failed).includes('PRIVATE'));
});
test('health aggregation detects stalled jobs and held index builds without reading job payloads', async () => {
    const { DB, sqlite } = memoryD1();
    try {
        const empty = await inspectOperationalHealth({ DB });
        assert.equal(empty.healthy, true);
        assert.equal(empty.metrics.vector_total, 0);
        sqlite.exec("INSERT INTO maintenance_jobs(id,kind,state,payload_json,dry_run,created_at,updated_at,owner_token,lease_expires_at) VALUES('private-job','reference_backfill','running','{\"private\":\"CASE FACT\"}',0,'2000-01-01','2000-01-01','owner','2000-01-01')");
        const unhealthy = await inspectOperationalHealth({ DB });
        assert.equal(unhealthy.healthy, false);
        assert.equal(unhealthy.metrics.maintenance_expired_leases, 1);
        assert.ok(unhealthy.issues.includes('maintenance_expired_leases'));
        assert.ok(!JSON.stringify(unhealthy).includes('CASE FACT'));
        assert.ok(!JSON.stringify(unhealthy).includes('private-job'));
        sqlite.exec("UPDATE maintenance_jobs SET lease_expires_at='not_a_calendar_timestamp'");
        assert.equal((await inspectOperationalHealth({ DB })).metrics.maintenance_expired_leases, 1);
    }
    finally {
        sqlite.close();
    }
});
test('fallback UI status uses fixed categories and cannot blame a key or echo an arbitrary provider payload', () => {
    assert.match(assistantGenerationStatus('provider_rate_limited'), /rate limited/);
    assert.match(assistantGenerationStatus('unconfigured'), /not configured/);
    assert.match(assistantGenerationStatus('timeout'), /timed out/);
    assert.match(assistantGenerationStatus('no_evidence'), /No usable evidence/);
    assert.ok(!assistantGenerationStatus('PRIVATE-PROVIDER-KEY').includes('PRIVATE'));
    assert.equal(assistantGenerationStatus(null), null);
});
test('success sampling is explicit, failures remain visible and logging/diagnostics faults cannot change a response', () => {
    const records = [];
    const sampled = new RequestTelemetry('http', '/search', 'preview', .1, null, row => records.push(row));
    Object.defineProperty(sampled, 'id', { value: 'ffffffff-ffff-4fff-bfff-ffffffffffff' });
    sampled.finish(200);
    assert.equal(records.length, 0);
    const failed = new RequestTelemetry('http', '/search', 'preview', 0, null, row => records.push(row));
    failed.finish(500);
    assert.equal(records[0].sampleRate, 1);
    const broken = new RequestTelemetry('http', '/search', 'preview', 1, null, () => { throw new Error('PRIVATE-SINK-FAILURE'); });
    broken.workBudget(() => { throw new Error('PRIVATE-BUDGET-FAILURE'); });
    assert.doesNotThrow(() => broken.finish(200));
});
test('background failures retain parent correlation and never log the rejected payload', async () => {
    const { DB, sqlite } = memoryD1(), records = [], original = console.info;
    console.info = record => records.push(record);
    try {
        const parent = span(), env = telemetryEnvironment({ ...envDefaults, DB }, parent);
        await assert.rejects(observedTask(env, 'reviewer_maintenance', async (observed) => { await observed.DB.prepare('SELECT 1 n').first(); throw new Error('PRIVATE-BACKGROUND-FACT'); }), /Background operation failed/);
        const child = records.find(row => row.kind === 'background');
        assert.equal(child.parentId, parent.id);
        assert.equal(child.status, 500);
        assert.ok(!JSON.stringify(records).includes('PRIVATE'));
    }
    finally {
        console.info = original;
        sqlite.close();
    }
});
test('intentional lexical-only capability does not look like a broken vector channel or spend provider work', async () => {
    const { vectorSearchWithDiagnostics } = loadTs('src/services/search-fts.ts');
    let calls = 0;
    const fail = () => { calls++; throw new Error('unexpected work'); };
    const result = await vectorSearchWithDiagnostics({ ...envDefaults, VECTOR_LIFECYCLE_READY: 'false', VECTOR_FILTERS_READY: 'false', DB: { prepare: fail }, AI: { run: fail }, VECTOR_INDEX: { query: fail } }, ['notice'], 5);
    assert.equal(result.vectorErrored, false);
    assert.equal(result.vectorErrorMessage, 'semantic_disabled');
    assert.equal(result.vectorQueryAttempted, false);
    assert.equal(calls, 0);
});

test('release configuration refuses invalid sampling rates before deployment',async()=>{
 const {readFileSync}=await import('node:fs'),{validateDeployment}=await import('../scripts/deploy-environment.mjs');const config=JSON.parse(readFileSync(new URL('../wrangler.json',import.meta.url)));
 for(const value of ['-1','2','0.0000000001','not-a-number']){const bad=structuredClone(config);bad.env.production.vars.TELEMETRY_SUCCESS_SAMPLE_RATE=value;assert.throws(()=>validateDeployment(bad,'production'),/sampling/);}
});
