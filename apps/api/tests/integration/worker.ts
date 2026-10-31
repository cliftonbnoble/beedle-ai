// Test-only entrypoint. Production wrangler.json continues to use src/index.ts.
import worker from '../../src/index';
import { requeueStaleVectorJobs } from '../../src/services/vector-jobs';
import { advanceMigrationBackfill } from '../../src/migration-backfill';
import { readyDocumentSql } from '../../src/services/publication-policy';
import { sha256Bytes } from '../../src/lib/digests';
import type { Env } from '../../src/lib/types';
type FixtureEnv = Env & {
    FIXTURE_VECTORS: R2Bucket;
    FIXTURE_PROVIDER_URL: string;
};
const nativeFetch = globalThis.fetch;
globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);
    if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || !url.pathname.startsWith('/fixture/'))
        throw new Error('External network is disabled in the integration fixture');
    return nativeFetch(input, init);
}) as typeof fetch;
async function takeFault(env: FixtureEnv, key: string) { return Boolean(await env.DB.prepare('DELETE FROM fixture_faults WHERE key=? RETURNING key').bind(key).first()); }
const json = (value: unknown, status = 200) => Response.json(value, { status });
function matches(metadata: Record<string, unknown>, filter: Record<string, unknown> = {}) {
    return Object.entries(filter).every(([key, value]) => value && typeof value === 'object' ? Object.entries(value).every(([op, target]) => op === '$in' ? Array.isArray(target) && target.includes(metadata[key]) : op === '$gte' ? String(metadata[key]) >= String(target) : op === '$lte' ? String(metadata[key]) <= String(target) : false) : metadata[key] === value);
}
function fixtureEnvironment(env: FixtureEnv, path: string): Env {
    const DB = new Proxy(env.DB, { get(target, key) {
            if (key === 'batch')
                return async (statements: D1PreparedStatement[]) => {
                    const fail = await takeFault(env, 'batch:' + path), lost = await takeFault(env, 'lost_ack:' + path);
                    const result = await target.batch(fail ? [...statements, target.prepare('INSERT INTO fixture_missing_table VALUES (1)')] : statements);
                    let lostPublication=false;
                    if(path.startsWith('/ingest/'))for(const part of result)for(const row of part.results){
                      if(row&&typeof row==='object'&&'id' in row&&typeof row.id==='string'&&await env.DB.prepare("SELECT 1 FROM ingestion_requests WHERE document_id=? AND state='complete'").bind(row.id).first())lostPublication=await takeFault(env,'lost_publication_ack');
                    }
                    if (lost||lostPublication)
                        throw new Error('Synthetic lost transaction acknowledgment');
                    return result;
                };
            const value = Reflect.get(target, key);
            return typeof value === 'function' ? value.bind(target) : value;
        } });
    const SOURCE_BUCKET = new Proxy(env.SOURCE_BUCKET, { get(target, key) { if (key === 'put')
            return async (...args: Parameters<R2Bucket['put']>) => { if (await takeFault(env, 'source_put'))
                throw new Error('Synthetic source persistence failure'); return target.put(...args); }; const value = Reflect.get(target, key); return typeof value === 'function' ? value.bind(target) : value; } });
    const VECTOR_INDEX = {
        async upsert(rows: VectorizeVector[]) { if (path === 'queue' && await takeFault(env, 'vector_upsert'))
            throw new Error('Synthetic vector provider failure'); await Promise.all(rows.map(row => env.FIXTURE_VECTORS.put('vectors/' + row.id, JSON.stringify(row)))); return { mutationId: 'synthetic' }; },
        async getByIds(ids: string[]) { return (await Promise.all(ids.map(async (id) => { const row = await env.FIXTURE_VECTORS.get('vectors/' + id); return row ? await row.json<VectorizeVector>() : null; }))).filter(Boolean); },
        async deleteByIds(ids: string[]) { await env.FIXTURE_VECTORS.delete(ids.map(id => 'vectors/' + id)); return { mutationId: 'synthetic' }; },
        async query(_vector: number[], options: VectorizeQueryOptions = {}) { const listing = await env.FIXTURE_VECTORS.list({ prefix: 'vectors/', limit: 200 }); if (listing.truncated)
            throw new Error('Fixture vector bound exceeded'); const rows = (await Promise.all(listing.objects.map(async (row) => (await env.FIXTURE_VECTORS.get(row.key))!.json<VectorizeVector>()))).filter(row => row.namespace === options.namespace && matches(row.metadata || {}, options.filter || {})); return { matches: rows.slice(0, options.topK || 10).map(row => ({ id: row.id, score: 0.7 })), count: rows.length }; }
    };
    return { ...env, DB, SOURCE_BUCKET, VECTOR_INDEX: VECTOR_INDEX as unknown as VectorizeIndex, AI: { async run(_model: string, input: {
                text: string[];
            }) { return { data: input.text.map(() => [0.3, 0.7]) }; } } as unknown as Ai };
}
async function observedQueue(batch: MessageBatch, env: FixtureEnv) {
    const messages = batch.messages.map(message => ({ id: message.id, timestamp: message.timestamp, body: message.body, attempts: message.attempts,
        ack() { message.ack(); events.push({ kind: 'queue_ack', body: message.body }); }, retry(options?: QueueRetryOptions) { message.retry(options); events.push({ kind: 'queue_retry', body: message.body, options }); } }));
    const events: Array<{
        kind: string;
        body: unknown;
        options?: QueueRetryOptions;
    }> = [];
    for (const message of messages)
        await env.DB.prepare('INSERT INTO fixture_events(kind,payload_json) VALUES(?,?)').bind('queue_delivery', JSON.stringify({ body: message.body })).run();
    try {
        await worker.queue({ queue: batch.queue, messages, ackAll: () => batch.ackAll(), retryAll: options => batch.retryAll(options) } as Parameters<typeof worker.queue>[0], fixtureEnvironment(env, 'queue'));
    }
    finally {
        for (const event of events)
            await env.DB.prepare('INSERT INTO fixture_events(kind,payload_json) VALUES(?,?)').bind(event.kind, JSON.stringify(event)).run();
    }
}
export default {
    async fetch(request: Request, env: FixtureEnv, ctx: ExecutionContext) {
        if (env.APP_ENV !== 'preview')
            return json({ error: 'Fixture requires isolated preview configuration' }, 503);
        const path = new URL(request.url).pathname;
        if (path === '/__test/ready')
            return json({ fixture: 'beedle-hermetic-v1' });
        if (path === '/__test/migration-tick' && request.method === 'POST') {
            const identity = await request.json() as { runId: string; target: string; bundle: string };
            try {
                return json(await advanceMigrationBackfill({ DB: env.DB, MIGRATION_ENABLED: 'true', MIGRATION_RUN_ID: identity.runId, MIGRATION_TARGET_SHA256: identity.target, MIGRATION_BUNDLE_SHA256: identity.bundle }));
            } catch {
                return json({ error: 'Migration guard rejected' }, 503);
            }
        }
        if (path === '/__test/control' && request.method === 'POST') {
            const body = await request.json() as {
                fault?: string;
                mode?: string;
                seedIds?: Record<string, string>;
            };
            if (body.seedIds) {
                if (Object.keys(body.seedIds).length > 8 || Object.values(body.seedIds).some(id => typeof id !== "string" || id.length > 200))
                    return json({ error: "Invalid seed identities" }, 400);
                await env.DB.prepare("INSERT OR REPLACE INTO fixture_faults(key,value) VALUES('seeded',?)").bind(JSON.stringify(body.seedIds)).run();
            }
            if (body.fault) {
                if (body.fault.length > 250)
                    return json({ error: 'Invalid fault' }, 400);
                await env.DB.prepare('INSERT OR REPLACE INTO fixture_faults(key) VALUES(?)').bind(body.fault).run();
            }
            if (body.mode) {
                if (!['valid', 'unknown_id', 'malformed', 'oversized', 'rate_limited'].includes(body.mode))
                    return json({ error: 'Invalid provider mode' }, 400);
                await nativeFetch(env.FIXTURE_PROVIDER_URL + '/control', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ mode: body.mode }) });
            }
            return json({ ok: true });
        }
        if (path === '/__test/seeded') {
            const row = await env.DB.prepare("SELECT value FROM fixture_faults WHERE key='seeded'").first<{
                value: string;
            }>();
            return row ? json({ fixture: 'beedle-hermetic-v1', seedIds: JSON.parse(row.value) }) : json({ error: 'Seeding incomplete' }, 503);
        }
        if(path==='/__test/legacy-date'&&request.method==='POST'){
          const body=await request.json() as {documentId?:unknown};
          if(typeof body.documentId!=='string'||body.documentId.length>200)return json({error:'Invalid fixture document'},400);
          await env.DB.prepare("UPDATE documents SET decision_date='2024-02-30' WHERE id=?").bind(body.documentId).run();
          return json({ok:true});
        }
        if (path === '/__test/taxonomy' && request.method === 'POST') {
            const body = await request.json() as {
                documentId: string;
                fallback: boolean;
                confidence: number;
            };
            if (typeof body.documentId !== 'string' || body.documentId.length > 200 || typeof body.fallback !== 'boolean' || typeof body.confidence !== 'number' || body.confidence < 0 || body.confidence > 1)
                return json({ error: 'Invalid fixture taxonomy' }, 400);
            await env.DB.prepare("UPDATE documents SET metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.taxonomy',json(?)) WHERE id=?").bind(JSON.stringify({ caseTypeId:'zoning_variance',caseTypeLabel:'Zoning Variance',fallback: body.fallback,confidence:body.confidence,inferenceVersion:'taxonomy-evidence-2',configVersion:'1.1.0',reviewStatus:'reviewed_for_inference' }), body.documentId).run();
            return json({ ok: true });
        }
        if (path === '/__test/publish-provisional' && request.method === 'POST') {
            const body = await request.json() as { documentId?: unknown };
            if (typeof body.documentId !== 'string' || body.documentId.length > 200) return json({ error: 'Invalid fixture document' }, 400);
            const row = await env.DB.prepare(`SELECT source_r2_key AS sourceKey,json_extract(metadata_json,'$.sourceSha256') AS sourceSha
              FROM documents d WHERE id=? AND ${readyDocumentSql()} AND approved_at IS NULL AND qc_required_confirmed=0`)
                .bind(body.documentId).first<{ sourceKey: string; sourceSha: string }>();
            const source = row ? await env.SOURCE_BUCKET.get(row.sourceKey) : null;
            if (!row || !source || await sha256Bytes(new Uint8Array(await source.arrayBuffer())) !== row.sourceSha) return json({ error: 'Fixture source is not verified and ready' }, 409);
            const published = await env.DB.prepare(`UPDATE documents SET searchable_at=?,published_revision=document_revision
              WHERE id=? AND ${readyDocumentSql('documents')} AND approved_at IS NULL AND qc_required_confirmed=0 RETURNING id`)
                .bind(new Date().toISOString(), body.documentId).first();
            return published ? json({ ok: true, corpusTier: 'provisional' }) : json({ error: 'Fixture changed' }, 409);
        }
        if (path === '/__test/events')
            return json((await env.DB.prepare('SELECT kind,payload_json FROM fixture_events ORDER BY id').all()).results);
        if (path === '/__test/vectors') {
            const documentId = new URL(request.url).searchParams.get('documentId');
            const listing = await env.FIXTURE_VECTORS.list({ prefix: 'vectors/', limit: 200 });
            if (listing.truncated)
                throw new Error('Fixture vector bound exceeded');
            const rows = await Promise.all(listing.objects.map(async (item) => (await env.FIXTURE_VECTORS.get(item.key))!.json<VectorizeVector>()));
            return json(rows.filter(row => row.metadata?.documentId === documentId).map(row => ({ id: row.id, chunkId: row.metadata?.chunkId })).sort((a, b) => a.id.localeCompare(b.id)));
        }
        if (path === '/__test/provider')
            return nativeFetch(env.FIXTURE_PROVIDER_URL + '/audit');
        if (path === '/__test/requeue-due' && request.method === 'POST') {
            const { documentId } = await request.json() as {
                documentId: string;
            };
            await env.DB.prepare("UPDATE document_vector_jobs SET next_attempt_at='2000-01-01T00:00:00.000Z',enqueued_at=NULL WHERE document_id=? AND state='queued'").bind(documentId).run();
            return json({ sent: await requeueStaleVectorJobs(fixtureEnvironment(env, 'scheduled')) });
        }
        if (path === '/__test/redeliver' && request.method === 'POST') {
            const { documentId, generationId } = await request.json() as {
                documentId: string;
                generationId: string;
            };
            await env.DB.prepare("UPDATE document_vector_jobs SET next_attempt_at='2000-01-01T00:00:00.000Z' WHERE document_id=? AND generation_id=?").bind(documentId, generationId).run();
            let ack = false, retry = false;
            await observedQueue({ queue: 'fixture', messages: [{ id: crypto.randomUUID(), timestamp: new Date(), attempts: 2, body: { documentId, generationId }, ack() { ack = true; }, retry() { retry = true; } }], ackAll() { }, retryAll() { } } as MessageBatch, env);
            return json({ ack, retry });
        }
        if (path === '/__test/lose-postings' && request.method === 'POST') {
            for (const table of ['search_body_substrings', 'search_metadata_substrings', 'search_chunks_fts'])
                await env.DB.prepare(`INSERT INTO ${table}(${table}) VALUES('delete-all')`).run();
            return json({ lost: true });
        }
        return worker.fetch(request, fixtureEnvironment(env, path), ctx);
    },
    async queue(batch: MessageBatch, env: FixtureEnv) { return observedQueue(batch, env); },
    async scheduled(controller: ScheduledController, env: FixtureEnv, ctx: ExecutionContext) { return worker.scheduled(controller, fixtureEnvironment(env, 'scheduled'), ctx); }
};
