import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { memoryD1 } from './helpers/d1-memory.mjs';
import { loadTs } from './helpers/load-ts.mjs';
import { contract, inspectDatabase, preflight, validatePreviewEvidence, targetFingerprint } from '../scripts/lib/release-preflight.mjs';
import { fingerprint, releaseProfileId } from '../scripts/lib/release-contract.mjs';
import { semanticSmoke,readSmokePlan } from '../scripts/lib/release-smoke.mjs';
const { inspectReleaseReadiness } = loadTs('src/services/release-readiness.ts');
const { vectorProfile, VECTOR_PIPELINE_VERSION, CHUNKER_VERSION, RETRIEVAL_CHUNKER_VERSION } = loadTs('src/services/vector-profile.ts');
const { REVIEWER_POLICY_VERSION } = loadTs('src/services/reviewer-queue-index.ts');
const { readyReviewerQueue } = await import('./helpers/reviewer-queue.mjs');
const { catalogPayload } = await import('./helpers/catalog-fixture.mjs');
const { rebuildLegalReferences } = loadTs('src/services/legal-references.ts');
const { startIndexRecovery, advanceIndexRecovery, indexRecoveryStatus } = loadTs('src/services/search-index-recovery.ts');
const { advanceFacetBuild, facetBuildStatus } = loadTs('src/services/search-facet-build.ts');
const config = JSON.parse(readFileSync(new URL('../wrangler.json', import.meta.url), 'utf8'));
const vars = { ...config.vars, RELEASE_REVISION: 'a'.repeat(40), RELEASE_CONFIG_SHA256: 'b'.repeat(64) };
function ledger(sqlite) { sqlite.exec('CREATE TABLE d1_migrations(id INTEGER PRIMARY KEY,name TEXT NOT NULL,applied_at TEXT)'); for (const name of readdirSync(new URL('../migrations/', import.meta.url)).filter(name => name.endsWith('.sql')).sort())
    sqlite.prepare('INSERT INTO d1_migrations(name) VALUES(?)').run(name); }
async function ready(env) { await rebuildLegalReferences(env, catalogPayload('Release fixture')); await readyReviewerQueue(env); }
function execute(sqlite) { return async (queries) => queries.map(sql => sqlite.prepare(sql).all()); }
function previewConfig() { const copy = structuredClone(config), preview = copy.env.preview; preview.vars.DEPLOYMENT_READY = 'true'; preview.vars.CORS_ALLOWED_ORIGINS = 'https://preview.example.com'; preview.vars.SOURCE_PROXY_BASE_URL = 'https://preview-api.example.com'; preview.d1_databases[0].database_id = '00000000-0000-0000-0000-000000000020'; return copy; }
test('release contract covers every migration, current reader policy and immutable embedding profile', async () => {
    assert.deepEqual(contract.requiredMigrations, readdirSync(new URL('../migrations/', import.meta.url)).filter(n => n.endsWith('.sql')).sort());
    assert.equal(contract.maxMigration, Number(contract.requiredMigrations.at(-1).slice(0, 4)));
    assert.equal(contract.reviewerPolicyVersion, REVIEWER_POLICY_VERSION);
    assert.equal(contract.vectorFilterVersion,loadTs("src/services/vector-filter-metadata.ts").VECTOR_FILTER_VERSION);
    assert.deepEqual(contract.vectorProfile, [VECTOR_PIPELINE_VERSION, CHUNKER_VERSION, RETRIEVAL_CHUNKER_VERSION]);
    assert.equal(await releaseProfileId(vars, contract), (await vectorProfile(vars)).id);
});
test('candidate and CLI share read-only schema/backfill policy, without initialization or writes', async () => {
    const { DB, sqlite } = memoryD1();
    try {
        ledger(sqlite);
        const env = { ...vars, DB };
        await ready(env);
        const before = sqlite.prepare('SELECT total_changes() n').get().n;
        const runtime = await inspectReleaseReadiness(env);
        assert.equal(runtime.ready, true, JSON.stringify(runtime));
        assert.equal(runtime.capability, 'lexical_only');
        assert.equal(runtime.schemaVersion, contract.maxMigration);
        const cli = await inspectDatabase(execute(sqlite), vars);
        assert.equal(cli.ready, true);
        assert.equal(cli.corpusRevision, runtime.corpusRevision);
        assert.equal(sqlite.prepare('SELECT total_changes() n').get().n, before);
    }
    finally {
        sqlite.close();
    }
});
test('release accepts completed index recovery and still requires every current posting trigger', async () => {
    const { DB, sqlite } = memoryD1({ finalizeEmptyPhraseIndex: false });
    try {
        ledger(sqlite);
        const env = { ...vars, DB };
        sqlite.exec("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at) VALUES('recovered','decision_docx','Test','Recovered fixture','Fixture','original.docx','https://example.invalid','now','now'); INSERT INTO document_sections(id,document_id,canonical_key,heading,section_order) VALUES('s','recovered','conclusions_of_law','Conclusions',0); INSERT INTO section_paragraphs(id,section_id,anchor,paragraph_order,text) VALUES('p','s','p1',0,'recoveryneedle'); INSERT INTO document_chunks(id,document_id,section_id,paragraph_id,paragraph_anchor,citation_anchor,section_label,chunk_order,chunk_text,token_estimate,created_at) VALUES('c','recovered','s','p','p1','L:p1','Conclusions',0,'recoveryneedle',5,'now');");
        const generation = crypto.randomUUID();
        await startIndexRecovery(env, { dryRun: false, generation, expectedGeneration: 'legacy', replaceArchive: true, restart: false });
        for (let step = 0; step < 30 && (await indexRecoveryStatus(env)).status !== 'completed'; step++)
            await advanceIndexRecovery(env, generation);
        assert.equal((await indexRecoveryStatus(env)).status, 'completed');
        for (let step = 0; step < 5 && (await facetBuildStatus(env)).phase !== 'ready'; step++)
            await advanceFacetBuild(env);
        await ready(env);
        assert.equal(sqlite.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE type='trigger' AND name GLOB '*_search_fts'").get().n, 0,
            'recovery deliberately retires the six frozen legacy FTS triggers');
        const before = sqlite.prepare('SELECT total_changes() n').get().n;
        const readiness = await inspectReleaseReadiness(env);
        assert.equal(readiness.ready, true, JSON.stringify(readiness));
        assert.equal((await inspectDatabase(execute(sqlite), vars)).ready, true);
        assert.equal(sqlite.prepare('SELECT total_changes() n').get().n, before);
        assert.equal(sqlite.prepare("SELECT COUNT(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'recoveryneedle'").get().n, 1);
        sqlite.exec("UPDATE document_chunks SET chunk_text='updatedneedle' WHERE id='c'");
        assert.equal(sqlite.prepare("SELECT COUNT(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'recoveryneedle'").get().n, 0);
        assert.equal(sqlite.prepare("SELECT COUNT(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'updatedneedle'").get().n, 1);
        await readyReviewerQueue(env);
        for (const source of ['document_chunks', 'retrieval_search_chunks', 'documents']) {
            for (const operation of ['insert', 'update', 'delete']) {
                const name = `${source}_substring_${operation}`;
                const row = sqlite.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?").get(name);
                assert.ok(row, name);
                sqlite.exec(`DROP TRIGGER ${name}`);
                assert.equal((await inspectReleaseReadiness(env)).ready, false, name);
                await assert.rejects(inspectDatabase(execute(sqlite), vars), /schema/);
                sqlite.exec(row.sql);
            }
        }
        assert.equal((await inspectDatabase(execute(sqlite), vars)).ready, true);
        assert.equal(sqlite.prepare('SELECT published_revision FROM documents').get().published_revision, null);
    }
    finally {
        sqlite.close();
    }
});
test('missing ledger, partial schema, deleted guards and future migrations refuse release', async () => {
    const { DB, sqlite } = memoryD1();
    try {
        const env = { ...vars, DB };
        assert.equal((await inspectReleaseReadiness(env)).ready, false);
        ledger(sqlite);
        await ready(env);
        sqlite.prepare('DELETE FROM d1_migrations WHERE name=?').run(contract.requiredMigrations.at(-1));
        await assert.rejects(inspectDatabase(execute(sqlite), vars), /schema/);
        sqlite.prepare('INSERT INTO d1_migrations(name) VALUES(?)').run(contract.requiredMigrations.at(-1));
        sqlite.prepare("INSERT INTO d1_migrations(name) VALUES('9999_future.sql')").run();
        await assert.rejects(inspectDatabase(execute(sqlite), vars), /range/);
        sqlite.exec("DELETE FROM d1_migrations WHERE name='9999_future.sql'");
        sqlite.exec('DROP TRIGGER reference_editions_immutable_update');
        await assert.rejects(inspectDatabase(execute(sqlite), vars), /reference_editions_immutable_update/);
        assert.equal((await inspectReleaseReadiness(env)).ready, false);
    }
    finally {
        sqlite.close();
    }
});
test('partial FTS, dirty reviewer state and unsafe vector mode cannot be reported ready', async () => {
    const { DB, sqlite } = memoryD1();
    try {
        ledger(sqlite);
        const env = { ...vars, DB };
        await ready(env);
        sqlite.exec("UPDATE search_facet_build SET phase='pending'");
        await assert.rejects(inspectDatabase(execute(sqlite), vars), /search_backfill/);
        sqlite.exec("UPDATE search_facet_build SET phase='ready'");
        sqlite.exec("UPDATE reviewer_queue_control SET phase='backfill'");
        await assert.rejects(inspectDatabase(execute(sqlite), vars), /reviewer_backfill/);
        sqlite.exec("UPDATE reviewer_queue_control SET phase='ready'");
        await assert.rejects(inspectDatabase(execute(sqlite), { ...vars, VECTOR_LIFECYCLE_READY: 'true', VECTOR_FILTERS_READY: 'false' }), /vector_configuration/);
        sqlite.exec("INSERT INTO vector_lifecycle_dirty(document_id,updated_at) VALUES('missing-sync','now')");
        await assert.rejects(inspectDatabase(execute(sqlite), { ...vars, VECTOR_LIFECYCLE_READY: 'true', VECTOR_FILTERS_READY: 'true' }), /vector_backfill/);
        assert.equal((await inspectReleaseReadiness(env)).ready, true, 'explicit lexical-only operation remains supported');
    }
    finally {
        sqlite.close();
    }
});
test('preflight selects the explicit target and production requires fresh matching isolated-preview evidence', async () => {
    const { DB, sqlite } = memoryD1();
    try {
        ledger(sqlite);
        await ready({ ...vars, DB });
        const selected = previewConfig();
        let reads = 0;
        const report = await preflight(selected, 'preview', { execute: async (queries) => { reads++; return execute(sqlite)(queries); } });
        assert.equal(reads, 2);
        assert.equal(report.environment, 'preview');
        const evidence = { version: 1, environment: 'preview', passed: true, capability: 'lexical_only', codeRevision: vars.RELEASE_REVISION, contractSha256: await fingerprint(contract), configSha256: await targetFingerprint(selected, 'preview'), checks: { positive: 1, negative: 1, filtered: 1, source: 1, readiness: true }, completedAt: new Date().toISOString() };
        assert.equal(await validatePreviewEvidence(selected, vars.RELEASE_REVISION, evidence), evidence);
        for (const change of [r => r.environment = 'production', r => r.codeRevision = 'c'.repeat(40), r => r.configSha256 = 'c'.repeat(64), r => r.checks.source = 0, r => delete r.checks.source, r => r.checks.source = '1', r => r.completedAt = '2020-01-01T00:00:00.000Z']) {
            const bad = structuredClone(evidence);
            change(bad);
            await assert.rejects(validatePreviewEvidence(selected, vars.RELEASE_REVISION, bad));
        }
    }
    finally {
        sqlite.close();
    }
});
function smokeFixture() {
    const readiness = { ready: true, environment: 'preview', codeRevision: vars.RELEASE_REVISION, configSha256: vars.RELEASE_CONFIG_SHA256, contractSha256: 'c'.repeat(64), capability: 'lexical_only', corpusRevision: 42 };
    const original = Buffer.from('Synthetic original source');
    const plan = { version: 1, capability: 'lexical_only', cases: [{ id: 'positive', category: 'positive', query: 'notice', filters: { approvedOnly: true }, expectedDocumentIds: ['doc-positive'] }, { id: 'negative', category: 'negative', query: 'unmatched nonsense', filters: { approvedOnly: true }, expectedDocumentIds: [] }, { id: 'filtered', category: 'filtered', query: 'notice', filters: { judgeNames: ['Synthetic Judge'] }, expectedDocumentIds: ['doc-positive'] }], sources: [{ documentId: 'doc-positive', sha256: '' }] };
    return { readiness, original, plan };
}
test('semantic smoke verifies identities, negative/filtered cases, exact original source and stable corpus', async () => {
    const { createHash } = await import('node:crypto');
    const f = smokeFixture();
    f.plan.sources[0].sha256 = createHash('sha256').update(f.original).digest('hex');
    let requests = 0;
    const options = { apiBase: 'http://127.0.0.1:8820', environment: 'preview', revision: vars.RELEASE_REVISION, configSha256: vars.RELEASE_CONFIG_SHA256, contractSha256: 'c'.repeat(64), plan: f.plan, fetchImpl: async (url, init) => {
        const headers = new Headers(init.headers);
        assert.match(headers.get('user-agent'), /^Beedle-Release-Smoke\/\d/);
        assert.equal(headers.has('content-type'), init.method === 'POST');
        requests++;
        return url.pathname.includes('readiness') ? Response.json(f.readiness) : url.pathname.includes('/source/') ? new Response(f.original) : Response.json({ corpusRevision: '42', results: JSON.parse(init.body).query === 'unmatched nonsense' ? [] : [{ documentId: 'doc-positive' }] });
    } };
    const result = await semanticSmoke(options);
    assert.equal(result.passed, true);
    assert.equal(requests, 6, 'readiness, search and original-source requests all identify the application');
    assert.deepEqual(result.checks, { readiness: true, positive: 1, negative: 1, filtered: 1, source: 1 });
    assert.ok(!JSON.stringify(result).includes('Synthetic original'));
    for (const mutate of [() => f.readiness.environment = 'production', () => f.readiness.codeRevision = 'wrong', () => f.plan.cases[0].expectedDocumentIds = ['wrong'], () => f.plan.sources[0].sha256 = 'd'.repeat(64)]) {
        const old = structuredClone(f);
        mutate();
        await assert.rejects(semanticSmoke(options));
        Object.assign(f, old);
        options.plan = f.plan;
    }
    await assert.rejects(semanticSmoke({ ...options, fetchImpl: async () => new Response('{}', { status: 503 }) }), /HTTP failure/);
    await assert.rejects(semanticSmoke({ ...options, fetchImpl: async (url, init) => url.pathname.includes('source') ? new Response(f.original, { headers: { 'x-beedle-source-fallback': 'r2-missing-db-text' } }) : options.fetchImpl(url, init) }), /Original source/);
});
test('release executor refuses malformed plans, incomplete preflight or missing preview proof before deploying', async () => {
    const { executeRelease } = await import('../scripts/deploy-environment.mjs');
    const f = smokeFixture();
    const selected = previewConfig();
    f.plan.sources[0].sha256 = 'd'.repeat(64);
    let deployed = 0;
    const options = { config: selected, environment: 'preview', revision: vars.RELEASE_REVISION, plan: f.plan };
    const dependencies = { inspect: async () => ({ ready: true, capability: 'lexical_only', configSha256: 'b'.repeat(64), contractSha256: 'c'.repeat(64) }), runDeployment: async () => { deployed++; }, smoke: async () => ({ passed: true }) };
    await assert.rejects(executeRelease({ ...options, plan: {} }, dependencies));
    const malformed = structuredClone(f.plan);
    malformed.cases[0].filters.approvedOnly = 'true';
    await assert.rejects(executeRelease({ ...options, plan: malformed }, dependencies));
    const { deploymentEnvironment } = loadTs('src/lib/deployment.ts');
    assert.throws(() => deploymentEnvironment({ ...config.env.production.vars, RELEASE_MAINTENANCE_ONLY: 'true' }), /preview/);
    await assert.rejects(executeRelease(options, { ...dependencies, inspect: async () => { throw new Error('partial backfill'); } }), /backfill/);
    await assert.rejects(executeRelease({ ...options, environment: 'production' }, dependencies), /preview evidence/);
    await assert.rejects(executeRelease({ ...options, environment: 'production', prepare: true }, dependencies), /preview/);
    assert.equal(deployed, 0);
    assert.equal((await executeRelease(options, dependencies)).passed, true);
    assert.equal(deployed, 1);
    await assert.rejects(executeRelease(options, { ...dependencies, smoke: async () => { throw new Error('negative smoke leaked'); } }), /was deployed but semantic smoke failed/);
    assert.equal(deployed, 2);
    const prepared = await executeRelease({ ...options, prepare: true }, { ...dependencies, prepareInspect: async () => ({ environment: 'preview', preparationOnly: true, configSha256: 'b'.repeat(64), contractSha256: 'c'.repeat(64) }) });
    assert.equal(prepared.passed, false);
    await assert.rejects(validatePreviewEvidence(selected, vars.RELEASE_REVISION, prepared));
});
test('provisional delivery is explicit, checks review labels and cannot reuse a trusted-only smoke plan',async()=>{
 const {createHash}=await import('node:crypto');const f=smokeFixture();
 const plan={...f.plan,version:2,publicationPolicy:'unreviewed-provisional-v1',corpusMode:'trusted_plus_provisional'};
 for(const row of plan.cases)row.filters={...row.filters,approvedOnly:false};
 plan.sources[0].sha256=createHash('sha256').update(f.original).digest('hex');
 const state={...f.readiness,partitionContract:'restored-corpus-32-v1',partitionCount:32,corpusReadOnly:true};
 let tier='provisional';
 const options={apiBase:'http://127.0.0.1:8820',environment:'preview',revision:vars.RELEASE_REVISION,configSha256:vars.RELEASE_CONFIG_SHA256,contractSha256:'c'.repeat(64),plan,
  fetchImpl:async(url,init)=>{
   if(url.pathname.includes('readiness'))return Response.json(state);
   if(url.pathname.includes('/source/'))return new Response(f.original);
   const request=JSON.parse(init.body);assert.equal(request.corpusMode,'trusted_plus_provisional');assert.equal(request.filters.approvedOnly,false);
   return Response.json({corpusRevision:'42',results:request.query==='unmatched nonsense'?[]:[{documentId:'doc-positive',corpusTier:tier}]});
  }};
 const result=await semanticSmoke(options);assert.equal(result.publicationPolicy,plan.publicationPolicy);assert.equal(result.corpusReadOnly,true);
 tier='trusted';await assert.rejects(semanticSmoke(options),/provisional result labels/);tier='provisional';
 delete state.partitionContract;await assert.rejects(semanticSmoke(options),/mismatch/);
 const old=structuredClone(plan);old.version=1;delete old.corpusMode;delete old.publicationPolicy;assert.throws(()=>readSmokePlan(old));
 const wrong=structuredClone(plan);wrong.cases[0].filters.approvedOnly=true;assert.throws(()=>readSmokePlan(wrong));
 const {executeRelease}=await import('../scripts/deploy-environment.mjs');let deployed=0;
 await assert.rejects(executeRelease({config:previewConfig(),environment:'preview',revision:vars.RELEASE_REVISION,plan},
  {inspect:async()=>({ready:true,capability:'lexical_only'}),runDeployment:async()=>{deployed++;}}),/explicitly match/);assert.equal(deployed,0);
});
test('populated additive migrations preserve source identity and legacy holds, then the prior reader still works', async () => {
    const { DB, sqlite } = memoryD1({ migrationThrough: 30 });
    try {
        sqlite.exec("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at) VALUES('legacy-release','decision_docx','Test','Retained source','T-OLD','retained.md','https://example.invalid','old','old')");
        const before = sqlite.prepare("SELECT id,title,source_r2_key,approved_at,document_revision,ingestion_state FROM documents WHERE id='legacy-release'").get();
        for (const name of contract.requiredMigrations.filter(name => Number(name.slice(0, 4)) > 30))
            sqlite.exec(readFileSync(new URL('../migrations/' + name, import.meta.url), 'utf8'));
        ledger(sqlite);
        await ready({ ...vars, DB });
        const after = sqlite.prepare("SELECT id,title,source_r2_key,approved_at,document_revision,ingestion_state FROM documents WHERE id='legacy-release'").get();
        assert.deepEqual(after, before);
        assert.equal(JSON.parse(sqlite.prepare("SELECT reference_edition_context_json c FROM documents WHERE id='legacy-release'").get().c).status, 'unknown');
        assert.equal((await inspectDatabase(execute(sqlite), vars)).ready, true);
        // Search is unchanged by M20: the last compatible reader can run after the
        // additive migration, and a legacy record is still held out of publication.
        const { search } = loadTs('src/services/search.ts');
        const result = await search({ ...vars, DB }, { query: 'retained source', filters: { approvedOnly: false }, corpusMode: 'trusted_plus_provisional' }, { session: false });
        assert.equal(result.results.length, 0);
    }
    finally {
        sqlite.close();
    }
});
