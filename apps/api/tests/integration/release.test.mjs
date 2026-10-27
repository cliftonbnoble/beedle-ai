import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createWorkerFixture } from './fixture.mjs';
import { caseSourceText, seedFixture } from './seeds.mjs';
import { semanticSmoke } from '../../scripts/lib/release-smoke.mjs';
import {inspectDatabase,wranglerJson} from '../../scripts/lib/release-preflight.mjs';
test('real migrated Worker holds partial readiness and passes semantic/source smoke after bounded builds', { timeout: 90000 }, async () => {
    const fixture = await createWorkerFixture({ lexicalOnly: true });
    try {
        const pending = await fixture.request('/admin/release-readiness', undefined, { status: 503 });
        assert.equal(pending.ready, false);
        const ids = await seedFixture(fixture), ready = await fixture.request('/admin/release-readiness');
        assert.equal(ready.ready, true);
        assert.equal(ready.capability, 'lexical_only');
        assert.equal(ready.codeRevision, 'a'.repeat(40));
        const inspected=await inspectDatabase(async queries=>{
            const result=wranglerJson(['d1','execute','DB','--local','--persist-to',fixture.dir+'/state','--command',queries.join(';\n')],{configPath:fixture.dir+'/wrangler.json'});
            assert.equal(result.length,queries.length);
            assert.ok(result.every(row=>row.success===true));
            return result.map(row=>row.results);
        },{AI_EMBEDDING_MODEL:'fixture',AI_EMBEDDING_DIMENSIONS:'2',VECTOR_NAMESPACE:'beedle-docs-preview',VECTOR_LIFECYCLE_READY:'false',VECTOR_FILTERS_READY:'false'});
        assert.equal(inspected.ready,true);
        assert.equal(inspected.corpusRevision,ready.corpusRevision);
        const report = await semanticSmoke({ apiBase: fixture.url, environment: 'preview', revision: 'a'.repeat(40), configSha256: 'b'.repeat(64), contractSha256: ready.contractSha256, plan: { version: 1, capability: 'lexical_only', cases: [
                    { id: 'positive', category: 'positive', query: 'repair notice', filters: { approvedOnly: true }, expectedDocumentIds: [ids.alpha, ids.beta] },
                    { id: 'negative', category: 'negative', query: 'zyxwvunmatchedword', filters: { approvedOnly: true }, expectedDocumentIds: [] },
                    { id: 'filtered', category: 'filtered', query: 'repair notice', filters: { approvedOnly: true, documentIds: [ids.beta] }, expectedDocumentIds: [ids.beta] }
                ],
                sources: [{ documentId: ids.beta, sha256: createHash('sha256').update(caseSourceText).digest('hex') }] } });
        assert.equal(report.passed, true);
        assert.equal(report.checks.source, 1);
    }
    finally {
        await fixture.stop();
    }
});
test('preview preparation serves maintenance but holds customer routes and cannot be accepted as ready', { timeout: 90000 }, async () => {
    const fixture = await createWorkerFixture({ lexicalOnly: true, maintenanceOnly: true });
    try {
        await fixture.request('/health');
        const report = await fixture.request('/admin/release-readiness', undefined, { status: 503 });
        assert.equal(report.maintenanceOnly, true);
        await fixture.request('/search', { query: 'notice' }, { status: 503 });
        await fixture.request('/api/assistant/chat', { messages: [{ role: 'user', content: 'notice' }] }, { status: 503 });
    }
    finally {
        await fixture.stop();
    }
});
