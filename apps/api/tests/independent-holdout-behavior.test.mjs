import test from 'node:test';
import assert from 'node:assert/strict';
import { categories, collectHoldout, digest, evaluateHoldout, readHoldout } from '../scripts/lib/independent-holdout.mjs';
function fixture() {
    const holdout = { version: 1, kind: 'synthetic-test', corpusSha256: 'a'.repeat(64), attestation: { curator: 'test-curator', independentReviewer: 'test-reviewer', unseenByImplementers: true, labelsNotDerivedFromGoldenOrSystemOutput: true, sealedAt: '2026-01-01T00:00:00.000Z' }, cases: categories.map((category, i) => ({ id: 'q' + i, category, query: 'synthetic question', filters: category === 'historical' ? { fromDate: '2010-01-01', toDate: '2011-01-01' } : category === 'filtered' ? { judgeNames: ['Synthetic Judge'] } : {}, expectedAbstention: category === 'negative', grades: category === 'negative' ? { irrelevant: 0 } : { relevant: 2, irrelevant: 0 }, relevantPoolExhaustive: false, rubric: 'Synthetic rubric; never independent relevance evidence.' })) };
    const run = { version: 1, holdoutSha256: digest(holdout), corpusSha256: holdout.corpusSha256, codeRevision: 'b'.repeat(40), startedAt: '2026-01-02T00:00:00.000Z', completedAt: '2026-01-02T00:01:00.000Z', rows: holdout.cases.map(row => ({ id: row.id, rankedDocumentIds: row.category === 'negative' ? [] : ['relevant', 'irrelevant'], claims: row.category === 'negative' ? [] : [{ id: 'claim-1', text: 'Synthetic claim', evidence: [] }], answer: row.category === 'negative' ? 'Insufficient evidence' : 'Synthetic claim', abstained: row.category === 'negative', corpusRevision: '1', citations: row.category === 'negative' ? [] : ['relevant'], retrievalContext: null, searchLatencyMs: 2, assistantLatencyMs: 4 })) };
    const judgments = { version: 1, runSha256: digest(run), reviewer: 'test-reviewer', reviewedAt: '2026-01-03T00:00:00.000Z', blindRelevanceReview: true, claimSupportCheckedAgainstOriginalSources: true, rows: run.rows.map(row => ({ id: row.id, grades: {}, claims: row.claims.map(c => ({ id: c.id, supported: true, reason: 'Synthetic source support' })), scopeSatisfied: true, abstentionAppropriate: true, answerSupported: true, reason: 'Synthetic judgment' })) };
    return { holdout, run, judgments };
}
const score = f => evaluateHoldout(f.holdout, f.run, f.judgments, { allowSynthetic: true });
test('independent holdout reports recall/ranking, human support and abstention separately', () => {
    const f = fixture(), report = score(f);
    assert.equal(report.acceptance, 'requires-domain-review');
    assert.equal(report.overall.retrieval.judgedPoolRecallAt5, 1);
    assert.equal(report.byCategory.positive.retrieval.precisionAt5, .2);
    assert.equal(report.byCategory.positive.retrieval.ndcgAt5, 1);
    assert.equal(report.overall.grounding.claimSupportRate, 1);
    assert.equal(report.overall.abstention.rate, .2);
    assert.equal(report.byCategory.negative.grounding.claimSupportRate, null);
    assert.ok(!JSON.stringify(report).includes('Synthetic claim'));
    assert.ok(report.perCase.every(row => row.recallBasis === 'judged-pool-only'));
    f.judgments.rows[0].claims[0].supported = false;
    assert.equal(score(f).overall.grounding.claimSupportRate, .75);
    assert.equal(score(f).overall.retrieval.ndcgAt5, 1);
});
test('release scoring refuses synthetic, incomplete, self-reviewed, golden-derived and missing-category holdouts', () => {
    const f = fixture();
    assert.throws(() => evaluateHoldout(f.holdout, f.run, f.judgments), /Synthetic/);
    for (const change of [h => h.cases.pop(), h => h.attestation.independentReviewer = h.attestation.curator, h => h.attestation.labelsNotDerivedFromGoldenOrSystemOutput = false, h => h.cases[0].grades = {}]) {
        const h = structuredClone(f.holdout);
        change(h);
        assert.throws(() => readHoldout(h, { allowSynthetic: true }));
    }
});
test('run and human judgments are bound to exact sealed query, corpus, code and output artifacts', () => {
    const mutations = [f => f.holdout.cases[0].query = 'changed query', f => f.run.corpusSha256 = 'c'.repeat(64), f => f.run.rows[0].answer = 'changed response', f => f.judgments.runSha256 = 'c'.repeat(64), f => f.judgments.reviewer = 'another-reviewer', f => f.judgments.reviewedAt = '2025-01-01T00:00:00.000Z'];
    for (const mutate of mutations) {
        const f = fixture();
        mutate(f);
        assert.throws(() => score(f));
    }
});
test('missing rows, unjudged retrievals or claims and rewritten sealed grades fail rather than improve scores', () => {
    for (const mutate of [f => f.run.rows.pop(), f => f.run.rows[0].rankedDocumentIds.push('unjudged'), f => f.judgments.rows[0].claims = [], f => f.judgments.rows[0].grades.relevant = 0, f => f.run.rows[0].rankedDocumentIds.push('relevant')]) {
        const f = fixture();
        mutate(f);
        f.judgments.runSha256 = digest(f.run);
        assert.throws(() => score(f));
    }
});
test('real collection requires explicit paid-call consent, isolated origin and matching corpus before any network', async () => {
    const f = fixture();
    f.holdout.kind = 'independent-human';
    let called = 0;
    const options = { apiBase: 'http://127.0.0.1:8819', corpusSha256: f.holdout.corpusSha256, codeRevision: 'b'.repeat(40), fetchImpl: async () => { called++; throw new Error('not permitted'); } };
    await assert.rejects(collectHoldout(f.holdout, options), /allow-provider-calls/);
    await assert.rejects(collectHoldout(f.holdout, { ...options, allowProviderCalls: true, apiBase: 'http://remote.invalid' }), /isolated/);
    await assert.rejects(collectHoldout(f.holdout, { ...options, allowProviderCalls: true, corpusSha256: 'c'.repeat(64) }), /corpus/);
    assert.equal(called, 0);
});
test('capture preserves scope/evidence, fails mixed revisions and bounds every provider response', async () => {
    const f = fixture();
    f.holdout.kind = 'independent-human';
    const calls = [];
    const options = { apiBase: 'http://127.0.0.1:8819', corpusSha256: f.holdout.corpusSha256, codeRevision: 'b'.repeat(40), allowProviderCalls: true, fetchImpl: async (url, init) => { calls.push({ path: url.pathname, payload: JSON.parse(init.body) }); return Response.json(url.pathname === '/search' ? { results: [{ documentId: 'relevant' }], corpusRevision: '1' } : { assessmentVersion: 'claims-1', claims: [{ id: 'c1', text: 'Synthetic claim', evidence: [{ documentId: 'relevant', chunkId: 'chunk1', citationAnchor: 'anchor1', quote: 'Synthetic source excerpt', excerpt: 'Synthetic source excerpt' }] }], answer: 'Synthetic claim', evidenceStatus: 'unverified_claims', evidenceCorpusRevision: '1', citations: [{ documentId: 'relevant' }], retrievalContext: null }); } };
    const run = await collectHoldout(f.holdout, options);
    assert.equal(run.rows.length, 5);
    assert.equal(calls.length, 10);
    assert.deepEqual(calls[6].payload.filters, f.holdout.cases[3].filters);
    assert.equal(run.rows[0].claims[0].evidence[0].quote, 'Synthetic source excerpt');
    await assert.rejects(collectHoldout(f.holdout, { ...options, fetchImpl: async (url, init) => { const response = await options.fetchImpl(url, init); const body = await response.json(); if (url.pathname.includes('assistant'))
            body.evidenceCorpusRevision = '2'; return Response.json(body); } }), /revision/);
    await assert.rejects(collectHoldout(f.holdout, { ...options, fetchImpl: async () => new Response('x'.repeat(2 * 1024 * 1024 + 1)) }), /bound/);
    await assert.rejects(collectHoldout(f.holdout, { ...options, fetchImpl: async () => new Response('{}', { status: 503 }) }), /incomplete/);
});
