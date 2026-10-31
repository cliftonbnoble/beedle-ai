import { z } from 'zod';
import { createHash } from 'node:crypto';
const id = z.string().min(1).max(160), sha = z.string().regex(/^[a-f0-9]{64}$/);
const scopeText = z.string().min(1).max(200);
const calendarDate = z.string().regex(/^\d{4}-\d{2}-\d{2}$/).refine(value => { const date = new Date(value); return !Number.isNaN(date.getTime()) && date.toISOString().slice(0, 10) === value; }, 'Use a valid calendar date');
// A deliberately small delivery canary DTO, validated before deployment. Full
// application mutation/search DTO bounds remain owned by their route contracts.
const canaryFilters = z.object({ approvedOnly: z.literal(true).default(true), documentIds: z.array(id).min(1).max(8).optional(), judgeNames: z.array(scopeText).max(12).optional(), indexCodes: z.array(scopeText).max(50).optional(), jurisdiction: scopeText.optional(), fromDate: calendarDate.optional(), toDate: calendarDate.optional() }).strict().refine(scope => !scope.fromDate || !scope.toDate || scope.fromDate <= scope.toDate, 'Date scope must be ordered');
const searchCase = z.object({ id, category: z.enum(['positive', 'negative', 'filtered']), query: z.string().min(2).max(2000), filters: canaryFilters, expectedDocumentIds: z.array(id).max(10) }).strict();
const sources=z.array(z.object({ documentId: id, sha256: sha }).strict()).min(1).max(3);
const legacyPlan=z.object({ version: z.literal(1), capability: z.enum(['semantic', 'lexical_only']), cases: z.array(searchCase).min(3).max(12), sources }).strict();
const provisionalFilters=canaryFilters.innerType().extend({approvedOnly:z.literal(false)}).strict()
  .refine(scope=>!scope.fromDate||!scope.toDate||scope.fromDate<=scope.toDate,'Date scope must be ordered');
const restoredPlan=z.object({version:z.literal(2),capability:z.literal('lexical_only'),corpusMode:z.literal('trusted_plus_provisional'),
  publicationPolicy:z.literal('unreviewed-provisional-v1'),cases:z.array(searchCase.extend({filters:provisionalFilters})).min(3).max(12),sources}).strict();
export const smokePlanSchema=z.discriminatedUnion('version',[legacyPlan,restoredPlan]);
export function readSmokePlan(plan) {
    plan = smokePlanSchema.parse(plan);
    for (const category of ['positive', 'negative', 'filtered'])
        if (!plan.cases.some(row => row.category === category))
            throw new Error('Positive, negative and filtered smoke cases are required');
    if (new Set(plan.cases.map(row => row.id)).size !== plan.cases.length)
        throw new Error('Smoke case identities must be unique');
    for (const row of plan.cases) {
        if (row.category === 'negative' ? row.expectedDocumentIds.length !== 0 : row.expectedDocumentIds.length === 0)
            throw new Error('Smoke expected identities must match their category');
        if (row.category === 'filtered' && Object.keys(row.filters).filter(key => key !== 'approvedOnly').length === 0)
            throw new Error('Filtered smoke needs an explicit predicate');
    }
    return plan;
}
export async function semanticSmoke({ apiBase, environment, revision, configSha256, contractSha256, plan, fetchImpl = fetch }) {
    plan = readSmokePlan(plan);
    const url = new URL(apiBase);
    if (url.username || url.password || url.search || url.hash || url.pathname !== '/' || (!['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname) && url.protocol !== 'https:'))
        throw new Error('An explicit API origin is required');
    async function request(route, body, maxBytes = 2 * 1024 * 1024) {
        const headers = { 'user-agent': 'Beedle-Release-Smoke/1.0' };
        if (body !== undefined)
            headers['content-type'] = 'application/json';
        const response = await fetchImpl(new URL(route, url), { method: body === undefined ? 'GET' : 'POST', headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: 'error', signal: AbortSignal.timeout(45000) });
        if (!response.ok){await response.body?.cancel().catch(()=>{});throw new Error('Semantic smoke HTTP failure: ' + response.status);}
        const reader = response.body.getReader(), parts = [];
        let bytes = 0;
        try {
            while (true) {
                const { done, value } = await reader.read();
                if (done)
                    break;
                bytes += value.byteLength;
                if (bytes > maxBytes)
                    throw new Error('Smoke response exceeds its bound');
                parts.push(value);
            }
        }
        catch (error) {
            await reader.cancel().catch(() => { });
            throw error;
        }
        return { response, bytes: Buffer.concat(parts) };
    }
    async function readiness() { const { bytes } = await request('/admin/release-readiness'); const state = JSON.parse(bytes.toString('utf8')); if (!state.ready || state.environment !== environment || state.codeRevision !== revision || state.configSha256 !== configSha256 || state.contractSha256 !== contractSha256 || state.capability !== plan.capability ||
      (plan.version===2&&(state.partitionContract!=='restored-corpus-32-v1'||state.partitionCount!==32||state.corpusReadOnly!==true)))
        throw new Error('Candidate identity, schema, backfill or capability mismatch'); return state; }
    const before = await readiness(), checks = { readiness: true, positive: 0, negative: 0, filtered: 0, source: 0 };
    for (const row of plan.cases) {
        const { bytes } = await request('/search', { query: row.query, filters: row.filters, limit: 10,...(plan.version===2?{corpusMode:plan.corpusMode}:{}) });
        const body = JSON.parse(bytes.toString('utf8'));
        if (!Array.isArray(body.results) || body.corpusRevision !== String(before.corpusRevision))
            throw new Error('Search contract or corpus revision changed');
        if(plan.version===2&&body.results.some(result=>result.corpusTier!=='provisional'))throw Error('Restored smoke requires truthful provisional result labels');
        const actual = [...new Set(body.results.map(result => result.documentId))].sort(), expected = [...new Set(row.expectedDocumentIds)].sort();
        if (JSON.stringify(actual) !== JSON.stringify(expected))
            throw new Error('Search semantic smoke identity mismatch for ' + row.id);
        checks[row.category]++;
    }
    for (const source of plan.sources) {
        const { response, bytes } = await request('/source/' + encodeURIComponent(source.documentId), undefined, 16 * 1024 * 1024);
        if (response.headers.has('x-beedle-source-fallback') || createHash('sha256').update(bytes).digest('hex') !== source.sha256)
            throw new Error('Original source smoke failed');
        checks.source++;
    }
    const after = await readiness();
    if (after.corpusRevision !== before.corpusRevision)
        throw new Error('Corpus changed across smoke checks; repeat on a stable snapshot');
    return { version: 1, passed: true, environment, codeRevision: revision, configSha256, contractSha256, capability: plan.capability, corpusRevision: before.corpusRevision, checks, completedAt: new Date().toISOString(),
      ...(plan.version===2?{partitionContract:before.partitionContract,partitionCount:32,corpusReadOnly:true,publicationPolicy:plan.publicationPolicy}:{}) };
}
