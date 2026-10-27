import { createHash } from 'node:crypto';
import { z } from 'zod';
const sha = z.string().regex(/^[a-f0-9]{64}$/), id = z.string().min(1).max(160), text = z.string().min(1).max(12000);
export const categories = ['positive', 'negative', 'filtered', 'historical', 'adversarial'];
const attestation = z.object({ curator: id, independentReviewer: id, unseenByImplementers: z.literal(true), labelsNotDerivedFromGoldenOrSystemOutput: z.literal(true), sealedAt: z.string().datetime() }).strict();
const gradeMap = z.record(id, z.number().int().min(0).max(2)).refine(value => Object.keys(value).length <= 1000, 'At most 1000 document judgments per case');
const caseSchema = z.object({ id, category: z.enum(categories), query: z.string().min(2).max(2000), filters: z.record(z.unknown()),
    expectedAbstention: z.boolean(), grades: gradeMap, relevantPoolExhaustive: z.boolean(), rubric: text }).strict();
const holdoutSchema = z.object({ version: z.literal(1), kind: z.enum(['independent-human', 'synthetic-test']), corpusSha256: sha, attestation, cases: z.array(caseSchema).min(5).max(200) }).strict();
function unique(values, label) { if (new Set(values).size !== values.length)
    throw new Error(`Duplicate ${label}`); }
export function canonical(value) { if (Array.isArray(value))
    return '[' + value.map(canonical).join(',') + ']'; if (value && typeof value === 'object')
    return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}'; return JSON.stringify(value); }
export const digest = value => createHash('sha256').update(canonical(value)).digest('hex');
export function readHoldout(raw, { allowSynthetic = false } = {}) {
    const data = holdoutSchema.parse(raw);
    if (!allowSynthetic && data.kind !== 'independent-human')
        throw new Error('Synthetic fixtures cannot establish independent quality');
    if (data.attestation.curator === data.attestation.independentReviewer)
        throw new Error('A separate independent reviewer is required');
    unique(data.cases.map(row => row.id), 'case identity');
    for (const category of categories)
        if (!data.cases.some(row => row.category === category))
            throw new Error(`Missing ${category} coverage`);
    for (const row of data.cases) {
        if (!Object.keys(row.grades).length)
            throw new Error(`${row.id}: judgments cannot be empty`);
        if (!row.expectedAbstention && !Object.values(row.grades).some(grade => grade > 0))
            throw new Error(`${row.id}: answerable cases need relevant evidence`);
        if (row.category === 'filtered' && !Object.keys(row.filters).length)
            throw new Error('Filtered cases require explicit scope');
        if (row.category === 'historical' && !(row.filters.fromDate || row.filters.toDate))
            throw new Error('Historical cases require explicit date scope');
    }
    return data;
}
const claim = z.object({ id, text, evidence: z.array(z.object({ documentId: id, chunkId: id, citationAnchor: z.string().max(2048), quote: z.string().max(1200), excerpt: z.string().max(16000) }).strict()).max(3) }).strict();
const runRow = z.object({ id, rankedDocumentIds: z.array(id).max(10), claims: z.array(claim).max(8), answer: text, abstained: z.boolean(),
    citations: z.array(id).max(8), corpusRevision: z.string().max(200), retrievalContext: z.unknown(), searchLatencyMs: z.number().nonnegative(), assistantLatencyMs: z.number().nonnegative() }).strict();
const runSchema = z.object({ version: z.literal(1), holdoutSha256: sha, corpusSha256: sha, codeRevision: z.string().regex(/^[a-f0-9]{40}$/), startedAt: z.string().datetime(), completedAt: z.string().datetime(), rows: z.array(runRow).min(5).max(200) }).strict();
const judgment = z.object({ id, grades: gradeMap, claims: z.array(z.object({ id, supported: z.boolean(), reason: text }).strict()).max(8), scopeSatisfied: z.boolean(), abstentionAppropriate: z.boolean(), answerSupported: z.boolean(), reason: text }).strict();
const judgmentSchema = z.object({ version: z.literal(1), runSha256: sha, reviewer: id, reviewedAt: z.string().datetime(), blindRelevanceReview: z.literal(true), claimSupportCheckedAgainstOriginalSources: z.literal(true), rows: z.array(judgment).min(5).max(200) }).strict();
const average = values => values.length ? values.reduce((a, b) => a + b, 0) / values.length : null;
const rate = (num, den) => den ? num / den : null;
const gain = grade => 2 ** grade - 1;
export function evaluateHoldout(rawHoldout, rawRun, rawJudgments, options = {}) {
    const holdout = readHoldout(rawHoldout, options), run = runSchema.parse(rawRun), judgments = judgmentSchema.parse(rawJudgments);
    if (run.holdoutSha256 !== digest(holdout) || run.corpusSha256 !== holdout.corpusSha256)
        throw new Error('Holdout or corpus does not match the captured run');
    if (judgments.runSha256 !== digest(run))
        throw new Error('Judgments do not match the captured run');
    if (judgments.reviewer !== holdout.attestation.independentReviewer)
        throw new Error('Independent reviewer identity does not match');
    if (Date.parse(holdout.attestation.sealedAt) > Date.parse(run.startedAt) || Date.parse(run.startedAt) > Date.parse(run.completedAt) || Date.parse(judgments.reviewedAt) < Date.parse(run.completedAt))
        throw new Error('Holdout sealing/run/review timestamps are inconsistent');
    unique(run.rows.map(row => row.id), 'run identity');
    unique(judgments.rows.map(row => row.id), 'judgment identity');
    if (run.rows.length !== holdout.cases.length || judgments.rows.length !== holdout.cases.length)
        throw new Error('Every holdout case must run and be judged');
    const perCase = holdout.cases.map(row => {
        const captured = run.rows.find(item => item.id === row.id), judged = judgments.rows.find(item => item.id === row.id);
        if (!captured || !judged)
            throw new Error(`${row.id}: missing run or judgment`);
        unique(captured.rankedDocumentIds, 'ranked document');
        unique(captured.claims.map(item => item.id), 'claim identity');
        unique(judged.claims.map(item => item.id), 'claim judgment');
        const grades = { ...row.grades, ...judged.grades };
        for (const [doc, grade] of Object.entries(row.grades))
            if (grades[doc] !== grade)
                throw new Error(`${row.id}: sealed relevance labels cannot be rewritten`);
        if (captured.rankedDocumentIds.some(doc => !Object.hasOwn(grades, doc)))
            throw new Error(`${row.id}: retrieved documents remain unjudged`);
        if (judged.claims.length !== captured.claims.length || captured.claims.some(c => !judged.claims.some(j => j.id === c.id)))
            throw new Error(`${row.id}: every generated claim needs its own source-support judgment`);
        const top = captured.rankedDocumentIds.slice(0, 5), relevant = Object.keys(grades).filter(doc => grades[doc] > 0), hits = top.filter(doc => grades[doc] > 0).length;
        const ideal = Object.values(grades).sort((a, b) => b - a).slice(0, 5).reduce((sum, grade, index) => sum + gain(grade) / Math.log2(index + 2), 0);
        const dcg = top.reduce((sum, doc, index) => sum + gain(grades[doc]) / Math.log2(index + 2), 0), first = top.findIndex(doc => grades[doc] > 0);
        return { id: row.id, category: row.category, precisionAt5: hits / 5, recallAt5: relevant.length ? hits / relevant.length : null,
            recallBasis: row.relevantPoolExhaustive ? 'exhaustive-reviewed-pool' : 'judged-pool-only', mrrAt5: first < 0 ? 0 : 1 / (first + 1), ndcgAt5: ideal ? dcg / ideal : null,
            supportedClaims: judged.claims.filter(c => c.supported).length, totalClaims: judged.claims.length, answerSupported: judged.answerSupported,
            expectedAbstention: row.expectedAbstention, abstained: captured.abstained, abstentionAppropriate: judged.abstentionAppropriate, scopeSatisfied: judged.scopeSatisfied,
            searchLatencyMs: captured.searchLatencyMs, assistantLatencyMs: captured.assistantLatencyMs };
    });
    function aggregate(rows) {
        return { cases: rows.length, retrieval: { precisionAt5: average(rows.map(r => r.precisionAt5)), judgedPoolRecallAt5: average(rows.map(r => r.recallAt5).filter(x => x !== null)), mrrAt5: average(rows.map(r => r.mrrAt5)), ndcgAt5: average(rows.map(r => r.ndcgAt5).filter(x => x !== null)) },
            grounding: { claimSupportRate: rate(rows.reduce((n, r) => n + r.supportedClaims, 0), rows.reduce((n, r) => n + r.totalClaims, 0)), supportedAnswerRate: average(rows.map(r => Number(r.answerSupported))) },
            abstention: { rate: average(rows.map(r => Number(r.abstained))), appropriateRate: average(rows.map(r => Number(r.abstentionAppropriate))), expectedRate: average(rows.map(r => Number(r.expectedAbstention))) }, scopeSatisfiedRate: average(rows.map(r => Number(r.scopeSatisfied))) };
    }
    // No invented pass threshold: this is an evaluation report for explicit domain
    // acceptance, not an automatic declaration of production relevance/entailment.
    return { version: 1, kind: holdout.kind, holdoutSha256: digest(holdout), runSha256: digest(run), judgmentsSha256: digest(judgments), corpusSha256: run.corpusSha256, codeRevision: run.codeRevision,
        acceptance: 'requires-domain-review', overall: aggregate(perCase), byCategory: Object.fromEntries(categories.map(category => [category, aggregate(perCase.filter(row => row.category === category))])), perCase };
}
export async function collectHoldout(rawHoldout, { apiBase, corpusSha256, codeRevision, allowProviderCalls = false, fetchImpl = fetch }) {
    const holdout = readHoldout(rawHoldout);
    const url = new URL(apiBase);
    if (url.username || url.password || url.search || url.hash || url.pathname !== '/' || (!['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname) && url.protocol !== 'https:'))
        throw new Error('Use an explicit isolated HTTPS API origin or loopback');
    if (!allowProviderCalls)
        throw new Error('Collection requires explicit --allow-provider-calls; it can consume paid inference');
    if (corpusSha256 !== holdout.corpusSha256 || !/^[a-f0-9]{40}$/.test(codeRevision || ''))
        throw new Error('Explicit corpus SHA-256 and code revision are required');
    const startedAt = new Date().toISOString(), rows = [];
    let capturedRevision;
    async function request(route, payload) { const started = Date.now(); const response = await fetchImpl(new URL(route, url), { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload), signal: AbortSignal.timeout(45000), redirect: 'error' }); if (!response.ok)
        throw new Error(`Holdout API returned ${response.status}; capture is incomplete`); const length = Number(response.headers.get('content-length')); if (length > 2 * 1024 * 1024)
        throw new Error('Holdout response exceeds bound'); let bytes = 0; const reader = response.body.getReader(), parts = []; try {
        while (true) {
            const { value, done } = await reader.read();
            if (done)
                break;
            bytes += value.byteLength;
            if (bytes > 2 * 1024 * 1024)
                throw new Error('Holdout response exceeds bound');
            parts.push(value);
        }
    }
    catch (error) {
        await reader.cancel().catch(() => { });
        throw error;
    } return { body: JSON.parse(Buffer.concat(parts).toString('utf8')), latency: Date.now() - started }; }
    for (const item of holdout.cases) {
        const search = await request('/search', { query: item.query, filters: item.filters, limit: 5 });
        const assistant = await request('/api/assistant/chat', { messages: [{ role: 'user', content: item.query }], filters: item.filters, limit: 6 });
        if (!Array.isArray(search.body.results) || !Array.isArray(assistant.body.claims) || assistant.body.assessmentVersion !== 'claims-1')
            throw new Error('Unexpected search or assistant response contract');
        const revision = search.body.corpusRevision;
        if (typeof revision !== 'string' || (assistant.body.evidenceCorpusRevision !== revision && !(assistant.body.model === 'deterministic-clarification' && assistant.body.claims.length === 0 && assistant.body.evidenceStatus === 'no_evidence')) || (capturedRevision !== undefined && revision !== capturedRevision))
            throw new Error('Corpus revision changed or could not be verified during capture');
        capturedRevision = revision;
        rows.push({ id: item.id, corpusRevision: revision, rankedDocumentIds: [...new Set(search.body.results.map(row => row.documentId))], claims: assistant.body.claims.map(({ id, text, evidence }) => ({ id, text, evidence: evidence.map(({ documentId, chunkId, citationAnchor, quote, excerpt }) => ({ documentId, chunkId, citationAnchor, quote, excerpt })) })), answer: assistant.body.answer, abstained: assistant.body.evidenceStatus === 'no_evidence', citations: assistant.body.citations.map(row => row.documentId), retrievalContext: assistant.body.retrievalContext, searchLatencyMs: search.latency, assistantLatencyMs: assistant.latency });
    }
    return runSchema.parse({ version: 1, holdoutSha256: digest(holdout), corpusSha256, codeRevision, startedAt, completedAt: new Date().toISOString(), rows });
}
