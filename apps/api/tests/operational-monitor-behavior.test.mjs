import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { extractOperationalRecords, summarizeOperations } from '../scripts/lib/operational-monitor.mjs';
const policy = JSON.parse(readFileSync(new URL('../observability/alerts.json', import.meta.url), 'utf8'));
const now = Date.parse('2026-10-26T12:00:00.000Z');
const window = { environment: 'preview', from: now - 3600000, to: now };
const zeroMetrics = { vector_total: 0, vector_queued: 0, vector_processing: 0, vector_failed: 0, vector_legacy_held: 0, vector_expired_leases: 0, vector_oldest_due_seconds: 0, vector_completed_incomplete: 0, maintenance_queued: 0, maintenance_running: 0, maintenance_failed: 0, maintenance_expired_leases: 0, maintenance_oldest_due_seconds: 0, reviewer_pending: 0, reviewer_build_pending: 0, substring_build_pending: 0, phrase_build_pending: 0, facet_build_pending: 0, search_recovery_running: 0 };
const health = (seconds = 0, metrics = {}) => ({ event: 'job_health', telemetryVersion: 1, requestId: randomUUID(), environment: 'preview', observedAt: new Date(now - seconds * 1000).toISOString(), metrics: { ...zeroMetrics, ...metrics } });
const request = (status = 200, sampleRate = 1, ms = 10, extras = {}) => ({ event: 'operation_complete', telemetryVersion: 1, kind: 'http', requestId: randomUUID(), environment: 'preview', observedAt: new Date(now).toISOString(), status, sampleRate, durationMs: ms, queryClass: 'keyword', ...extras });
test('aggregate monitor uses sampling weights, successful latency and separate provider/research fallback denominators', () => {
    const samples = [health(), request(200, .1, 20, { counters: { search_calls: 1, search_results: 0 } }), request(500, 1, 100), request(200, 1, 30, { operations: { llm: { calls: 1, failed: 0 } }, tags: { assistant_fallback: ['invalid_claim_structure'] }, counters: { search_calls: 1, search_results: 2 } })];
    const report = summarizeOperations(samples, policy, window);
    assert.equal(report.estimatedRequests, 12);
    assert.equal(report.zeroResultRate, 10 / 11);
    assert.equal(report.serverErrorRate, 1 / 12);
    assert.equal(report.successfulLatency.p50Milliseconds, 20);
    assert.equal(report.successfulLatency.p95Milliseconds, 30);
    assert.equal(report.generationFallbackRate, 1);
    assert.equal(report.providerOperationFailureRate, 0);
    assert.equal(report.baseline.readyForSloProposal, false);
    assert.equal(report.baseline.targets.p95Milliseconds, null);
});
test('health rules require distinct separated samples and missing heartbeat cannot be mistaken for health', () => {
    const one = health(0, { vector_expired_leases: 1 });
    assert.ok(!summarizeOperations([one, one], policy, window).alerts.some(row => row.id === 'vector-stalled-lease'));
    assert.ok(summarizeOperations([one, health(300, { vector_expired_leases: 1 })], policy, window).alerts.some(row => row.id === 'vector-stalled-lease'));
    assert.ok(summarizeOperations([health(721)], policy, window).alerts.some(row => row.id === 'missing-health-heartbeat'));
    assert.ok(summarizeOperations([health(0, { vector_completed_incomplete: 1 })], policy, window).alerts.some(row => row.id === 'vector-completed-incomplete'));
});
test('duplicate log delivery, private wrappers and cross-environment rows cannot inflate a baseline or leak facts', () => {
    const entry = request(200, .1, 20, { query: 'PRIVATE-QUESTION', route: '/source/PRIVATE-CASE' });
    const wrapper = { event: { request: { url: 'https://private.example/PRIVATE-QUERY' } }, logs: [{ message: [entry] }] };
    const extracted = extractOperationalRecords(wrapper);
    assert.equal(extracted.length, 1);
    assert.equal(extractOperationalRecords({ logs: [{ message: [JSON.stringify(entry)] }] }).length, 1);
    const result = summarizeOperations([entry, entry, { ...entry, requestId: randomUUID(), environment: 'production' }, health(0, { PRIVATE_CUSTOMER: 1 })], policy, window);
    assert.equal(result.sampledRequests, 1);
    assert.equal(result.duplicates, 1);
    assert.equal(result.ignored, 1);
    assert.ok(!JSON.stringify(result).includes('PRIVATE'));
});
test('malformed metrics fail closed and rate alerts require enough actual sampled observations', () => {
    assert.throws(() => summarizeOperations([request(200, 0)], policy, window), /Invalid aggregate/);
    assert.throws(() => summarizeOperations([health(0, { vector_total: null })], policy, window), /incomplete health/);
    const few = Array.from({ length: 3 }, () => request(500));
    assert.ok(!summarizeOperations([...few, health()], policy, window).alerts.some(row => row.id === 'http-server-errors'));
    const sufficient = Array.from({ length: 100 }, () => request(500));
    assert.ok(summarizeOperations([...sufficient, health()], policy, window).alerts.some(row => row.id === 'http-server-errors'));
});

test('report CLI projects private JSONL, writes a new private artifact and refuses overwrite or oversized input',async()=>{
 const fs=await import('node:fs/promises'),os=await import('node:os'),path=await import('node:path'),{execFileSync}=await import('node:child_process');const dir=await fs.mkdtemp(path.join(os.tmpdir(),'beedle-m21-report-'));
 try{const input=path.join(dir,'events.jsonl'),out=path.join(dir,'report.json');await fs.writeFile(input,[health(),request(200,.1,20,{query:'PRIVATE-LEGAL-FACT'})].map(row=>JSON.stringify(row)).join('\n')+'\n',{mode:0o600});
 const args=['scripts/operational-telemetry-report.mjs','--input',input,'--out',out,'--environment','preview','--from',new Date(window.from).toISOString(),'--to',new Date(window.to).toISOString()];
 const stdout=execFileSync(process.execPath,args,{encoding:'utf8'});const result=JSON.parse(await fs.readFile(out,'utf8'));assert.equal(result.estimatedRequests,10);assert.equal(result.alerts.length,0);assert.equal(result.baseline.readyForSloProposal,false);assert.equal((await fs.stat(out)).mode&0o777,0o600);assert.ok(!stdout.includes('PRIVATE'));assert.ok(!JSON.stringify(result).includes('PRIVATE'));
 assert.throws(()=>execFileSync(process.execPath,args,{stdio:'pipe'}));await fs.truncate(input,64*1024*1024+1);assert.throws(()=>execFileSync(process.execPath,[...args.slice(0,4),path.join(dir,'oversize.json'),...args.slice(5)],{stdio:'pipe'}));
 }finally{await fs.rm(dir,{recursive:true,force:true});}
});

test('SLO proposal eligibility requires real sample volume, one known cohort and a fresh heartbeat while targets stay unset',()=>{
 const release='a'.repeat(40),samples=Array.from({length:1000},()=>request(200,.1,30,{releaseRevision:release,corpusRevision:'42',retrievalCapability:'lexical_only',counters:{search_calls:1,search_results:2}})),heartbeat={...health(),releaseRevision:release};
 const options={...window,releaseRevision:release,representative:true};
 assert.equal(summarizeOperations([...samples,heartbeat],policy,options).baseline.readyForSloProposal,true);assert.equal(summarizeOperations([...samples,heartbeat],policy,options).baseline.targets.p95Milliseconds,null);
 const mixed=structuredClone(samples);mixed[0].corpusRevision='43';assert.equal(summarizeOperations([...mixed,heartbeat],policy,options).baseline.readyForSloProposal,false);
 assert.equal(summarizeOperations([...Array(1000).fill(samples[0]),heartbeat],policy,options).baseline.readyForSloProposal,false);assert.equal(summarizeOperations(samples,policy,options).baseline.readyForSloProposal,false);
});
