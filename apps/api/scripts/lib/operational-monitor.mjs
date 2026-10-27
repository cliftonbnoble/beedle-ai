const healthMetrics = ['vector_total', 'vector_queued', 'vector_processing', 'vector_failed', 'vector_legacy_held', 'vector_expired_leases', 'vector_oldest_due_seconds', 'vector_completed_incomplete', 'maintenance_queued', 'maintenance_running', 'maintenance_failed', 'maintenance_expired_leases', 'maintenance_oldest_due_seconds', 'reviewer_pending', 'reviewer_build_pending', 'substring_build_pending', 'phrase_build_pending', 'facet_build_pending', 'search_recovery_running'];
const classes = new Set(['keyword', 'exact_phrase', 'citation_lookup', 'party_name', 'index_code', 'rules_ordinance']);
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const noGeneration = new Set(['no_evidence', 'clarification_required']);
const finite = value => typeof value === 'number' && Number.isFinite(value) && value >= 0;
const rate = (num, den) => den ? num / den : null;
export function extractOperationalRecords(value) {
    if (value?.telemetryVersion === 1 && ['operation_complete', 'job_health'].includes(value.event))
        return [value];
    const records = [];
    for (const log of Array.isArray(value?.logs) ? value.logs : [])
        for (const item of Array.isArray(log.message) ? log.message : []) {
            let candidate = item;
            if (typeof item === 'string') {
                try {
                    candidate = JSON.parse(item);
                }
                catch {
                    continue;
                }
            }
            if (candidate?.telemetryVersion === 1 && ['operation_complete', 'job_health'].includes(candidate.event))
                records.push(candidate);
        }
    return records;
}
function quantile(rows, p) { if (!rows.length)
    return null; const sorted = rows.slice().sort((a, b) => a.ms - b.ms), target = sorted.reduce((sum, row) => sum + row.weight, 0) * p; let current = 0; for (const row of sorted) {
    current += row.weight;
    if (current >= target)
        return row.ms;
} return sorted.at(-1).ms; }
export function summarizeOperations(input, policy, { environment, from, to, releaseRevision = null, representative = false } = {}) {
    if (!['preview', 'production'].includes(environment) || !Number.isFinite(from) || !Number.isFinite(to) || to <= from || to - from > 24 * 3600000)
        throw new Error('An explicit environment and ordered window of at most 24 hours are required');
    if (releaseRevision !== null && !/^[a-f0-9]{40}$/.test(releaseRevision))
        throw new Error('Use a full committed release revision');
    const seen = new Set(), http = [], health = [];
    let duplicates = 0, ignored = 0;
    for (const record of input) {
        const time = Date.parse(record.observedAt);
        if (record.environment !== environment || !uuid.test(record.requestId || '') || !Number.isFinite(time) || time < from || time > to || releaseRevision && record.releaseRevision !== releaseRevision) {
            ignored++;
            continue;
        }
        const key = record.event + ':' + record.requestId;
        if (seen.has(key)) {
            duplicates++;
            continue;
        }
        seen.add(key);
        if (record.event === 'job_health') {
            const metrics = Object.fromEntries(healthMetrics.filter(key => finite(record.metrics?.[key])).map(key => [key, record.metrics[key]]));
            if (Object.keys(metrics).length !== healthMetrics.length)
                throw new Error('An incomplete health sample cannot establish health');
            health.push({ time, metrics });
        }
        else if (record.kind === 'http') {
            if (!finite(record.durationMs) || !finite(record.status) || !Number.isInteger(record.status) || record.status < 100 || record.status > 599 || !finite(record.sampleRate) || record.sampleRate < 0.000001 || record.sampleRate > 1)
                throw new Error('Invalid aggregate request sample');
            const fallback = [...(record.tags?.assistant_fallback || []), ...(record.tags?.draft_fallback || [])].filter(reason => !noGeneration.has(reason));
            const eligible = Number(record.operations?.llm?.calls || 0) > 0 || fallback.length > 0;
            http.push({ weight: 1 / record.sampleRate, status: record.status, ms: record.durationMs, queryClass: classes.has(record.queryClass) ? record.queryClass : 'other',corpusRevision:/^\d{1,20}$/.test(record.corpusRevision||'')?record.corpusRevision:null,capability:['semantic_enabled','lexical_only'].includes(record.retrievalCapability)?record.retrievalCapability:'unspecified', eligible, fallback: fallback.length > 0,
                llmCalls: finite(record.operations?.llm?.calls) ? record.operations.llm.calls : 0, llmFailures: finite(record.operations?.llm?.failed) ? record.operations.llm.failed : 0,
                searchCalls: finite(record.counters?.search_calls) ? record.counters.search_calls : 0, searchResults: finite(record.counters?.search_results) ? record.counters.search_results : 0,
                sqlRead: finite(record.counters?.db_rows_read_reported) ? record.counters.db_rows_read_reported : 0, sqlMissing: finite(record.counters?.db_meta_missing) ? record.counters.db_meta_missing : 0 });
        }
    }
    const weighted = (rows, predicate = () => true) => rows.filter(predicate).reduce((sum, row) => sum + row.weight, 0), estimated = weighted(http), eligible = http.filter(row => row.eligible);
    const successful = http.filter(row => row.status >= 200 && row.status < 300), serverErrorRate = rate(weighted(http, row => row.status >= 500), estimated), generationFallbackRate = rate(weighted(eligible, row => row.fallback), weighted(eligible));
    const latest = health.slice().sort((a, b) => b.time - a.time)[0], alerts = [];
    if (!latest || to - latest.time > policy.heartbeatMaxAgeSeconds * 1000)
        alerts.push({ id: 'missing-health-heartbeat', severity: 'high', action: 'Check the scheduled Worker and log delivery; absence of telemetry is not evidence of health.' });
    const separated = [];
    for (const sample of health.slice().sort((a, b) => b.time - a.time))
        if (!separated.length || separated.at(-1).time - sample.time >= policy.minimumHealthSeparationSeconds * 1000)
            separated.push(sample);
    if (latest && to - latest.time <= policy.heartbeatMaxAgeSeconds * 1000)
        for (const rule of policy.health) {
            const samples = separated.slice(0, rule.consecutiveSamples);
            if (samples.length === rule.consecutiveSamples && samples.every(row => row.metrics[rule.metric] > rule.threshold))
                alerts.push({ id: rule.id, severity: rule.severity, action: rule.action });
        }
    for (const rule of policy.rates) {
        const value = rule.metric === 'serverErrorRate' ? serverErrorRate : generationFallbackRate, raw = rule.metric === 'serverErrorRate' ? http.length : eligible.length;
        if (raw >= rule.minimumSampled && value !== null && value > rule.threshold)
            alerts.push({ id: rule.id, severity: rule.severity, action: rule.action });
    }
    const corpusRevisions=[...new Set(http.map(row=>row.corpusRevision).filter(Boolean))].sort();
    const capabilities=[...new Set(http.map(row=>row.capability))].sort();
    const latency = rows => ({ sampled: rows.length, p50Milliseconds: quantile(rows, .5), p95Milliseconds: quantile(rows, .95) });
    return { version: 1, environment, releaseRevision, from: new Date(from).toISOString(), to: new Date(to).toISOString(), sampledRequests: http.length, corpusRevisions,capabilities,estimatedRequests: estimated, duplicates, ignored,
        serverErrorRate, quotaRate: rate(weighted(http, row => row.status === 429), estimated), generationSampled: eligible.length, generationFallbackRate,
        providerOperationFailureRate: rate(http.reduce((n, row) => n + row.weight * row.llmFailures, 0), http.reduce((n, row) => n + row.weight * row.llmCalls, 0)),
        zeroResultRate: rate(weighted(http, row => row.searchCalls > 0 && row.searchResults === 0), weighted(http, row => row.searchCalls > 0)),
        successfulLatency: latency(successful), byQueryClass: Object.fromEntries([...classes, 'other'].map(key => [key, latency(successful.filter(row => row.queryClass === key))])),
        reportedSqlRowsReadEstimate: http.reduce((n, row) => n + row.weight * row.sqlRead, 0), sqlMetadataMissingEstimate: http.reduce((n, row) => n + row.weight * row.sqlMissing, 0),
        latestHealth: latest ? { observedAt: new Date(latest.time).toISOString(), metrics: latest.metrics } : null,
        baseline: { kind: representative ? 'declared_representative' : 'diagnostic_only', readyForSloProposal: representative && Boolean(releaseRevision) && http.filter(row=>row.searchCalls>0).length>=1000&&corpusRevisions.length===1&&capabilities.length===1&&capabilities[0]!=='unspecified'&&Boolean(latest&&to-latest.time<=policy.heartbeatMaxAgeSeconds*1000)&&to-from>=3600000, sloStatus: policy.sloStatus, targets: policy.latencySlo }, alerts };
}
