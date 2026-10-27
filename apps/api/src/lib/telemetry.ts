import type { Env } from './types';
export type Operation = 'd1' | 'r2_get' | 'r2_put' | 'r2_other' | 'ai_run' | 'vector_query' | 'vector_read' | 'vector_write' | 'llm';
type Kind = 'http' | 'queue' | 'scheduled' | 'background';
type Tag = 'assistant_fallback' | 'draft_fallback' | 'embedding_result' | 'vector_result' | 'quota' | 'pipeline' | 'job_state' | 'llm_result' | 'error';
type Counter = 'db_statements' | 'db_rows_returned' | 'db_rows_read_reported' | 'db_meta_missing' | 'embedding_texts_requested' | 'embedding_texts_submitted' | 'vectors_submitted' | 'ingest_warnings' | 'search_results' | 'search_calls';
const reasons = new Set(['ok', 'unconfigured', 'no_evidence', 'clarification_required', 'timeout', 'cancelled', 'response_too_large', 'invalid_response', 'truncated', 'provider_rejected', 'provider_rate_limited', 'network_failure', 'invalid_claim_structure', 'unknown_evidence_id', 'quote_not_in_evidence', 'invalid_evidence', 'claim_not_related_to_quotes', 'possible_contradiction', 'provision_not_in_quotes', 'unmatched_number_or_provision', 'unavailable', 'schema_unavailable', 'operation_failed', 'request_limited', 'compute_limited', 'budget_unavailable', 'query_failed', 'lifecycle_pending', 'scope_unrepresentable', 'queued', 'dispatched', 'completed', 'continued', 'retryable', 'failed', 'skipped', 'lease_lost', 'dispatch_deferred', 'invalid_message']);
const stageNames = new Set(['scope_build', 'lexical_scope_fetch', 'lexical_search', 'vector_search', 'vector_chunk_fetch', 'initial_scoring', 'issue_seed_scope_prep', 'decision_scope_fallback_fetch', 'decision_scope_fetch', 'decision_scope_build', 'finalize_results']);
const queryClasses = new Set(['keyword', 'exact_phrase', 'citation_lookup', 'party_name', 'index_code', 'rules_ordinance']);
const bounded = (value: number) => Number.isFinite(value) ? Math.max(0, Math.min(1e12, value)) : 0;
export function operationalReason(error: unknown): string {
    if (error && typeof error === 'object') {
        const value = error as {
            reason?: unknown;
            name?: unknown;
            message?: unknown;
            retryAfterSeconds?: unknown;
        };
        if (typeof value.reason === 'string' && reasons.has(value.reason))
            return value.reason;
        if (value.name === 'AbortError')
            return 'cancelled';
        if (typeof value.retryAfterSeconds === 'number')
            return 'compute_limited';
        if (typeof value.message === 'string' && /no such table|no such column|malformed database schema|SQLITE_CORRUPT/i.test(value.message))
            return 'schema_unavailable';
    }
    return 'operation_failed';
}
type Bindings = Pick<Env, 'DB' | 'SOURCE_BUCKET' | 'AI' | 'VECTOR_INDEX'>;
export class RequestTelemetry {
    readonly id = crypto.randomUUID();
    readonly startedAt = performance.now();
    readonly operations: Partial<Record<Operation, {
        calls: number;
        failed: number;
        active: number;
        ms: number;
    }>> = {};
    private counters: Partial<Record<Counter, number>> = {};
    private tags: Partial<Record<Tag, string[]>> = {};
    private stages: Record<string, number> = {};
    private providerStatuses:Record<string,number>={};
    private queryClass: string | null = null;
    private revision: string | null = null;
    releaseRevision: string | null = null;
    retrievalCapability:'semantic_enabled'|'lexical_only'|'unspecified'='unspecified';
    private budget?: () => Record<string, number>;
    private finished = false;
    bindings?: Bindings;
    constructor(readonly kind: Kind, readonly route: string, readonly environment: string, readonly successSampleRate = 0.1, readonly parentId: string | null = null, private readonly sink: (record: Record<string, unknown>) => void = record => console.info(record)) { }
    count(name: Counter, value = 1) { if (!Number.isFinite(value) || value < 0)
        return; if (!this.finished)
        this.counters[name] = bounded((this.counters[name] || 0) + value); }
    tag(name: Tag, reason: string) { if (this.finished)
        return; const safe = reasons.has(reason) ? reason : 'operation_failed'; const values = this.tags[name] || []; if (!values.includes(safe) && values.length < 8)
        this.tags[name] = [...values, safe]; }
    stage(name: string, details: Record<string, unknown>) { if (!this.finished && stageNames.has(name) && typeof details.ms === 'number')
        this.stages[name] = bounded((this.stages[name] || 0) + details.ms); }
    search(count: number, queryClass: string, revision?: string) { this.count('search_calls'); this.count('search_results', count); this.queryClass = queryClasses.has(queryClass) ? queryClass : 'other'; if (revision && /^\d{1,20}$/.test(revision))
        this.revision = revision; }
    workBudget(snapshot: () => Record<string, number>) { this.budget = snapshot; }
    async measure<T>(name: Operation, operation: () => Promise<T>, observe?: (value: T) => void): Promise<T> {
        const stat = this.operations[name] ||= { calls: 0, failed: 0, active: 0, ms: 0 };
        stat.calls++;
        stat.active++;
        const start = performance.now();
        try {
            const value = await operation();
            try {
                observe?.(value);
            }
            catch { /* Metrics never change binding semantics. */ }
            return value;
        }
        catch (error) {
            stat.failed++;
            throw error;
        }
        finally {
            stat.active--;
            stat.ms = bounded(stat.ms + performance.now() - start);
        }
    }
    providerStatus(status:number){if(this.finished||!Number.isInteger(status)||status<100||status>599)return;const key=String(status);if(Object.hasOwn(this.providerStatuses,key)||Object.keys(this.providerStatuses).length<8)this.providerStatuses[key]=(this.providerStatuses[key]||0)+1;}
    elapsed() { return bounded(performance.now() - this.startedAt); }
    private emit(record: Record<string, unknown>) { try {
        this.sink(record);
    }
    catch { /* Observability failure must not fail the application. */ } }
    async job(documentId: string, generationId: string, state: string, attempts: number, completed: number) {
        try {
            const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(['vector', documentId, generationId])));
            const jobId = Array.from(new Uint8Array(bytes).slice(0, 16), n => n.toString(16).padStart(2, '0')).join('');
            this.tag('job_state', state);
            this.emit({ event: 'vector_job', telemetryVersion: 1, requestId: this.id, parentId: this.parentId, jobId, outcome: reasons.has(state) ? state : 'failed', attempts: bounded(attempts), completed: bounded(completed), environment: this.environment, releaseRevision: this.releaseRevision, observedAt: new Date().toISOString() });
        }
        catch { /* A telemetry digest cannot alter durable job state. */ }
    }
    health(value: Record<string, number>) { this.emit({ event: 'job_health', telemetryVersion: 1, requestId: this.id, environment: this.environment, releaseRevision: this.releaseRevision, observedAt: new Date().toISOString(), metrics: Object.fromEntries(Object.entries(value).filter(([key, n]) => /^[a-z_]{1,60}$/.test(key) && typeof n === 'number' && Number.isFinite(n)).map(([key, n]) => [key, bounded(n)])) }); }
    finish(status: number) {
        if (this.finished)
            return;
        this.finished = true;
        const degraded = Object.entries(this.tags).some(([name, values]) => name === 'error' || (name === 'assistant_fallback' || name === 'draft_fallback') && values.some(value => !['no_evidence', 'clarification_required'].includes(value)) || name === 'embedding_result' && values.some(value => !['ok', 'unavailable'].includes(value)) || (name === 'vector_result' || name === 'pipeline') && values.some(value => !['ok', 'unavailable'].includes(value)) || name === 'job_state' && values.some(value => ['retryable', 'failed', 'dispatch_deferred'].includes(value)));
        const rate = this.kind !== 'http' || status >= 500 || degraded ? 1 : this.successSampleRate;
        if (rate <= 0 || parseInt(this.id.slice(0, 8), 16) / 4294967296 >= rate)
            return;
        let work: Record<string, number> | undefined;
        try {
            work = this.budget ? Object.fromEntries(Object.entries(this.budget()).filter(([key, value]) => /^[A-Za-z]{1,40}$/.test(key) && typeof value === 'number').map(([key, value]) => [key, bounded(value)])) : undefined;
        }
        catch { /* A broken diagnostics callback cannot fail a response. */ }
        this.emit({ event: 'operation_complete', telemetryVersion: 1, kind: this.kind, requestId: this.id, parentId: this.parentId, route: this.route, environment: this.environment,
            status, observedAt: new Date().toISOString(), releaseRevision: this.releaseRevision, retrievalCapability:this.retrievalCapability, durationMs: Math.round(this.elapsed() * 100) / 100, sampleRate: rate, queryClass: this.queryClass, corpusRevision: this.revision, counters: { ...this.counters },
            providerStatuses:{...this.providerStatuses},operations: Object.fromEntries(Object.entries(this.operations).map(([name, value]) => [name, { ...value, ms: Math.round(value.ms * 100) / 100 }])), tags: { ...this.tags }, stages: { ...this.stages }, searchWork: work });
    }
}
function databaseTelemetry(database: D1Database, span: RequestTelemetry): D1Database;
function databaseTelemetry(database: D1DatabaseSession, span: RequestTelemetry): D1DatabaseSession;
function databaseTelemetry(database: D1Database | D1DatabaseSession, span: RequestTelemetry): D1Database | D1DatabaseSession {
    const originalStatements = new WeakMap<D1PreparedStatement, D1PreparedStatement>();
    const rows = (result: unknown) => {
        if (result && typeof result === 'object' && 'results' in result && Array.isArray(result.results))
            span.count('db_rows_returned', result.results.length);
        const meta = result && typeof result === 'object' && 'meta' in result ? result.meta : undefined;
        if (meta && typeof meta === 'object' && 'rows_read' in meta && typeof meta.rows_read === 'number')
            span.count('db_rows_read_reported', meta.rows_read);
        else
            span.count('db_meta_missing');
    };
    function statement(target: D1PreparedStatement): D1PreparedStatement {
        const proxy = new Proxy(target, { get(object, key) {
                if (key === 'bind')
                    return (...args: unknown[]) => statement(object.bind(...args));
                if (['all', 'run', 'first', 'raw'].includes(String(key)))
                    return (...args: unknown[]) => { span.count('db_statements'); return span.measure('d1', () => Reflect.get(object, key).apply(object, args), value => { rows(value); if (key === 'first' && value !== null)
                        span.count('db_rows_returned'); if (key === 'raw' && Array.isArray(value))
                        span.count('db_rows_returned', Math.max(0, value.length - (args[0] && typeof args[0] === 'object' && 'columnNames' in args[0] && args[0].columnNames ? 1 : 0))); }); };
                const value = Reflect.get(object, key);
                return typeof value === 'function' ? value.bind(object) : value;
            } });
        originalStatements.set(proxy, target);
        return proxy;
    }
    return new Proxy(database, { get(object, key) {
            if (key === 'prepare')
                return (sql: string) => statement(object.prepare(sql));
            if (key === 'batch')
                return (statements: D1PreparedStatement[]) => { span.count('db_statements', statements.length); return span.measure('d1', () => object.batch(statements.map(item => originalStatements.get(item) || item)), result => result.forEach(rows)); };
            if (key === 'exec' && 'exec' in object)
                return (sql: string) => { span.count('db_statements'); return span.measure('d1', () => object.exec(sql), rows); };
            if (key === 'withSession' && 'withSession' in object)
                return (...args: Parameters<D1Database['withSession']>) => databaseTelemetry(object.withSession(...args), span);
            const value = Reflect.get(object, key);
            return typeof value === 'function' ? value.bind(object) : value;
        } });
}
export function telemetryEnvironment(env: Env, span: RequestTelemetry): Env {
    const bindings = env.TELEMETRY?.bindings || { DB: env.DB, SOURCE_BUCKET: env.SOURCE_BUCKET, AI: env.AI, VECTOR_INDEX: env.VECTOR_INDEX };
    span.bindings = bindings;
    const SOURCE_BUCKET = bindings.SOURCE_BUCKET && new Proxy(bindings.SOURCE_BUCKET, { get(object, key) { const value = Reflect.get(object, key); if (typeof value !== 'function')
            return value; if (!['get', 'head', 'put', 'delete', 'list'].includes(String(key)))
            return value.bind(object); return (...args: unknown[]) => span.measure(key === 'get' ? 'r2_get' : key === 'put' ? 'r2_put' : 'r2_other', () => value.apply(object, args)); } });
    const AI = bindings.AI && new Proxy(bindings.AI, { get(object, key) { const value = Reflect.get(object, key); if (key === 'run')
            return (...args: unknown[]) => span.measure('ai_run', () => value.apply(object, args)); return typeof value === 'function' ? value.bind(object) : value; } });
    const VECTOR_INDEX = bindings.VECTOR_INDEX && new Proxy(bindings.VECTOR_INDEX, { get(object, key) { const value = Reflect.get(object, key); if (typeof value !== 'function')
            return value; if (!['query', 'getByIds', 'upsert', 'deleteByIds'].includes(String(key)))
            return value.bind(object); return (...args: unknown[]) => { if (key === 'upsert' && Array.isArray(args[0]))
            span.count('vectors_submitted', args[0].length); return span.measure(key === 'query' ? 'vector_query' : key === 'upsert' || key === 'deleteByIds' ? 'vector_write' : 'vector_read', () => value.apply(object, args)); }; } });
    return { ...env, TELEMETRY: span, DB: bindings.DB ? databaseTelemetry(bindings.DB, span) : bindings.DB, SOURCE_BUCKET, AI, VECTOR_INDEX };
}
export function newTelemetry(env: Env, kind: Kind, route: string, parentId: string | null = null) {
    const configured = Number(env.TELEMETRY_SUCCESS_SAMPLE_RATE ?? (env.APP_ENV === 'local' ? '0' : '0.1'));
    const rate = Number.isFinite(configured) && (configured === 0 || configured >= 0.000001) && configured <= 1 ? configured : 0.1;
    const span = new RequestTelemetry(kind, route, ['local', 'preview', 'production'].includes(env.APP_ENV) ? env.APP_ENV : 'unknown', rate, parentId);
    span.retrievalCapability=env.VECTOR_LIFECYCLE_READY==='true'&&env.VECTOR_FILTERS_READY==='true'?'semantic_enabled':env.VECTOR_LIFECYCLE_READY==='false'&&env.VECTOR_FILTERS_READY==='false'?'lexical_only':'unspecified';
    span.releaseRevision = /^[a-f0-9]{40}$/.test(env.RELEASE_REVISION || '') ? env.RELEASE_REVISION! : null;
    return span;
}
export async function observedTask(env: Env, name: string, operation: (observed: Env) => Promise<unknown>) {
    const span = newTelemetry(env, 'background', name, env.TELEMETRY?.id || null), observed = telemetryEnvironment(env, span);
    try {
        const result = await operation(observed);
        span.finish(200);
        return result;
    }
    catch (error) {
        span.tag('error', operationalReason(error));
        span.finish(500);
        throw new Error('Background operation failed; inspect correlated telemetry');
    }
}
