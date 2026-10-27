import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import net from 'node:net';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url), here = path.dirname(fileURLToPath(import.meta.url));
const apiRoot = path.resolve(here, '../..'), wrangler = require.resolve('wrangler');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
export async function eventually(fn, { timeout = 20000, interval = 100 } = {}) { const end = Date.now() + timeout; let error; while (Date.now() < end) {
    try {
        return await fn();
    }
    catch (next) {
        error = next;
    }
    await sleep(interval);
} throw error || new Error('Fixture condition did not become ready'); }
async function availablePort() { const server = net.createServer(); await new Promise(resolve => server.listen(0, '127.0.0.1', resolve)); const { port } = server.address(); await new Promise(resolve => server.close(resolve)); return port; }
function cleanEnv(extra = {}) { const env = { ...process.env, CI: '1', WRANGLER_SEND_METRICS: 'false', ...extra }; for (const key of ['CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID', 'CLOUDFLARE_API_KEY', 'CLOUDFLARE_EMAIL'])
    delete env[key]; return env; }
async function command(args, env) { const child = spawn(process.execPath, [wrangler, ...args], { cwd: apiRoot, env, stdio: ['ignore', 'pipe', 'pipe'] }); let output = ''; child.stdout.on('data', data => output = (output + data).slice(-20000)); child.stderr.on('data', data => output = (output + data).slice(-20000)); const timer = setTimeout(() => child.kill('SIGKILL'), 60000); let code; try {
    code = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); });
}
finally {
    clearTimeout(timer);
} if (code !== 0)
    throw new Error(`Fixture setup command failed (${code}): ${output}`); }
async function stopProcess(child) { if (child.exitCode !== null)
    return; const done = new Promise(resolve => child.once('exit', resolve)); try {
    process.kill(-child.pid, 'SIGTERM');
}
catch {
    child.kill('SIGTERM');
} await Promise.race([done, sleep(2000)]); if (child.exitCode === null) {
    try {
        process.kill(-child.pid, 'SIGKILL');
    }
    catch {
        child.kill('SIGKILL');
    }
    await done;
} }
async function provider() {
    let mode = 'valid', calls = 0, cancelled = 0;
    const server = createServer(async (request, response) => {
        if (request.url === '/fixture/audit') {
            response.setHeader('content-type', 'application/json');
            response.end(JSON.stringify({ mode, calls, cancelled }));
            return;
        }
        let raw = '';
        for await (const data of request) {
            raw += data;
            if (raw.length > 1024 * 1024) {
                response.writeHead(413).end();
                return;
            }
        }
        if (request.url === '/fixture/control') {
            mode = JSON.parse(raw).mode;
            response.end('{}');
            return;
        }
        if (request.url !== '/fixture/chat/completions') {
            response.writeHead(404).end();
            return;
        }
        calls++;
        const input = JSON.parse(raw), data = JSON.parse(input.messages.find(message => message.role === 'user').content);
        response.setHeader('content-type', 'application/json');
        if (mode === 'oversized') {
            const chunk = 'x'.repeat(65536);
            let chunks = 0;
            const timer = setInterval(() => { if (chunks++ >= 32) {
                clearInterval(timer);
                response.end();
            }
            else
                response.write(chunk); }, 2);
            response.on('close', () => { clearInterval(timer); cancelled++; });
            return;
        }
        if (mode === 'malformed') {
            response.end('{invalid');
            return;
        }
        const source = data.evidence?.[0];
        const content = source ? JSON.stringify({ claims: [{ text: source.text, evidence: [{ sourceId: mode === 'unknown_id' ? 'invented' : source.id, quote: source.text }] }] }) : data.authorities?.length ? `1. ${data.authorities[0].excerpt} [${data.authorities[0].citationId}]` : JSON.stringify({ claims: [] });
        response.end(JSON.stringify({ choices: [{ message: { content }, finish_reason: 'stop' }] }));
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    return { url: `http://127.0.0.1:${server.address().port}/fixture`, async stop() { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); } };
}
export async function createWorkerFixture({ port, webPort = 5819 } = {}) {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'beedle-hermetic-'));
    const fake = await provider();
    let child;
    try {
        port = port || await availablePort();
        const url = `http://127.0.0.1:${port}`;
        const rateNames = ['SEARCH', 'INGEST', 'LLM', 'ADMIN_WRITE', 'REVIEW_READ', 'EXPORT'];
        const config = { name: 'beedle-hermetic-test', main: path.join(here, 'worker.ts'), compatibility_date: '2025-02-15', compatibility_flags: ['nodejs_compat'], workers_dev: false, preview_urls: false,
            vars: { APP_ENV: 'preview', DEPLOYMENT_READY: 'true', CORS_ALLOWED_ORIGINS: `http://127.0.0.1:${webPort}`, SOURCE_PROXY_BASE_URL: url, R2_PUBLIC_BASE_URL: 'https://example.invalid/fixture', VECTOR_NAMESPACE: 'beedle-docs-preview', AI_EMBEDDING_MODEL: 'fixture', AI_EMBEDDING_DIMENSIONS: '2', VECTOR_LIFECYCLE_READY: 'true', VECTOR_FILTERS_READY: 'true', LLM_BASE_URL: fake.url, LLM_API_KEY: 'synthetic-only', LLM_MODEL: 'synthetic', FIXTURE_PROVIDER_URL: fake.url, COMPUTE_BUDGETS_JSON: JSON.stringify({ llm: { perMinute: 200, perDay: 2000 }, embedding: { perMinute: 1000, perDay: 10000 }, vector_write: { perMinute: 1000, perDay: 10000 } }) },
            ratelimits: rateNames.map((name, index) => ({ name: name + '_RATE_LIMIT', namespace_id: String(752001 + index), simple: { limit: 1000, period: 60 } })),
            d1_databases: [{ binding: 'DB', database_name: 'beedle-hermetic-isolated', database_id: '00000000-0000-0000-0000-000000000019', migrations_dir: path.resolve(apiRoot, 'migrations') }],
            r2_buckets: [{ binding: 'SOURCE_BUCKET', bucket_name: 'beedle-hermetic-sources' }, { binding: 'FIXTURE_VECTORS', bucket_name: 'beedle-hermetic-vectors' }],
            queues: { producers: [{ binding: 'VECTOR_JOBS_QUEUE', queue: 'fixture-vectors' }], consumers: [{ queue: 'fixture-vectors', max_batch_size: 1, max_batch_timeout: 1, max_retries: 2 }] } };
        const configPath = path.join(dir, 'wrangler.json');
        await fs.writeFile(configPath, JSON.stringify(config));
        const state = path.join(dir, 'state');
        const env = cleanEnv({ WRANGLER_LOG_PATH: path.join(dir, 'wrangler.log') });
        await command(['d1', 'migrations', 'apply', 'DB', '--config', configPath, '--local', '--persist-to', state], env);
        await command(['d1', 'execute', 'DB', '--config', configPath, '--local', '--persist-to', state, '--file', path.join(here, 'control.sql')], env);
        child = spawn(process.execPath, [wrangler, 'dev', '--config', configPath, '--local', '--persist-to', state, '--port', String(port), '--ip', '127.0.0.1', '--inspector-port', '0'], { cwd: apiRoot, env, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
        let logs = '';
        for (const stream of [child.stdout, child.stderr])
            stream.on('data', data => logs = (logs + data).slice(-30000));
        await eventually(async () => { if (child.exitCode !== null)
            throw new Error(`Fixture Worker exited: ${logs}`); const response = await fetch(url + '/__test/ready', { signal: AbortSignal.timeout(1000) }); if (!response.ok)
            throw new Error(`Fixture not ready: ${logs}`); return response; }, { timeout: 45000 });
        return { url, dir, async request(route, body, { status = 200, method = body === undefined ? 'GET' : 'POST' } = {}) { const response = await fetch(url + route, { method, headers: body === undefined ? {} : { 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(20000) }); const raw = await response.text(); let value; try {
                value = JSON.parse(raw);
            }
            catch {
                throw new Error(`${method} ${route}: non-JSON status ${response.status}: ${raw.slice(0, 1500)}; ${logs.slice(-3000)}`);
            } if (response.status !== status)
                throw Object.assign(new Error(`${method} ${route}: expected ${status}, got ${response.status}: ${JSON.stringify(value)}`), { status: response.status }); return value; }, async stop() { await stopProcess(child); await fake.stop(); await fs.rm(dir, { recursive: true, force: true }); }, logs: () => logs };
    }
    catch (error) {
        if (child)
            await stopProcess(child);
        await fake.stop();
        await fs.rm(dir, { recursive: true, force: true });
        throw error;
    }
}
