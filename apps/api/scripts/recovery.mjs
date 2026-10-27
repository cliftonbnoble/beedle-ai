import fs from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { createHash, randomUUID } from 'node:crypto';
import { sha, fileSha, atomicJson, locked, open, snapshot, prepareRestore, requiredObjects, rowExpression, ident, writeRestoreSql, claimStatements } from './lib/recovery-bundle.mjs';
const require = createRequire(import.meta.url), root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const contractPath = path.join(root, 'release/contract.json');
async function toolSha() {
    return sha(await fs.readFile(fileURLToPath(import.meta.url), 'utf8') + await fs.readFile(new URL('./lib/recovery-bundle.mjs', import.meta.url), 'utf8'));
}
const usage = 'recovery.mjs capture <source.json> <new-bundle-dir> | plan <bundle-dir> <target.json> | apply <bundle-dir> <target.json> <plan-sha256> | verify <bundle-dir> <target.json>';
async function read(file) {
    return JSON.parse(await fs.readFile(file, 'utf8'));
}
async function endpoint(file, { target = false } = {}) {
    const spec = await read(file);
    if (!['local', 'remote'].includes(spec.mode) || !path.isAbsolute(spec.config))
        throw new Error('Explicit mode and absolute JSON Wrangler config required');
    const config = await read(spec.config);
    if (config.env || !Array.isArray(config.d1_databases) || config.d1_databases.length !== 1)
        throw new Error('Use a dedicated flat recovery config with exactly one D1 binding');
    const db = config.d1_databases.find(x => x.binding === 'DB'), bucket = config.r2_buckets?.find(x => x.binding === 'SOURCE_BUCKET');
    if (!db?.database_id || !db.database_name || !bucket?.bucket_name)
        throw new Error('Explicit DB identity and SOURCE_BUCKET required');
    if (spec.mode === 'remote' && !/^[a-f0-9]{32}$/.test(config.account_id || ''))
        throw new Error('Explicit Cloudflare account ID required for remote operation');
    if (spec.mode === 'local' && (!path.isAbsolute(spec.persistTo || '')))
        throw new Error('Explicit absolute local persistTo required');
    if (target && (spec.purpose !== 'isolated-recovery' || spec.writersQuiesced !== true || config.vars?.VECTOR_LIFECYCLE_READY !== 'false' || config.vars?.VECTOR_FILTERS_READY !== 'false' || config.vars?.RELEASE_MAINTENANCE_ONLY !== 'true' || config.queues?.consumers?.length || config.triggers?.crons?.length))
        throw new Error('Target must attest isolated recovery, quiesced writers, lexical-only maintenance mode and no cron/Queue consumers');
    const identity = { mode: spec.mode, accountId: config.account_id || null, databaseId: db.database_id, databaseName: db.database_name, bucket: bucket.bucket_name, configSha256: await fileSha(spec.config), persistTo: spec.mode === 'local' ? await fs.realpath(spec.persistTo) : null };
    return { ...spec, identity, identitySha256: sha(JSON.stringify(identity)) };
}
async function command(args, dir, { file } = {}) {
    const log = path.join(dir, 'wrangler-private.log');
    const handle = await fs.open(log, 'a', 0o600);
    let stderr = '';
    const env = { ...process.env, WRANGLER_SEND_METRICS: 'false', WRANGLER_LOG_PATH: path.join(dir, 'wrangler-detail-private.log') };
    delete env.CLOUDFLARE_ACCOUNT_ID;
    if (args.includes('--local'))
        for (const key of ['CLOUDFLARE_API_TOKEN', 'CLOUDFLARE_ACCOUNT_ID', 'CLOUDFLARE_API_KEY', 'CLOUDFLARE_EMAIL'])
            delete env[key];
    let stdout = '', overflow = false;
    const child = spawn(process.execPath, [require.resolve('wrangler'), ...args], { cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stderr.on('data', bytes => {
        stderr = (stderr + bytes).slice(-32000);
        handle.write(bytes).catch(() => {
        });
    });
    child.stdout.on('data', bytes => {
        if (stdout.length + bytes.length > 32 * 1024 * 1024) {
            overflow = true;
            child.kill('SIGTERM');
        }
        else
            stdout += bytes;
    });
    let killTimer;
    const timer = setTimeout(() => {
        child.kill('SIGTERM');
        killTimer = setTimeout(() => child.kill('SIGKILL'), 5000);
    }, 120000);
    let code;
    try {
        code = await new Promise((resolve, reject) => {
            child.once('error', reject);
            child.once('exit', resolve);
        });
    }
    finally {
        clearTimeout(timer);
        clearTimeout(killTimer);
        if (code !== 0 || overflow)
            await handle.write(stdout.slice(-64000));
        await handle.close();
    }
    if (code !== 0 || overflow) {
        const error = new Error('Wrangler operation failed or timed out; inspect private logs. Import acknowledgment may be uncertain: rerun apply to inspect the durable receipt.');
        error.missingObject = !overflow && stderr.includes('The specified key does not exist.');
        throw error;
    }
    if (file)
        return;
    return stdout;
}
function flags(ep) {
    return ['--config', ep.config, ep.mode === 'local' ? '--local' : '--remote', ...(ep.mode === 'local' ? ['--persist-to', ep.persistTo] : [])];
}
export async function query(ep, sql, dir) {
    const raw = await command(['d1', 'execute', 'DB', ...flags(ep), '--json', '--command', sql], dir);
    return parseRecoveryQuery(raw);
}
export function parseRecoveryQuery(raw) {
    let result;
    try { result = JSON.parse(raw); } catch { throw new Error('Recovery D1 inspection returned invalid JSON'); }
    if (!Array.isArray(result) || result.length === 0 || result.some(x => !x || x.success !== true || !Array.isArray(x.results)
        || x.results.some(row => !row || typeof row !== 'object' || Array.isArray(row))))
        throw new Error('Recovery D1 inspection returned incomplete or invalid results');
    return result.flatMap(x => x.results);
}
async function getObject(ep, key, file, dir) {
    await command(['r2', 'object', 'get', ep.identity.bucket + '/' + key, ...flags(ep), '--file', file], dir, { file: true });
}
async function putObject(ep, key, file, dir) {
    await command(['r2', 'object', 'put', ep.identity.bucket + '/' + key, ...flags(ep), '--file', file], dir, { file: true });
}
async function objectExists(ep, key, dir) {
    const escaped = key.replaceAll('"', '');
    if (escaped !== key || key.includes('\0'))
        throw new Error('Unsupported object key');
    try {
        const tmp = path.join(dir, 'object-probe-' + randomUUID());
        try {
            await getObject(ep, key, tmp, dir);
            return { exists: true, sha256: await fileSha(tmp) };
        }
        finally {
            await fs.rm(tmp, { force: true });
        }
    }
    catch (error) {
        if (error.missingObject)
            return { exists: false };
        throw error;
    }
}
async function loadBundle(dir) {
    const manifest = await read(path.join(dir, 'bundle.json'));
    if (manifest.version !== 1 || manifest.toolSha256 !== await toolSha() || manifest.contractSha256 !== await fileSha(contractPath))
        throw new Error('Recovery contract differs from the sealed bundle');
    for (const [name, hash] of Object.entries(manifest.files)) {
        if (!['source.sqlite', 'restore.sqlite'].includes(name) || await fileSha(path.join(dir, name)) !== hash)
            throw new Error('Bundle snapshot checksum mismatch');
    }
    for (const o of manifest.objects)
        if (o.present && await fileSha(path.join(dir, 'objects', o.sha256)) !== o.sha256)
            throw new Error('Bundle object checksum mismatch');
    return { manifest, bundleSha256: sha(JSON.stringify(manifest)) };
}
async function capture(sourceFile, dir) {
    const ep = await endpoint(sourceFile);
    if (!path.isAbsolute(ep.sqlite || ''))
        throw new Error('Explicit source SQLite file required');
    if (ep.mode === 'local') {
        const real = await fs.realpath(ep.sqlite);
        if (!real.startsWith(ep.identity.persistTo + path.sep))
            throw new Error('Source SQLite must belong to the explicit local state directory');
    }
    else if (ep.consistentExport !== true)
        throw new Error('Remote-origin snapshot needs an independently consistent export; live remote multi-query copies are unsupported');
    await fs.mkdir(dir, { mode: 0o700 });
    return locked(dir, async () => {
        await fs.mkdir(path.join(dir, 'objects'), { mode: 0o700 });
        const source = await snapshot(ep.sqlite, path.join(dir, 'source.sqlite'));
        const db = open(path.join(dir, 'source.sqlite'));
        let objects, migrations;
        try {
            objects = requiredObjects(db);
            migrations = db.prepare('SELECT name FROM d1_migrations ORDER BY id').all().map(x => x.name);
        }
        finally {
            db.close();
        }
        const contract = await read(contractPath);
        if (JSON.stringify(migrations) !== JSON.stringify(contract.requiredMigrations))
            throw new Error('Source migrations do not exactly match this release contract');
        const manifestObjects = [];
        for (const obj of objects) {
            const tmp = path.join(dir, 'object-download');
            try {
                await getObject(ep, obj.key, tmp, dir);
            }
            catch (error) {
                if (!error.missingObject)
                    throw error;
                if (obj.required)
                    throw new Error('A referenced R2 object is missing or unreadable; capture is incomplete');
                manifestObjects.push({ ...obj, present: false });
                continue;
            }
            const hash = await fileSha(tmp);
            if (obj.expected && hash !== obj.expected)
                throw new Error('Referenced R2 object differs from its recorded source checksum');
            await fs.rename(tmp, path.join(dir, 'objects', hash));
            manifestObjects.push({ ...obj, present: true, sha256: hash, bytes: (await fs.stat(path.join(dir, 'objects', hash))).size, provenance: obj.expected ? 'recorded-hash-matched' : 'observed-legacy-bytes' });
        }
        const restore = await prepareRestore(path.join(dir, 'source.sqlite'), path.join(dir, 'restore.sqlite'));
        const manifest = { version: 1, runId: randomUUID(), createdAt: new Date().toISOString(), source: ep.identity, sourceDescription: source, restore, contractSha256: await fileSha(contractPath), toolSha256: await toolSha(), migrations, objects: manifestObjects, files: { 'source.sqlite': await fileSha(path.join(dir, 'source.sqlite')), 'restore.sqlite': await fileSha(path.join(dir, 'restore.sqlite')) }, readiness: 'not-promoted: verify D1/R2, rebuild FTS/reviewer and run application canaries; semantic rebuild remains separate' };
        await atomicJson(path.join(dir, 'bundle.json'), manifest);
        return { captured: true, runId: manifest.runId, objectCount: manifestObjects.length };
    });
}
async function targetFor(dir, file) {
    const bundle = await loadBundle(dir), ep = await endpoint(file, { target: true });
    const s = bundle.manifest.source, t = ep.identity;
    const sameStorage = s.mode === t.mode && (s.mode === 'remote' ? s.accountId === t.accountId : s.persistTo === t.persistTo);
    if (sameStorage && (s.databaseId === t.databaseId || s.bucket === t.bucket))
        throw new Error('Recovery must use distinct source and target D1/R2 resources');
    return { ...bundle, ep };
}
async function names(ep, dir) {
    return query(ep, "SELECT name,type FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' ORDER BY name", dir);
}
async function plan(dir, targetFile) {
    return locked(dir, async () => {
        const { manifest, bundleSha256, ep } = await targetFor(dir, targetFile);
        if ((await names(ep, dir)).length)
            throw new Error('Target is not empty; conflicting schema/data must be investigated, never merged or ignored');
        try {
            await fs.access(path.join(dir, 'plan.json'));
            throw new Error('Plan already exists: use its original target or capture a fresh bundle');
        }
        catch (error) {
            if (error.code !== 'ENOENT')
                throw error;
        }
        const db = open(path.join(dir, 'restore.sqlite'));
        let sql;
        try {
            sql = await writeRestoreSql(db, path.join(dir, 'restore.sql'), { runId: manifest.runId, bundleSha256, targetSha256: ep.identitySha256 });
        }
        finally {
            db.close();
        }
        const value = { version: 1, runId: manifest.runId, bundleSha256, target: ep.identity, targetSha256: ep.identitySha256, sql, createdAt: new Date().toISOString() };
        await atomicJson(path.join(dir, 'plan.json'), value);
        const planSha256 = sha(JSON.stringify(value));
        return { planned: true, planSha256, target: ep.identity, sqlBytes: sql.bytes };
    });
}
async function loadPlan(dir, targetFile) {
    const bundle = await targetFor(dir, targetFile), plan = await read(path.join(dir, 'plan.json'));
    if (plan.runId !== bundle.manifest.runId || plan.bundleSha256 !== bundle.bundleSha256 || plan.targetSha256 !== bundle.ep.identitySha256 || await fileSha(path.join(dir, 'restore.sql')) !== plan.sql.sha256)
        throw new Error('Plan/checkpoint source, schema, SQL or target binding mismatch');
    return { ...bundle, plan, planSha256: sha(JSON.stringify(plan)) };
}
async function receipt(ep, dir) {
    const all = await names(ep, dir);
    if (!all.some(x => x.name === 'beedle_recovery_receipt'))
        return null;
    const rows = await query(ep, 'SELECT * FROM beedle_recovery_receipt', dir);
    if (rows.length !== 1)
        throw new Error('Incomplete or conflicting recovery receipt; target must remain offline');
    return rows[0];
}
function sameReceipt(actual, expected) {
    if (actual.run_id !== expected.manifest.runId || actual.bundle_sha256 !== expected.bundleSha256 || actual.target_sha256 !== expected.ep.identitySha256)
        throw new Error('Target belongs to another recovery run or source');
}
async function verifyData(ctx, dir) {
    const { ep, manifest } = ctx;
    const actual = await receipt(ep, dir);
    if (!actual)
        throw new Error('Target has no committed recovery receipt');
    sameReceipt(actual, ctx);
    const mismatches = [];
    const targetSchema = await query(ep, "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' AND name NOT GLOB 'beedle_recovery_*' AND name NOT IN(SELECT name FROM pragma_table_list WHERE type='shadow') ORDER BY type,name", dir);
    if (sha(JSON.stringify(targetSchema)) !== manifest.restore.schemaSha256)
        mismatches.push({ reason: 'schema_mismatch' });
    for (const table of manifest.restore.tables) {
        const hash = createHash('sha256');
        let count = 0;
        for (let offset = 0;; offset += 20) {
            const rows = await query(ep, `SELECT ${rowExpression(table.columns)} AS value FROM ${ident(table.name)} ORDER BY value LIMIT 20 OFFSET ${offset}`, dir);
            for (const row of rows) {
                hash.update(row.value + '\n');
                count++;
            }
            if (rows.length < 20)
                break;
        }
        if (count !== table.count || hash.digest('hex') !== table.sha256)
            mismatches.push({ table: table.name, expectedRows: table.count, actualRows: count });
    }
    const foreign = await query(ep, 'PRAGMA foreign_key_check', dir);
    if (foreign.length)
        mismatches.push({ table: 'foreign_key_check' });
    for (const obj of manifest.objects.filter(x => x.present)) {
        const state = await objectExists(ep, obj.key, dir);
        if (state.sha256 !== obj.sha256)
            mismatches.push({ objectSha256: obj.sha256, reason: 'missing_unreadable_or_conflicting' });
    }
    const report = { runId: manifest.runId, verifiedAt: new Date().toISOString(), dataVerified: mismatches.length === 0, mismatches, readiness: manifest.readiness };
    await atomicJson(path.join(dir, 'verification.json'), report);
    if (mismatches.length)
        throw new Error('Restored content differs from manifest; inspect private verification.json');
    return report;
}
async function apply(dir, targetFile, approvedHash) {
    return locked(dir, async () => {
        const ctx = await loadPlan(dir, targetFile);
        if (ctx.planSha256 !== approvedHash)
            throw new Error('Explicit exact plan checksum required');
        const checkpointFile = path.join(dir, 'checkpoint.json');
        let checkpoint;
        try {
            checkpoint = await read(checkpointFile);
        }
        catch (error) {
            if (error.code !== 'ENOENT')
                throw error;
        }
        if (checkpoint && (checkpoint.planSha256 !== ctx.planSha256 || checkpoint.runId !== ctx.manifest.runId))
            throw new Error('Checkpoint belongs to another run or plan');
        const committed = await receipt(ctx.ep, dir);
        if (committed) {
            sameReceipt(committed, ctx);
            const report = await verifyData(ctx, dir);
            await atomicJson(checkpointFile, { runId: ctx.manifest.runId, planSha256: ctx.planSha256, phase: 'data_verified_not_promoted', updatedAt: new Date().toISOString() });
            return report;
        }
        const existingNames = await names(ctx.ep, dir);
        if (existingNames.some(row => row.name !== 'beedle_recovery_claim')) {
            throw new Error('Conflicting target schema/data; no rows will be overwritten');
        }
        // Claim the target before copying R2. A different run cannot share its resources.
        for (const sql of claimStatements({ runId: ctx.manifest.runId, bundleSha256: ctx.bundleSha256, targetSha256: ctx.ep.identitySha256 })) {
            await query(ctx.ep, sql, dir);
        }
        const claims = await query(ctx.ep, 'SELECT * FROM beedle_recovery_claim', dir);
        if (claims.length !== 1)
            throw new Error('Incomplete target recovery claim');
        sameReceipt(claims[0], ctx);
        // A positive missing-key response permits a write; unreadable objects fail closed.
        if (ctx.ep.freshBucket !== true)
            throw new Error('Target must explicitly attest a newly created empty bucket');
        for (const obj of ctx.manifest.objects.filter(x => x.present)) {
            const state = await objectExists(ctx.ep, obj.key, dir);
            if (state.exists && state.sha256 !== obj.sha256)
                throw new Error('Conflicting R2 key content');
            if (!state.exists)
                await putObject(ctx.ep, obj.key, path.join(dir, 'objects', obj.sha256), dir);
            const readback = await objectExists(ctx.ep, obj.key, dir);
            if (readback.sha256 !== obj.sha256)
                throw new Error('R2 write not verified; D1 import held');
        }
        await atomicJson(checkpointFile, { runId: ctx.manifest.runId, planSha256: ctx.planSha256, phase: 'sql_acknowledgment_pending', updatedAt: new Date().toISOString() });
        await command(['d1', 'execute', 'DB', ...flags(ctx.ep), '--yes', '--file', path.join(dir, 'restore.sql')], dir, { file: true });
        const report = await verifyData(ctx, dir);
        await atomicJson(checkpointFile, { runId: ctx.manifest.runId, planSha256: ctx.planSha256, phase: 'data_verified_not_promoted', updatedAt: new Date().toISOString() });
        return report;
    });
}
export async function main(args = process.argv.slice(2)) {
    const [action, first, second, third] = args;
    if (!first || !second || !['capture', 'plan', 'apply', 'verify'].includes(action) || args.length !== (action === 'apply' ? 4 : 3)) {
        throw new Error(usage);
    }
    if (action === 'capture')
        return capture(path.resolve(first), path.resolve(second));
    const dir = path.resolve(first);
    if (action === 'plan')
        return plan(dir, path.resolve(second));
    if (action === 'apply')
        return apply(dir, path.resolve(second), third);
    return locked(dir, async () => verifyData(await loadPlan(dir, path.resolve(second)), dir));
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
    main().then(result => console.log(JSON.stringify(result, null, 2))).catch(error => {
        console.error(error.message);
        process.exitCode = 1;
    });
