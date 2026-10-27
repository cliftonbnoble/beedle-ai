// Recovery bundles contain private corpus data. Never send their contents to telemetry.
import { DatabaseSync, backup } from 'node:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
export const sha = value => createHash('sha256').update(value).digest('hex');
export async function fileSha(file) {
    const hash = createHash('sha256');
    for await (const bytes of createReadStream(file))
        hash.update(bytes);
    return hash.digest('hex');
}
export const ident = name => '"' + name.replaceAll('"', '""') + '"';
export const literal = value => value === null ? 'NULL' : `CAST(X'${Buffer.from(String(value)).toString('hex')}' AS TEXT)`;
export async function atomicJson(file, value) {
    const tmp = file + '.' + randomUUID() + '.tmp';
    await fs.writeFile(tmp, JSON.stringify(value, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
    await fs.rename(tmp, file);
}
export async function locked(dir, fn) {
    const lock = path.join(dir, 'operation.lock');
    const handle = await fs.open(lock, 'wx', 0o600).catch(() => {
        throw new Error('Recovery directory is locked. Inspect the owning process before removing an abandoned lock.');
    });
    try {
        await handle.writeFile(JSON.stringify({ pid: process.pid, createdAt: new Date().toISOString() }));
        return await fn();
    }
    finally {
        await handle.close();
        await fs.unlink(lock);
    }
}
export function open(file) {
    return new DatabaseSync(file, { readOnly: true });
}
export function check(db) {
    if (db.prepare('PRAGMA quick_check').get().quick_check !== 'ok' || db.prepare('PRAGMA foreign_key_check').all().length)
        throw new Error('Snapshot integrity or foreign-key check failed');
}
const isPrivate = name => name.startsWith('sqlite_') || name.startsWith('_cf_') || name.startsWith('beedle_recovery_');
export function schema(db) {
    const shadows = new Set(db.prepare('PRAGMA table_list').all().filter(row => row.type === 'shadow').map(row => row.name));
    return db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name").all().filter(row => !isPrivate(row.name) && !shadows.has(row.name));
}
export function columns(db, table) {
    return db.prepare(`PRAGMA table_xinfo(${ident(table)})`).all().filter(row => row.hidden === 0).map(row => row.name);
}
// All values travel as typed ASCII strings: no int64 rounding, NUL truncation or JSON blob coercion.
export function rowExpression(cols) {
    return `json_array(${cols.map(col => {
        const c = ident(col);
        return `CASE typeof(${c}) WHEN 'null' THEN 'null:' WHEN 'text' THEN 'text:'||hex(CAST(${c} AS BLOB)) WHEN 'blob' THEN 'blob:'||hex(${c}) WHEN 'integer' THEN 'integer:'||CAST(${c} AS TEXT) ELSE 'real:'||printf('%!.26g',${c}) END`;
    }).join(',')})`;
}
export function tableSpec(db, table) {
    const cols = columns(db, table);
    const expression = rowExpression(cols);
    let count = 0;
    const hash = createHash('sha256');
    for (const row of db.prepare(`SELECT ${expression} AS value FROM ${ident(table)} ORDER BY value`).iterate()) {
        hash.update(row.value + '\n');
        count++;
    }
    return { name: table, columns: cols, count, sha256: hash.digest('hex') };
}
export function describe(db) {
    const objects = schema(db);
    const tables = objects.filter(row => row.type === 'table' && !/^CREATE VIRTUAL TABLE/i.test(row.sql)).map(row => tableSpec(db, row.name));
    return { schema: objects, schemaSha256: sha(JSON.stringify(objects)), tables };
}
export async function snapshot(source, destination) {
    if (path.resolve(source) === path.resolve(destination))
        throw new Error('Snapshot must not overwrite source');
    await fs.access(source);
    const db = open(source);
    let start = Date.now();
    try {
        await backup(db, destination, { rate: 128, progress() {
                if (Date.now() - start > 120000)
                    throw new Error('Snapshot deadline exceeded');
            } });
    }
    finally {
        db.close();
    }
    await fs.chmod(destination, 0o600);
    const copy = open(destination);
    try {
        check(copy);
        return describe(copy);
    }
    finally {
        copy.close();
    }
}
export function requiredObjects(db) {
    const objects = new Map();
    function add(key, expected, required = true) {
        if (!key)
            return;
        const old = objects.get(key);
        if (old?.expected && expected && old.expected !== expected)
            throw new Error('Conflicting source hashes for one R2 key');
        objects.set(key, { key, expected: expected || old?.expected || null, required: required || old?.required || false });
    }
    for (const d of db.prepare("SELECT source_r2_key,metadata_json FROM documents").iterate()) {
        let meta;
        try {
            meta = JSON.parse(d.metadata_json || '{}');
        }
        catch {
            throw new Error('Invalid source metadata');
        }
        add(d.source_r2_key, meta.sourceSha256);
    }
    for (const g of db.prepare('SELECT source_key,source_sha256 FROM document_generations').iterate())
        add(g.source_key, g.source_sha256);
    for (const r of db.prepare('SELECT source_key,source_sha256,state FROM ingestion_requests').iterate())
        add(r.source_key, r.source_sha256, r.state === 'complete');
    for (const r of db.prepare('SELECT source_key,source_sha256 FROM document_vector_jobs WHERE outbox_ready_at IS NOT NULL').iterate())
        add(r.source_key, r.source_sha256);
    return [...objects.values()].sort((a, b) => a.key.localeCompare(b.key));
}
// Preserve the untouched backup too. This copy holds cross-resource work and drops disposable caches.
export async function prepareRestore(source, destination) {
    await snapshot(source, destination);
    const db = new DatabaseSync(destination);
    try {
        db.exec('PRAGMA foreign_keys=ON; BEGIN;');
        const triggers = schema(db).filter(x => x.type === 'trigger');
        for (const t of triggers)
            db.exec(`DROP TRIGGER ${ident(t.name)}`);
        const clears = ['search_sessions', 'compute_budget_counters', 'vector_sync_locks', 'vector_submission_guards', 'vector_filter_sync', 'vector_profile_sync', 'vector_document_metadata', 'vector_entries', 'vector_profiles'];
        for (const table of clears)
            db.exec(`DELETE FROM ${ident(table)}`);
        db.exec(`UPDATE search_session_capacity SET reserved_bytes=0,reserved_count=0;
 UPDATE vector_lifecycle_epoch SET revision=revision+1;
 UPDATE vector_lifecycle_dirty SET revision=revision+1,dirty=1;
 UPDATE document_vector_jobs SET state='failed',owner_token=NULL,lease_expires_at=NULL,next_attempt_at=NULL,last_error='recovery_hold: explicitly recreate work for the new vector namespace' WHERE state IN ('queued','processing');
 UPDATE maintenance_jobs SET state='cancelled',owner_token=NULL,lease_expires_at=NULL,next_attempt_at=NULL,last_error='recovery_hold: inspect frozen plan before recreating work' WHERE state IN ('queued','running','retryable');
 UPDATE ingestion_requests SET state='retryable',owner_token=NULL,lease_expires_at=NULL WHERE state='processing';
 UPDATE reviewer_queue_control SET phase='backfill',after_id='',upper_id='',lease_owner=NULL,lease_until=0,last_token=NULL,epoch=epoch+1;
 UPDATE reviewer_queue_sources SET dirty=1;
 UPDATE search_index_build SET owner_token=NULL,lease_expires_at=NULL;
 UPDATE search_phrase_build SET owner_token=NULL,lease_expires_at=NULL;
 UPDATE search_facet_build SET owner_token=NULL,lease_expires_at=NULL;
 UPDATE search_index_rebuild SET owner_token=NULL,lease_expires_at=NULL;`);
        for (const t of triggers)
            db.exec(t.sql);
        db.exec('COMMIT;');
        check(db);
        return describe(db);
    }
    catch (error) {
        try {
            db.exec('ROLLBACK');
        }
        catch {
        }
        throw error;
    }
    finally {
        db.close();
    }
}
function typedSql(value) {
    const split = value.indexOf(':'), type = value.slice(0, split), raw = value.slice(split + 1);
    if (type === 'null')
        return 'NULL';
    if (type === 'text')
        return `CAST(X'${raw}' AS TEXT)`;
    if (type === 'blob')
        return `X'${raw}'`;
    if (type === 'integer')
        return `CAST('${raw}' AS INTEGER)`;
    if (type === 'real' && /^[+\-0-9.e]+$/.test(raw))
        return `CAST('${raw}' AS REAL)`;
    throw new Error('Unsupported SQLite value');
}
export function claimStatements(receipt) {
    return [
        'CREATE TABLE IF NOT EXISTS beedle_recovery_claim(singleton INTEGER PRIMARY KEY CHECK(singleton=1),run_id TEXT NOT NULL,bundle_sha256 TEXT NOT NULL,target_sha256 TEXT NOT NULL)',
        `INSERT INTO beedle_recovery_claim VALUES(1,${literal(receipt.runId)},${literal(receipt.bundleSha256)},${literal(receipt.targetSha256)}) ON CONFLICT(singleton) DO NOTHING`
    ];
}
export async function writeRestoreSql(db, file, receipt) {
    const handle = await fs.open(file, 'wx', 0o600);
    let size = 0;
    async function write(sql) {
        if (Buffer.byteLength(sql) > 90000)
            throw new Error('Recovery statement exceeds 90KB ceiling');
        size += Buffer.byteLength(sql) + 2;
        if (size > 512 * 1024 * 1024)
            throw new Error('Recovery SQL exceeds 512MiB rehearsal ceiling; use a reviewed larger-dataset plan');
        await handle.writeFile(sql + ';\n');
    }
    async function insert(table, cols, values) {
        let expressions = [];
        for (let i = 0; i < values.length; i++) {
            const value = values[i];
            if (value.length > Math.min(8000, Math.floor(60000 / values.length))) {
                const type = value.slice(0, value.indexOf(':')), raw = value.slice(value.indexOf(':') + 1);
                if (type !== 'text' && type !== 'blob')
                    throw new Error('Unsupported large value');
                await write(`INSERT INTO beedle_recovery_values(id,value) VALUES(${i},X'')`);
                for (let offset = 0; offset < raw.length; offset += 16000)
                    await write(`UPDATE beedle_recovery_values SET value=CAST(value||X'${raw.slice(offset, offset + 16000)}' AS BLOB) WHERE id=${i}`);
                expressions.push(`(SELECT ${type === 'text' ? 'CAST(value AS TEXT)' : 'value'} FROM beedle_recovery_values WHERE id=${i})`);
            }
            else
                expressions.push(typedSql(value));
        }
        await write(`INSERT INTO ${ident(table)}(${cols.map(ident).join(',')}) SELECT ${expressions.join(',')}`);
        await write('DELETE FROM beedle_recovery_values');
    }
    try {
        // D1 file imports are transactional. CREATE (without IF NOT EXISTS) fences concurrent/late imports.
        await write('PRAGMA defer_foreign_keys=ON');
        await write('CREATE TABLE beedle_recovery_receipt(singleton INTEGER PRIMARY KEY CHECK(singleton=1),run_id TEXT NOT NULL,bundle_sha256 TEXT NOT NULL,target_sha256 TEXT NOT NULL)');
        await write('CREATE TABLE beedle_recovery_assert(ok INTEGER CHECK(ok=1))');
        await write(`INSERT INTO beedle_recovery_assert SELECT CASE WHEN EXISTS(SELECT 1 FROM beedle_recovery_claim WHERE singleton=1 AND run_id=${literal(receipt.runId)} AND bundle_sha256=${literal(receipt.bundleSha256)} AND target_sha256=${literal(receipt.targetSha256)}) THEN 1 ELSE 0 END`);
        await write("INSERT INTO beedle_recovery_assert SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' AND name NOT GLOB 'beedle_recovery_*') THEN 1 ELSE 0 END");
        await write('CREATE TABLE beedle_recovery_values(id INTEGER PRIMARY KEY,value BLOB NOT NULL)');
        const objects = schema(db);
        const tables = objects.filter(x => x.type === 'table' && !/^CREATE VIRTUAL TABLE/i.test(x.sql));
        const virtual = objects.filter(x => x.type === 'table' && /^CREATE VIRTUAL TABLE/i.test(x.sql));
        for (const t of tables)
            await write(t.sql);
        for (const v of objects.filter(x => x.type === 'view'))
            await write(v.sql);
        for (const t of tables) {
            const cols = columns(db, t.name);
            for (const row of db.prepare(`SELECT ${rowExpression(cols)} AS value FROM ${ident(t.name)}`).iterate())
                await insert(t.name, cols, JSON.parse(row.value));
        }
        for (const v of virtual) {
            await write(v.sql);
            if (/using\s+fts5vocab/i.test(v.sql))
                continue;
            if (!/using\s+fts5\s*\(/i.test(v.sql))
                throw new Error('Unsupported virtual table');
            if (/\bcontent\s*=/i.test(v.sql)) {
                await write(`INSERT INTO ${ident(v.name)}(${ident(v.name)}) VALUES('rebuild')`);
            }
            else {
                const cols = columns(db, v.name);
                for (const row of db.prepare(`SELECT ${rowExpression(['rowid', ...cols])} AS value FROM ${ident(v.name)}`).iterate())
                    await insert(v.name, ['rowid', ...cols], JSON.parse(row.value));
            }
        }
        for (const o of objects.filter(x => x.type === 'index' || x.type === 'trigger'))
            await write(o.sql);
        await write('INSERT INTO beedle_recovery_assert SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM pragma_foreign_key_check) THEN 1 ELSE 0 END');
        await write(`INSERT INTO beedle_recovery_receipt VALUES(1,${literal(receipt.runId)},${literal(receipt.bundleSha256)},${literal(receipt.targetSha256)})`);
        await write('DROP TABLE beedle_recovery_values');
        await write('DROP TABLE beedle_recovery_assert');
    }
    finally {
        await handle.close();
    }
    return { sha256: await fileSha(file), bytes: size };
}
