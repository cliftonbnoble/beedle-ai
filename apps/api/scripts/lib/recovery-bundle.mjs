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
    const values = cols.map(col => {
        const c = ident(col);
        return `CASE typeof(${c}) WHEN 'null' THEN 'null:' WHEN 'text' THEN 'text:'||hex(CAST(${c} AS BLOB)) WHEN 'blob' THEN 'blob:'||hex(${c}) WHEN 'integer' THEN 'integer:'||CAST(${c} AS TEXT) ELSE 'real:'||printf('%!.26g',${c}) END`;
    });
    // D1 allows 32 function arguments, while documents already has >40 columns.
    const groups = [];
    for (let i = 0; i < values.length; i += 24) groups.push(`json_array(${values.slice(i, i + 24).join(',')})`);
    return groups.length ? groups.reduce((left, right) => `rtrim(${left},']')||','||substr(${right},2)`) : 'json_array()';
}
export function canonicalRow(value) {
    return JSON.stringify(JSON.parse(value).map(item => {
        if (!item.startsWith('real:')) return item;
        const number = Number(item.slice(5));
        if (!Number.isFinite(number)) throw new Error('Non-finite SQLite REAL cannot be verified portably');
        // SQLite versions render %.26g differently; normalize lossless decoded
        // binary64 values consistently on both source and verification clients.
        return 'real:' + number.toString();
    }));
}
export function tableCursor(db, table) {
    const info = db.prepare(`PRAGMA table_xinfo(${ident(table)})`).all();
    const withoutRowid = db.prepare('PRAGMA table_list').all().find(row => row.name === table)?.wr;
    if (!withoutRowid) {
        const names = new Set(info.map(row => row.name.toLowerCase()));
        const alias = ['rowid', '_rowid_', 'oid'].find(name => !names.has(name));
        if (alias) return { kind: 'rowid', columns: [alias] };
    }
    const pk = info.filter(row => row.pk).sort((a, b) => a.pk - b.pk).map(row => row.name);
    if (!withoutRowid || !pk.length) throw new Error('Table has no unambiguous stable recovery cursor');
    return { kind: 'primary', columns: pk };
}
export function tableSpec(db, table) {
    const cols = columns(db, table);
    const expression = rowExpression(cols);
    const cursor = tableCursor(db, table);
    let count = 0, maxRowBytes = 0;
    const hash = createHash('sha256');
    for (const row of db.prepare(`SELECT ${rowExpression(cursor.columns)} AS cursor,${expression} AS value FROM ${ident(table)} ORDER BY ${cursor.columns.map(ident).join(',')}`).iterate()) {
        hash.update(canonicalRow(row.cursor) + ':' + canonicalRow(row.value) + '\n');
        maxRowBytes = Math.max(maxRowBytes, Buffer.byteLength(row.value) + Buffer.byteLength(row.cursor) + 1024);
        count++;
    }
    return { name: table, columns: cols, cursor, maxRowBytes, count, sha256: hash.digest('hex') };
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
export function requiredObjects(db, { legacyBaseline = false } = {}) {
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
    if (legacyBaseline) return [...objects.values()].sort((a, b) => a.key.localeCompare(b.key));
    for (const g of db.prepare('SELECT source_key,source_sha256 FROM document_generations').iterate())
        add(g.source_key, g.source_sha256);
    for (const r of db.prepare('SELECT source_key,source_sha256,state FROM ingestion_requests').iterate())
        add(r.source_key, r.source_sha256, r.state === 'complete');
    for (const r of db.prepare('SELECT source_key,source_sha256 FROM document_vector_jobs WHERE outbox_ready_at IS NOT NULL').iterate())
        add(r.source_key, r.source_sha256);
    return [...objects.values()].sort((a, b) => a.key.localeCompare(b.key));
}
// Preserve the untouched backup too. This copy holds cross-resource work and drops disposable caches.
export async function prepareRestore(source, destination, { deferFtsRebuild = false } = {}) {
    await snapshot(source, destination);
    const db = new DatabaseSync(destination);
    try {
        db.exec('PRAGMA foreign_keys=ON; BEGIN;');
        const triggers = schema(db).filter(x => x.type === 'trigger');
        for (const t of triggers)
            db.exec(`DROP TRIGGER ${ident(t.name)}`);
        if (deferFtsRebuild) {
            // These are disposable frozen postings, not document or revision
            // history. The untouched source snapshot keeps their original bytes.
            for (const table of ['search_chunks_fts_legacy', 'search_previous_body_fts', 'search_previous_metadata_fts', 'search_previous_phrase_fts'])
                db.exec(`DROP TABLE IF EXISTS ${ident(table)}`);
        }
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
        if (deferFtsRebuild) {
            // Block search readiness until a fresh bounded M10 recovery finishes.
            // Never run an unbounded FTS 'rebuild' during a large file import.
            db.exec("UPDATE search_index_build SET phase='documents',after_id=''; UPDATE search_facet_build SET phase='pending',after_id='',visited=0;");
        }
        for (const t of triggers)
            if (!deferFtsRebuild || !/\bsearch_chunks_fts_legacy\b|\bsearch_previous_(body|metadata|phrase)_fts\b/.test(t.sql)) db.exec(t.sql);
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
export function typedSql(value) {
    const split = value.indexOf(':'), type = value.slice(0, split), raw = value.slice(split + 1);
    if (type === 'null')
        return 'NULL';
    if (type === 'text') {
        const bytes = Buffer.from(raw, 'hex'), text = bytes.toString('utf8');
        // Keep valid text compact, while NUL/invalid UTF-8 retain exact hex bytes.
        if (!text.includes('\0') && Buffer.from(text).equals(bytes)) return "'" + text.replaceAll("'", "''") + "'";
        return `CAST(X'${raw}' AS TEXT)`;
    }
    if (type === 'blob')
        return `X'${raw}'`;
    if (type === 'integer' && /^-?\d+$/.test(raw))
        return raw;
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
function restoreOrder(db, tables) {
    const remaining = new Map(tables.map(table => [table.name.toLowerCase(), table]));
    const relations = new Map(tables.map(table => [table.name, db.prepare(`PRAGMA foreign_key_list(${ident(table.name)})`).all()]));
    const ordered = [], indexed = new Set();
    while (remaining.size) {
        const ready = [...remaining.values()].filter(table => relations.get(table.name).every(fk => fk.table.toLowerCase() === table.name.toLowerCase() || !remaining.has(fk.table.toLowerCase())));
        if (!ready.length) {
            // Cycles still need deferred constraints, with child-key indexes to
            // prevent quadratic scans while resolving temporarily absent parents.
            for (const table of remaining.values()) { ordered.push(table); indexed.add(table.name); }
            break;
        }
        for (const table of ready) {
            ordered.push(table); remaining.delete(table.name.toLowerCase());
            if (relations.get(table.name).some(fk => fk.table.toLowerCase() === table.name.toLowerCase())) indexed.add(table.name);
        }
    }
    const indexes = [];
    for (const table of ordered.filter(table => indexed.has(table.name))) {
        const keys = new Map();
        for (const fk of relations.get(table.name)) { if (!keys.has(fk.id)) keys.set(fk.id, []); keys.get(fk.id).push(fk); }
        for (const [id, rows] of keys) indexes.push({ name: 'beedle_recovery_fk_' + sha(table.name + ':' + id).slice(0,16), table: table.name, columns: rows.sort((a,b) => a.seq-b.seq).map(fk => fk.from) });
    }
    return { tables: ordered, indexes };
}
export async function writeRestoreSql(db, file, receipt, { maxSqlBytes = 512 * 1024 * 1024, deferFtsRebuild = false, legacyBaseline = false } = {}) {
    if (!Number.isSafeInteger(maxSqlBytes) || maxSqlBytes < 1024 || maxSqlBytes > 5_000_000_000)
        throw new Error('Explicit recovery SQL budget must be between 1KiB and the conservative 5GB import ceiling');
    if (maxSqlBytes > 512 * 1024 * 1024 && !deferFtsRebuild)
        throw new Error('Large recovery requires a deferred bounded FTS rebuild plan');
    const handle = await fs.open(file, 'wx', 0o600);
    let size = 0, completed = false;
    let pending = null;
    async function write(sql) {
        if (Buffer.byteLength(sql) > 90000)
            throw new Error('Recovery statement exceeds 90KB ceiling');
        size += Buffer.byteLength(sql) + 2;
        if (size > maxSqlBytes)
            throw new Error('Recovery SQL exceeds its sealed byte budget; use a reviewed larger-dataset plan');
        await handle.writeFile(sql + ';\n');
    }
    async function flushInsert() {
        if (!pending) return;
        await write(pending.prefix + pending.rows.join(','));
        pending = null;
    }
    async function insert(table, cols, values) {
        const prefix = `INSERT INTO ${ident(table)}(${cols.map(ident).join(',')}) VALUES`;
        const inline = values.map(typedSql);
        const needsStaging = Buffer.byteLength(prefix) + Buffer.byteLength(inline.join(',')) + 2 > 88000;
        if (needsStaging) await flushInsert();
        let expressions = [];
        for (let i = 0; i < values.length; i++) {
            const value = values[i];
            if (needsStaging && Buffer.byteLength(inline[i]) > Math.floor(60000 / values.length)) {
                const type = value.slice(0, value.indexOf(':')), raw = value.slice(value.indexOf(':') + 1);
                if (type !== 'text' && type !== 'blob')
                    throw new Error('Unsupported large value');
                await write(`INSERT INTO beedle_recovery_values(id,value) VALUES(${i},X'')`);
                for (let offset = 0; offset < raw.length; offset += 16000)
                    await write(`UPDATE beedle_recovery_values SET value=CAST(value||X'${raw.slice(offset, offset + 16000)}' AS BLOB) WHERE id=${i}`);
                expressions.push(`(SELECT ${type === 'text' ? 'CAST(value AS TEXT)' : 'value'} FROM beedle_recovery_values WHERE id=${i})`);
            }
            else
                expressions.push(inline[i]);
        }
        if (needsStaging) {
            await write(prefix + '(' + expressions.join(',') + ')');
            await write('DELETE FROM beedle_recovery_values');
        }
        else {
            const row = '(' + expressions.join(',') + ')', bytes = Buffer.byteLength(row) + 1;
            if (pending && (pending.prefix !== prefix || pending.bytes + bytes > 88000)) await flushInsert();
            pending ||= { prefix, rows: [], bytes: Buffer.byteLength(prefix) };
            pending.rows.push(row);
            pending.bytes += bytes;
        }
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
        const loadOrder = restoreOrder(db, tables);
        const virtual = objects.filter(x => x.type === 'table' && /^CREATE VIRTUAL TABLE/i.test(x.sql));
        for (const t of tables)
            await write(t.sql);
        for (const v of objects.filter(x => x.type === 'view'))
            await write(v.sql);
        for (const index of loadOrder.indexes) await write(`CREATE INDEX ${ident(index.name)} ON ${ident(index.table)}(${index.columns.map(ident).join(',')})`);
        for (const t of loadOrder.tables) {
            const cursor = tableCursor(db, t.name);
            const cols = [...(cursor.kind === 'rowid' ? cursor.columns : []), ...columns(db, t.name)];
            for (const row of db.prepare(`SELECT ${rowExpression(cols)} AS value FROM ${ident(t.name)}`).iterate())
                await insert(t.name, cols, JSON.parse(row.value));
            await flushInsert();
        }
        for (const v of virtual) {
            await write(v.sql);
            if (/using\s+fts5vocab/i.test(v.sql))
                continue;
            if (!/using\s+fts5\s*\(/i.test(v.sql))
                throw new Error('Unsupported virtual table');
            if (deferFtsRebuild) {
                const knownLegacy = legacyBaseline && v.name === 'search_chunks_fts' && JSON.stringify(columns(db, v.name)) === JSON.stringify(['source_kind','chunk_id','document_id','active','section_label','paragraph_anchor','citation_anchor','chunk_text','created_at','order_rank']);
                if (!/\bcontent\s*=/i.test(v.sql) && !knownLegacy) throw new Error('Deferred recovery requires source-backed external-content FTS or the explicit legacy 0012 cache');
            }
            else if (/\bcontent\s*=/i.test(v.sql)) {
                await write(`INSERT INTO ${ident(v.name)}(${ident(v.name)}) VALUES('rebuild')`);
            }
            else {
                const cols = columns(db, v.name);
                for (const row of db.prepare(`SELECT ${rowExpression(['rowid', ...cols])} AS value FROM ${ident(v.name)}`).iterate())
                    await insert(v.name, ['rowid', ...cols], JSON.parse(row.value));
                await flushInsert();
            }
        }
        for (const o of objects.filter(x => x.type === 'index' || x.type === 'trigger'))
            await write(o.sql);
        for (const index of loadOrder.indexes) await write(`DROP INDEX ${ident(index.name)}`);
        await write('INSERT INTO beedle_recovery_assert SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM pragma_foreign_key_check) THEN 1 ELSE 0 END');
        await write(`INSERT INTO beedle_recovery_receipt VALUES(1,${literal(receipt.runId)},${literal(receipt.bundleSha256)},${literal(receipt.targetSha256)})`);
        await write('DROP TABLE beedle_recovery_values');
        await write('DROP TABLE beedle_recovery_assert');
        completed = true;
    }
    finally {
        await handle.close();
        if (!completed) await fs.unlink(file);
    }
    return { sha256: await fileSha(file), bytes: size, maxSqlBytes, deferFtsRebuild };
}
