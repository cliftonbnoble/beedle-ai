import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { snapshot, open, describe, writeRestoreSql, locked, atomicJson, claimStatements } from '../scripts/lib/recovery-bundle.mjs';
async function temporary(fn) {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'beedle-recovery-unit-'));
    try {
        await fn(dir);
    }
    finally {
        await fs.rm(dir, { recursive: true, force: true });
    }
}
test('WAL-aware online backup captures committed WAL data without changing source or pending transaction', () => temporary(async (dir) => {
    const source = new DatabaseSync(path.join(dir, 'source.sqlite'));
    try {
        source.exec('PRAGMA journal_mode=WAL; CREATE TABLE rows(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO rows VALUES(1,\'committed\'); BEGIN; INSERT INTO rows VALUES(2,\'uncommitted\');');
        await snapshot(path.join(dir, 'source.sqlite'), path.join(dir, 'backup.sqlite'));
        const copy = open(path.join(dir, 'backup.sqlite'));
        try {
            assert.deepEqual(copy.prepare('SELECT value FROM rows').all().map(x => x.value), ['committed']);
            assert.equal(source.prepare('SELECT COUNT(*) n FROM rows').get().n, 2);
        }
        finally {
            copy.close();
        }
    }
    finally {
        source.close();
    }
}));
test('portable SQL preserves int64, blobs, NUL, Unicode, generated columns, large text, FTS and immutability triggers', () => temporary(async (dir) => {
    const source = new DatabaseSync(path.join(dir, 'source.sqlite')), target = new DatabaseSync(':memory:');
    try {
        source.exec(`CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT,bytes BLOB,computed TEXT GENERATED ALWAYS AS (substr(value,1,2)) VIRTUAL); CREATE VIRTUAL TABLE terms USING fts5(value); CREATE TRIGGER immutable BEFORE UPDATE ON records BEGIN SELECT RAISE(ABORT,'immutable'); END;`);
        source.prepare('INSERT INTO records(id,value,bytes) VALUES(?,?,?)').run(9223372036854775807n, 'λ\0' + 'z'.repeat(200000), Uint8Array.from({ length: 50000 }, (_, index) => index % 256));
        source.exec("INSERT INTO terms(value) VALUES('repair notice')");
        source.exec('CREATE TABLE wide(' + Array.from({ length: 25 }, (_, i) => 'c' + i + ' TEXT').join(',') + ')');
        source.prepare('INSERT INTO wide VALUES(' + Array(25).fill('?').join(',') + ')').run(...Array(25).fill('wide'.repeat(1000)));
        const expected = describe(source);
        await writeRestoreSql(source, path.join(dir, 'restore.sql'), { runId: 'run', bundleSha256: 'bundle', targetSha256: 'target' });
        target.exec(claimStatements({ runId: 'run', bundleSha256: 'bundle', targetSha256: 'target' }).join(';') + ';');
        target.exec('PRAGMA foreign_keys=ON; BEGIN;' + await fs.readFile(path.join(dir, 'restore.sql'), 'utf8') + 'COMMIT;');
        const actual = describe(target);
        assert.deepEqual(actual.tables.filter(x => x.name !== 'beedle_recovery_receipt'), expected.tables);
        assert.equal(target.prepare("SELECT COUNT(*) n FROM terms WHERE terms MATCH 'repair'").get().n, 1);
        assert.throws(() => target.exec("UPDATE records SET value='changed'"), /immutable/);
    }
    finally {
        source.close();
        target.close();
    }
}));
test('a late/repeated import cannot overwrite a committed recovery receipt and failed transaction leaves no schema', () => temporary(async (dir) => {
    const source = new DatabaseSync(':memory:');
    source.exec('CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO records VALUES(1,\'source\');');
    await writeRestoreSql(source, path.join(dir, 'restore.sql'), { runId: 'run', bundleSha256: 'bundle', targetSha256: 'target' });
    source.close();
    const sql = await fs.readFile(path.join(dir, 'restore.sql'), 'utf8');
    for (const existing of [false, true]) {
        const db = new DatabaseSync(':memory:');
        try {
            db.exec(claimStatements({ runId: 'run', bundleSha256: 'bundle', targetSha256: 'target' }).join(';') + ';');
            if (existing)
                db.exec("CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO records VALUES(1,'conflicting')");
            db.exec('BEGIN');
            assert.throws(() => db.exec(existing ? sql : sql + "INSERT INTO absent VALUES(1);"));
            db.exec('ROLLBACK');
            assert.deepEqual(db.prepare("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").all().map(x => x.name), existing ? ['beedle_recovery_claim', 'records'] : ['beedle_recovery_claim']);
            if (existing)
                assert.equal(db.prepare('SELECT value FROM records').get().value, 'conflicting');
            else {
                db.exec('BEGIN;' + sql + 'COMMIT;');
                db.exec('BEGIN');
                assert.throws(() => db.exec(sql), /already exists/);
                db.exec('ROLLBACK');
                assert.equal(db.prepare('SELECT value FROM records').get().value, 'source');
            }
        }
        finally {
            db.close();
        }
    }
}));
test('operation lock rejects concurrent writers and checkpoints replace atomically', () => temporary(async (dir) => {
    await locked(dir, async () => {
        await assert.rejects(locked(dir, () => assert.fail()), /locked/);
        await atomicJson(path.join(dir, 'state.json'), { phase: 1 });
        await atomicJson(path.join(dir, 'state.json'), { phase: 2 });
        assert.deepEqual(JSON.parse(await fs.readFile(path.join(dir, 'state.json'), 'utf8')), { phase: 2 });
    });
    await locked(dir, () => Promise.resolve());
}));
test('a different recovery run cannot replace a durable target claim', () => temporary(async (dir) => {
    const source = new DatabaseSync(':memory:'), target = new DatabaseSync(':memory:');
    try {
        source.exec('CREATE TABLE source_rows(id INTEGER PRIMARY KEY);');
        await writeRestoreSql(source, path.join(dir, 'restore.sql'), { runId: 'second', bundleSha256: 'second-bundle', targetSha256: 'target' });
        target.exec(claimStatements({ runId: 'first', bundleSha256: 'first-bundle', targetSha256: 'target' }).join(';'));
        target.exec(claimStatements({ runId: 'second', bundleSha256: 'second-bundle', targetSha256: 'target' }).join(';'));
        assert.equal(target.prepare('SELECT run_id FROM beedle_recovery_claim').get().run_id, 'first');
        const sql = await fs.readFile(path.join(dir, 'restore.sql'), 'utf8');
        target.exec('BEGIN');
        assert.throws(() => target.exec(sql), /CHECK constraint/);
        target.exec('ROLLBACK');
        assert.equal(target.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='source_rows'").get().n, 0);
    }
    finally {
        source.close();
        target.close();
    }
}));

test('recovery inspection requires positive complete D1 results rather than treating missing results as an empty target',async()=>{
 const {parseRecoveryQuery}=await import('../scripts/recovery.mjs');
 assert.deepEqual(parseRecoveryQuery(JSON.stringify([{success:true,results:[]}])),[]);
 assert.deepEqual(parseRecoveryQuery(JSON.stringify([{success:true,results:[{id:1}]},{success:true,results:[{id:2}]}])),[{id:1},{id:2}]);
 for(const value of [[],{},[{}],[{success:true}],[{success:'true',results:[]}],[{success:true,results:null}],[{success:true,results:[]},{results:[]}],[{success:true,results:[null]}]])assert.throws(()=>parseRecoveryQuery(JSON.stringify(value)),/inspection|query|result/i);
});
