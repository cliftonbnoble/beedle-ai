import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { snapshot, open, describe, rowExpression, canonicalRow, requiredObjects, writeRestoreSql, locked, atomicJson, claimStatements } from '../scripts/lib/recovery-bundle.mjs';
import {memoryD1} from './helpers/d1-memory.mjs';
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
        source.prepare('INSERT INTO records(id,value,bytes) VALUES(?,?,?)').run(-1n,"Quoted '); DROP TABLE records;--\nλ",new Uint8Array());
        source.exec("INSERT INTO records(id,value) VALUES(2,CAST(X'FF00' AS TEXT))");
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

test('wide recovery rows respect D1 function argument limits and preserve sparse int64 rowids and composite keys',()=>temporary(async dir=>{
 const source=new DatabaseSync(':memory:'),target=new DatabaseSync(':memory:');
 try{
  source.exec('CREATE TABLE wide('+Array.from({length:100},(_,i)=>'c'+i+' TEXT').join(',')+'); CREATE TABLE composite(a TEXT,b INTEGER,value REAL,PRIMARY KEY(a,b)) WITHOUT ROWID;');
  source.prepare('INSERT INTO wide(rowid,'+Array.from({length:100},(_,i)=>'c'+i).join(',')+') VALUES('+Array(101).fill('?').join(',')+')').run(-9223372036854775807n,...Array.from({length:100},(_,i)=>'λ\0'+i));
  source.exec("INSERT INTO composite VALUES('a',1,0.1),('a',2,0.10000000000000002)");
  const cols=Array.from({length:100},(_,i)=>'c'+i),expr=rowExpression(cols);
  assert.equal((expr.match(/json_array\(/g)||[]).length,5,'each argument group is bounded');
  assert.equal(JSON.parse(source.prepare('SELECT '+expr+' value FROM wide').get().value).length,100);
  const receipt={runId:'wide',bundleSha256:'b',targetSha256:'t'};
  await writeRestoreSql(source,path.join(dir,'wide.sql'),receipt);
  target.exec(claimStatements(receipt).join(';')+';BEGIN;'+await fs.readFile(path.join(dir,'wide.sql'),'utf8')+'COMMIT;');
  assert.deepEqual(describe(target).tables,describe(source).tables);
  const stmt=target.prepare('SELECT rowid FROM wide');stmt.setReadBigInts(true);assert.equal(stmt.get().rowid,-9223372036854775807n);
 }finally{source.close();target.close();}
}));

test('REAL verification is portable across SQLite decimal renderings without collapsing neighboring values or int64 strings',()=>{
 assert.equal(canonicalRow('["real:0.10000000000000000555","integer:9223372036854775807"]'),canonicalRow('["real:0.1","integer:9223372036854775807"]'));
 assert.notEqual(canonicalRow('["real:0.1"]'),canonicalRow('["real:0.10000000000000002"]'));
 assert.notEqual(canonicalRow('["integer:9223372036854775807"]'),canonicalRow('["integer:9223372036854775806"]'));
 assert.throws(()=>canonicalRow('["real:Inf"]'),/Non-finite/);
});

test('explicit SQL capacity budgets are enforced and deferred FTS requires a source-backed index',()=>temporary(async dir=>{
 const db=new DatabaseSync(':memory:');try{
  db.exec("CREATE TABLE content(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO content VALUES(1,'rent'); CREATE VIRTUAL TABLE terms USING fts5(value,content='content',content_rowid='id'); INSERT INTO terms(terms) VALUES('rebuild');");
  const receipt={runId:'budget',bundleSha256:'b',targetSha256:'t'};
  await assert.rejects(writeRestoreSql(db,path.join(dir,'invalid.sql'),receipt,{maxSqlBytes:5_000_000_001}),/budget|ceiling/);
  await assert.rejects(writeRestoreSql(db,path.join(dir,'small.sql'),receipt,{maxSqlBytes:1024}),/byte budget/);
  await assert.rejects(fs.access(path.join(dir,'small.sql')),/ENOENT/,'a failed unsealed writer removes only its own partial file');
  await assert.rejects(writeRestoreSql(db,path.join(dir,'unbounded.sql'),receipt,{maxSqlBytes:1024**3}),/bounded FTS/);
  const result=await writeRestoreSql(db,path.join(dir,'deferred.sql'),receipt,{maxSqlBytes:4096,deferFtsRebuild:true});
  assert.equal(result.deferFtsRebuild,true);assert.doesNotMatch(await fs.readFile(path.join(dir,'deferred.sql'),'utf8'),/VALUES\('rebuild'\)/);
  const target=new DatabaseSync(':memory:');try{
   target.exec(claimStatements(receipt).join(';')+';BEGIN;'+await fs.readFile(path.join(dir,'deferred.sql'),'utf8')+'COMMIT;');
   assert.equal(target.prepare("SELECT count(*) n FROM terms WHERE terms MATCH 'rent'").get().n,0);
   target.exec("INSERT INTO terms(terms) VALUES('rebuild')");assert.equal(target.prepare("SELECT count(*) n FROM terms WHERE terms MATCH 'rent'").get().n,1);
  }finally{target.close();}
  db.exec('CREATE VIRTUAL TABLE unbacked USING fts5(value)');
  await assert.rejects(writeRestoreSql(db,path.join(dir,'unsafe.sql'),receipt,{deferFtsRebuild:true}),/source-backed/);
 }finally{db.close();}
}));

test('explicit legacy 0012 recovery preserves baseline evidence while deferring only its known disposable FTS cache',()=>temporary(async dir=>{
 const {sqlite:source}=memoryD1({migrationThrough:12});const target=new DatabaseSync(':memory:');
 try{
  source.exec("INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at,approved_at) VALUES('legacy','decision_docx','Test','Original','L','original.docx','https://example.invalid','now','now','historical-evidence')");
  assert.equal(requiredObjects(source,{legacyBaseline:true})[0].key,'original.docx');
  const receipt={runId:'legacy',bundleSha256:'b',targetSha256:'t'};
  await assert.rejects(writeRestoreSql(source,path.join(dir,'implicit.sql'),receipt,{deferFtsRebuild:true}),/explicit legacy/);
  await writeRestoreSql(source,path.join(dir,'legacy.sql'),receipt,{deferFtsRebuild:true,legacyBaseline:true});
  target.exec(claimStatements(receipt).join(';')+';BEGIN;'+await fs.readFile(path.join(dir,'legacy.sql'),'utf8')+'COMMIT;');
  assert.deepEqual(describe(target).tables,describe(source).tables);
  assert.equal(target.prepare('SELECT approved_at FROM documents').get().approved_at,'historical-evidence');
  assert.equal(target.prepare("SELECT COUNT(*) n FROM search_chunks_fts WHERE search_chunks_fts MATCH 'Original'").get().n,0);
 }finally{source.close();target.close();}
}));

test('bulk restore loads parents before children and indexes cyclic child keys without retaining support indexes',()=>temporary(async dir=>{
 const source=new DatabaseSync(':memory:'),target=new DatabaseSync(':memory:');
 try{
  source.exec("PRAGMA foreign_keys=OFF; CREATE TABLE z_parent(id INTEGER PRIMARY KEY); CREATE TABLE a_child(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES z_parent(id)); CREATE TABLE cycle_a(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES cycle_b(id)); CREATE TABLE cycle_b(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES cycle_a(id)); INSERT INTO z_parent VALUES(1); INSERT INTO a_child VALUES(1,1); INSERT INTO cycle_a VALUES(1,1); INSERT INTO cycle_b VALUES(1,1);");
  const receipt={runId:'relations',bundleSha256:'b',targetSha256:'t'};
  await writeRestoreSql(source,path.join(dir,'relations.sql'),receipt);
  const sql=await fs.readFile(path.join(dir,'relations.sql'),'utf8');
  assert.ok(sql.indexOf('INSERT INTO "z_parent"')<sql.indexOf('INSERT INTO "a_child"'),'parent data precedes child data even when names sort in reverse');
  assert.match(sql,/CREATE INDEX "beedle_recovery_fk_/,'cycles have bounded lookup support before inserts');
  target.exec(claimStatements(receipt).join(';')+';PRAGMA foreign_keys=ON;BEGIN;'+sql+'COMMIT;');
  assert.deepEqual(describe(target).tables,describe(source).tables);
  assert.equal(target.prepare("SELECT count(*) n FROM sqlite_master WHERE name GLOB 'beedle_recovery_fk_*'").get().n,0);
  assert.deepEqual(target.prepare('PRAGMA foreign_key_check').all(),[]);
 }finally{source.close();target.close();}
}));
