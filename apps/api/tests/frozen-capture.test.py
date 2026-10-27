import importlib.util
import pathlib
import sqlite3
import unittest
import tempfile
import json

module=importlib.util.spec_from_file_location('frozen_capture',pathlib.Path(__file__).resolve().parents[1]/'scripts/capture-frozen-baseline.py')
capture=importlib.util.module_from_spec(module);module.loader.exec_module(capture)


class Source:
    def __init__(self,db):self.db=db
    def query(self,sql,params=()):
        self.db.row_factory=sqlite3.Row
        return [dict(row) for row in self.db.execute(sql,params)]


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.source=sqlite3.connect(':memory:');self.target=sqlite3.connect(':memory:')
        self.source.execute('CREATE TABLE records(id TEXT PRIMARY KEY,value REAL,n INTEGER,bytes BLOB)')
        self.target.execute('CREATE TABLE records(id TEXT PRIMARY KEY,value REAL,n INTEGER,bytes BLOB)')
        self.target.execute('CREATE TABLE beedle_recovery_capture_state(name TEXT PRIMARY KEY,after_id TEXT,count INTEGER,chain TEXT,complete INTEGER)')
        self.client=Source(self.source);self.table={'name':'records'}
        for i in range(260):self.source.execute('INSERT INTO records(rowid,id,value,n,bytes) VALUES(?,?,?,?,?)',(i-130,'row-'+str(i),0.1,9223372036854775807,b'\x00\xff'))
        self.source.commit()

    def tearDown(self):self.source.close();self.target.close()

    def test_resume_after_committed_page_preserves_sparse_negative_keys_and_exact_values(self):
        def interrupt(*args):raise RuntimeError('simulated stop after committed page')
        with self.assertRaisesRegex(RuntimeError,'simulated stop'):capture.copy_table(self.client,self.target,self.table,interrupt)
        self.assertEqual(self.target.execute('SELECT COUNT(*) FROM records').fetchone()[0],128)
        self.assertEqual(capture.copy_table(self.client,self.target,self.table),260)
        self.assertEqual(capture.verify_table(self.client,self.target,self.table,260)['rows'],260)
        self.assertEqual(self.target.execute('SELECT MIN(rowid),MAX(rowid),MAX(n) FROM records').fetchone(),(-130,129,9223372036854775807))

    def test_page_write_and_checkpoint_roll_back_together(self):
        self.target.execute("CREATE TRIGGER fail BEFORE INSERT ON records WHEN new.rowid=0 BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):capture.copy_table(self.client,self.target,self.table)
        self.assertEqual(self.target.execute('SELECT COUNT(*) FROM records').fetchone()[0],128)
        self.target.execute('DROP TRIGGER fail')
        self.assertEqual(capture.copy_table(self.client,self.target,self.table),260)
        capture.verify_table(self.client,self.target,self.table,260)

    def test_final_verification_detects_changed_source_counts_and_old_target_rows(self):
        capture.copy_table(self.client,self.target,self.table)
        self.target.execute("UPDATE records SET value=0.2 WHERE rowid=-100");self.target.commit()
        with self.assertRaisesRegex(RuntimeError,'verification'):capture.verify_table(self.client,self.target,self.table,260)

    def test_wide_encoding_and_float_format_differences(self):
        columns=['c'+str(i) for i in range(100)]
        self.source.execute('CREATE TABLE wide('+','.join(capture.ident(c)+' TEXT' for c in columns)+')')
        self.source.execute('INSERT INTO wide VALUES('+','.join('?' for _ in columns)+')',['λ\x00']*100)
        value=self.source.execute('SELECT '+capture.encoded(columns)+' FROM wide').fetchone()[0]
        self.assertEqual(len(__import__('json').loads(value)),100)
        self.assertEqual(capture.row_bytes('1',['real:0.10000000000000000555']),capture.row_bytes('1',['real:0.1']))
        self.assertNotEqual(capture.row_bytes('1',['real:0.1']),capture.row_bytes('1',['real:0.10000000000000002']))

    def test_all_fences_block_writes_but_allow_reads(self):
        for sql in capture.freeze_statements([self.table]):self.source.execute(sql)
        for sql in ["INSERT INTO records(id) VALUES('new')","UPDATE records SET value=1","DELETE FROM records"]:
            with self.assertRaisesRegex(sqlite3.IntegrityError,'production_recovery_capture_freeze'):self.source.execute(sql)
        self.assertEqual(self.source.execute('SELECT COUNT(*) FROM records').fetchone()[0],260)

    def test_reviewed_freeze_and_rollback_are_owned_and_schema_verified(self):
        contract=json.loads((pathlib.Path(__file__).resolve().parents[1]/'release/contract.json').read_text())
        self.source.execute('CREATE TABLE d1_migrations(id INTEGER PRIMARY KEY,name TEXT)')
        self.source.executemany('INSERT INTO d1_migrations(name) VALUES(?)',[(name,) for name in contract['requiredMigrations'][:12]])
        self.source.commit()
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory)
            capture.plan_capture(self.client,root,{'accountId':'test','databaseId':'test','bucket':'test'})
            plan=json.loads((root/'freeze-plan.json').read_text())
            with self.assertRaisesRegex(RuntimeError,'already exists'):
                capture.plan_capture(self.client,root,{'accountId':'test','databaseId':'test','bucket':'test'})
            self.assertEqual(json.loads((root/'freeze-plan.json').read_text()),plan)
            self.source.executescript('BEGIN;'+(root/'freeze.sql').read_text()+'COMMIT;')
            capture.validate_freeze(self.client,plan)
            self.source.execute("UPDATE beedle_recovery_freeze SET run_id='other'");self.source.commit()
            with self.assertRaisesRegex(RuntimeError,'ownership'):capture.validate_freeze(self.client,plan)
            with self.assertRaises(sqlite3.IntegrityError):self.source.executescript('BEGIN;'+(root/'unfreeze.sql').read_text()+'COMMIT;')
            self.source.rollback()
            self.assertEqual(self.source.execute("SELECT COUNT(*) FROM sqlite_master WHERE name GLOB 'beedle_recovery_freeze_*'").fetchone()[0],6)
            self.source.execute('UPDATE beedle_recovery_freeze SET run_id=?',(plan['runId'],));self.source.commit()
            self.source.executescript('BEGIN;'+(root/'unfreeze.sql').read_text()+'COMMIT;')
            self.source.execute("UPDATE records SET value=0.2 WHERE rowid=0")


if __name__=='__main__':unittest.main()
