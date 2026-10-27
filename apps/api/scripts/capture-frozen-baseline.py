"""Exact typed capture of an explicitly fenced legacy D1 baseline and R2 originals.

Python 3.11+, standard library only. No production mutation is performed here:
plan writes reviewed freeze/unfreeze SQL; capture requires that freeze to exist.
All contents and progress live in a private, explicitly selected directory.
"""
import concurrent.futures
import fcntl
import hashlib
import http.client
import json
import os
import pathlib
import shutil
import sqlite3
import ssl
import struct
import sys
import threading
import time
import tomllib
import urllib.parse
import uuid


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def encoded(columns):
    items = []
    for column in columns:
        c = ident(column)
        items.append(f"CASE typeof({c}) WHEN 'null' THEN 'null:' WHEN 'text' THEN 'text:'||hex(CAST({c} AS BLOB)) WHEN 'blob' THEN 'blob:'||hex({c}) WHEN 'integer' THEN 'integer:'||CAST({c} AS TEXT) ELSE 'real:'||printf('%!.26g',{c}) END")
    groups = ['json_array(' + ','.join(items[i:i+24]) + ')' for i in range(0, len(items), 24)]
    result = groups[0] if groups else 'json_array()'
    for group in groups[1:]:
        result = f"rtrim({result},']')||','||substr({group},2)"
    return result


def decode(value):
    kind, raw = value.split(':', 1)
    if kind == 'null': return None
    if kind == 'text': return bytes.fromhex(raw).decode('utf8')
    if kind == 'blob': return bytes.fromhex(raw)
    if kind == 'integer': return int(raw)
    if kind == 'real': return float(raw)
    raise RuntimeError('Unknown SQLite type')


def row_bytes(rid, values):
    canonical = []
    for value in values:
        kind, raw = value.split(':', 1)
        canonical.append('real:' + struct.pack('>d', float(raw)).hex() if kind == 'real' else value)
    return (str(rid) + ':' + json.dumps(canonical, separators=(',', ':'))).encode()


def chain(previous, rid, values):
    return hashlib.sha256(bytes.fromhex(previous) + row_bytes(rid, values)).hexdigest()


def atomic_json(file, value):
    tmp = file.with_suffix('.' + uuid.uuid4().hex + '.tmp')
    with open(tmp, 'x', encoding='utf8') as out:
        os.chmod(tmp, 0o600)
        json.dump(value, out, indent=2)
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, file)


class Cloudflare:
    def __init__(self, spec):
        self.spec = spec
        self.local = threading.local()

    def request(self, path, payload=None, raw=False):
        for attempt in range(5):
            try:
                token = os.environ.get('CLOUDFLARE_API_TOKEN')
                if not token:
                    token = tomllib.loads(pathlib.Path(self.spec['oauthFile']).read_text())['oauth_token']
                if not getattr(self.local, 'connection', None):
                    self.local.connection = http.client.HTTPSConnection('api.cloudflare.com', timeout=60, context=ssl.create_default_context(cafile=self.spec.get('caFile')))
                self.local.connection.request('GET' if payload is None else 'POST', '/client/v4/accounts/' + self.spec['accountId'] + path, body=None if payload is None else json.dumps(payload).encode(), headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
                response = self.local.connection.getresponse()
                limit = self.spec.get('maxObjectBytes', 33554432) if raw else 12*1024*1024
                body = response.read(limit + 1)
                if len(body) > limit: raise RuntimeError('Capture response exceeds byte bound')
                if response.status in (401, 429, 500, 502, 503, 504): raise OSError('Retryable Cloudflare response')
                if response.status != 200: raise RuntimeError('Capture HTTP ' + str(response.status))
                if raw: return body
                result = json.loads(body)
                if result.get('success') is not True: raise RuntimeError('Cloudflare rejected capture inspection')
                return result['result']
            except (OSError, http.client.HTTPException, tomllib.TOMLDecodeError):
                if getattr(self.local, 'connection', None): self.local.connection.close()
                self.local.connection = None
                if attempt == 4: raise RuntimeError('Capture read retries exhausted') from None
                time.sleep(min(2**attempt, 8))

    def query(self, sql, params=()):
        if not sql.startswith(('SELECT ', 'PRAGMA table_list', 'PRAGMA table_xinfo')):
            raise RuntimeError('Capture permits only fixed read-only inspections')
        rows = self.request('/d1/database/' + self.spec['databaseId'] + '/query', {'sql': sql, 'params': list(params)})
        if not rows or any(row.get('success') is not True or not isinstance(row.get('results'), list) for row in rows):
            raise RuntimeError('Incomplete capture query result')
        return rows[0]['results']

    def object(self, key):
        return self.request('/r2/buckets/' + urllib.parse.quote(self.spec['bucket'], safe='') + '/objects/' + urllib.parse.quote(key, safe=''), raw=True)


def schema(client):
    listing = client.query('PRAGMA table_list')
    types = {row['name']: row['type'] for row in listing}
    rows = client.query("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name")
    objects = [row for row in rows if not row['name'].startswith(('sqlite_', '_cf_', 'beedle_recovery_')) and types.get(row['name']) != 'shadow']
    tables = [row for row in objects if row['type'] == 'table' and types[row['name']] == 'table']
    for table in tables:
        if any(row['name']==table['name'] and row.get('wr') for row in listing): raise RuntimeError('Legacy capture requires rowid tables')
        info = client.query('PRAGMA table_xinfo(' + ident(table['name']) + ')')
        if any(x['name'].lower() in ('rowid', '_rowid_', 'oid') for x in info): raise RuntimeError('Unsupported baseline rowid alias')
    return objects, tables


def freeze_statements(tables):
    result = []
    for table in tables:
        name = table['name']
        for verb in ('INSERT', 'UPDATE', 'DELETE'):
            trigger = 'beedle_recovery_freeze_' + hashlib.sha256(name.encode()).hexdigest()[:12] + '_' + verb.lower()
            result.append(f"CREATE TRIGGER {ident(trigger)} BEFORE {verb} ON {ident(name)} BEGIN SELECT RAISE(ABORT,'production_recovery_capture_freeze'); END")
    return result


def schema_sha(objects):
    return hashlib.sha256(json.dumps(objects, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def validate_freeze(client, plan):
    rows = client.query('SELECT run_id,schema_sha256 FROM beedle_recovery_freeze WHERE singleton=1')
    if rows != [{'run_id': plan['runId'], 'schema_sha256': plan['schemaSha256']}]: raise RuntimeError('Source freeze ownership differs from plan')
    actual = client.query("SELECT sql FROM sqlite_master WHERE type='trigger' AND name GLOB 'beedle_recovery_freeze_*' ORDER BY name")
    if sorted(row['sql'].rstrip(';') for row in actual) != sorted(freeze_statements(plan['tables'])): raise RuntimeError('Source write fences are incomplete or changed')
    objects, _ = schema(client)
    if schema_sha(objects) != plan['schemaSha256']: raise RuntimeError('Source schema changed during capture')


def copy_table(client, db, table, progress=None):
    name = table['name']
    columns = [row['name'] for row in client.query('PRAGMA table_xinfo(' + ident(name) + ')') if row['hidden'] == 0]
    state = db.execute('SELECT after_id,count,chain,complete FROM beedle_recovery_capture_state WHERE name=?', (name,)).fetchone()
    after, count, digest, complete = state or (None, 0, '0'*64, 0)
    if complete: return count
    while True:
        sql = 'SELECT CAST(rowid AS TEXT) rid,' + encoded(columns) + ' value FROM ' + ident(name) + (' WHERE rowid>CAST(? AS INTEGER)' if after is not None else '') + ' ORDER BY rowid LIMIT 128'
        rows = client.query(sql, [after] if after is not None else [])
        with db:
            for row in rows:
                values = json.loads(row['value'])
                db.execute('INSERT INTO ' + ident(name) + '(rowid,' + ','.join(map(ident, columns)) + ') VALUES(' + ','.join('?' for _ in range(len(columns)+1)) + ')', (int(row['rid']), *map(decode, values)))
                digest = chain(digest, row['rid'], values)
                count += 1
            next_id = rows[-1]['rid'] if rows else after
            if rows and after is not None and int(next_id) <= int(after): raise RuntimeError('Capture cursor did not advance')
            after = next_id
            db.execute('INSERT OR REPLACE INTO beedle_recovery_capture_state VALUES(?,?,?,?,?)', (name, after, count, digest, int(not rows)))
        if progress: progress(name, count)
        if not rows: return count


def verify_table(client, db, table, expected):
    name = table['name']
    columns = [row[1] for row in db.execute('PRAGMA table_xinfo(' + ident(name) + ')') if row[6] == 0]
    digest, count = '0'*64, 0
    for rid, value in db.execute('SELECT CAST(rowid AS TEXT),' + encoded(columns) + ' FROM ' + ident(name) + ' ORDER BY rowid'):
        digest = chain(digest, rid, json.loads(value))
        count += 1
    state = db.execute('SELECT count,chain,complete FROM beedle_recovery_capture_state WHERE name=?', (name,)).fetchone()
    remote = client.query('SELECT COUNT(*) n FROM ' + ident(name))[0]['n']
    if state != (count, digest, 1) or count != expected or count != remote: raise RuntimeError('Captured content/count verification failed')
    return {'name': name, 'rows': count, 'canonicalChainSha256': digest}


def plan_capture(client, root, spec):
    if any((root/name).exists() for name in ('freeze-plan.json','freeze.sql','unfreeze.sql','source.sqlite')):
        raise RuntimeError('Capture plan already exists; preserve its original freeze ownership and choose a new private directory')
    objects, tables = schema(client)
    migrations = [r['name'] for r in client.query('SELECT name FROM d1_migrations ORDER BY id')]
    contract = json.loads((pathlib.Path(__file__).resolve().parents[1] / 'release/contract.json').read_text())
    if migrations != contract['requiredMigrations'][:12]: raise RuntimeError('Capture requires the explicit legacy 0012 baseline')
    plan = {'runId': str(uuid.uuid4()), 'schemaSha256': schema_sha(objects), 'schema': objects, 'tables': tables, 'source': {k:spec[k] for k in ('accountId','databaseId','bucket')}, 'createdAt': time.time()}
    atomic_json(root/'freeze-plan.json', plan)
    control = "CREATE TABLE beedle_recovery_freeze(singleton INTEGER PRIMARY KEY CHECK(singleton=1),run_id TEXT NOT NULL,schema_sha256 TEXT NOT NULL);\nINSERT INTO beedle_recovery_freeze VALUES(1,'" + plan['runId'] + "','" + plan['schemaSha256'] + "');\n"
    (root/'freeze.sql').write_text(control + ';\n'.join(freeze_statements(tables)) + ';\n')
    undo = "CREATE TABLE beedle_recovery_unfreeze_assert(ok INTEGER CHECK(ok=1));\nINSERT INTO beedle_recovery_unfreeze_assert SELECT CASE WHEN EXISTS(SELECT 1 FROM beedle_recovery_freeze WHERE singleton=1 AND run_id='" + plan['runId'] + "' AND schema_sha256='" + plan['schemaSha256'] + "') THEN 1 ELSE 0 END;\n"
    for sql in freeze_statements(tables): undo += 'DROP TRIGGER ' + sql.split(' BEFORE ')[0].removeprefix('CREATE TRIGGER ') + ';\n'
    (root/'unfreeze.sql').write_text(undo + 'DROP TABLE beedle_recovery_freeze;\nDROP TABLE beedle_recovery_unfreeze_assert;\n')
    for path in (root/'freeze.sql',root/'unfreeze.sql'): path.chmod(0o600)
    return {'planned': True, 'tables': len(tables), 'fences': len(tables)*3, 'runId': plan['runId']}


def capture(client, root, spec):
    plan = json.loads((root/'freeze-plan.json').read_text())
    if plan['source'] != {k:spec[k] for k in ('accountId','databaseId','bucket')}: raise RuntimeError('Capture source differs from sealed plan')
    validate_freeze(client, plan)
    file = root/'source.sqlite'
    new = not file.exists()
    db = sqlite3.connect(file); file.chmod(0o600)
    db.execute('PRAGMA foreign_keys=OFF'); db.execute('PRAGMA journal_mode=WAL')
    db.execute('CREATE TABLE IF NOT EXISTS beedle_recovery_capture_state(name TEXT PRIMARY KEY,after_id TEXT,count INTEGER,chain TEXT,complete INTEGER)')
    db.execute('CREATE TABLE IF NOT EXISTS beedle_recovery_capture_meta(name TEXT PRIMARY KEY,value TEXT)')
    db.execute('CREATE TABLE IF NOT EXISTS beedle_recovery_capture_objects(key TEXT PRIMARY KEY,sha256 TEXT,bytes INTEGER)')
    if new:
        for table in plan['tables']: db.execute(table['sql'])
        db.execute("INSERT INTO beedle_recovery_capture_meta VALUES('run_id',?)", (plan['runId'],)); db.commit()
    if db.execute("SELECT value FROM beedle_recovery_capture_meta WHERE name='run_id'").fetchone() != (plan['runId'],): raise RuntimeError('Existing capture belongs to another run')
    status = {'runId': plan['runId'], 'pid':os.getpid(), 'phase':'copying','updatedAt':time.time()}
    atomic_json(root/'capture-status.json',status)
    last_report = 0
    def progress(table, count):
        nonlocal last_report
        if time.monotonic()-last_report > 15:
            if shutil.disk_usage(root).free<20*1024**3: raise RuntimeError('Private capture disk reserve reached')
            validate_freeze(client, plan)
            status.update(table=table,tableRows=count,updatedAt=time.time());atomic_json(root/'capture-status.json',status)
            print(json.dumps(status),flush=True);last_report=time.monotonic()
    descriptions = []
    for table in plan['tables']:
        expected = client.query('SELECT COUNT(*) n FROM ' + ident(table['name']))[0]['n']
        copy_table(client, db, table, progress)
        descriptions.append(verify_table(client, db, table, expected))
    validate_freeze(client, plan)
    if not db.execute("SELECT 1 FROM beedle_recovery_capture_meta WHERE name='schema_complete'").fetchone():
        db.execute('BEGIN')
        # Reconstruct this disposable legacy FTS cache from captured source rows.
        for obj in plan['schema']:
            if obj['type']=='table' and obj not in plan['tables']: db.execute(obj['sql'])
        for obj in plan['schema']:
            if obj['type']=='view': db.execute(obj['sql'])
        db.execute("INSERT INTO search_chunks_fts(source_kind,chunk_id,document_id,active,section_label,paragraph_anchor,citation_anchor,chunk_text,created_at,order_rank) SELECT 'document',id,document_id,1,section_label,paragraph_anchor,citation_anchor,chunk_text,created_at,chunk_order FROM document_chunks")
        db.execute("INSERT INTO search_chunks_fts(source_kind,chunk_id,document_id,active,section_label,paragraph_anchor,citation_anchor,chunk_text,created_at,order_rank) SELECT 'retrieval',chunk_id,document_id,active,section_label,paragraph_anchor,citation_anchor,chunk_text,created_at,999999 FROM retrieval_search_chunks")
        for obj in plan['schema']:
            if obj['type'] in ('index','trigger'): db.execute(obj['sql'])
        db.execute("INSERT INTO beedle_recovery_capture_meta VALUES('schema_complete','true')");db.commit()
    status.update(phase='integrity',updatedAt=time.time());atomic_json(root/'capture-status.json',status)
    if db.execute('PRAGMA quick_check').fetchone()[0]!='ok' or db.execute('PRAGMA foreign_key_check').fetchall(): raise RuntimeError('Capture integrity failed')
    directory=root/'objects';directory.mkdir(mode=0o700,exist_ok=True)
    objects = db.execute('SELECT DISTINCT source_r2_key FROM documents ORDER BY source_r2_key').fetchall()
    expected_hashes = {}
    for key,metadata in db.execute('SELECT source_r2_key,metadata_json FROM documents'):
        expected=json.loads(metadata or '{}').get('sourceSha256')
        if expected:
            if key in expected_hashes and expected_hashes[key]!=expected: raise RuntimeError('Conflicting recorded original hashes')
            expected_hashes[key]=expected
    def transfer(key):
        body=client.object(key);digest=hashlib.sha256(body).hexdigest();destination=directory/digest
        if expected_hashes.get(key,digest)!=digest: raise RuntimeError('Original differs from its recorded hash')
        if destination.exists():
            if hashlib.sha256(destination.read_bytes()).hexdigest()!=digest: raise RuntimeError('Conflicting private object file')
        else:
            temporary=directory/uuid.uuid4().hex;temporary.write_bytes(body);temporary.chmod(0o600);os.replace(temporary,destination)
        return key,digest,len(body)
    pending=[]
    for (key,) in objects:
        old=db.execute('SELECT sha256 FROM beedle_recovery_capture_objects WHERE key=?',(key,)).fetchone()
        if old:
            file=directory/old[0]
            if not file.exists() or hashlib.sha256(file.read_bytes()).hexdigest()!=old[0] or expected_hashes.get(key,old[0])!=old[0]: raise RuntimeError('Previously captured original differs')
        else: pending.append(key)
    status.update(phase='originals',objectsTotal=len(objects));atomic_json(root/'capture-status.json',status)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        # Bound completed response bodies as well as the four active transfers.
        for start in range(0,len(pending),8):
            for key,digest,size in pool.map(transfer,pending[start:start+8]):
                db.execute('INSERT INTO beedle_recovery_capture_objects VALUES(?,?,?)',(key,digest,size));db.commit()
                status.update(objectsDone=db.execute('SELECT COUNT(*) FROM beedle_recovery_capture_objects').fetchone()[0],updatedAt=time.time())
                if status['objectsDone']%100==0:
                    validate_freeze(client,plan);atomic_json(root/'capture-status.json',status);print(json.dumps(status),flush=True)
    validate_freeze(client,plan)
    db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    manifest={'classification':'Quiesced typed legacy baseline; ordinary rows exact, disposable FTS reconstructed; no new provenance or approval inferred','runId':plan['runId'],'source':plan['source'],'schemaSha256':plan['schemaSha256'],'tables':descriptions,'objects':[{'key':k,'sha256':s,'bytes':b,'provenance':'recorded-hash-matched' if k in expected_hashes else 'observed-legacy-bytes'} for k,s,b in db.execute('SELECT key,sha256,bytes FROM beedle_recovery_capture_objects ORDER BY key')],'completedAt':time.time()}
    atomic_json(root/'capture-manifest.json',manifest)
    status.update(phase='captured',objectsDone=len(objects),updatedAt=time.time());atomic_json(root/'capture-status.json',status);print(json.dumps(status),flush=True)
    db.close()


def main():
    if len(sys.argv)!=4 or sys.argv[1] not in ('plan','capture'): raise RuntimeError('Usage: capture-frozen-baseline.py plan|capture /absolute/source.json /absolute/private-directory')
    action,descriptor,directory=sys.argv[1:]
    if not pathlib.Path(descriptor).is_absolute() or not pathlib.Path(directory).is_absolute(): raise RuntimeError('Explicit absolute paths required')
    root=pathlib.Path(directory);root.mkdir(mode=0o700,exist_ok=True)
    if root.stat().st_mode&0o077: raise RuntimeError('Capture directory must be private to its owner')
    spec=json.loads(pathlib.Path(descriptor).read_text());client=Cloudflare(spec)
    with open(root/'operation.lock','a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if action=='plan':
            if (root/'freeze-plan.json').exists(): raise RuntimeError('Plan already exists; resume its original capture')
            print(json.dumps(plan_capture(client,root,spec)),flush=True)
        else: capture(client,root,spec)


if __name__=='__main__':
    try: main()
    except Exception as error:
        # Corpus values, source keys, SQL and provider responses never go to stdout.
        failure={'failed':True,'errorType':type(error).__name__,'message':str(error) if isinstance(error,RuntimeError) else 'Inspect private capture state; no completion is claimed'}
        if not isinstance(error,BlockingIOError) and len(sys.argv)==4 and pathlib.Path(sys.argv[3]).is_absolute():
            path=pathlib.Path(sys.argv[3])/'capture-status.json'
            if path.exists():
                previous=json.loads(path.read_text());atomic_json(path,{**previous,**failure,'phase':'held','lastPhase':previous.get('phase'),'updatedAt':time.time()})
        print(json.dumps(failure),flush=True)
        sys.exit(1)
