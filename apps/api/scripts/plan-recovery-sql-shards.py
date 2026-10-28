"""Partition sealed SQL without rewriting a byte. No remote access or case output."""
import hashlib
import json
import pathlib
import sqlite3
import sys


def statements(file):
    offset, pending = 0, b''
    with pathlib.Path(file).open('rb') as stream:
        while line := stream.readline(90003):
            pending += line
            if len(pending) > 90002:
                raise ValueError('Sealed statement exceeds its original byte ceiling')
            if sqlite3.complete_statement(pending.decode('utf-8')):
                yield offset, pending
                offset += len(pending)
                pending = b''
    if pending:
        raise ValueError('Incomplete sealed SQL statement')


def assert_acyclic(snapshot):
    db = sqlite3.connect('file:' + str(pathlib.Path(snapshot).resolve()) + '?mode=ro', uri=True)
    try:
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
        graph = {name: {r[2] for r in db.execute('PRAGMA foreign_key_list("' + name.replace('"', '""') + '")')}
                 for name in tables}
        visited, active = set(), set()

        def visit(name):
            if name in active:
                raise ValueError('Cyclic/self foreign keys require a separate atomic recovery plan')
            if name in visited:
                return
            active.add(name)
            for parent in graph.get(name, ()):
                if parent not in tables:
                    raise ValueError('Missing foreign-key parent')
                visit(parent)
            active.remove(name)
            visited.add(name)
        for name in tables:
            visit(name)
    finally:
        db.close()


def plan_shards(sql, snapshot, original_plan_sha, max_bytes=8 * 1024 * 1024, max_statements=128):
    if not (1024 <= max_bytes <= 8 * 1024 * 1024 and 8 <= max_statements <= 128):
        raise ValueError('Bounded shard size/statement count required')
    assert_acyclic(snapshot)
    chunks, chunk, group, staging, final = [], [], [], False, False
    whole = hashlib.sha256()

    def flush():
        if not chunk:
            return
        data = b''.join(value for _, value in chunk)
        chunks.append({'index': len(chunks), 'offset': chunk[0][0], 'bytes': len(data),
                       'statements': len(chunk), 'sha256': hashlib.sha256(data).hexdigest()})
        chunk.clear()

    def append_group():
        size = sum(len(value) for _, value in group)
        if size > max_bytes or len(group) > max_statements:
            raise ValueError('An indivisible staged value/finalization exceeds the shard budget')
        if chunk and (sum(len(value) for _, value in chunk) + size > max_bytes or
                      len(chunk) + len(group) > max_statements):
            flush()
        chunk.extend(group)
        group.clear()

    count = 0
    for offset, data in statements(sql):
        text = data.decode('utf-8').strip()
        if count == 0 and text != 'PRAGMA defer_foreign_keys=ON;':
            raise ValueError('Unsupported original recovery SQL header')
        if text.upper().startswith(('BEGIN', 'COMMIT', 'ROLLBACK', 'ATTACH', 'DETACH')):
            raise ValueError('Explicit transaction/database directives are unsupported')
        whole.update(data)
        count += 1
        group.append((offset, data))
        if text.startswith('INSERT INTO beedle_recovery_values('):
            staging = True
        if text == 'DELETE FROM beedle_recovery_values;':
            if not staging:
                raise ValueError('Unexpected staged-value cleanup')
            staging = False
        if text == 'INSERT INTO beedle_recovery_assert SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM pragma_foreign_key_check) THEN 1 ELSE 0 END;':
            final = True
        if not staging and not final:
            append_group()
    if staging or not final or not group or group[-1][1].decode().strip() != 'DROP TABLE beedle_recovery_assert;':
        raise ValueError('Incomplete original recovery finalization')
    append_group()
    flush()
    return {'version': 1, 'originalPlanSha256': original_plan_sha,
            'originalSqlSha256': whole.hexdigest(), 'originalSqlBytes': sum(c['bytes'] for c in chunks),
            'maxChunkBytes': max_bytes, 'maxChunkStatements': max_statements,
            'statementCount': count, 'foreignKeyGraphAcyclic': True, 'chunks': chunks}


if __name__ == '__main__':
    if len(sys.argv) != 5:
        raise SystemExit('Usage: plan-recovery-sql-shards.py SQL SNAPSHOT ORIGINAL_PLAN_SHA OUTPUT')
    result = plan_shards(*sys.argv[1:4])
    with pathlib.Path(sys.argv[4]).open('x', encoding='utf-8') as output:
        json.dump(result, output, separators=(',', ':'))
        output.write('\n')
    pathlib.Path(sys.argv[4]).chmod(0o600)
    print(json.dumps({'chunks': len(result['chunks']), 'bytes': result['originalSqlBytes']}))
