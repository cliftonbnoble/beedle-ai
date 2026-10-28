"""Split only the final sealed SQL range; preserve every original byte and guard."""
import hashlib
import importlib.machinery
import json
import os
import pathlib
import sys
import tempfile

parser = importlib.machinery.SourceFileLoader(
    'recovery_sql_parser', str(pathlib.Path(__file__).with_name('plan-recovery-sql-shards.py'))).load_module()
FINAL_CHECK = 'INSERT INTO beedle_recovery_assert SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM pragma_foreign_key_check) THEN 1 ELSE 0 END;'


def final_steps(sql, snapshot, shard_plan, shard_sha):
    canonical = json.dumps(shard_plan, separators=(',', ':'), ensure_ascii=False).encode()
    if hashlib.sha256(canonical).hexdigest() != shard_sha:
        raise ValueError('Exact parent shard plan required')
    parser.assert_acyclic(snapshot)
    parent = shard_plan['chunks'][-1]
    if parent['index'] < 1:
        raise ValueError('Final-step recovery requires preceding committed ranges')
    with pathlib.Path(sql).open('rb') as stream:
        stream.seek(parent['offset'])
        body = stream.read(parent['bytes'])
    if len(body) != parent['bytes'] or hashlib.sha256(body).hexdigest() != parent['sha256']:
        raise ValueError('Final parent byte range differs')
    steps, pending, staged, final = [], [], False, False

    def flush(kind):
        if not pending:
            return
        value = b''.join(data for _, data in pending)
        if len(value) > 8 * 1024 * 1024 or len(pending) > 128:
            raise ValueError('Indivisible final step exceeds original bounds')
        steps.append({'index': len(steps), 'offset': parent['offset'] + pending[0][0],
                      'bytes': len(value), 'statements': len(pending),
                      'sha256': hashlib.sha256(value).hexdigest(), 'kind': kind})
        pending.clear()

    # The temporary file contains private source bytes and is always removed.
    with tempfile.TemporaryDirectory(prefix='beedle-final-plan-', dir=pathlib.Path(sql).resolve().parent.parent) as tmp:
        file = pathlib.Path(tmp) / 'parent.sql'
        with os.fdopen(os.open(file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'wb') as out:
            out.write(body)
        for offset, data in parser.statements(file):
            text = data.decode().strip()
            if text == FINAL_CHECK:
                flush('data')
                final = True
            ddl = text.upper().startswith(('CREATE ', 'DROP ', 'ALTER ')) and not final
            starts_stage = text.startswith('INSERT INTO beedle_recovery_values(')
            if (ddl or starts_stage) and not staged:
                flush('data')
            if not staged and not final and pending and (
                    sum(len(v) for _, v in pending) + len(data) > 1024 * 1024 or len(pending) >= 8):
                flush('data')
            pending.append((offset, data))
            if starts_stage:
                staged = True
            if text == 'DELETE FROM beedle_recovery_values;':
                if not staged:
                    raise ValueError('Unexpected staged cleanup')
                staged = False
                flush('large-value')
            elif ddl:
                flush('ddl')
        if staged or not final or not pending or pending[-1][1].decode().strip() != 'DROP TABLE beedle_recovery_assert;':
            raise ValueError('Final guard/completion group is incomplete')
        flush('finalize')
    if sum(s['bytes'] for s in steps) != parent['bytes'] or sum(s['statements'] for s in steps) != parent['statements']:
        raise ValueError('Incomplete final parent coverage')
    return {'version': 1, 'kind': 'isolated-final-range-steps',
            'originalPlanSha256': shard_plan['originalPlanSha256'], 'shardPlanSha256': shard_sha,
            'originalSqlSha256': shard_plan['originalSqlSha256'], 'parentChunk': parent,
            'foreignKeyGraphAcyclic': True, 'steps': steps}


if __name__ == '__main__':
    if len(sys.argv) != 6:
        raise SystemExit('Usage: plan-recovery-final-steps.py SQL SNAPSHOT SHARD_PLAN SHARD_SHA OUTPUT')
    result = final_steps(sys.argv[1], sys.argv[2], json.loads(pathlib.Path(sys.argv[3]).read_text()), sys.argv[4])
    with os.fdopen(os.open(sys.argv[5], os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as out:
        json.dump(result, out, separators=(',', ':'))
        out.write('\n')
    print(json.dumps({'steps': len(result['steps']), 'parentIndex': result['parentChunk']['index']}))
