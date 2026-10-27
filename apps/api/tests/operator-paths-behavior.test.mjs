import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { execFileSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { requireExplicitDatabasePath, repositoryRoot, apiRoot } from '../scripts/lib/operator-paths.mjs';

test('operator data selection rejects missing, relative, guessed and non-SQLite paths without creating a database', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'beedle-operator-paths-'));
  try {
    const missing = path.join(dir, 'missing.sqlite');
    for (const value of ['', 'relative.sqlite', missing, dir]) assert.throws(() => requireExplicitDatabasePath(value), /D1_DB_PATH/);
    await assert.rejects(fs.stat(missing), { code: 'ENOENT' });
    const text = path.join(dir, 'text.sqlite'); await fs.writeFile(text, 'not a database');
    assert.throws(() => requireExplicitDatabasePath(text), /SQLite/);
    const snapshot = path.join(dir, 'snapshot with spaces.sqlite');
    const db = new DatabaseSync(snapshot); db.exec('CREATE TABLE example(id INTEGER)'); db.close();
    const alias = path.join(dir, 'alias.sqlite'); await fs.symlink(snapshot, alias);
    assert.equal(requireExplicitDatabasePath(alias), await fs.realpath(snapshot));
    assert.equal(requireExplicitDatabasePath(snapshot), await fs.realpath(snapshot));
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test('SQLite operator entrypoints fail before work when no explicit database was selected', async () => {
  const scripts = path.join(apiRoot, 'scripts');
  const entries = [];
  for (const name of await fs.readdir(scripts)) {
    if (!name.endsWith('.mjs')) continue;
    if ((await fs.readFile(path.join(scripts, name), 'utf8')).includes('const dbPath = requireExplicitDatabasePath();')) entries.push(name);
  }
  assert.equal(entries.length, 31);
  const cwd = await fs.mkdtemp(path.join(os.tmpdir(), 'beedle-no-db-'));
  try {
    for (const name of entries) {
      assert.throws(() => execFileSync(process.execPath, [path.join(scripts, name)], {
        cwd, env: { PATH: process.env.PATH, D1_DB_PATH: '' }, timeout: 10000, stdio: 'pipe'
      }), error => error.status !== 0 && /D1_DB_PATH must explicitly|Legacy activation writers are retired by H13/.test(String(error.stderr)), name);
    }
    assert.deepEqual(await fs.readdir(cwd), []);
  } finally { await fs.rm(cwd, { recursive: true, force: true }); }
});

test('document audit reads an explicit migrated snapshot from another working directory without changing it', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'beedle-snapshot-audit-'));
  try {
    const snapshot = path.join(dir, 'operator snapshot.sqlite'), db = new DatabaseSync(snapshot);
    for (const file of (await fs.readdir(path.join(apiRoot, 'migrations'))).filter(name => name.endsWith('.sql')).sort())
      db.exec(await fs.readFile(path.join(apiRoot, 'migrations', file), 'utf8'));
    db.exec(`INSERT INTO documents(id,file_type,jurisdiction,title,citation,source_r2_key,source_link,created_at,updated_at)
      VALUES('fixture','decision_docx','Test','Synthetic  spacing','T-OPERATOR','source','https://example.invalid','now','now')`);
    db.close();
    const digest = async () => createHash('sha256').update(await fs.readFile(snapshot)).digest('hex');
    const before = await digest();
    execFileSync(process.execPath, [path.join(apiRoot, 'scripts/document-metadata-anomaly-audit.mjs')], {
      cwd: dir, env: { PATH: process.env.PATH, D1_DB_PATH: snapshot }, stdio: 'pipe', timeout: 10000
    });
    const report = JSON.parse(await fs.readFile(path.join(dir, 'reports/document-metadata-anomaly-audit-report.json'), 'utf8'));
    assert.equal(report.summary.totalDecisionRows, 1); assert.equal(report.summary.missingDecisionDateCount, 1);
    assert.equal(report.summary.suspiciousTitleSpacingCount, 1); assert.equal(await digest(), before);
    assert.equal(report.dbPath, await fs.realpath(snapshot));
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test('search-stack wrapper relocates with spaces and rejects remote targets before any probe', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'beedle-relocated-'));
  try {
    const root = path.join(dir, 'another checkout'), bin = path.join(dir, 'bin'), log = path.join(dir, 'calls');
    await fs.mkdir(path.join(root, 'scripts'), { recursive: true });
    await fs.mkdir(path.join(root, 'apps/api'), { recursive: true }); await fs.mkdir(bin);
    const script = path.join(root, 'scripts/test-search-stack.sh');
    await fs.copyFile(path.join(repositoryRoot, 'scripts/test-search-stack.sh'), script);
    for (const name of ['curl', 'pnpm']) {
      const stub = path.join(bin, name);
      await fs.writeFile(stub, '#!/bin/sh\nprintf "%s|%s\\n" "$PWD" "$*" >> "$OPERATOR_TEST_LOG"\n'); await fs.chmod(stub, 0o755);
    }
    const env = { PATH: bin + path.delimiter + process.env.PATH, OPERATOR_TEST_LOG: log, API_BASE_URL: 'http://127.0.0.1:8787', WEB_URL: 'http://localhost:5555/search' };
    execFileSync('bash', [script, '--help'], { cwd: os.tmpdir(), env, stdio: 'pipe' });
    await assert.rejects(fs.stat(log), { code: 'ENOENT' });
    for (const target of ['https://example.invalid', 'http://127.0.0.1:8787?x=1', 'http://user:pass@localhost:8787', 'http://localhost:8787/path']) {
      assert.throws(() => execFileSync('bash', [script], { cwd: os.tmpdir(), env: { ...env, API_BASE_URL: target }, stdio: 'pipe' }));
    }
    await assert.rejects(fs.stat(log), { code: 'ENOENT' });
    execFileSync('bash', [script], { cwd: os.tmpdir(), env, stdio: 'pipe' });
    const calls = (await fs.readFile(log, 'utf8')).trim().split('\n');
    assert.equal(calls.length, 4);
    const physicalApiRoot = await fs.realpath(path.join(root, 'apps/api'));
    for (const call of calls.slice(1)) assert.ok(call.startsWith(physicalApiRoot + '|'), call);
    assert.match(calls[0], /--fail.*http:\/\/127\.0\.0\.1:8787\/health/);
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test('maintained operations entrypoints contain resolvable local documentation links', async () => {
  for (const name of ['README.md', 'docs/operations.md', 'docs/architecture.md', 'docs/corpus-operations.md', 'docs/cloudflare-deployment-runbook.md', 'apps/api/scripts/README.md']) {
    const file = path.join(repositoryRoot, name), text = await fs.readFile(file, 'utf8');
    assert.doesNotMatch(text, /\/Users\/|\/home\//, name);
    for (const [, href] of text.matchAll(/\]\(([^)]+)\)/g)) {
      if (/^https?:|^#/.test(href)) continue;
      await fs.access(path.resolve(path.dirname(file), href.split('#')[0]));
    }
  }
});
