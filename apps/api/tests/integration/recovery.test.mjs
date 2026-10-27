import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { createWorkerFixture } from './fixture.mjs';
import { seedFixture, readyIndexes, readyReviewer } from './seeds.mjs';
import { main, query } from '../../scripts/recovery.mjs';
async function sqliteFile(dir) {
    for (const item of await fs.readdir(dir, { withFileTypes: true })) {
        const file = path.join(dir, item.name);
        if (item.isDirectory()) {
            const found = await sqliteFile(file);
            if (found)
                return found;
        }
        else if (file.endsWith('.sqlite'))
            return file;
    }
    return null;
}
async function json(file, value) {
    await fs.writeFile(file, JSON.stringify(value));
    return file;
}
test('sealed recovery restores disposable native D1/R2, resumes lost acknowledgments and preserves search, source and mutation receipts', { timeout: 900000 }, async () => {
    const source = await createWorkerFixture({ lexicalOnly: true, consumers: false });
    let target;
    try {
        const ids = await seedFixture(source), before = await source.request('/search', { query: 'repair notice', limit: 10 });
        const docPath = '/admin/ingestion/documents/' + ids.alpha;
        await source.request(docPath + '/reprocess', {});
        const detail = await source.request(docPath), mutationId = crypto.randomUUID();
        const payload = { expectedRevision: detail.documentRevision, mutationId, case_number: 'RECOVERY-RECEIPT', confirm_required_metadata: true };
        const changed = await source.request(docPath + '/metadata', payload);
        await source.request(docPath + '/approve', { expectedRevision: changed.documentRevision });
        const originalResponse = await fetch(source.url + '/source/' + ids.alpha);
        assert.equal(originalResponse.status, 200);
        const originalBytes = Buffer.from(await originalResponse.arrayBuffer());
        // Freeze all fixture writers, then capture the exact D1 snapshot and immutable source objects.
        await source.pause();
        const sourceSpec = await json(path.join(source.dir, 'source.json'), { mode: 'local', config: source.configPath, persistTo: source.persistTo, sqlite: await sqliteFile(path.join(source.persistTo, 'v3', 'd1')) });
        const bundle = path.join(source.dir, 'bundle');
        console.log('recovery rehearsal: capture');
        const captured = await main(['capture', sourceSpec, bundle]);
        assert.equal(captured.captured, true);
        target = await createWorkerFixture({ lexicalOnly: true, maintenanceOnly: true, consumers: false, initialize: false, autoStart: false });
        const targetSpec = await json(path.join(target.dir, 'target.json'), { mode: 'local', config: target.configPath, persistTo: target.persistTo, purpose: 'isolated-recovery', writersQuiesced: true, freshBucket: true });
        console.log('recovery rehearsal: plan');
        const planned = await main(['plan', bundle, targetSpec]);
        assert.match(planned.planSha256, /^[a-f0-9]{64}$/);
        await assert.rejects(main(['apply', bundle, targetSpec, 'wrong']), /checksum/);
        const wrong = await json(path.join(target.dir, 'wrong.json'), { ...JSON.parse(await fs.readFile(targetSpec)), persistTo: source.persistTo });
        await assert.rejects(main(['apply', bundle, wrong, planned.planSha256]), /distinct|binding mismatch/);
        const ep = { mode: 'local', config: target.configPath, persistTo: target.persistTo };
        // Real Wrangler file execution must roll back DDL and earlier writes on failure.
        // Use a separate database binding ID/state so the actual recovery target stays empty.
        const rollbackDir = path.join(target.dir, 'rollback');
        await fs.mkdir(rollbackDir);
        const { spawn } = await import('node:child_process'), { createRequire } = await import('node:module');
        const require = createRequire(import.meta.url);
        const badSql = await json(path.join(target.dir, 'unused.json'), {});
        await fs.writeFile(badSql, 'CREATE TABLE should_rollback(id INTEGER); INSERT INTO should_rollback VALUES(1); INSERT INTO absent VALUES(1);');
        const child = spawn(process.execPath, [require.resolve('wrangler'), 'd1', 'execute', 'DB', '--config', target.configPath, '--local', '--persist-to', rollbackDir, '--file', badSql], { stdio: 'ignore', env: { ...process.env, CLOUDFLARE_API_TOKEN: '', WRANGLER_SEND_METRICS: 'false' } });
        assert.notEqual(await new Promise(resolve => child.once('exit', resolve)), 0);
        const rollbackRows = await query({ ...ep, persistTo: rollbackDir }, "SELECT name FROM sqlite_master WHERE name='should_rollback'", target.dir);
        assert.deepEqual(rollbackRows, [], 'D1 import must be atomic');
        console.log('recovery rehearsal: apply');
        const report = await main(['apply', bundle, targetSpec, planned.planSha256]);
        assert.equal(report.dataVerified, true);
        console.log('recovery rehearsal: first verification complete');
        await fs.writeFile(path.join(bundle, 'checkpoint.json'), JSON.stringify({ runId: 'wrong', planSha256: planned.planSha256 }));
        await assert.rejects(main(['apply', bundle, targetSpec, planned.planSha256]), /Checkpoint/);
        // A vanished checkpoint/uncertain command response cannot replay domain inserts.
        await fs.rm(path.join(bundle, 'checkpoint.json'));
        assert.equal((await main(['apply', bundle, targetSpec, planned.planSha256])).dataVerified, true);
        const manifest = JSON.parse(await fs.readFile(path.join(bundle, 'bundle.json')));
        assert.equal(manifest.restore.tables.find(x => x.name === 'reviewer_mutations').count, 4);
        assert.ok(manifest.restore.tables.find(x => x.name === 'legal_reference_editions').count > 0);
        assert.ok(manifest.restore.tables.find(x => x.name === 'document_generations').count > 0);
        await query(ep, "UPDATE documents SET case_number='conflicting-content' WHERE id='" + ids.alpha + "'", target.dir);
        await assert.rejects(main(['verify', bundle, targetSpec]), /differs from manifest/);
        // Conflict test is destructive only to this disposable target. Restore exact DB again into another fresh local state.
        await fs.rm(target.persistTo, { recursive: true });
        await fs.mkdir(target.persistTo);
        assert.equal((await main(['apply', bundle, targetSpec, planned.planSha256])).dataVerified, true);
        const config = JSON.parse(await fs.readFile(target.configPath));
        config.vars.RELEASE_MAINTENANCE_ONLY = 'false';
        await json(target.configPath, config);
        await target.start();
        // Rebuild FTS explicitly, even though portable external-content postings were reconstructed.
        let recovery = await target.request('/admin/search-index/recovery');
        recovery = await target.request('/admin/search-index/rebuild', { dryRun: false, generation: crypto.randomUUID(), expectedGeneration: recovery.generation, replaceArchive: true, restart: recovery.status === 'running' });
        for (let n = 0; n < 50 && recovery.status !== 'completed'; n++)
            recovery = await target.request('/admin/search-index/rebuild/step', { generation: recovery.generation });
        assert.equal(recovery.status, 'completed');
        await readyIndexes(target);
        await readyReviewer(target);
        const after = await target.request('/search', { query: 'repair notice', limit: 10 });
        assert.deepEqual(new Set(after.results.map(x => x.documentId)), new Set(before.results.map(x => x.documentId)));
        const restored = await target.request(docPath);
        assert.equal(restored.caseNumber, 'RECOVERY-RECEIPT');
        assert.ok(restored.approvedAt);
        const replayed = await target.request(docPath + '/metadata', payload);
        assert.equal(replayed.mutationReceipt.replayed, true);
        assert.equal(replayed.documentRevision, changed.documentRevision);
        const download = await fetch(target.url + '/source/' + ids.alpha);
        assert.equal(download.status, 200);
        assert.match(download.headers.get('content-disposition'), /^attachment/);
        assert.equal(download.headers.get('content-type'), 'application/octet-stream');
        assert.deepEqual(Buffer.from(await download.arrayBuffer()), originalBytes);
        assert.equal((await fetch(target.url + '/source/' + ids.staged)).status, 404);
        const { open } = await import('../../scripts/lib/recovery-bundle.mjs');
        const snapshot = open(path.join(bundle, 'source.sqlite'));
        let requestId;
        try {
            requestId = snapshot.prepare('SELECT request_id FROM ingestion_requests WHERE document_id=?').get(ids.alpha).request_id;
        }
        finally {
            snapshot.close();
        }
        const ingested = await target.request('/ingest/requests/' + requestId + '?operation=decision_docx');
        assert.equal(ingested.state, 'complete');
        const object = manifest.objects.find(x => x.present);
        const sourceRows = await query(ep, "SELECT source_r2_key FROM documents WHERE id='" + ids.alpha + "'", target.dir);
        assert.ok(sourceRows[0].source_r2_key);
        assert.ok(object.sha256);
        // Sealing detects even a one-byte snapshot change before any command can reach a target.
        await fs.appendFile(path.join(bundle, 'source.sqlite'), 'tamper');
        await assert.rejects(main(['verify', bundle, targetSpec]), /checksum/);
    }
    finally {
        await source.stop();
        await target?.stop();
    }
});
