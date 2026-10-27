import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
const repoRoot = path.resolve(process.cwd(), '../..');
const read = name => fs.readFile(path.join(repoRoot, '.github/workflows', name), 'utf8');
test('production migration and deployment workflows share a non-cancelling lock and explicit targets', async () => {
    const deploy = await read('deploy-api.yml'), migrate = await read('apply-d1-migrations.yml');
    assert.doesNotMatch(deploy, /d1 migrations apply/);
    assert.match(deploy, /workflow_dispatch:/);
    assert.match(migrate, /workflow_dispatch:/);
    assert.match(migrate, /environment:\s*production-d1-migrations/);
    assert.match(migrate, /group: api-release-production/);
    assert.match(deploy, /group: api-release-\$\{\{ inputs.target \}\}/);
    for (const file of [deploy, migrate])
        assert.match(file, /cancel-in-progress: false/);
    assert.match(migrate, /pnpm wrangler d1 migrations apply beedle --remote --env production/);
    assert.match(deploy, /environment: api-\$\{\{ inputs.target \}\}/);
});
test('release reuses the full secretless gate and requires matching preview proof before production', async () => {
    const deploy = await read('deploy-api.yml'), ci = await read('ci.yml');
    assert.match(deploy, /uses: \.\/\.github\/workflows\/ci.yml/);
    assert.match(deploy, /needs: quality/);
    assert.match(ci, /workflow_call:/);
    for (const command of ['typecheck', 'test:behavior', 'test:source', 'test:utils', 'test:web', 'test:integration', 'pages:build', 'test:e2e', 'retrieval-search-queryability-gate.test.mjs'])
        assert.ok(ci.includes(command), command);
    assert.match(ci, /BEEDLE_E2E_ADAPTER: pages/);
    for (const marker of ['Verify preview evidence provenance', 'Download matching preview evidence', 'BEEDLE_PREVIEW_EVIDENCE'])
        assert.ok(deploy.indexOf(marker) < deploy.indexOf('pnpm deploy:production'));
    assert.match(deploy, /github.ref == 'refs\/heads\/main'/);
    assert.doesNotMatch(deploy, /curl .*health|> \/dev\/null/);
});
