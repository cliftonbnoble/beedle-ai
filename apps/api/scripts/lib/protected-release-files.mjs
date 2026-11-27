import { lstatSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { bindProtectedReleaseTargets } from './protected-release-bindings.mjs';

const apiRoot = fileURLToPath(new URL('../../', import.meta.url));

export function readProtectedReleaseConfig(file = process.env.BEEDLE_RELEASE_BINDINGS_FILE) {
    if (!file)
        throw new Error('Supply the protected release bindings in a private file');
    const details = lstatSync(file);
    if (!details.isFile() || (details.mode & 0o077) || details.size > 40 * 1024)
        throw new Error('Supply the protected release bindings in a private file of at most 40 KiB');
    const template = JSON.parse(readFileSync(join(apiRoot, 'wrangler.json'), 'utf8'));
    try {
        return bindProtectedReleaseTargets(template, JSON.parse(readFileSync(file, 'utf8')));
    }
    catch (error) {
        if (error instanceof SyntaxError)
            throw new Error('Invalid protected release bindings');
        throw error;
    }
}

export async function withProtectedWranglerConfig(config, callback) {
    const path = mkdtempSync(join(tmpdir(), 'beedle-protected-release-'));
    try {
        const configPath = join(path, 'wrangler.json');
        const rendered = structuredClone(config);
        rendered.main = resolve(apiRoot, rendered.main);
        delete rendered.$schema;
        for (const target of [rendered, ...Object.values(rendered.env)])
            for (const database of target.d1_databases)
                if (database.migrations_dir)
                    database.migrations_dir = resolve(apiRoot, database.migrations_dir);
        writeFileSync(configPath, JSON.stringify(rendered), { flag: 'wx', mode: 0o600 });
        return await callback(configPath, path);
    }
    finally {
        rmSync(path, { recursive: true, force: true });
    }
}
