import fs from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { semanticSmoke } from './lib/release-smoke.mjs';
import { contract, targetFingerprint } from './lib/release-preflight.mjs';
import { fingerprint } from './lib/release-contract.mjs';
import { validateDeployment } from './lib/deployment-config.mjs';
try {
    const { values } = parseArgs({ options: { environment: { type: 'string' }, plan: { type: 'string' }, revision: { type: 'string' }, out: { type: 'string' } } });
    if (!values.plan || !values.out || !/^[a-f0-9]{40}$/.test(values.revision || ''))
        throw new Error('Explicit --environment, --plan, --revision and new --out file are required');
    const config = JSON.parse(await fs.readFile(new URL('../wrangler.json', import.meta.url), 'utf8')), target = validateDeployment(config, values.environment);
    const stat = await fs.stat(values.plan);
    if (!stat.isFile() || stat.size > 256 * 1024)
        throw new Error('Smoke plan must be a JSON file no larger than 256 KiB');
    const report = await semanticSmoke({ apiBase: target.vars.SOURCE_PROXY_BASE_URL, environment: values.environment, revision: values.revision, configSha256: await targetFingerprint(config, values.environment), contractSha256: await fingerprint(contract), plan: JSON.parse(await fs.readFile(values.plan, 'utf8')) });
    await fs.writeFile(values.out, JSON.stringify(report, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    console.log(JSON.stringify({ passed: true, checks: report.checks }));
}
catch (error) {
    console.error(error.message);
    process.exitCode = 1;
}
