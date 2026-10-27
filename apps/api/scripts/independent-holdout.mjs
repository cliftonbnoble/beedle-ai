import fs from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { collectHoldout, digest, evaluateHoldout } from './lib/independent-holdout.mjs';
const { values } = parseArgs({ options: { mode: { type: 'string' }, holdout: { type: 'string' }, run: { type: 'string' }, judgments: { type: 'string' }, out: { type: 'string' }, api: { type: 'string' }, 'target-kind': { type: 'string' }, 'corpus-sha256': { type: 'string' }, 'code-revision': { type: 'string' }, 'allow-provider-calls': { type: 'boolean', default: false } } });
async function read(file) { if (!file)
    throw new Error('Required input path is missing'); const stat = await fs.stat(file); if (!stat.isFile() || stat.size > 16 * 1024 * 1024)
    throw new Error('Input must be a JSON file no larger than 16 MiB'); return JSON.parse(await fs.readFile(file, 'utf8')); }
try {
    if (!values.out)
        throw new Error('An explicit --out path is required');
    let result;
    if (values.mode === 'score')
        result = evaluateHoldout(await read(values.holdout), await read(values.run), await read(values.judgments));
    else if (values.mode === 'collect') {
        if (values['target-kind'] !== 'isolated-preview')
            throw new Error('Collection requires --target-kind isolated-preview; never use production');
        result = await collectHoldout(await read(values.holdout), { apiBase: values.api, corpusSha256: values['corpus-sha256'], codeRevision: values['code-revision'], allowProviderCalls: values['allow-provider-calls'] });
    }
    else
        throw new Error('Choose --mode collect or --mode score');
    // Exclusive creation prevents accidental overwrite of sealed evidence. Artifacts
    // can contain legal facts: private permissions, no payloads printed to CI logs.
    await fs.writeFile(values.out, JSON.stringify(result, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    console.log(JSON.stringify({ artifactSha256: digest(result), mode: values.mode, acceptance: result.acceptance || 'human-judgments-required' }));
}
catch (error) {
    console.error(error instanceof Error ? error.message : 'Holdout evaluation failed');
    process.exitCode = 1;
}
