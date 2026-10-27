import fs from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { preflight } from './lib/release-preflight.mjs';
try {
    const { values } = parseArgs({ options: { environment: { type: 'string' }, out: { type: 'string' } } });
    const config = JSON.parse(await fs.readFile(new URL('../wrangler.json', import.meta.url), 'utf8'));
    const report = await preflight(config, values.environment);
    if (values.out)
        await fs.writeFile(values.out, JSON.stringify(report, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    console.log(JSON.stringify(report));
}
catch (error) {
    console.error(error.message);
    process.exitCode = 1;
}
