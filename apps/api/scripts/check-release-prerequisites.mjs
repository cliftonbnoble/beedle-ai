import fs from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { preflight } from './lib/release-preflight.mjs';
import { readProtectedReleaseConfig, withProtectedWranglerConfig } from './lib/protected-release-files.mjs';
try {
    const { values } = parseArgs({ options: { environment: { type: 'string' }, out: { type: 'string' } } });
    const config = readProtectedReleaseConfig();
    const report = await withProtectedWranglerConfig(config, configPath => preflight(config, values.environment, { configPath }));
    if (values.out)
        await fs.writeFile(values.out, JSON.stringify(report, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    console.log(JSON.stringify({ ready: report.ready, environment: report.environment,
        capability: report.capability, configSha256: report.configSha256, partitionCount: report.partitionCount }));
}
catch (error) {
    console.error(error.message);
    process.exitCode = 1;
}
