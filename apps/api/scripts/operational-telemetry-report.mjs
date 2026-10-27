import fs from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { parseArgs } from 'node:util';
import { extractOperationalRecords, summarizeOperations } from './lib/operational-monitor.mjs';
try {
    const { values } = parseArgs({ options: { input: { type: 'string' }, out: { type: 'string' }, environment: { type: 'string' }, from: { type: 'string' }, to: { type: 'string' }, release: { type: 'string' }, representative: { type: 'boolean', default: false } } });
    if (!values.input || !values.out)
        throw new Error('Explicit --input JSONL and new --out paths are required');
    const stat = await fs.stat(values.input);
    if (!stat.isFile() || stat.size > 64 * 1024 * 1024)
        throw new Error('Input must be a private JSONL file no larger than 64 MiB');
    const records = [];
    for await (const line of createInterface({ input: createReadStream(values.input), crlfDelay: Infinity })) {
        if (!line.trim())
            continue;
        if (line.length > 64 * 1024)
            throw new Error('A telemetry input line exceeds 64 KiB');
        let row;
        try {
            row = JSON.parse(line);
        }
        catch {
            throw new Error('Expected JSONL; normalize event JSON before importing it');
        }
        records.push(...extractOperationalRecords(row));
        if (records.length > 100000)
            throw new Error('At most 100000 aggregate records per report');
    }
    const policy = JSON.parse(await fs.readFile(new URL('../observability/alerts.json', import.meta.url), 'utf8'));
    const report = summarizeOperations(records, policy, { environment: values.environment, from: Date.parse(values.from), to: Date.parse(values.to), releaseRevision: values.release || null, representative: values.representative });
    await fs.writeFile(values.out, JSON.stringify(report, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    console.log(JSON.stringify({ sampledRequests: report.sampledRequests, alertIds: report.alerts.map(row => row.id), sloStatus: report.baseline.sloStatus }));
    if (report.alerts.length)
        process.exitCode = 2;
}
catch (error) {
    console.error(error&&typeof error==='object'&&'code' in error?'Operational report I/O failed; check the private input/output paths.':error.message);
    process.exitCode = 1;
}
