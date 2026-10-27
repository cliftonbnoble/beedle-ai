import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { evaluateReleaseState, fingerprint, releaseProfileId, schemaProblems } from './release-contract.mjs';
import { validateDeployment } from './deployment-config.mjs';
export const contract = JSON.parse(readFileSync(new URL('../../release/contract.json', import.meta.url), 'utf8'));
const apiRoot = fileURLToPath(new URL('../../', import.meta.url)), require = createRequire(import.meta.url);
export async function targetFingerprint(config, environment) { return fingerprint({ environment, compatibilityDate: config.compatibility_date, compatibilityFlags: config.compatibility_flags, main: config.main, target: config.env[environment] }); }
export async function inspectDatabase(execute, vars) {
    const snapshots = await execute([contract.queries.objects, contract.queries.columns, contract.queries.migrations]);
    if (!Array.isArray(snapshots) || snapshots.length !== 3 || snapshots.some(rows => !Array.isArray(rows)))
        throw new Error('Incomplete schema inspection');
    const problems = schemaProblems(contract, ...snapshots);
    if (problems.length)
        throw new Error('Release schema is incompatible: ' + problems.slice(0, 8).join(', '));
    const profileId = await releaseProfileId(vars, contract);
    const [rows] = await execute([contract.queries.state.replaceAll('?1', "'" + profileId + "'")]);
    if (rows?.length !== 1)
        throw new Error('Readiness state is missing');
    const result = evaluateReleaseState(contract, rows[0], vars);
    if (!result.ready)
        throw new Error('Release maintenance is incomplete: ' + result.problems.join(', '));
    return result;
}
export function wranglerJson(args, { configPath = fileURLToPath(new URL('../../wrangler.json', import.meta.url)) } = {}) {
    let output;
    try {
        output = execFileSync(process.execPath, [require.resolve('wrangler'), ...args, '--config', configPath, '--json'], { cwd: apiRoot, env: { ...process.env, WRANGLER_SEND_METRICS: 'false' }, encoding: 'utf8', timeout: 60000, maxBuffer: 8 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] });
    }
    catch {
        throw new Error('Read-only Wrangler inspection failed; no deployment was attempted');
    }
    try {
        return JSON.parse(output);
    }
    catch {
        throw new Error('Wrangler returned an invalid inspection contract');
    }
}
export async function preflight(config, environment, { execute } = {}) {
    const target = validateDeployment(config, environment);
    if (target.d1_databases.filter(row => row.binding === 'DB').length !== 1)
        throw new Error('Exactly one explicit DB binding is required');
    const inspect = execute || (async (sql) => {
        const result = wranglerJson(['d1', 'execute', 'DB', '--env', environment, '--remote', '--command', sql.join(';\n')]);
        if (!Array.isArray(result) || result.length !== sql.length || result.some(row => row.success !== true || !Array.isArray(row.results)))
            throw new Error('D1 inspection failed or returned partial results');
        return result.map(row => row.results);
    });
    return { environment, configSha256: await targetFingerprint(config, environment), contractSha256: await fingerprint(contract), ...await inspectDatabase(inspect, target.vars) };
}
export async function validatePreviewEvidence(config, revision, report, { now = Date.now() } = {}) {
    validateDeployment(config, 'preview');
    if (!report || report.version !== 1 || report.environment !== 'preview' || report.passed !== true || report.codeRevision !== revision || report.contractSha256 !== await fingerprint(contract) || report.configSha256 !== await targetFingerprint(config, 'preview') || report.capability !== (config.env.preview.vars.VECTOR_LIFECYCLE_READY === 'true' && config.env.preview.vars.VECTOR_FILTERS_READY === 'true' ? 'semantic' : 'lexical_only') || report.preparationOnly === true || !['positive', 'negative', 'filtered', 'source'].every(key => Number.isSafeInteger(report.checks?.[key]) && report.checks[key] >= 1 && report.checks[key] <= 12) || report.checks?.readiness !== true)
        throw new Error('Matching successful isolated-preview evidence is required');
    const age = now - Date.parse(report.completedAt);
    if (!Number.isFinite(age) || age < 0 || age > 24 * 60 * 60 * 1000)
        throw new Error('Preview evidence must be no older than 24 hours');
    return report;
}
export async function preparationPreflight(config, environment, { execute } = {}) {
    if (environment !== 'preview')
        throw new Error('Preparation is restricted to isolated preview');
    const target = validateDeployment(config, environment);
    const inspect = execute || (async (sql) => { const result = wranglerJson(['d1', 'execute', 'DB', '--env', environment, '--remote', '--command', sql.join(';\n')]); return result.map(row => { if (row.success !== true || !Array.isArray(row.results))
        throw new Error('D1 inspection failed'); return row.results; }); });
    const [migrations] = await inspect([contract.queries.migrations]);
    const names = migrations.map(row => row.name).sort();
    if (JSON.stringify(names) !== JSON.stringify(contract.requiredMigrations))
        throw new Error('Apply the exact reviewed migrations before preview preparation');
    return { ready: false, preparationOnly: true, environment, configSha256: await targetFingerprint(config, environment), contractSha256: await fingerprint(contract) };
}
