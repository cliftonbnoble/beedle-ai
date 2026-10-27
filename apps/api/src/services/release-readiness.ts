import contract from '../../release/contract.json';
import { evaluateReleaseState, fingerprint, releaseProfileId, schemaProblems } from '../../scripts/lib/release-contract.mjs';
import type { Env } from '../lib/types';
/** Administrative probe only. It never creates schema, initializes a catalog or advances jobs. */
export async function inspectReleaseReadiness(env: Env) {
    const identity = { contract: contract.id, contractSha256: await fingerprint(contract), environment: env.APP_ENV,
        codeRevision: env.RELEASE_REVISION || null, configSha256: env.RELEASE_CONFIG_SHA256 || null, maintenanceOnly: env.RELEASE_MAINTENANCE_ONLY === "true" };
    try {
        const [objects, columns, migrations] = await env.DB.batch([
            env.DB.prepare(contract.queries.objects), env.DB.prepare(contract.queries.columns), env.DB.prepare(contract.queries.migrations)
        ]);
        const problems = schemaProblems(contract, objects!.results as {
            name: string;
            type: string;
        }[], columns!.results as {
            tableName: string;
            columnName: string;
        }[], migrations!.results as {
            name: string;
        }[]);
        if (problems.length)
            return { ...identity, ready: false, problems: problems.slice(0, 30), problemCount: problems.length };
        const profileId = await releaseProfileId({ ...env }, contract);
        const state = await env.DB.prepare(contract.queries.state).bind(profileId).first<Record<string, unknown>>();
        if (!state)
            return { ...identity, ready: false, problems: ['readiness_state_missing'] };
        const result = evaluateReleaseState(contract, state, { ...env });
        if (identity.maintenanceOnly) {
            result.ready = false;
            result.problems.push('preview_preparation');
        }
        return { ...identity, ...result };
    }
    catch {
        // Missing ledgers, partial migrations and transient binding failures all fail
        // closed. Raw D1/provider errors can contain operational data and stay private.
        return { ...identity, ready: false, problems: ['inspection_unavailable'] };
    }
}
