import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { evaluateReleaseState, fingerprint, releaseProfileId, schemaProblems } from './release-contract.mjs';
import { validateDeployment } from './deployment-config.mjs';
import {parseCorpusPartitionManifest,partitionResourceBindings,partitionProofQuery,partitionProofProblems,
  corpusReviewCacheProofQuery,corpusReviewCacheProblems,manifestTextFingerprint,corpusPartitionPolicy} from './corpus-partition-contract.mjs';
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
    const inspect = execute || (async (sql,binding='DB') => {
        const result = wranglerJson(['d1', 'execute', binding, '--env', environment, '--remote', '--command', sql.join(';\n')]);
        if (!Array.isArray(result) || result.length !== sql.length || result.some(row => row.success !== true || !Array.isArray(row.results)))
            throw new Error('D1 inspection failed or returned partial results');
        return result.map(row => row.results);
    });
    const identity={environment,configSha256:await targetFingerprint(config,environment),contractSha256:await fingerprint(contract)};
    if(!target.vars.CORPUS_PARTITIONS_JSON){
      if(target.d1_databases.length!==1)throw Error('Additional corpus bindings require the exact partition manifest');
      return{...identity,...await inspectDatabase(inspect,target.vars)};
    }
    const manifest=parseCorpusPartitionManifest(target.vars.CORPUS_PARTITIONS_JSON);
    if(manifest.documents<1||manifest.publishedDocuments<1)throw Error('An empty or entirely held corpus cannot authorize a normal restored release');
    const bindings=partitionResourceBindings(target,manifest);
    if(target.vars.VECTOR_LIFECYCLE_READY!=='false'||target.vars.VECTOR_FILTERS_READY!=='false'||target.ai||target.vectorize?.length||target.queues?.consumers?.length||target.triggers?.crons?.length)
      throw Error('Restored corpus requires read-only lexical configuration without provider/queue/cron writers');
    const reports=[];
    for(const [index,binding] of bindings.entries()){
      const read=queries=>inspect(queries,binding.binding);
      const report=await inspectDatabase(read,target.vars);
      const [proofs]=await read([partitionProofQuery]);
      const problems=partitionProofProblems(manifest,index,proofs?.length===1?proofs[0]:null);
      if(report.corpusRevision!==manifest.partitions[index].revision)problems.push('partition:'+index+':revision');
      if(problems.length)throw Error('Restored corpus release is held: '+problems.join(', '));
      reports.push(report);
    }
    const [cache]=await inspect([corpusReviewCacheProofQuery],'DB');
    const problems=corpusReviewCacheProblems(manifest,await manifestTextFingerprint(target.vars.CORPUS_PARTITIONS_JSON),cache?.length===1?cache[0]:null,contract.reviewerPolicyVersion);
    if(problems.length)throw Error('Restored corpus release is held: '+problems.join(', '));
    return{...identity,...reports[0],partitionContract:corpusPartitionPolicy.id,partitionCount:32,corpusReadOnly:true,
      documents:manifest.documents,publishedDocuments:manifest.publishedDocuments,restoreOwnerId:manifest.restoreOwnerId,restorePlanSha256:manifest.restorePlanSha256};
}
export async function validatePreviewEvidence(config, revision, report, { now = Date.now() } = {}) {
    validateDeployment(config, 'preview');
    const partitioned=Boolean(config.env.production.vars.CORPUS_PARTITIONS_JSON);
    if(partitioned&&(!config.env.preview.vars.CORPUS_PARTITIONS_JSON||report?.partitionContract!==corpusPartitionPolicy.id||report?.partitionCount!==32||report?.corpusReadOnly!==true||report?.publicationPolicy!=='unreviewed-provisional-v1'))
      throw Error('A matching protected provisional partitioned-preview proof is required');
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
