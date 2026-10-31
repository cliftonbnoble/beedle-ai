import { readFileSync, statSync, writeFileSync, existsSync } from 'node:fs';
import { spawnSync, execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { validateDeployment } from './lib/deployment-config.mjs';
import { preflight, preparationPreflight, validatePreviewEvidence } from './lib/release-preflight.mjs';
import { semanticSmoke, readSmokePlan } from './lib/release-smoke.mjs';
export { validateDeployment } from './lib/deployment-config.mjs';
export async function executeRelease({ config, environment, revision, plan, previewEvidence, prepare = false }, { inspect = preflight, prepareInspect = preparationPreflight, runDeployment, smoke = semanticSmoke } = {}) {
    const target = validateDeployment(config, environment);
    if (!/^[a-f0-9]{40}$/.test(revision || ''))
        throw new Error('A full committed code revision is required');
    if (typeof runDeployment !== 'function')
        throw new Error('Explicit deployment executor is required');
    if (prepare) {
        if (environment !== "preview")
            throw new Error("Preparation is restricted to isolated preview");
        const report = await prepareInspect(config, environment);
        await runDeployment({ environment, revision, configSha256: report.configSha256, maintenanceOnly: true });
        return { version: 1, ...report, passed: false, codeRevision: revision, completedAt: new Date().toISOString() };
    }
    plan = readSmokePlan(plan);
    if (environment === 'production')
        await validatePreviewEvidence(config, revision, previewEvidence);
    const before = await inspect(config, environment);
    if (!before.ready)
        throw new Error('Release prerequisites are incomplete');
    if (before.capability !== plan.capability)
        throw new Error('Release plan capability does not match the database preflight');
    if(Boolean(before.partitionContract)!==(plan.version===2)||
      (plan.version===2&&(before.partitionContract!=='restored-corpus-32-v1'||before.corpusReadOnly!==true)))
        throw new Error('The release plan must explicitly match the restored provisional corpus policy');
    await runDeployment({ environment, revision, configSha256: before.configSha256, maintenanceOnly: false });
    try {
        return await smoke({ apiBase: target.vars.SOURCE_PROXY_BASE_URL, environment, revision, configSha256: before.configSha256, contractSha256: before.contractSha256, plan });
    }
    catch (error) {
        throw new Error("Candidate was deployed but semantic smoke failed; hold rollout and use the reviewed rollback procedure. " + error.message);
    }
}
function readPrivateJson(file, max = 256 * 1024) { if (!file)
    throw new Error('An explicit release input file is required'); const stat = statSync(file); if (!stat.isFile() || stat.size > max)
    throw new Error('Release input exceeds its bound'); return JSON.parse(readFileSync(file, 'utf8')); }
function runWrangler(args) { const result = spawnSync('pnpm', ['exec', 'wrangler', ...args], { stdio: 'inherit', timeout: 180000 }); if (result.error || result.status !== 0)
    throw new Error('Wrangler command failed'); }
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    let receiptOutput;
    try {
        const environment = process.argv[2], flags = process.argv.slice(3);
        if (flags.some(flag => flag !== '--dry-run'))
            throw new Error('Only --dry-run is accepted after the environment');
        const config = JSON.parse(readFileSync(new URL('../wrangler.json', import.meta.url), 'utf8'));
        validateDeployment(config, environment);
        if (flags.includes('--dry-run'))
            runWrangler(['deploy', '--env', environment, '--dry-run']);
        else {
            const revision = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
            if (execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }).trim())
                throw new Error('Real releases require a clean committed checkout');
            const output = process.env.BEEDLE_RELEASE_REPORT;
            if (!output || existsSync(output))
                throw new Error('BEEDLE_RELEASE_REPORT must select a new output file');
            const prepare = process.env.BEEDLE_PREPARE_PREVIEW === '1';
            const plan=prepare?undefined:readSmokePlan(readPrivateJson(process.env.BEEDLE_RELEASE_SMOKE_PLAN));
            const previewEvidence=environment==='production'?readPrivateJson(process.env.BEEDLE_PREVIEW_EVIDENCE):undefined;
            writeFileSync(output,JSON.stringify({version:1,passed:false,phase:'preflight',environment,codeRevision:revision})+'\n',{flag:'wx',mode:0o600});
            receiptOutput=output;
            const report = await executeRelease({ config, environment, revision, prepare, plan, previewEvidence }, { runDeployment: async (target) => runWrangler(['deploy', '--env', environment, '--var', 'RELEASE_REVISION:' + revision, '--var', 'RELEASE_CONFIG_SHA256:' + target.configSha256, '--var', 'RELEASE_MAINTENANCE_ONLY:' + String(target.maintenanceOnly), '--tag', revision]) });
            writeFileSync(output, JSON.stringify(report, null, 2) + '\n', { mode: 0o600 });
            console.log(JSON.stringify({ passed: report.passed, preparationOnly: report.preparationOnly || false }));
        }
    }
    catch (error) {
        if(receiptOutput){try{writeFileSync(receiptOutput,JSON.stringify({version:1,passed:false,phase:'failed',completedAt:new Date().toISOString()})+'\n',{mode:0o600});}catch{/* Preserve original failure; incomplete evidence cannot authorize release. */}}
        console.error(error.name==='ZodError'?'Invalid private release plan; review its DTO fields.':error.message);
        process.exitCode = 1;
    }
}
