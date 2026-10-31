import contract from '../../release/contract.json';
import { evaluateReleaseState, fingerprint, releaseProfileId, schemaProblems } from '../../scripts/lib/release-contract.mjs';
import type { Env } from '../lib/types';
import {corpusPartitions,assertCorpusPartitionRevisions} from './corpus-partitions';
import {inspectCorpusReviewerQueue} from './corpus-review-cache';
import {partitionProofQuery,partitionProofProblems,corpusPartitionPolicy} from '../../scripts/lib/corpus-partition-contract.mjs';
/** Administrative probe only. It never creates schema, initializes a catalog or advances jobs. */
export async function inspectReleaseReadiness(env: Env) {
    try{
        const layout=corpusPartitions(env);
        if(!layout)return inspectSingleReleaseReadiness(env);
        await assertCorpusPartitionRevisions(env);
        const reports=[];
        const problems:string[]=[];
        if(layout.manifest.documents<1||layout.manifest.publishedDocuments<1)problems.push('empty_restored_corpus');
        if(env.VECTOR_LIFECYCLE_READY!=='false'||env.VECTOR_FILTERS_READY!=='false'||env.AI||env.VECTOR_INDEX)problems.push('partition_lexical_configuration');
        for(let at=0;at<layout.partitions.length;at+=4){
            const batch=await Promise.all(layout.partitions.slice(at,at+4).map(async part=>{
                const local={...env,DB:part.database,CORPUS_PARTITIONS_JSON:undefined,CORPUS_COORDINATOR_DB:undefined};
                const result=await inspectSingleReleaseReadiness(local);
                const proof=await part.database.prepare(partitionProofQuery).first<Record<string,unknown>>();
                return{part,result,proof};
            }));
            for(const {part,result,proof} of batch){
                if(!result.ready)problems.push('partition:'+part.index+':release_readiness');
                if(!('corpusRevision' in result)||result.corpusRevision!==part.revision)problems.push('partition:'+part.index+':revision');
                problems.push(...partitionProofProblems(layout.manifest,part.index,proof));reports.push(result);
            }
        }
        const reviewer=await inspectCorpusReviewerQueue(env);if(!reviewer?.ready)problems.push('corpus_review_cache');
        await assertCorpusPartitionRevisions(env);
        return{...reports[0]!,ready:problems.length===0,problems,partitionContract:corpusPartitionPolicy.id,
            partitionCount:32,documents:layout.manifest.documents,publishedDocuments:layout.manifest.publishedDocuments,
            corpusReadOnly:true,restoreOwnerId:layout.manifest.restoreOwnerId,restorePlanSha256:layout.manifest.restorePlanSha256};
    }catch{
        return{ready:false,problems:['partition_inspection_unavailable'],environment:env.APP_ENV,codeRevision:env.RELEASE_REVISION||null};
    }
}
async function inspectSingleReleaseReadiness(env: Env) {
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
