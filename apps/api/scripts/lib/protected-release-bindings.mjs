import { parseCorpusPartitionManifest, partitionResourceBindings } from './corpus-partition-contract.mjs';
import { validateDeployment } from './deployment-config.mjs';
import { releaseCollectionCoverage } from './release-collection-coverage.mjs';

const environments = ['preview', 'production'];
const keys = (value, expected) => value && typeof value === 'object' && !Array.isArray(value) &&
    Object.keys(value).sort().join(',') === [...expected].sort().join(',');
const databaseName = /^[a-z0-9][a-z0-9_-]{0,62}$/;
const bucketName = /^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/;

export function bindProtectedReleaseTargets(template, bindings) {
    if (!keys(bindings, ['version', ...environments]) || bindings.version !== 1)
        throw new Error('Expected the protected release bindings for both environments');
    const config = structuredClone(template);
    for (const environment of environments) {
        const target = config.env[environment], input = bindings[environment];
        if (target.vars.DEPLOYMENT_READY !== 'false' ||
            target.d1_databases.length !== 32 || target.r2_buckets.length !== 1 ||
            target.r2_buckets[0].binding !== 'SOURCE_BUCKET' ||
            !keys(input, ['databases', 'sourceBucket', 'partitionManifest', 'collectionInventory']) ||
            !Array.isArray(input.databases) || input.databases.length !== 32 ||
            typeof input.sourceBucket !== 'string' || !bucketName.test(input.sourceBucket) ||
            input.sourceBucket.endsWith('-unconfigured'))
            throw new Error('Protected release bindings do not match the phased template');

        const manifest = parseCorpusPartitionManifest(input.partitionManifest);
        if (manifest.documents < 1 || manifest.publishedDocuments !== manifest.documents ||
            !releaseCollectionCoverage(input.collectionInventory, manifest, environment))
            throw new Error('Protected release collection does not match the partition manifest');
        target.d1_databases = target.d1_databases.map((row, index) => {
            const database = input.databases[index];
            if (!keys(database, ['binding', 'database_name', 'database_id']) ||
                database.binding !== row.binding || row.binding !== manifest.partitions[index].binding ||
                typeof database.database_name !== 'string' || !databaseName.test(database.database_name) ||
                database.database_name.includes('-unconfigured-') || typeof database.database_id !== 'string' ||
                database.database_id.startsWith('00000000-0000-0000-0000-'))
                throw new Error('Protected database binding does not match the declared partition');
            return { ...row, ...database };
        });
        target.r2_buckets[0].bucket_name = input.sourceBucket;
        target.vars.CORPUS_PARTITIONS_JSON = input.partitionManifest;
        target.vars.CORPUS_COLLECTION_INVENTORY = input.collectionInventory;
        target.vars.DEPLOYMENT_READY = 'true';
        partitionResourceBindings(target, manifest);
    }
    for (const environment of environments)
        validateDeployment(config, environment);
    return config;
}
