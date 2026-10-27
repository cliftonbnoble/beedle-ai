// Pure shared policy: used by the candidate Worker and the read-only CLI gate.
// No filesystem, network, credentials, DDL or initialization occurs here.
export function canonical(value) {
    if (Array.isArray(value))
        return '[' + value.map(canonical).join(',') + ']';
    if (value && typeof value === 'object')
        return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
    return JSON.stringify(value);
}
export async function fingerprint(value) {
    const bytes = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(canonical(value))));
    return Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
}
export async function releaseProfileId(vars, contract) {
    const dimensions = Number(vars.AI_EMBEDDING_DIMENSIONS);
    if (!Number.isSafeInteger(dimensions) || dimensions < 1 || dimensions > 4096 || typeof vars.AI_EMBEDDING_MODEL !== 'string' || !vars.AI_EMBEDDING_MODEL || typeof vars.VECTOR_NAMESPACE !== 'string')
        throw new Error('Explicit embedding profile is required');
    // Matches vectorProfile's immutable input order, verified by behavioral tests.
    return fingerprint([vars.VECTOR_NAMESPACE, vars.AI_EMBEDDING_MODEL, dimensions, ...contract.vectorProfile]);
}
export function schemaProblems(contract, objects, columns, migrations) {
    const problems = [];
    if (objects.length > 2000 || columns.length > 5000 || migrations.length > 1000)
        return ['inspection_bound_exceeded'];
    const objectMap = new Map(objects.map(row => [row.name, row.type]));
    for (const row of contract.objects)
        if (objectMap.get(row.name) !== row.type)
            problems.push('object:' + row.name);
    const columnMap = new Map();
    for (const row of columns) {
        if (!columnMap.has(row.tableName))
            columnMap.set(row.tableName, new Set());
        columnMap.get(row.tableName).add(row.columnName);
    }
    for (const [table, names] of Object.entries(contract.columns))
        for (const name of names)
            if (!columnMap.get(table)?.has(name))
                problems.push('column:' + table + '.' + name);
    const applied = new Set(migrations.map(row => row.name));
    for (const name of contract.requiredMigrations)
        if (!applied.has(name))
            problems.push('migration:' + name);
    const versions = migrations.map(row => Number(String(row.name).slice(0, 4)));
    if (!versions.length || versions.some(version => !Number.isSafeInteger(version)) || Math.max(...versions) < contract.minMigration || Math.max(...versions) > contract.maxMigration || [...applied].some(name => !contract.requiredMigrations.includes(name)))
        problems.push('migration_range');
    return problems;
}
export function evaluateReleaseState(contract, state, vars) {
    const problems = [];
    if (!Number.isSafeInteger(state.latestMigration) || Number(state.latestMigration) < contract.minMigration || Number(state.latestMigration) > contract.maxMigration)
        problems.push('migration_range');
    if (!Number.isSafeInteger(state.searchSchemaVersion) || Number(state.searchSchemaVersion) < contract.searchSchemaVersion || !['substrings', 'phrases', 'facets'].every(key => state[key] === 'ready') || state.recoveryValid !== 1)
        problems.push('search_backfill');
    if (state.reviewerPhase !== 'ready' || state.reviewerPolicy !== contract.reviewerPolicyVersion || state.reviewerCatalog !== state.activeCatalog || state.reviewerPending !== 0 || !Number.isSafeInteger(state.sourceCount) || state.sourceCount < 0 || state.sourceCount !== state.reviewerCount)
        problems.push('reviewer_backfill');
    if (state.catalogReady !== 1 || typeof state.activeCatalog !== 'string' || !state.activeCatalog)
        problems.push('reference_catalog');
    if (!Number.isSafeInteger(state.corpusRevision) || state.corpusRevision < 1)
        problems.push('corpus_revision');
    const flags = [vars.VECTOR_LIFECYCLE_READY, vars.VECTOR_FILTERS_READY];
    const semantic = flags.every(flag => flag === 'true'), lexical = flags.every(flag => flag === 'false');
    if (!semantic && !lexical)
        problems.push('vector_configuration');
    if (semantic && (state.vectorPending !== 0 || state.vectorMissing !== 0))
        problems.push('vector_backfill');
    return { ready: problems.length === 0, problems, capability: semantic ? 'semantic' : 'lexical_only', corpusRevision: state.corpusRevision,
        schemaVersion: state.latestMigration, searchSchemaVersion: state.searchSchemaVersion, reviewerPolicy: state.reviewerPolicy };
}
