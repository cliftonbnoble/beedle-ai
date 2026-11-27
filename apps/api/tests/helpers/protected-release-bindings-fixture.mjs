export function protectedBindingsFixture(template) {
    const bindings = { version: 1 };
    for (const [slot, environment] of ['preview', 'production'].entries()) {
        const target = template.env[environment];
        const manifests = {
            sourceManifestSha256: String(slot + 1).repeat(64),
            restorePlanSha256: String(slot + 3).repeat(64),
        };
        bindings[environment] = {
            databases: target.d1_databases.map(({ binding }, index) => ({
                binding,
                database_name: `fixture-${environment}-${index}`,
                database_id: `00000000-0000-4000-8000-${String((slot + 1) * 100 + index).padStart(12, '0')}`,
            })),
            sourceBucket: `fixture-${environment}-sources`,
            partitionManifest: JSON.stringify({
                version: 1, partitionCount: 32, readOnly: true, ...manifests,
                catalogVersion: 'fixture', catalogSha256: 'a'.repeat(64),
                restoreOwnerId: `00000000-0000-4000-8000-${String(slot + 1).padStart(12, '0')}`,
                documents: 32, publishedDocuments: 32,
                partitions: target.d1_databases.map(({ binding }, index) => ({
                    index, binding, documents: 1, publishedDocuments: 1, revision: 1,
                })),
            }),
            collectionInventory: JSON.stringify({ inventoryDocuments: 64, ...manifests }),
        };
    }
    return bindings;
}
