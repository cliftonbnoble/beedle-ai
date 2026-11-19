// The aggregate inventory belongs to one sealed restoration plan and its source manifest.
export function releaseCollectionCoverage(text, manifest, environment) {
  if (typeof text !== 'string' || text.length > 512 || !manifest ||
      (environment !== 'production' && environment !== 'preview')) return undefined;
  let inventory;
  try { inventory = JSON.parse(text); } catch { return undefined; }
  if (!inventory || typeof inventory !== 'object' || Object.keys(inventory).length !== 3 ||
      !/^[a-f0-9]{64}$/.test(inventory.sourceManifestSha256) ||
      !/^[a-f0-9]{64}$/.test(inventory.restorePlanSha256) ||
      inventory.sourceManifestSha256 !== manifest.sourceManifestSha256 ||
      inventory.restorePlanSha256 !== manifest.restorePlanSha256 ||
      !Number.isSafeInteger(inventory.inventoryDocuments) || inventory.inventoryDocuments < 1 ||
      !Number.isSafeInteger(manifest.documents) || manifest.documents > inventory.inventoryDocuments ||
      !Number.isSafeInteger(manifest.publishedDocuments) || manifest.publishedDocuments < 0 ||
      manifest.publishedDocuments > manifest.documents) return undefined;
  return {environment, searchableDocuments: manifest.publishedDocuments, inventoryDocuments: inventory.inventoryDocuments};
}
