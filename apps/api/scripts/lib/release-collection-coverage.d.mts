import type { CorpusPartitionManifest } from './corpus-partition-contract.mjs';

export function releaseCollectionCoverage(
  text: string | undefined,
  manifest: CorpusPartitionManifest | undefined,
  environment: string,
): { environment: 'preview' | 'production'; searchableDocuments: number; inventoryDocuments: number } | undefined;
