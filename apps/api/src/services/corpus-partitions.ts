import type { Env } from "../lib/types";
import { RequestConflictError, SearchIndexUnavailableError } from "../lib/http";
import {parseCorpusPartitionManifest,corpusPartitionBinding as bindingName,type CorpusPartitionManifest} from '../../scripts/lib/corpus-partition-contract.mjs';
export type {CorpusPartitionManifest};
export type CorpusPartition = CorpusPartitionManifest["partitions"][number] & { database: Env["DB"] };
type PartitionEnvironment = Pick<Env, "DB"> & {
  CORPUS_PARTITIONS_JSON?: string;
  CORPUS_COORDINATOR_DB?: Env["DB"];
  CORPUS_LOCAL_PARTITION?: number;
};

export function corpusPartitionBinding(index: number) {
  if (!Number.isInteger(index) || index < 0 || index >= 32) throw new Error("Invalid corpus partition index");
  return bindingName(index);
}

// This is a deployment declaration. The release controller independently verifies
// physical binding IDs, source ownership and complete per-database proof.
export function corpusPartitions(env: PartitionEnvironment): {manifest: CorpusPartitionManifest; partitions: CorpusPartition[]} | null {
  if (env.CORPUS_PARTITIONS_JSON === undefined || env.CORPUS_PARTITIONS_JSON === "") {
    if(env.CORPUS_COORDINATOR_DB||Array.from({length:31},(_,index)=>bindingName(index+1)).some(name=>Reflect.get(env,name)))
      throw new SearchIndexUnavailableError('Restored corpus partition manifest is missing.');
    return null;
  }
  try {
    const manifest = parseCorpusPartitionManifest(env.CORPUS_PARTITIONS_JSON);
    const seen = new Set<Env["DB"]>();
    const partitions = manifest.partitions.map((part, index) => {
      if (part.index !== index || part.binding !== corpusPartitionBinding(index) || part.publishedDocuments > part.documents) throw new Error("Invalid partition mapping");
      const database = (index === 0 ? env.CORPUS_COORDINATOR_DB ?? env.DB : Reflect.get(env, part.binding)) as Env["DB"] | undefined;
      if (!database || typeof database.prepare !== "function" || typeof database.batch !== "function" || seen.has(database)) throw new Error("Missing or aliased partition binding");
      seen.add(database);
      return { ...part, database };
    });
    if (partitions.reduce((n, part) => n + part.documents, 0) !== manifest.documents ||
        partitions.reduce((n, part) => n + part.publishedDocuments, 0) !== manifest.publishedDocuments) throw new Error("Incomplete corpus counts");
    return { manifest, partitions };
  } catch {
    throw new SearchIndexUnavailableError("Restored corpus partition configuration is unavailable.");
  }
}

export async function corpusPartitionIndex(documentId: string): Promise<number> {
  if (!documentId || new TextEncoder().encode(documentId).length > 512) throw new Error("Invalid document identity");
  const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(documentId));
  return new DataView(bytes).getUint32(0, false) % 32;
}

export async function documentPartitionEnvironment(env: Env, documentId: string): Promise<Env> {
  const layout = corpusPartitions(env);
  if (!layout) return env;
  const index = await corpusPartitionIndex(documentId);
  return { ...env, DB: layout.partitions[index]!.database,
    CORPUS_COORDINATOR_DB: layout.partitions[0]!.database, CORPUS_LOCAL_PARTITION: index };
}

// The first restoration release is immutable/read-only. Every partition epoch
// must match its sealed release manifest; a changed partition fails closed even
// when the coordinator's own epoch has not changed.
export async function assertCorpusPartitionRevisions(env: PartitionEnvironment): Promise<number | null> {
  const layout = corpusPartitions(env);
  if (!layout) return null;
  const revisions: number[] = [];
  for (let at = 0; at < layout.partitions.length; at += 4) {
    const values = await Promise.all(layout.partitions.slice(at, at + 4).map(async part => {
      let revision: number | null;
      try { revision = await part.database.prepare("SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1").first<number>("revision"); }
      catch { throw new SearchIndexUnavailableError("Restored corpus revision is unavailable."); }
      if (!Number.isSafeInteger(revision) || revision !== part.revision) throw new RequestConflictError("The restored corpus changed. Reload after the release state is reconciled.");
      return revision!;
    }));
    revisions.push(...values);
  }
  return revisions[0]!;
}
