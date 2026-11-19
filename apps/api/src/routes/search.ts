import { search } from "../services/search";
import { readJson, json, toErrorResponse } from "../lib/http";
import type { Env } from "../lib/types";
import { corpusPartitions } from "../services/corpus-partitions";
import { releaseCollectionCoverage } from "../../scripts/lib/release-collection-coverage.mjs";

export async function handleSearch(request: Request, env: Env): Promise<Response> {
  try {
    const payload = await readJson(request);
    const result = await search(env, payload, {session:true});
    const collectionCoverage = env.CORPUS_COLLECTION_INVENTORY
      ? releaseCollectionCoverage(env.CORPUS_COLLECTION_INVENTORY, corpusPartitions(env)?.manifest, env.APP_ENV)
      : undefined;
    return json({ ...result, collectionCoverage }, { headers: { "cache-control": "private, no-store" } });
  } catch (error) {
    return toErrorResponse(error);
  }
}
