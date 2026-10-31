import type { Env } from '../lib/types';
import { corpusPartitions, assertCorpusPartitionRevisions } from './corpus-partitions';
import { corpusReviewCacheProofQuery, manifestTextFingerprint, corpusReviewCacheProblems } from '../../scripts/lib/corpus-partition-contract.mjs';
import { REVIEWER_POLICY_VERSION } from './reviewer-queue-index';
import { ReviewerQueueUnavailableError } from '../lib/http';

// Operational read cache, separate from each partition's canonical documents,
// source versions, reviewer rows and migration ledger. Only the owned offline
// build controller populates it; application requests perform no cache writes.
export const corpusReviewCacheSql=[
  `CREATE TABLE beedle_corpus_review_control(singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    epoch INTEGER NOT NULL CHECK(epoch>0),policy_version TEXT NOT NULL,catalog_version TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),rows_sha256 TEXT NOT NULL CHECK(length(rows_sha256)=64),
    phase TEXT NOT NULL CHECK(phase IN ('building','ready')),source_count INTEGER NOT NULL CHECK(source_count>=0))`,
  `CREATE TABLE beedle_corpus_review_rows(id TEXT PRIMARY KEY,source_version INTEGER NOT NULL,policy_version TEXT NOT NULL,
    catalog_version TEXT NOT NULL,created_at TEXT NOT NULL,title TEXT NOT NULL,confidence REAL NOT NULL,
    warning_count INTEGER NOT NULL,unresolved_count INTEGER NOT NULL,critical_count INTEGER NOT NULL,approval_score REAL NOT NULL,
    reviewer_ready INTEGER NOT NULL,risk_rank INTEGER NOT NULL,effort_rank INTEGER NOT NULL,group_key TEXT NOT NULL,
    batch_key TEXT NOT NULL,batch_safe INTEGER NOT NULL,payload_json TEXT NOT NULL CHECK(json_valid(payload_json)))`,
  `CREATE TABLE beedle_corpus_review_groups(group_key TEXT PRIMARY KEY,member_count INTEGER NOT NULL CHECK(member_count>0))`,
  `CREATE INDEX beedle_corpus_review_created ON beedle_corpus_review_rows(created_at,id)`,
  `CREATE INDEX beedle_corpus_review_group ON beedle_corpus_review_rows(group_key,id)`,
  `CREATE INDEX beedle_corpus_review_title ON beedle_corpus_review_rows(title,created_at,id)`
] as const;

export async function inspectCorpusReviewerQueue(env:Env){
  const layout=corpusPartitions(env);if(!layout)return null;
  await assertCorpusPartitionRevisions(env);
  try{
    const state=await (env.CORPUS_COORDINATOR_DB??env.DB).prepare(corpusReviewCacheProofQuery).first<{
        epoch:number;policy_version:string;catalog_version:string;activeCatalog:string;manifest_sha256:string;
        rows_sha256:string;phase:string;source_count:number;indexedCount:number;groupCount:number;missingGroups:number;
      }>();
    const digest=await manifestTextFingerprint(env.CORPUS_PARTITIONS_JSON!);
    if(!state)throw new Error('Missing cache');
    return{...state,sourceCount:state.source_count,pending:0,
      ready:corpusReviewCacheProblems(layout.manifest,digest,state,REVIEWER_POLICY_VERSION).length===0};
  }catch{throw new ReviewerQueueUnavailableError('The restored corpus review cache is unavailable.');}
}
