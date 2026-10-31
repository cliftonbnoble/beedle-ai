import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { loadTs } from './helpers/load-ts.mjs';
const {corpusPartitionBinding,corpusPartitions,corpusPartitionIndex,documentPartitionEnvironment,assertCorpusPartitionRevisions}=loadTs('src/services/corpus-partitions.ts');

function setup() {
  const revisions=Array.from({length:32},(_,i)=>100+i);
  const databases=revisions.map((_,index)=>({prepare(sql){assert.equal(sql,'SELECT revision FROM vector_lifecycle_epoch WHERE singleton=1');return{async first(column){assert.equal(column,'revision');return revisions[index];}};},async batch(){throw Error('No writes expected');}}));
  const manifest={version:1,partitionCount:32,readOnly:true,restoreOwnerId:'11111111-1111-4111-8111-111111111111',restorePlanSha256:'c'.repeat(64),sourceManifestSha256:'a'.repeat(64),catalogVersion:'legacy',catalogSha256:'b'.repeat(64),documents:64,publishedDocuments:32,partitions:databases.map((_,index)=>({index,binding:corpusPartitionBinding(index),documents:2,publishedDocuments:1,revision:revisions[index]}))};
  const env={DB:databases[0],CORPUS_PARTITIONS_JSON:JSON.stringify(manifest)};
  for(let i=1;i<32;i++)env[corpusPartitionBinding(i)]=databases[i];
  return{env,manifest,databases,revisions};
}

test('document partitioning matches the independently rehearsed SHA256 rule without losing identities',async()=>{
  const seen=new Set();
  for(let i=0;i<1000;i++){const id='synthetic-document-'+i;const expected=parseInt(createHash('sha256').update(id).digest('hex').slice(0,8),16)%32;assert.equal(await corpusPartitionIndex(id),expected);seen.add(expected);}
  assert.equal(seen.size,32);await assert.rejects(corpusPartitionIndex(''));await assert.rejects(corpusPartitionIndex('x'.repeat(513)));
});

test('unconfigured single-database operation preserves the existing environment and does not read partitions',async()=>{
  const env={DB:{prepare(){throw Error('Unexpected SQL');}}};assert.equal(corpusPartitions(env),null);assert.equal(await documentPartitionEnvironment(env,'synthetic'),env);assert.equal(await assertCorpusPartitionRevisions(env),null);
});

test('routing preserves coordinator identity and does not mutate the original request environment',async()=>{
  const {env,databases}=setup();const id='synthetic-route';const index=await corpusPartitionIndex(id);const routed=await documentPartitionEnvironment(env,id);
  assert.equal(routed.DB,databases[index]);assert.equal(routed.CORPUS_COORDINATOR_DB,databases[0]);assert.equal(env.DB,databases[0]);assert.equal(env.CORPUS_LOCAL_PARTITION,undefined);assert.equal(corpusPartitions(routed).partitions[0].database,databases[0]);
});

test('malformed, missing, aliased, reordered or count-inconsistent layouts fail closed',()=>{
  for(const change of [x=>delete x.env.CORPUS_DB_17,x=>x.env.CORPUS_DB_17=x.databases[0],x=>x.manifest.partitions.reverse(),x=>x.manifest.documents++,x=>x.manifest.readOnly=false,x=>x.manifest.partitions[2].publishedDocuments=3,x=>x.manifest.unrecognized=true]){
    const x=setup();change(x);x.env.CORPUS_PARTITIONS_JSON=JSON.stringify(x.manifest);assert.throws(()=>corpusPartitions(x.env),/partition configuration/);
  }
  const {env}=setup();env.CORPUS_PARTITIONS_JSON='{';assert.throws(()=>corpusPartitions(env),/partition configuration/);
  delete env.CORPUS_PARTITIONS_JSON;assert.throws(()=>corpusPartitions(env),/manifest is missing/);
});

test('every partition epoch is checked, including changes invisible to the coordinator epoch',async()=>{
  const {env,revisions}=setup();assert.equal(await assertCorpusPartitionRevisions(env),100);revisions[17]++;await assert.rejects(assertCorpusPartitionRevisions(env),/corpus changed/);revisions[17]--;revisions[31]=null;await assert.rejects(assertCorpusPartitionRevisions(env),/corpus changed/);
});
