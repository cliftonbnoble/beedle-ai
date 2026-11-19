import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {releaseCollectionCoverage} from '../scripts/lib/release-collection-coverage.mjs';
import {preflight} from '../scripts/lib/release-preflight.mjs';
import {createPartitionFixture} from './helpers/corpus-partitions.mjs';
import {loadTs} from './helpers/load-ts.mjs';

const {handleSearch}=loadTs('src/routes/search.ts');
const {handleDashboardSummary}=loadTs('src/routes/admin-dashboard.ts');
const {dashboardSummarySchema,searchResponseSchema}=loadTs('../../packages/shared/src/index.ts');
const manifest={documents:32,publishedDocuments:28,sourceManifestSha256:'a'.repeat(64),restorePlanSha256:'c'.repeat(64)};
const inventory={inventoryDocuments:70,sourceManifestSha256:manifest.sourceManifestSha256,restorePlanSha256:manifest.restorePlanSha256};

test('coverage uses the published count and belongs to one source manifest and one restore plan',()=>{
  assert.deepEqual(releaseCollectionCoverage(JSON.stringify(inventory),manifest,'production'),
    {environment:'production',searchableDocuments:28,inventoryDocuments:70});
  for(const override of [{inventoryDocuments:0},{inventoryDocuments:30},{inventoryDocuments:'70'},
    {sourceManifestSha256:'b'.repeat(64)},{restorePlanSha256:'d'.repeat(64)}]) {
    assert.equal(releaseCollectionCoverage(JSON.stringify({...inventory,...override}),manifest,'production'),undefined);
  }
  for(const optional of [undefined,'', '{', 'null']) {
    assert.equal(releaseCollectionCoverage(optional,manifest,'preview'),undefined);
  }
});
test('protected preflight refuses an inventory with no matching partition release before hosted inspection',async()=>{
  const config=JSON.parse(readFileSync(new URL('../wrangler.json',import.meta.url),'utf8'));
  config.env.preview.vars.CORPUS_COLLECTION_INVENTORY=JSON.stringify(inventory);
  let ran=false;
  await assert.rejects(preflight(config,'preview',{execute(){ran=true;throw Error('unexpected inspection');}}),/Collection inventory/);
  assert.equal(ran,false);
});
test('dashboard and zero-hit filtered search return the same independently counted collection coverage',async()=>{
  const f=await createPartitionFixture();
  try {
    const env={...f.env,APP_ENV:'preview',CORPUS_COLLECTION_INVENTORY:JSON.stringify(inventory)};
    const expected={environment:'preview',searchableDocuments:32,inventoryDocuments:70};
    const dashboard=await handleDashboardSummary(new Request('https://test.invalid/admin/dashboard'),env);
    assert.equal(dashboard.status,200);
    const summary=dashboardSummarySchema.parse(await dashboard.json());
    assert.equal(summary.searchableDecisionCount,32);
    assert.deepEqual(summary.collectionCoverage,expected);
    const request=new Request('https://test.invalid/search',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({
      query:'possession',corpusMode:'trusted_only',limit:5
    })});
    const result=await handleSearch(request,env);
    assert.equal(result.status,200);
    const page=searchResponseSchema.parse(await result.json());
    assert.equal(page.results.length,0);
    assert.deepEqual(page.collectionCoverage,expected);
  } finally {f.close();}
});
