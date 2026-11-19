import assert from 'node:assert/strict';
import test from 'node:test';
import {loadTs} from '../../api/tests/helpers/load-ts.mjs';

const {collectionCoverageText}=loadTs('../web/src/lib/collection-coverage.ts');
test('release copy distinguishes searchable documents from the inventory and pluralizes the remainder',()=>{
  const text=(searchableDocuments,inventoryDocuments)=>collectionCoverageText({environment:'production',searchableDocuments,inventoryDocuments});
  assert.equal(text(1000,1020),'Search covers 1,000 documents. 20 are outside this release.');
  assert.equal(text(1,2),'Search covers 1 document. 1 is outside this release.');
  assert.equal(text(55,55),'Search covers the full collection of 55 documents.');
});
test('preview copy describes its own subset',()=>{
  assert.equal(collectionCoverageText({environment:'preview',searchableDocuments:5,inventoryDocuments:1020}),
    'This preview contains 5 searchable documents.');
});
