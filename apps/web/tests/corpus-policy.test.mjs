import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from '../../api/tests/helpers/load-ts.mjs';
const {resolveCorpusPolicy,initialIncludeProvisional}=loadTs('../web/src/lib/corpus-policy.ts');
test('only the explicit provisional deployment enables its read-only scope and explicit user scope remains respected',()=>{
 assert.deepEqual(resolveCorpusPolicy(undefined),{readOnly:false,includeProvisionalByDefault:false});
 assert.deepEqual(resolveCorpusPolicy('unreviewed-provisional-v1'),{readOnly:true,includeProvisionalByDefault:true});
 assert.throws(()=>resolveCorpusPolicy('trusted'),/Unknown/);
 assert.equal(initialIncludeProvisional('trusted_only'),false);assert.equal(initialIncludeProvisional('trusted_plus_provisional'),true);
});
