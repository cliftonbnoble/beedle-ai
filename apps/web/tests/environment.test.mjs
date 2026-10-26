import test from 'node:test';
import assert from 'node:assert/strict';
import { loadTs } from '../../api/tests/helpers/load-ts.mjs';
const { resolveApiBase }=loadTs('../web/src/lib/deployment.ts');
const prod='https://beedle-api.clifton23.workers.dev';
test('build environments must be explicit and preview cannot fall back to production',()=>{
  for(const input of [{},{environment:'preview'},{environment:'production'},{environment:'preview',apiUrl:prod},{environment:'local',apiUrl:prod}]) assert.throws(()=>resolveApiBase(input));
  assert.equal(resolveApiBase({environment:'local'}),'http://127.0.0.1:8787');
  assert.equal(resolveApiBase({environment:'preview',apiUrl:'https://preview-api.example.com/'}),'https://preview-api.example.com');
  assert.equal(resolveApiBase({environment:'production',apiUrl:prod}),prod);
});
test('unsafe URLs and mismatched Pages deployment branches fail the build',()=>{
  for(const apiUrl of ['http://preview.example.com','https://user:secret@preview.example.com','https://preview.example.com/path','https://preview.example.com?x=1','https://missing.invalid'])assert.throws(()=>resolveApiBase({environment:'preview',apiUrl}));
  const input={environment:'production',apiUrl:prod,onPages:'1',pagesBranch:'feature',productionBranch:'main'};
  assert.throws(()=>resolveApiBase(input),/branch/);
  assert.throws(()=>resolveApiBase({...input,pagesBranch:'main',productionBranch:undefined}),/branch/);
  assert.equal(resolveApiBase({...input,pagesBranch:'main'}),prod);
});

test('preview cannot address production through a trailing-dot hostname',()=>{
  assert.throws(()=>resolveApiBase({environment:'preview',apiUrl:'https://beedle-api.clifton23.workers.dev.'}));
});
