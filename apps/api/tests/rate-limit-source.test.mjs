import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
test('route declarations require explicit cost classes and all environments define limiter bindings',async()=>{
  const root=process.cwd();const [worker,raw,env]=await Promise.all([fs.readFile(path.resolve(root,'src/index.ts'),'utf8'),fs.readFile(path.resolve(root,'wrangler.json'),'utf8'),fs.readFile(path.resolve(root,'src/worker-configuration.d.ts'),'utf8')]);
  const config=JSON.parse(raw);
  for(const binding of ['SEARCH_RATE_LIMIT','INGEST_RATE_LIMIT','LLM_RATE_LIMIT','ADMIN_WRITE_RATE_LIMIT','REVIEW_READ_RATE_LIMIT','EXPORT_RATE_LIMIT']){
    assert.match(env,new RegExp(`${binding}: RateLimit`));
    for(const selected of [config,...Object.values(config.env)])assert.ok(selected.ratelimits.some(row=>row.name===binding));
  }
  assert.match(worker,/router\[method\]\(path,async/);
  assert.doesNotMatch(worker,/router\.(?:get|post)\(/);
  assert.match(worker,/const limited = await enforceCostControls\(request, env, cost\)/);
  assert.match(worker,/control\.limiter\.limit\(\{ key: rateLimitKey/);
});
