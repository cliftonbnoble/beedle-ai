import test from 'node:test';
import assert from 'node:assert/strict';
import {reviewRestorationWorkflowCapacity as review} from '../scripts/lib/restoration-workflow-capacity.mjs';
test('a full source Workflow cannot inherit the default 10,000-request budget',()=>{
  assert.throws(()=>review({maximumSteps:416,configuredSubrequests:10000,cpuMs:30000}),/Whole Workflow request budget/);
  assert.equal(review({maximumSteps:416,configuredSubrequests:1000000,cpuMs:30000}).individualSqlLimitsUnchanged,true);
});
test('D1 calls alone cannot hide the storage and proof requests in the same instance',()=>{
  assert(1000+12*700<10000);
  assert.throws(()=>review({maximumSteps:12,configuredSubrequests:10000,cpuMs:30000}),/Whole Workflow request budget/);
});
test('a bounded two-target disposition fits without increasing the default request limit',()=>{
  const result=review({maximumSteps:2,configuredSubrequests:10000,cpuMs:30000});
  assert.equal(result.maximumPlannedRequests,2800);assert.equal(result.cpuMs,30000);
});
test('unknown limits, unbounded executions and changed CPU limits are rejected',()=>{
  for(const patch of [{configuredSubrequests:undefined},{configuredSubrequests:10000001},{maximumSteps:0},{maximumSteps:1001},{cpuMs:300000}])
    assert.throws(()=>review({maximumSteps:2,configuredSubrequests:10000,cpuMs:30000,...patch}),/Explicit supported/);
});
