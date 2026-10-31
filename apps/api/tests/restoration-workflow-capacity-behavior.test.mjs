import test from 'node:test';
import assert from 'node:assert/strict';
import {reviewRestorationWorkflowCapacity as review,reviewRestorationControlVariables as variables} from '../scripts/lib/restoration-workflow-capacity.mjs';
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
test('an oversized combined plan is rejected while individually bounded plans are accepted',()=>{
  assert.throws(()=>variables({OPERATION_JSON:'x'.repeat(6713)}),/UTF-8 byte limit/);
  assert.equal(variables({OPERATION_JSON:'x'.repeat(350),TARGET0_PLAN_JSON:'x'.repeat(3305),TARGET2_PLAN_JSON:'x'.repeat(3276)}).variables,3);
});
test('control-variable limits count UTF-8 bytes and refuse unserialized objects',()=>{
  assert.throws(()=>variables({OPERATION_JSON:'é'.repeat(2501)}),/UTF-8 byte limit/);
  assert.throws(()=>variables({OPERATION_JSON:{version:2}}),/String control/);
});
