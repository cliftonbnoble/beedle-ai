// Whole-instance capacity is separate from the 700-call D1 tick meter.
// https://developers.cloudflare.com/workflows/reference/limits/
export function reviewRestorationWorkflowCapacity({maximumSteps,configuredSubrequests,cpuMs}){
  if(!Number.isSafeInteger(maximumSteps)||maximumSteps<1||maximumSteps>1000||
    !Number.isSafeInteger(configuredSubrequests)||configuredSubrequests<1000||configuredSubrequests>10000000||cpuMs!==30000)
    throw Error('Explicit supported Workflow request capacity and unchanged CPU limit required');
  const databaseCallsPerStep=700,storageAndControlCallsPerStep=200,initializationReserve=1000;
  const maximumPlannedRequests=initializationReserve+maximumSteps*(databaseCallsPerStep+storageAndControlCallsPerStep);
  if(maximumPlannedRequests>configuredSubrequests)throw Error('Whole Workflow request budget is smaller than the reviewed bound');
  return {maximumSteps,configuredSubrequests,maximumPlannedRequests,databaseCallsPerStep,storageAndControlCallsPerStep,
    initializationReserve,cpuMs,individualSqlLimitsUnchanged:true};
}
