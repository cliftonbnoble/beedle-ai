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

// Leave headroom below the provider's 5 KiB per-variable text limit.
export function reviewRestorationControlVariables(variables){
  if(!variables||typeof variables!=='object'||Array.isArray(variables)||Object.keys(variables).length>32)
    throw Error('Explicit bounded text control variables required');
  const bytes={};
  for(const[name,value]of Object.entries(variables)){
    if(!/^[A-Z][A-Z0-9_]{0,63}$/.test(name)||typeof value!=='string')throw Error('String control variable required');
    bytes[name]=new TextEncoder().encode(value).length;
    if(bytes[name]>5000)throw Error('Control variable exceeds reviewed UTF-8 byte limit');
  }
  return {variables:Object.keys(bytes).length,bytes,maximumVariableBytes:5000};
}
