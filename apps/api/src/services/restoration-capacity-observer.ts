export type RestorationCapacityPhase='bootstrap'|'source'|'staging'|'retained'|'failed'|'reprocess'|'restore';
const phases=new Set<RestorationCapacityPhase>(['bootstrap','source','staging','retained','failed','reprocess','restore']);
export class RestorationCapacityError extends Error {
  constructor(readonly reason:'unacknowledged'|'missing_sample'|'limit_reached',readonly sqlResponseAcknowledged:boolean){
    super('restoration_capacity_'+reason);this.name='RestorationCapacityError';
  }
}
// Observe acknowledged result metadata only. No SQL, requests, payloads, source
// identities or retries belong here. The caller retains the committed checkpoint
// and holds its exact writer when an acknowledged sample reaches the limit.
export function restorationCapacityObserver(limitBytes=9_000_000_000){
  if(!Number.isSafeInteger(limitBytes)||limitBytes<1||limitBytes>9_000_000_000)throw Error('restoration_capacity_invalid_limit');
  let samples=0,peakBytes:number|null=null;
  const phasePeaks:Partial<Record<RestorationCapacityPhase,number>>={};
  return {
    observe<T>(response:T,phase:RestorationCapacityPhase):T{
      if(!phases.has(phase))throw Error('restoration_capacity_invalid_phase');
      const rows=Array.isArray(response)?response:[response];
      if(rows.length===0||rows.some(row=>typeof row!=='object'||row===null||row.success!==true))
        throw new RestorationCapacityError('unacknowledged',false);
      const sizes=rows.map(row=>row.meta?.size_after);
      if(sizes.some(value=>!Number.isSafeInteger(value)||value<1))
        throw new RestorationCapacityError('missing_sample',true);
      for(const bytes of sizes){samples++;peakBytes=Math.max(peakBytes??bytes,bytes);phasePeaks[phase]=Math.max(phasePeaks[phase]??bytes,bytes);}
      if(peakBytes!==null&&peakBytes>=limitBytes)throw new RestorationCapacityError('limit_reached',true);
      return response;
    },
    snapshot(){return {samples,peakBytes,limitBytes,limitReached:peakBytes!==null&&peakBytes>=limitBytes,phasePeaks:{...phasePeaks},
      measures:'acknowledged_committed_database_bytes' as const,unobservedTransientPeakCredited:false as const};}
  };
}
