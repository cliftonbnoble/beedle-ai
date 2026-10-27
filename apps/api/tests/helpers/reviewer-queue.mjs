import {loadTs} from './load-ts.mjs';
const {advanceReviewerQueueIndex}=loadTs('src/services/admin-ingestion.ts');
const {inspectReviewerQueue}=loadTs('src/services/reviewer-queue-index.ts');
export async function readyReviewerQueue(env) {
  for(let count=0;count<1000;count++){
    const state=await inspectReviewerQueue(env);
    if(state.ready)return state;
    await advanceReviewerQueueIndex(env);
  }
  throw new Error('Reviewer fixture failed to finish bounded materialization');
}
