import {runSelectedRestorationReprocess} from './services/restoration-selected-reprocess';

interface SelectedReprocessBindings {
  DB:D1Database;ORIGINALS:R2Bucket;TARGET_ORIGINALS:R2Bucket;PROOFS:R2Bucket;
  OPERATION_ENABLED:string;OPERATION_JSON:string;
}
export async function advanceSelectedReprocess(env:SelectedReprocessBindings){
  if(env.OPERATION_ENABLED!=='true')return{state:'disabled' as const};
  if(new TextEncoder().encode(env.OPERATION_JSON).length>5000)throw Error('selected_reprocess_config_bound');
  const input:unknown=JSON.parse(env.OPERATION_JSON);
  // The service validates the complete strict plan and finite start window.
  return runSelectedRestorationReprocess(input,{db:env.DB,originals:env.ORIGINALS,target:env.TARGET_ORIGINALS,proofs:env.PROOFS});
}
export default {
  fetch(){return new Response('Private selected restoration reprocess',{status:503});},
  async scheduled(_event:ScheduledController,env:SelectedReprocessBindings){
    const result=await advanceSelectedReprocess(env);
    if(result.state==='held')throw Error('Selected reprocess held; inspect private durable evidence');
  }
} satisfies ExportedHandler<SelectedReprocessBindings>;
