import {WorkflowEntrypoint,type WorkflowEvent,type WorkflowStep} from 'cloudflare:workers';
import {runSourceBatchWorkflow,type SourceBatchBindings,type SourceBatchParams} from './services/restoration-source-batch-workflow';
export class SourceBatchWorkflow extends WorkflowEntrypoint<SourceBatchBindings,SourceBatchParams>{
  async run(event:WorkflowEvent<SourceBatchParams>,step:WorkflowStep){return runSourceBatchWorkflow(this.env,event,step);}
}
export default {fetch(){return new Response('Private bounded restoration Workflow',{status:503});}} satisfies ExportedHandler<SourceBatchBindings>;
