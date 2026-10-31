import {WorkflowEntrypoint,type WorkflowEvent,type WorkflowStep} from 'cloudflare:workers';
import {runSourceSuccessorWorkflow,type SourceSuccessorParams,type SourceSuccessorWorkflowBindings} from './services/restoration-source-successor-workflow';

export class SourceRestorationSuccessorWorkflow extends WorkflowEntrypoint<SourceSuccessorWorkflowBindings,SourceSuccessorParams>{
  async run(event:WorkflowEvent<SourceSuccessorParams>,step:WorkflowStep){return runSourceSuccessorWorkflow(this.env,event,step);}
}
export default {fetch(){return new Response('Private source restoration successor Workflow',{status:503});}} satisfies ExportedHandler<SourceSuccessorWorkflowBindings>;
