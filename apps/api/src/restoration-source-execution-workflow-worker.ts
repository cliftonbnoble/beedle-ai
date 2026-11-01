import {WorkflowEntrypoint,type WorkflowEvent,type WorkflowStep} from 'cloudflare:workers';
import {runSourceExecutionWorkflow,type SourceExecutionWorkflowBindings,type SourceExecutionWorkflowParams} from './services/restoration-source-execution-workflow';
export class SourceExecutionWorkflow extends WorkflowEntrypoint<SourceExecutionWorkflowBindings,SourceExecutionWorkflowParams>{
  async run(event:WorkflowEvent<SourceExecutionWorkflowParams>,step:WorkflowStep){return runSourceExecutionWorkflow(this.env,event,step);}
}
export default{fetch(){return new Response('Private restoration source execution',{status:503});}} satisfies ExportedHandler<SourceExecutionWorkflowBindings>;
