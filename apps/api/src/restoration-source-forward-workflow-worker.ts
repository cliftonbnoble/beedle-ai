import {WorkflowEntrypoint,type WorkflowEvent,type WorkflowStep} from 'cloudflare:workers';
import {runSourceForwardWorkflow,type SourceForwardBindings,type SourceForwardParams} from './services/restoration-source-forward-workflow';

export class SourceForwardCompletionWorkflow extends WorkflowEntrypoint<SourceForwardBindings,SourceForwardParams>{
  async run(event:WorkflowEvent<SourceForwardParams>,step:WorkflowStep){
    return runSourceForwardWorkflow(this.env,event,step);
  }
}
export default {
  fetch(){return new Response('Private source forward-completion Workflow',{status:503});}
} satisfies ExportedHandler<SourceForwardBindings>;
