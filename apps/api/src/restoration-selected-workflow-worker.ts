import {WorkflowEntrypoint,type WorkflowEvent,type WorkflowStep} from 'cloudflare:workers';
import {runSelectedWorkflowStep,type SelectedWorkflowParams} from './services/restoration-selected-workflow';
import type {SelectedReprocessBindings} from './restoration-selected-reprocess-worker';

export class SelectedRestorationWorkflow extends WorkflowEntrypoint<SelectedReprocessBindings,SelectedWorkflowParams>{
  async run(event:WorkflowEvent<SelectedWorkflowParams>,step:WorkflowStep){
    return runSelectedWorkflowStep(this.env,event,step);
  }
}
export default {
  fetch(){return new Response('Private selected restoration Workflow',{status:503});}
} satisfies ExportedHandler<SelectedReprocessBindings>;
