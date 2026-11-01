import {WorkflowEntrypoint,type WorkflowEvent,type WorkflowStep} from 'cloudflare:workers';
import {runRestorationPublicationWorkflow,type RestorationPublicationWorkflowBindings,type RestorationPublicationWorkflowParams} from './services/restoration-publication-workflow';

export class RestorationPublicationWorkflow extends WorkflowEntrypoint<RestorationPublicationWorkflowBindings,RestorationPublicationWorkflowParams> {
  async run(event:WorkflowEvent<RestorationPublicationWorkflowParams>,step:WorkflowStep) {
    return runRestorationPublicationWorkflow(this.env,event,step);
  }
}
export default {fetch(){return new Response('Private provisional restoration publication',{status:503});}} satisfies ExportedHandler<RestorationPublicationWorkflowBindings>;
