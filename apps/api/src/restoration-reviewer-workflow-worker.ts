import {WorkflowEntrypoint,type WorkflowEvent,type WorkflowStep} from 'cloudflare:workers';
import {runRestorationReviewerWorkflow,type RestorationReviewerWorkflowBindings,type RestorationReviewerWorkflowParams} from './services/restoration-reviewer-workflow';

export class RestorationReviewerWorkflow extends WorkflowEntrypoint<RestorationReviewerWorkflowBindings,RestorationReviewerWorkflowParams>{
  async run(event:WorkflowEvent<RestorationReviewerWorkflowParams>,step:WorkflowStep){return runRestorationReviewerWorkflow(this.env,event,step);}
}
export default{fetch(){return new Response('Private restoration reviewer maintenance',{status:503});}} satisfies ExportedHandler<RestorationReviewerWorkflowBindings>;
