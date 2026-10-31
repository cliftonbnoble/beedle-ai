import {WorkflowEntrypoint,type WorkflowEvent,type WorkflowStep} from 'cloudflare:workers';
import {runPublishedSourceReceiptWorkflow,type PublishedSourceReceiptBindings,type PublishedSourceReceiptParams} from './services/restoration-published-source-workflow';

export class PublishedSourceReceiptWorkflow extends WorkflowEntrypoint<PublishedSourceReceiptBindings,PublishedSourceReceiptParams>{
  async run(event:WorkflowEvent<PublishedSourceReceiptParams>,step:WorkflowStep){return runPublishedSourceReceiptWorkflow(this.env,event,step);}
}
export default {fetch(){return new Response('Private published-source receipt reconciliation',{status:503});}} satisfies ExportedHandler<PublishedSourceReceiptBindings>;
