import {WorkflowEntrypoint,type WorkflowEvent,type WorkflowStep} from 'cloudflare:workers';
import {runRestorationTerminalWorkflow,type RestorationTerminalBindings,type RestorationTerminalParams} from './services/restoration-terminal-workflow';

export class RestorationTerminalWorkflow extends WorkflowEntrypoint<RestorationTerminalBindings,RestorationTerminalParams>{
  async run(event:WorkflowEvent<RestorationTerminalParams>,step:WorkflowStep){return runRestorationTerminalWorkflow(this.env,event,step);}
}
export default {fetch(){return new Response('Private restoration terminal disposition',{status:503});}} satisfies ExportedHandler<RestorationTerminalBindings>;
