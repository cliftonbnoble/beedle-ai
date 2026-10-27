export const assistantContextLimits = Object.freeze({
  transportMessages:20,modelMessages:8,messageChars:12000,historyChars:24000,retrievalQueryChars:2000
});
export type AssistantContextMessage={role:'user'|'assistant';content:string;pending?:boolean};
const searchableCharacters=(value:string)=>(value.match(/[\p{L}\p{N}]/gu)||[]).length;
export function assistantQuestionError(value:string):string|null {
 const text=value.trim();
 if(!text)return 'Enter a question.';
 if(text.length>assistantContextLimits.messageChars)return `Keep the question within ${assistantContextLimits.messageChars.toLocaleString('en-US')} characters.`;
 if(searchableCharacters(text)<3)return 'Include at least three letters or numbers in the question.';
 return null;
}
function safeHead(value:string,limit:number){let text=value.slice(0,limit);if(/[\uD800-\uDBFF]$/.test(text))text=text.slice(0,-1);return text;}
function safeTail(value:string,limit:number){let text=value.slice(-limit);if(/^[\uDC00-\uDFFF]/.test(text))text=text.slice(1);return text;}
export function assistantContextWindow(input:ReadonlyArray<AssistantContextMessage>) {
 const candidates=input.filter(message=>!message.pending && message.content.trim().length>0);
 const selected:Array<{role:'user'|'assistant';content:string}>=[];let chars=0;let clipped=false;
 for(let index=candidates.length-1;index>=0 && selected.length<assistantContextLimits.modelMessages;index--) {
  const message=candidates[index]!;const original=message.content.trim();
  // Oversized legacy history is shortened only in the request copy, never in the stored conversation.
  const content=safeHead(original,assistantContextLimits.messageChars);
  if(chars+content.length>assistantContextLimits.historyChars)break;
  clipped ||= content.length!==original.length;
  selected.push({role:message.role,content});chars+=content.length;
 }
 return {messages:selected.reverse(),retainedChars:chars,omittedMessages:candidates.length-selected.length,legacyMessageClipped:clipped};
}
export function boundAssistantRetrievalQuery(value:string) {
 const compact=value.replace(/\s+/g,' ').trim();const max=assistantContextLimits.retrievalQueryChars;
 if(compact.length<=max)return {query:compact,truncated:false};
 const head=Math.floor(max*0.7),tail=max-head-3;
 let query=`${safeHead(compact,head)} … ${safeTail(compact,tail)}`;
 if(searchableCharacters(query)<3) {
  const words=(compact.match(/[\p{L}\p{N}][\p{L}\p{N}.()’'-]*/gu)||[]).join(' ');
  query=safeHead(words,max);
 }
 return {query,truncated:true};
}
