import test from 'node:test';
import assert from 'node:assert/strict';
import {loadTs} from '../../api/tests/helpers/load-ts.mjs';
const {AssistantLiveStore}=loadTs('../web/src/components/assistant-live-store.ts');
const {durableThread,planThreadCommit,readStoredThread,readThreadRecord}=loadTs('../web/src/components/assistant-thread-records.ts');
const {createEmptyThread,createMessage}=loadTs('../web/src/components/assistant-thread-store.ts');
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
function repository(){
 const records=new Map(),receipts=new Map();const stats={writes:0,fail:false,loseAck:false,gate:null};
 return {records,receipts,stats,async readAll(){if(stats.fail)throw new Error('Injected unavailable storage');return [...records.values()].map(value=>structuredClone(value));},async read(id){if(stats.fail)throw new Error('Injected unavailable storage');return structuredClone(records.get(id));},
 async commit(operation){if(stats.gate){const gate=stats.gate;stats.gate=null;await gate.promise;}if(stats.fail)throw new Error('Injected quota failure');
  if(receipts.has(operation.id)){const receipt=receipts.get(operation.id);return {...receipt,record:structuredClone(records.get(receipt.record.id))};}
  const result=planThreadCommit(records.get(operation.threadId)||null,operation);records.set(result.record.id,structuredClone(result.record));receipts.set(operation.id,structuredClone(result));stats.writes++;
  if(stats.loseAck){stats.loseAck=false;throw new Error('Injected lost acknowledgment');}return structuredClone(result);
 }};
}
async function store(repo,legacy){const live=new AssistantLiveStore(repo,()=>legacy||null,()=>{},100000);await live.start();return live;}
const turn=(live,id,text)=>{const user=createMessage('user',text),pending=createMessage('assistant','Thinking',{pending:true});live.update(id,thread=>({...thread,messages:[...thread.messages,user,pending],updatedAt:Date.now()}));return pending.id;};

test('saving pending/streaming output never normalizes the active in-memory messages',async()=>{
 const repo=repository(),live=await store(repo);const thread=live.add(createEmptyThread());const pending=turn(live,thread.id,'Notice question');
 await live.flush();assert.equal(live.snapshot().threads[0].messages.at(-1).pending,true);assert.equal(repo.records.get(thread.id).thread.messages.length,1);assert.equal(repo.records.get(thread.id).thread.interrupted,true);
 const answer=createMessage('assistant','Reviewed source text',{streaming:true,assessmentVersion:'claims-1',evidenceStatus:'no_evidence'});live.complete(pending,answer);await live.flush();
 assert.equal(live.snapshot().threads[0].messages.at(-1).streaming,true);assert.equal(repo.records.get(thread.id).thread.messages.at(-1).streaming,undefined);
 const writes=repo.stats.writes;live.update(thread.id,t=>({...t,messages:t.messages.map(m=>({...m,streaming:false}))}));await live.flush();assert.equal(repo.stats.writes,writes,'typing completion does not serialize another durable record');
 const reload=await store(repo);assert.equal(reload.snapshot().threads[0].messages.length,2);assert.equal(reload.snapshot().threads[0].interrupted,false);
});

test('quota failures preserve the complete current answer and a retry reconciles a lost acknowledgment',async()=>{
 const repo=repository(),live=await store(repo);const thread=live.add(createEmptyThread());repo.stats.fail=true;const pending=turn(live,thread.id,'Saved question');await live.flush();
 live.complete(pending,createMessage('assistant','The entire current answer'));assert.equal(live.snapshot().threads[0].messages.at(-1).content,'The entire current answer');assert.equal(live.snapshot().status,'unavailable');assert.match(live.exportLive(),/entire current answer/);
 repo.stats.fail=false;repo.stats.loseAck=true;await live.retry();assert.equal(live.snapshot().status,'unavailable');const receipts=repo.receipts.size;
 await live.retry();await live.flush();assert.equal(live.snapshot().status,'saved');assert.equal(repo.records.size,1);assert.equal(repo.records.get(thread.id).thread.messages.length,2);assert.ok(repo.receipts.size>=receipts);
});

test('versioned migration validates stored fields, preserves provenance, and never resurrects tombstoned legacy chats',async()=>{
 const repo=repository();const thread=createEmptyThread();const pending=createMessage('assistant','Thinking',{pending:true});
 thread.messages=[createMessage('user','Legacy issue'),createMessage('assistant','Legacy answer',{streaming:true,assessmentVersion:'claims-1',referenceContext:{version:'reference-context-1',scopeKey:'a'.repeat(64),subjectQuery:'Notice',referenceDocumentIds:['a'],activeDocumentIds:['a']}}),pending];thread.judgeNames=['Erin E. Katayama'];thread.selectedDocumentIds=['a'];
 const raw=JSON.stringify([thread,{id:'bad',messages:[null] }]);const live=await store(repo,raw);assert.match(live.snapshot().warning,/could not be loaded/);assert.equal(live.snapshot().threads.length,1);assert.equal(live.snapshot().threads[0].messages.length,2);assert.equal(live.snapshot().threads[0].messages[1].referenceContext.activeDocumentIds[0],'a');assert.equal(live.snapshot().threads[0].interrupted,true);
 await live.flush();assert.equal(JSON.parse(await live.exportStored()).legacy,raw);live.remove(thread.id);await live.flush();const reload=await store(repo,raw);assert.equal(reload.snapshot().threads.length,0);
 for(const invalid of [null,{}, {...thread,messages:[{id:'a',role:'alien',content:'X'}]}, {...thread,messages:[{id:'a',role:'assistant',content:7}]}])assert.ok(readStoredThread(invalid).issues);
 assert.equal(readThreadRecord({version:3,id:'a',revision:1,thread:null}).record,null);
});

test('two tabs keep unrelated threads and merge independent filter edits atomically',async()=>{
 const repo=repository(),a=await store(repo),b=await store(repo);const one=a.add(createEmptyThread()),two=b.add(createEmptyThread());await Promise.all([a.flush(),b.flush()]);await Promise.all([a.refresh(),b.refresh()]);assert.equal(a.snapshot().threads.length,2);assert.equal(b.snapshot().threads.length,2);
 a.update(one.id,t=>({...t,indexCodes:['100']}));b.update(one.id,t=>({...t,judgeNames:['Erin E. Katayama']}));await Promise.all([a.flush(),b.flush()]);await a.refresh();
 const merged=a.snapshot().threads.find(t=>t.id===one.id);assert.deepEqual(merged.indexCodes,['100']);assert.deepEqual(merged.judgeNames,['Erin E. Katayama']);assert.equal(repo.records.size,2);
});

test('concurrent transcript edits fork, retain both questions, and route a late answer to its pending identity',async()=>{
 const repo=repository(),a=await store(repo);const thread=a.add(createEmptyThread());await a.flush();const b=await store(repo);
 const pa=turn(a,thread.id,'Question from tab A'),pb=turn(b,thread.id,'Question from tab B');await Promise.all([a.flush(),b.flush()]);await Promise.all([a.refresh(),b.refresh()]);
 const moved=b.snapshot().relocations.at(-1);assert.ok(moved);assert.notEqual(moved.to,thread.id);assert.equal(b.snapshot().threads.find(t=>t.id===moved.to).messages.at(-1).id,pb);
 b.complete(pb,createMessage('assistant','Answer for B'));a.complete(pa,createMessage('assistant','Answer for A'));await Promise.all([a.flush(),b.flush()]);await a.refresh();
 const transcripts=[...repo.records.values()].filter(r=>r.thread).map(r=>r.thread.messages.map(m=>m.content));
 assert.equal(transcripts.length,2);assert.ok(transcripts.some(t=>t.join('|')==='Question from tab A|Answer for A'));assert.ok(transcripts.some(t=>t.join('|')==='Question from tab B|Answer for B'));
});

test('remote deletion during a pending turn keeps a recoverable local copy; local deletion cannot be undone by a late response',async()=>{
 const repo=repository(),a=await store(repo);const thread=a.add(createEmptyThread());await a.flush();const b=await store(repo);const pending=turn(a,thread.id,'Pending local issue');await a.flush();
 await b.refresh();b.remove(thread.id);await b.flush();await a.refresh(thread.id);const copy=a.snapshot().relocations.at(-1).to;assert.equal(repo.records.get(thread.id).thread,null);
 assert.equal(a.complete(pending,createMessage('assistant','Late surviving answer')),copy);await a.flush();assert.equal(repo.records.get(copy).thread.messages.length,2);
 const deleted=turn(a,copy,'Will be deleted');a.remove(copy);assert.equal(a.complete(deleted,createMessage('assistant','Must not return')),null);await a.flush();assert.equal(repo.records.get(copy).thread,null);
});

test('response completion during an asynchronous save is retained in the next revision',async()=>{
 const repo=repository(),live=await store(repo);const thread=live.add(createEmptyThread());await live.flush();const pending=turn(live,thread.id,'In flight');repo.stats.gate=deferred();const gate=repo.stats.gate;const write=live.flush();
 live.complete(pending,createMessage('assistant','Finished during save'));gate.resolve();await write;await live.flush();assert.equal(repo.records.get(thread.id).thread.messages.at(-1).content,'Finished during save');assert.equal(live.snapshot().threads[0].messages.length,2);
});

test('initial storage failure still loads legacy chats in memory and retries without overwriting newer durable work',async()=>{
 const repo=repository();repo.stats.fail=true;const thread=createEmptyThread();thread.messages=[createMessage('user','Legacy question')];const live=await store(repo,JSON.stringify([thread]));assert.equal(live.snapshot().threads.length,1);assert.equal(live.snapshot().status,'unavailable');
 repo.stats.fail=false;const other=await store(repo);other.add({...thread,messages:[...thread.messages,createMessage('assistant','Other saved answer')]});await other.flush();
 live.update(thread.id,t=>({...t,messages:[...t.messages,createMessage('user','Unsaved new issue')]}));await live.retry();await live.flush();assert.equal([...repo.records.values()].filter(r=>r.thread).length,2);assert.match(live.snapshot().warning,/Concurrent/);
});

test('a remote refresh cannot resurrect a locally deleted chat before its tombstone is saved',async()=>{
 const repo=repository(),live=await store(repo);const thread=live.add(createEmptyThread());await live.flush();live.remove(thread.id);await live.refresh(thread.id);
 assert.equal(live.snapshot().threads.length,0);await live.flush();assert.equal(repo.records.get(thread.id).thread,null);
});

test('deletion while a conflicting save is in flight deletes only the local conflict copy',async()=>{
 const repo=repository(),a=await store(repo);const thread=a.add(createEmptyThread());await a.flush();const b=await store(repo);
 turn(a,thread.id,'A independent question');turn(b,thread.id,'B independent question');await b.flush();repo.stats.gate=deferred();const gate=repo.stats.gate;const write=a.flush();a.remove(thread.id);gate.resolve();await write;await a.flush();await a.refresh();
 assert.ok(repo.records.get(thread.id).thread.messages.some(m=>m.content==='B independent question'));
 assert.equal([...repo.records.values()].filter(r=>r.thread).length,1);
});

test('an acknowledged write retry after another tab advances the record preserves both completed branches',async()=>{
 const repo=repository(),a=await store(repo);const thread=a.add(createEmptyThread());await a.flush();const pending=turn(a,thread.id,'A initial question');repo.stats.loseAck=true;await a.flush();
 const b=await store(repo);turn(b,thread.id,'B later question');await b.flush();a.complete(pending,createMessage('assistant','A late answer'));await a.retry();await a.flush();await a.refresh();
 const saved=[...repo.records.values()].filter(r=>r.thread).map(r=>r.thread.messages.map(m=>m.content));
 assert.ok(saved.some(messages=>messages.includes('A late answer')&&!messages.includes('B later question')));assert.ok(saved.some(messages=>messages.includes('B later question')));
});

test('deleting a chat after quota failure can free space without retrying its obsolete failed write',async()=>{
 const repo=repository(),live=await store(repo);const thread=live.add(createEmptyThread());await live.flush();
 const commit=repo.commit;repo.commit=async operation=>{if(operation.thread)throw new Error('Quota full');return commit(operation);};
 turn(live,thread.id,'Unsaved content');await live.flush();assert.equal(live.snapshot().status,'unavailable');live.remove(thread.id);await live.retry();
 assert.equal(repo.records.get(thread.id).thread,null);assert.equal(live.snapshot().status,'saved');assert.equal(live.snapshot().threads.length,0);
});

test('normalization changing object field order does not fork a pending transcript on a remote filter edit',async()=>{
 const repo=repository(),a=await store(repo);const thread=createEmptyThread();thread.messages=[createMessage('user','Earlier issue'),createMessage('assistant','Saved text',{referenceContext:{version:'reference-context-1',scopeKey:'a'.repeat(64),subjectQuery:'notice',referenceDocumentIds:['a'],activeDocumentIds:['a']},evidenceStatus:'no_evidence',assessmentVersion:'claims-1',citations:[]})];
 a.add(thread);await a.flush();const b=await store(repo);const pending=turn(a,thread.id,'Current question');await a.flush();await b.refresh(thread.id);
 b.update(thread.id,t=>({...t,judgeNames:['Erin E. Katayama']}));await b.flush();await a.refresh(thread.id);
 assert.equal(a.snapshot().relocations.length,0);assert.equal(a.snapshot().threads[0].messages.at(-1).id,pending);assert.deepEqual(a.snapshot().threads[0].judgeNames,['Erin E. Katayama']);
 a.complete(pending,createMessage('assistant','Completed'));await a.flush();assert.equal(repo.records.size,1);
});
