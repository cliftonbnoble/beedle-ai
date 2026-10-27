import test from 'node:test';
import assert from 'node:assert/strict';
import {fetchWithReviewerPagination} from '../scripts/lib/reviewer-pagination-client.mjs';
test('operator collections follow bounded cursors, preserve totals and expose partial collection limits',async()=>{
 const original=globalThis.fetch;const calls=[];
 globalThis.fetch=async(input,init)=>{
  const url=new URL(input);calls.push({url,init});const start=Number(url.searchParams.get('cursor')||0);const limit=Number(url.searchParams.get('limit'));
  const documents=Array.from({length:Math.min(limit,245-start)},(_,i)=>({id:String(start+i)}));
  return Response.json({documents,summary:{total:245,scope:'matching_queue'},page:{epoch:7},nextCursor:start+documents.length<245?String(start+documents.length):null});
 };
 try{
  const result=await(await fetchWithReviewerPagination('https://api.example/admin/ingestion/documents?limit=220&realOnly=1')).json();
  assert.equal(result.documents.length,220);assert.equal(result.summary.total,245);assert.equal(result.collection.complete,false);assert.equal(result.nextCursor,'220');
  assert.deepEqual(calls.map(call=>Number(call.url.searchParams.get('limit'))),[100,100,20]);assert.ok(calls.every(call=>call.url.searchParams.get('realOnly')==='1'));
 }finally{globalThis.fetch=original;}
});
test('operator collections propagate a conflict and reject mixed epochs, duplicates and unsafe collection limits',async()=>{
 const original=globalThis.fetch;
 try{
  for(const mode of ['conflict','epoch','duplicate']){
   let count=0;globalThis.fetch=async()=>{
    count++;if(count===2&&mode==='conflict')return Response.json({error:'Queue changed'},{status:409});
    return Response.json({documents:Array.from({length:100},(_,i)=>({id:String((mode==='duplicate'?0:(count-1)*100)+i)})),summary:{total:300},page:{epoch:mode==='epoch'?count:1},nextCursor:'next'});
   };
   if(mode==='conflict')assert.equal((await fetchWithReviewerPagination('https://api.example/admin/ingestion/documents?limit=200')).status,409);
   else await assert.rejects(fetchWithReviewerPagination('https://api.example/admin/ingestion/documents?limit=200'),mode==='epoch'?/changed/:/repeated/);
  }
  await assert.rejects(fetchWithReviewerPagination('https://api.example/admin/ingestion/documents?limit=2001'),/between 1 and 2000/);
 }finally{globalThis.fetch=original;}
});
