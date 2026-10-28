// One-shot metadata capture for accounts where remote preview sessions are
// unavailable. No HTTP route, D1 binding, source-body reads or original writes.
export default {
 async scheduled(_event,env){
  if(!/^[a-f0-9-]{36}$/.test(env.CAPTURE_ID||'')||!/^[a-f0-9]{64}$/.test(env.TARGET_SHA256||'')||!/^[a-f0-9]{64}$/.test(env.TOOL_SHA256||''))throw Error('Invalid private version capture identity');
  const key='versions/'+env.CAPTURE_ID+'.json';if(await env.PROOF_BUCKET.head(key))return;
  const objects=[],seen=new Set();let cursor,bytes=0;
  for(let page=0;page<100;page++){
   const result=await env.SOURCE_BUCKET.list({limit:1000,...(cursor?{cursor}:{})});
   if(!Array.isArray(result.objects)||typeof result.truncated!=='boolean')throw Error('Incomplete version capture');
   for(const o of result.objects){if(typeof o.key!=='string'||!o.version||!Number.isSafeInteger(o.size))throw Error('Invalid version metadata');const row={key:o.key,version:o.version,size:o.size};bytes+=new TextEncoder().encode(JSON.stringify(row)).byteLength+1;if(bytes>16*1024*1024)throw Error('Version capture exceeds metadata bound');objects.push(row);}
   if(!result.truncated){
    const proof={version:1,captureId:env.CAPTURE_ID,targetSha256:env.TARGET_SHA256,toolSha256:env.TOOL_SHA256,capturedAt:new Date().toISOString(),objects};
    const body=JSON.stringify(proof);if(new TextEncoder().encode(body).byteLength>32*1024*1024)throw Error('Version capture exceeds byte bound');
    await env.PROOF_BUCKET.put(key,body,{onlyIf:{etagDoesNotMatch:'*'},httpMetadata:{contentType:'application/json'}});return;
   }
   if(typeof result.cursor!=='string'||!result.cursor||seen.has(result.cursor))throw Error('Version capture cursor did not advance');seen.add(result.cursor);cursor=result.cursor;
  }
  throw Error('Version capture exceeds page bound');
 }
};
