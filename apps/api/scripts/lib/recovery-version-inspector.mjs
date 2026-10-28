// Runs only on loopback through Wrangler local dev. Its sole binding is the
// explicitly chosen candidate R2 bucket (remote binding in cloud rehearsals).
// No writes, public route, document content, credentials, or HTTP metadata.
export default {
 async fetch(request,env){
  const url=new URL(request.url),headers={'cache-control':'no-store'};
  if(url.hostname!=='127.0.0.1'||request.headers.has('origin')||request.headers.get('x-recovery-execution')!==env.EXECUTION_ID)return new Response(null,{status:403,headers});
  if(request.method!=='GET')return new Response(null,{status:405,headers});
  if(url.pathname==='/ready')return Response.json({executionId:env.EXECUTION_ID},{headers});
  if(url.pathname!=='/versions'||[...url.searchParams.keys()].some(k=>k!=='cursor')||(url.searchParams.get('cursor')?.length||0)>8192)return new Response(null,{status:400,headers});
  const result=await env.SOURCE_BUCKET.list({limit:1000,...(url.searchParams.has('cursor')?{cursor:url.searchParams.get('cursor')}:{})});
  return Response.json({objects:result.objects.map(o=>({key:o.key,version:o.version,size:o.size})),truncated:result.truncated,cursor:result.truncated?result.cursor:null},{headers});
 }
};
