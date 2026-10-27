// Preserve existing operator limits while replacing oversized list responses with
// bounded pages. This helper performs no writes and never hides a cursor conflict.
export async function fetchWithReviewerPagination(input,init) {
  const url=new URL(input instanceof Request?input.url:String(input));
  const method=(init?.method||(input instanceof Request?input.method:'GET')).toUpperCase();
  if(url.pathname!='/admin/ingestion/documents'||method!=='GET')return fetch(input,init);
  const requested=Number(url.searchParams.get('limit')||200);
  if(!Number.isSafeInteger(requested)||requested<1||requested>2000)throw new Error('Reviewer operator collection limit must be between 1 and 2000; use cursor traversal for a larger queue.');
  const documents=[];let first;let last;let headers;let epoch;const seen=new Set();
  for(let page=0;page<20;page++) {
    url.searchParams.set('limit',String(Math.min(100,requested-documents.length)));
    const response=await fetch(input instanceof Request?new Request(url,input):url,init);
    if(!response.ok)return response;
    const payload=await response.json();
    if(!Array.isArray(payload.documents)||!Number.isSafeInteger(payload.page?.epoch))throw new Error('Reviewer server must expose versioned keyset pagination.');
    if(epoch!==undefined&&epoch!==payload.page.epoch)throw new Error('Reviewer queue changed; restart the operator snapshot.');
    epoch=payload.page.epoch;first??=payload;last=payload;headers=new Headers(response.headers);
    for(const document of payload.documents){if(seen.has(document.id))throw new Error('Reviewer pagination repeated a document; restart the snapshot.');seen.add(document.id);documents.push(document);}
    if(!payload.nextCursor||documents.length>=requested)break;
    url.searchParams.set('cursor',payload.nextCursor);
  }
  headers.delete('content-length');headers.delete('content-encoding');headers.delete('etag');
  return new Response(JSON.stringify({...first,documents,nextCursor:last.nextCursor,pageSummary:{total:documents.length},
    collection:{requestedLimit:requested,count:documents.length,complete:!last.nextCursor,epoch}}),{status:200,headers});
}
