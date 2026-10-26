export async function compareVectorReleases({baselineUrl,candidateUrl,queries,request=boundedJson}) {
  for(const url of [baselineUrl,candidateUrl]) {const parsed=new URL(url);if(!['https:','http:'].includes(parsed.protocol)||parsed.username||parsed.password)throw Error('Use explicit HTTP(S) API origins without URL credentials');}
  if(!Array.isArray(queries)||!queries.length||queries.length>20||queries.some(query=>typeof query!=='string'||!query.trim()||query.length>2000))throw Error('Supply 1–20 queries of 1–2000 characters');
  const root=url=>url.replace(/\/+$/,'');
  const baseline=await request(root(baselineUrl)+'/admin/vector-generations');
  const candidate=await request(root(candidateUrl)+'/admin/vector-generations');
  const comparisons=[];
  for(const query of queries) {
    const input={query,corpusMode:'trusted_only',limit:10};
    const run=async url=>{const started=Date.now();const response=await request(root(url)+'/search',input);return {milliseconds:Date.now()-started,documentIds:[...new Set((response.results||[]).map(row=>row.documentId))]};};
    const [a,b]=await Promise.all([run(baselineUrl),run(candidateUrl)]);
    const overlap=a.documentIds.filter(id=>b.documentIds.includes(id));const union=new Set([...a.documentIds,...b.documentIds]);
    comparisons.push({query,baseline:a,candidate:b,overlapCount:overlap.length,jaccard:union.size?overlap.length/union.size:null});
  }
  return {generatedAt:new Date().toISOString(),baseline:{url:baselineUrl,profile:baseline.profile,coverage:baseline.coverage,ready:baseline.ready},candidate:{url:candidateUrl,profile:candidate.profile,coverage:candidate.coverage,ready:candidate.ready},comparisons,
    limitation:'Overlap and latency are diagnostic comparisons, not independent relevance judgments or authorization to promote a model.'};
}
async function boundedJson(url,payload) {
  const response=await fetch(url,{method:payload?'POST':'GET',headers:payload?{'content-type':'application/json'}:undefined,body:payload?JSON.stringify(payload):undefined,signal:AbortSignal.timeout(45_000)});
  if(!response.ok) {void response.body?.cancel().catch(()=>{});throw Error(`Shadow read failed (${response.status})`);}
  const reader=response.body?.getReader();if(!reader)throw Error('Empty shadow response');let bytes=0;const chunks=[];
  try {while(true){const {done,value}=await reader.read();if(done)break;bytes+=value.byteLength;if(bytes>2*1024*1024){void reader.cancel().catch(()=>{});throw Error('Shadow response exceeds 2 MiB');}chunks.push(value);}}finally{reader.releaseLock();}
  const merged=new Uint8Array(bytes);let at=0;for(const chunk of chunks){merged.set(chunk,at);at+=chunk.length;}return JSON.parse(new TextDecoder().decode(merged));
}
