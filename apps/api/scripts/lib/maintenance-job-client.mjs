// Keep a finite HTTP step and its durable job identity across retries/restarts.
export async function completeMaintenanceJob(apiBase, initial, options={}) {
  if(!initial?.jobId) return initial;
  const {fetchImpl=fetch,sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms)),pollMs=2100,maxMs=24*60*60*1000,onProgress=job=>console.log(`Maintenance ${job.jobId}: ${job.state} (${job.counts.completed}/${job.counts.total}; skipped ${job.counts.skipped})`)}=options;
  const root=String(apiBase).replace(/\/+$/,'');
  const path=`/admin/maintenance/jobs/${encodeURIComponent(initial.jobId)}`;
  const end=Date.now()+maxMs;
  let job=initial;
  while(true) {
    onProgress(job);
    if(job.state==='completed') return job.result??job;
    if(job.state==='failed'||job.state==='cancelled') throw new Error(`Maintenance job ${job.jobId} is ${job.state}: ${job.lastError||'inspect its status before retrying'}`);
    if(Date.now()>=end) throw new Error(`Polling deadline reached; resume maintenance job ${job.jobId}`);
    const wait=Math.max(pollMs,Math.min(60_000,Date.parse(job.nextAttemptAt||'')-Date.now()||0));
    await sleep(wait);
    const response=await fetchImpl(new URL(`${root}${path}/step`),{method:'POST',headers:{'content-type':'application/json'},body:'{}',signal:AbortSignal.timeout(90_000)});
    if(response.status===429||response.status===503) {await sleep(Math.max(1000,Math.min(60_000,Number(response.headers.get('retry-after')||60)*1000)));continue;}
    if(!response.ok) throw new Error(`Maintenance step failed (${response.status}); resume ${job.jobId}`);
    job=await response.json();
    if(job.jobId!==initial.jobId) throw new Error('Maintenance response changed job identity');
  }
}
export async function readMaintenanceJob(apiBase,jobId) {
  const response=await fetch(new URL(`${String(apiBase).replace(/\/+$/,'')}/admin/maintenance/jobs/${encodeURIComponent(jobId)}`),{signal:AbortSignal.timeout(30_000)});
  if(!response.ok) throw new Error(`Cannot inspect maintenance job ${jobId} (${response.status})`);
  return response.json();
}
