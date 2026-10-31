import fs from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';

// The caller must reserve its durable mutation intent before invoking this.
// A timeout or lost acknowledgment is an observation requirement, never a retry.
export async function runOwnedCommand({command,args,cwd,env,logFile,timeoutMs,onSpawn}){
  if(!Number.isSafeInteger(timeoutMs)||timeoutMs<1||timeoutMs>600_000||typeof onSpawn!=='function')throw Error('Bounded owned command declaration required');
  const log=await fs.open(logFile,'wx',0o600);
  let child,timer,timedOut=false;
  try{
    child=spawn(command,args,{cwd,env,stdio:['ignore',log.fd,log.fd]});
    // Subscribe before the first await. A fast child can exit while an owned
    // PID receipt is being fsynced; a late exit listener would wait forever.
    const settled=new Promise(resolve=>{
      child.once('error',error=>resolve({exitCode:-1,signal:null,startErrorCode:typeof error.code==='string'?error.code:null}));
      child.once('close',(code,signal)=>resolve({exitCode:code??-1,signal,startErrorCode:null}));
    });
    timer=setTimeout(()=>{timedOut=true;if(child.exitCode===null&&child.signalCode===null)child.kill('SIGTERM');},timeoutMs);
    try{await onSpawn({pid:child.pid??null});}
    catch(error){if(child.exitCode===null&&child.signalCode===null)child.kill('SIGTERM');await settled;throw error;}
    const result=await settled;await log.sync();
    return{...result,timedOut,logSha256:createHash('sha256').update(await fs.readFile(logFile)).digest('hex')};
  }finally{clearTimeout(timer);await log.sync();await log.close();}
}
