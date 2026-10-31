import test from'node:test';import assert from'node:assert/strict';import{mkdtemp,rm,readFile}from'node:fs/promises';import os from'node:os';import path from'node:path';
import{runOwnedCommand}from'../scripts/lib/owned-command.mjs';
test('a fast child completing during durable PID bookkeeping still has one acknowledged exit',async()=>{const dir=await mkdtemp(path.join(os.tmpdir(),'beedle-owned-command-'));try{
 let spawned=0;const result=await runOwnedCommand({command:process.execPath,args:['-e',"process.stdout.write('fixed-output')"],cwd:dir,env:{},logFile:path.join(dir,'private.log'),timeoutMs:1000,onSpawn:async({pid})=>{assert(pid>0);spawned++;await new Promise(resolve=>setTimeout(resolve,100));}});
 assert.equal(result.exitCode,0);assert.equal(result.timedOut,false);assert.equal(spawned,1);assert.equal(await readFile(path.join(dir,'private.log'),'utf8'),'fixed-output');assert.match(result.logSha256,/^[a-f0-9]{64}$/);
 }finally{await rm(dir,{recursive:true,force:true});}});
test('start failure and finite timeout return unknown command outcomes without another launch',async()=>{const dir=await mkdtemp(path.join(os.tmpdir(),'beedle-owned-command-'));try{
 const missing=await runOwnedCommand({command:path.join(dir,'missing'),args:[],cwd:dir,env:{},logFile:path.join(dir,'missing.log'),timeoutMs:1000,onSpawn:async()=>{}});assert.equal(missing.exitCode,-1);assert.equal(missing.startErrorCode,'ENOENT');
 let spawned=0;const timed=await runOwnedCommand({command:process.execPath,args:['-e','setInterval(()=>{},1000)'],cwd:dir,env:{},logFile:path.join(dir,'timeout.log'),timeoutMs:100,onSpawn:async()=>{spawned++;}});assert.equal(timed.timedOut,true);assert.equal(timed.exitCode,-1);assert.equal(spawned,1);
 }finally{await rm(dir,{recursive:true,force:true});}});
test('failed owner bookkeeping stops only its newly owned child and preserves its log',async()=>{const dir=await mkdtemp(path.join(os.tmpdir(),'beedle-owned-command-'));try{
 await assert.rejects(runOwnedCommand({command:process.execPath,args:['-e','setInterval(()=>{},1000)'],cwd:dir,env:{},logFile:path.join(dir,'private.log'),timeoutMs:1000,onSpawn:async()=>{throw Error('Durable owner receipt failed');}}),/owner receipt/);assert.equal((await readFile(path.join(dir,'private.log'))).length,0);
 }finally{await rm(dir,{recursive:true,force:true});}});
