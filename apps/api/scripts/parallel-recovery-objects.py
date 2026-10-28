"""Internal bounded R2 executor. Input/output are private execution artifacts, never secrets."""
import concurrent.futures
import datetime
import hashlib
import http.client
import json
import os
import pathlib
import re
import signal
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import urllib.parse
import uuid

CANCELLED=False


def file_sha(file):
    with pathlib.Path(file).open('rb') as source:return hashlib.file_digest(source,'sha256').hexdigest()


class IntegrityHold(RuntimeError): pass
class TransportHold(RuntimeError): pass
class RequestFailure(RuntimeError):
    def __init__(self, retryable=False, delay=0):
        super().__init__('Transient request exhausted' if retryable else 'Request rejected')
        self.retryable, self.delay = retryable, delay


def atomic(file, value):
    tmp=file.with_name(file.name+'.'+uuid.uuid4().hex+'.tmp')
    with tmp.open('x') as f:
        os.chmod(tmp,0o600);json.dump(value,f);f.flush();os.fsync(f.fileno())
    os.replace(tmp,file)


class Pacer:
    def __init__(self, per_minute, clock=time.monotonic, sleep=time.sleep):
        if not 1<=per_minute<=180: raise IntegrityHold('Request rate outside reviewed bound')
        self.interval=60/per_minute;self.next=0;self.until=0;self.lock=threading.Lock();self.clock=clock;self.sleep=sleep
    def wait(self):
        while True:
            if CANCELLED:raise TransportHold('Operator interrupted')
            with self.lock:
                now=self.clock();delay=max(self.next,self.until)-now
                if delay<=0:self.next=now+self.interval;return
            self.sleep(min(delay,1))
    def cooldown(self, seconds):
        with self.lock:self.until=max(self.until,self.clock()+max(1,seconds))


class Credentials:
    def __init__(self,cfg):self.cfg=cfg;self.lock=threading.Lock()
    def token(self):
        if os.environ.get('CLOUDFLARE_API_TOKEN'):return os.environ['CLOUDFLARE_API_TOKEN']
        with self.lock:
            file=pathlib.Path(self.cfg.get('oauthFile',''))
            if not file.is_absolute():raise TransportHold('Explicit existing Wrangler session required')
            for attempt in range(3):
                try:auth=tomllib.loads(file.read_text());break
                except (OSError,tomllib.TOMLDecodeError):
                    if attempt==2:raise TransportHold('Wrangler session unreadable') from None
                    time.sleep(.1)
            expiry=datetime.datetime.fromisoformat(auth['expiration_time'].replace('Z','+00:00')).timestamp()
            if expiry-time.time()<60:
                # Drain new starts before expiry. Only this owner refreshes; PUT children
                # receive an access-token snapshot and cannot race on the session file.
                while expiry>time.time():time.sleep(max(0,min(1,expiry-time.time())))
                env={**os.environ,'CI':'1','WRANGLER_SEND_METRICS':'false','WRANGLER_LOG_PATH':self.cfg['logDir']+'/refresh-detail.log'}
                for key in ['CLOUDFLARE_API_TOKEN','CLOUDFLARE_API_KEY','CLOUDFLARE_EMAIL']:env.pop(key,None)
                with open(self.cfg['logDir']+'/refresh-private.log','ab') as log:
                    subprocess.run([self.cfg['node'],self.cfg['wrangler'],'d1','info','DB','--config',self.cfg['config']],env=env,cwd=self.cfg['apiRoot'],stdout=log,stderr=subprocess.STDOUT,timeout=90)
                auth=tomllib.loads(file.read_text());expiry=datetime.datetime.fromisoformat(auth['expiration_time'].replace('Z','+00:00')).timestamp()
                if expiry<=time.time()+60:raise TransportHold('Existing Wrangler session needs sign-in')
            token=auth.get('oauth_token')
            if not isinstance(token,str) or not token:raise TransportHold('Wrangler access token absent')
            return token


class Store:
    def __init__(self,cfg,pacer):self.cfg=cfg;self.pacer=pacer;self.auth=Credentials(cfg);self.local=threading.local()
    def raw(self,path,payload=None,limit=32*1024*1024):
        token=self.auth.token();self.pacer.wait()
        try:
            if not getattr(self.local,'connection',None):self.local.connection=http.client.HTTPSConnection('api.cloudflare.com',timeout=60,context=ssl.create_default_context(cafile=self.cfg.get('caFile')))
            self.local.connection.request('POST' if payload is not None else 'GET','/client/v4/accounts/'+self.cfg['accountId']+path,body=json.dumps(payload).encode() if payload is not None else None,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
            response=self.local.connection.getresponse();body=response.read(limit+1)
            if len(body)>limit:raise IntegrityHold('Object/response exceeds byte bound')
            if response.status==404:return None
            if response.status==429:
                raw=response.getheader('retry-after','300')
                delay=int(raw) if raw.isdigit() else 300
                self.pacer.cooldown(delay);raise RequestFailure(True,delay)
            if 500<=response.status<=599:raise RequestFailure(True)
            if response.status!=200:raise RequestFailure(False)
            return body
        except (OSError,http.client.HTTPException):
            if getattr(self.local,'connection',None):self.local.connection.close();self.local.connection=None
            raise RequestFailure(True) from None
        except RequestFailure:
            if getattr(self.local,'connection',None):self.local.connection.close();self.local.connection=None
            raise
    def api(self,path,payload=None):
        value=self.raw(path,payload,1024*1024)
        if value is None:raise IntegrityHold('Guard resource missing')
        result=json.loads(value)
        if result.get('success') is not True:raise IntegrityHold('Guard response incomplete')
        return result['result']
    def cli(self,args,output=None):
        self.pacer.wait();env={**os.environ,'CI':'1','WRANGLER_SEND_METRICS':'false'}
        log=pathlib.Path(self.cfg['logDir'])/(uuid.uuid4().hex+'.log');env['WRANGLER_LOG_PATH']=str(log)+'.detail'
        if self.cfg['mode']=='remote':env['CLOUDFLARE_API_TOKEN']=self.auth.token()
        else:
            for key in ['CLOUDFLARE_API_TOKEN','CLOUDFLARE_ACCOUNT_ID','CLOUDFLARE_API_KEY','CLOUDFLARE_EMAIL']:env.pop(key,None)
        try:result=subprocess.run([self.cfg['node'],self.cfg['wrangler'],*args],capture_output=True,env=env,cwd=self.cfg['apiRoot'],timeout=120)
        except subprocess.TimeoutExpired as error:
            log.write_bytes((error.stdout or b'')+(error.stderr or b''));raise RequestFailure(True) from None
        log.write_bytes(result.stdout+result.stderr);log.chmod(0o600)
        if result.returncode:
            message=(result.stdout+result.stderr).decode(errors='replace')
            if output and 'The specified key does not exist.' in message:return False
            transient=bool(re.search(r'-\s*(?:429|5\d\d)\b|HTTP\s*(?:429|5\d\d)\b|fetch failed|ETIMEDOUT|ECONNRESET|SQLITE_BUSY',message,re.I))
            if re.search(r'-\s*(?:401|403)\b',message):transient=False
            raise RequestFailure(transient,300 if re.search(r'-\s*429\b',message) else 0)
        return result.stdout
    def flags(self):return ['--config',self.cfg['config'],'--remote'] if self.cfg['mode']=='remote' else ['--config',self.cfg['config'],'--local','--persist-to',self.cfg['persistTo']]
    def guard(self):
        expected={'run_id':self.cfg['runId'],'bundle_sha256':self.cfg['bundleSha256'],'target_sha256':self.cfg['targetSha256']}
        sql='SELECT run_id,bundle_sha256,target_sha256 FROM beedle_recovery_claim'
        if self.cfg['mode']=='remote':
            rows=self.api('/d1/database/'+self.cfg['databaseId']+'/query',{'sql':sql})
            if not rows or any(x.get('success') is not True for x in rows) or rows[0].get('results')!=[expected]:raise IntegrityHold('Candidate claim differs')
            base='/r2/buckets/'+urllib.parse.quote(self.cfg['bucket'],safe='')+'/domains/'
            if self.api(base+'managed').get('enabled') is not False or self.api(base+'custom').get('domains')!=[]:raise IntegrityHold('Candidate bucket is public')
        else:
            rows=json.loads(self.cli(['d1','execute','DB',*self.flags(),'--json','--command',sql]))
            if len(rows)!=1 or rows[0].get('success') is not True or rows[0].get('results')!=[expected]:raise IntegrityHold('Candidate claim differs')
    def get(self,key):
        if self.cfg['mode']=='remote':
            body=self.raw('/r2/buckets/'+urllib.parse.quote(self.cfg['bucket'],safe='')+'/objects/'+urllib.parse.quote(key,safe=''))
            return None if body is None else (hashlib.sha256(body).hexdigest(),len(body))
        fd,name=tempfile.mkstemp(prefix='get-',dir=self.cfg['logDir']);os.close(fd)
        try:
            found=self.cli(['r2','object','get',self.cfg['bucket']+'/'+key,*self.flags(),'--file',name],output=name)
            if found is False:return None
            file=pathlib.Path(name)
            if file.stat().st_size>32*1024*1024:raise IntegrityHold('Object exceeds byte bound')
            return file_sha(file),file.stat().st_size
        finally:pathlib.Path(name).unlink(missing_ok=True)
    def put(self,key,file):self.cli(['r2','object','put',self.cfg['bucket']+'/'+key,*self.flags(),'--file',str(file)])


def retry_read(store,key,on_retry=lambda:None,sleep=time.sleep):
    for attempt in range(4):
        try:return store.get(key)
        except RequestFailure as error:
            if CANCELLED:raise TransportHold('Operator interrupted') from None
            if not error.retryable or attempt==3:raise TransportHold('Object read unavailable') from None
            on_retry();store.pacer.cooldown(error.delay) if error.delay else None;sleep(min(2**attempt,8))


def transfer(store,obj,objects_dir,copy,on_retry=lambda:None,sleep=time.sleep):
    expected=(obj['sha256'],obj['bytes']);file=objects_dir/obj['sha256']
    if file.stat().st_size!=obj['bytes'] or file_sha(file)!=obj['sha256']:raise IntegrityHold('Local original differs')
    existing=retry_read(store,obj['key'],on_retry,sleep)
    if existing is not None and existing!=expected:raise IntegrityHold('Conflicting R2 key content')
    if existing==expected:return 'existing' # A full fresh GET already proves its bytes; final verification reads again.
    if not copy:raise IntegrityHold('Required candidate original missing')
    for attempt in range(4):
        if CANCELLED:raise TransportHold('Operator interrupted')
        failure=None
        try:store.put(obj['key'],file)
        except RequestFailure as error:failure=error
        readback=retry_read(store,obj['key'],on_retry,sleep)
        if readback==expected:return 'copied'
        if readback is not None:raise IntegrityHold('R2 readback differs')
        if CANCELLED:raise TransportHold('Operator interrupted')
        if failure is not None and not failure.retryable:raise TransportHold('Object upload rejected')
        if attempt==3:raise TransportHold('Object write acknowledgment unresolved')
        on_retry();store.pacer.cooldown(failure.delay) if failure and failure.delay else None;sleep(min(2**attempt,8))


def run(cfg):
    if cfg.get('workerSha256') and file_sha(__file__)!=cfg['workerSha256']:raise IntegrityHold('Frozen executor changed')
    if not 1<=cfg['concurrency']<=4:raise IntegrityHold('Concurrency outside reviewed bound')
    root=pathlib.Path(cfg['bundle']);raw=(root/'bundle.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=cfg['manifestFileSha256']:raise IntegrityHold('Manifest bytes changed')
    if hashlib.sha256(pathlib.Path(cfg['config']).read_bytes()).hexdigest()!=cfg['configSha256']:raise IntegrityHold('Target config changed')
    manifest=json.loads(raw);objects=[o for o in manifest['objects'] if o['present']]
    if len({o['key'] for o in objects})!=len(objects):raise IntegrityHold('Duplicate object keys')
    if any(not re.fullmatch('[a-f0-9]{64}',o['sha256']) or not isinstance(o['bytes'],int) or not 0<=o['bytes']<=32*1024*1024 or '"' in o['key'] or '\x00' in o['key'] for o in objects):raise IntegrityHold('Invalid original descriptor')
    pacer=Pacer(cfg['requestsPerMinute']);store=Store(cfg,pacer);store.guard()
    status={'phase':cfg['operation'],'pid':os.getpid(),'total':len(objects),'completed':0,'copied':0,'existing':0,'retries':0,'active':0,'maxActive':0,'startedAt':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    status_file=pathlib.Path(cfg['statusFile']);lock=threading.Lock();stop=threading.Event();pulse_stop=threading.Event();first_error=[]
    def persist():
        with lock:status['updatedAt']=datetime.datetime.now(datetime.timezone.utc).isoformat();atomic(status_file,status)
    def pulse():
        while not pulse_stop.wait(5):persist()
    def retried():
        with lock:status['retries']+=1
    def task(index,obj):
        global CANCELLED
        if stop.is_set() or CANCELLED:return
        with lock:status['active']+=1;status['maxActive']=max(status['maxActive'],status['active'])
        try:
            result=transfer(store,obj,root/'objects',cfg['operation']=='copy',retried)
            with lock:status['completed']+=1;status[result]+=1
        except Exception as error:
            stop.set()
            with lock:
                if not first_error:first_error.append(error);status.update(failedIndex=index,errorClass=type(error).__name__,error=str(error) if isinstance(error,(IntegrityHold,TransportHold)) else 'Private transport failure')
            CANCELLED=True # No more requests/writes while the active wave drains.
        finally:
            with lock:status['active']-=1
    persist();thread=threading.Thread(target=pulse,daemon=True);thread.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=cfg['concurrency']) as pool:
            # At most one small wave is scheduled, and every active operation is drained
            # before the process/parent releases its recovery lock after a failure.
            for start in range(0,len(objects),cfg['concurrency']):
                futures=[pool.submit(task,i,objects[i]) for i in range(start,min(start+cfg['concurrency'],len(objects)))]
                for future in futures:future.result()
                persist()
                if stop.is_set() or CANCELLED:break
        if first_error:raise first_error[0]
        if CANCELLED:raise TransportHold('Operator interrupted')
        if status['completed']!=len(objects):raise IntegrityHold('Incomplete object coverage')
        status['phase']='complete';persist()
    finally:pulse_stop.set();thread.join()
    return status


if __name__=='__main__':
    def cancel(_signum,_frame):
        global CANCELLED
        CANCELLED=True
    signal.signal(signal.SIGTERM,cancel);signal.signal(signal.SIGINT,cancel)
    try:
        cfg=json.loads(pathlib.Path(sys.argv[1]).read_text());result=run(cfg)
        print(json.dumps({'phase':result['phase'],'completed':result['completed'],'total':result['total'],'maxActive':result['maxActive'],'retries':result['retries']}))
    except Exception as error:
        print(json.dumps({'phase':'held','errorClass':type(error).__name__,'error':str(error) if isinstance(error,(IntegrityHold,TransportHold)) else 'Inspect private transport state'}));sys.exit(1)
