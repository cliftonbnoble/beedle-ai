import hashlib
import importlib.util
import json
import pathlib
import tempfile
import threading
import time
import unittest
import datetime
import os
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('parallel_objects',pathlib.Path(__file__).resolve().parents[1]/'scripts/parallel-recovery-objects.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)


class Fake:
    def __init__(self):self.data={};self.gets=0;self.puts=0;self.read_faults=[];self.write_faults=[];self.bad_write=False;self.pacer=p.Pacer(180,sleep=lambda _:None)
    def get(self,key):
        self.gets+=1
        if self.read_faults:raise self.read_faults.pop(0)
        body=self.data.get(key);return None if body is None else (hashlib.sha256(body).hexdigest(),len(body))
    def put(self,key,file):
        self.puts+=1
        if self.write_faults:
            error,commit=self.write_faults.pop(0)
            if commit:self.data[key]=file.read_bytes()
            raise error
        self.data[key]=b'wrong bytes' if self.bad_write else file.read_bytes()


class Transfers(unittest.TestCase):
    def setUp(self):
        p.CANCELLED=False;self.tmp=tempfile.TemporaryDirectory();self.root=pathlib.Path(self.tmp.name);self.body=b'exact original\x00\xff';sha=hashlib.sha256(self.body).hexdigest();(self.root/sha).write_bytes(self.body);self.obj={'key':'private-test-only','sha256':sha,'bytes':len(self.body)};self.store=Fake()
    def tearDown(self):self.tmp.cleanup();p.CANCELLED=False
    def call(self,copy=True):return p.transfer(self.store,self.obj,self.root,copy,sleep=lambda _:None)
    def test_missing_is_copied_and_read_back(self):
        self.assertEqual(self.call(),'copied');self.assertEqual((self.store.gets,self.store.puts),(2,1));self.assertEqual(self.store.data[self.obj['key']],self.body)
    def test_existing_matching_bytes_need_no_write(self):
        self.store.data[self.obj['key']]=self.body;self.assertEqual(self.call(),'existing');self.assertEqual((self.store.gets,self.store.puts),(1,0))
    def test_conflict_is_never_overwritten_or_retried(self):
        self.store.data[self.obj['key']]=b'conflict'
        with self.assertRaises(p.IntegrityHold):self.call()
        self.assertEqual(self.store.puts,0);self.assertEqual(self.store.data[self.obj['key']],b'conflict')
    def test_verify_missing_never_writes(self):
        with self.assertRaises(p.IntegrityHold):self.call(False)
        self.assertEqual(self.store.puts,0)
    def test_lost_put_ack_is_resolved_by_hash_readback_without_duplicate_put(self):
        self.store.write_faults=[(p.RequestFailure(True),True)]
        self.assertEqual(self.call(),'copied');self.assertEqual(self.store.puts,1)
    def test_failed_uncommitted_put_gets_bounded_retry(self):
        self.store.write_faults=[(p.RequestFailure(True),False)]
        self.assertEqual(self.call(),'copied');self.assertEqual(self.store.puts,2)
    def test_permission_failure_stays_a_hold(self):
        self.store.write_faults=[(p.RequestFailure(False),False)]
        with self.assertRaises(p.TransportHold):self.call()
        self.assertEqual(self.store.puts,1)
    def test_wrong_readback_cannot_become_success(self):
        self.store.bad_write=True
        with self.assertRaises(p.IntegrityHold):self.call()
        self.assertEqual(self.store.puts,1)
    def test_transient_reads_retry_but_exhaustion_never_writes(self):
        self.store.read_faults=[p.RequestFailure(True)]*4
        with self.assertRaises(p.TransportHold):self.call()
        self.assertEqual((self.store.gets,self.store.puts),(4,0))
    def test_changed_local_source_stops_before_network(self):
        (self.root/self.obj['sha256']).write_bytes(b'changed')
        with self.assertRaises(p.IntegrityHold):self.call()
        self.assertEqual((self.store.gets,self.store.puts),(0,0))
    def test_operator_cancellation_never_starts_a_new_put(self):
        p.CANCELLED=True
        with self.assertRaises(p.TransportHold):self.call()
        self.assertEqual(self.store.puts,0)


class Scheduling(unittest.TestCase):
    def setUp(self):p.CANCELLED=False
    def tearDown(self):p.CANCELLED=False
    def test_pacer_has_no_burst_and_honors_shared_cooldown(self):
        now=[0.0];clock=lambda:now[0]
        pace=p.Pacer(180,clock=clock,sleep=lambda n:now.__setitem__(0,now[0]+n));observed=[]
        for _ in range(4):pace.wait();observed.append(now[0])
        self.assertTrue(all(b-a>=1/3-1e-9 for a,b in zip(observed,observed[1:])))
        pace.cooldown(5);pace.wait();self.assertGreaterEqual(now[0],6)
        pace.cooldown(600);pace.wait();self.assertGreaterEqual(now[0],606)
        with self.assertRaises(p.IntegrityHold):p.Pacer(181)
    def fixture(self,root):
        (root/'objects').mkdir();objects=[]
        for i in range(12):
            body=('fixture '+str(i)).encode();sha=hashlib.sha256(body).hexdigest();(root/'objects'/sha).write_bytes(body);objects.append({'key':str(i),'sha256':sha,'bytes':len(body),'present':True})
        raw=json.dumps({'objects':objects}).encode();(root/'bundle.json').write_bytes(raw);(root/'config.json').write_text('{}')
        return {'bundle':str(root),'manifestFileSha256':hashlib.sha256(raw).hexdigest(),'config':str(root/'config.json'),'configSha256':hashlib.sha256(b'{}').hexdigest(),'concurrency':4,'requestsPerMinute':180,'operation':'copy','statusFile':str(root/'status.json')}
    def test_four_workers_finish_and_failure_drains_the_active_wave_before_return(self):
        for fail in [False,True]:
            p.CANCELLED=False
            with tempfile.TemporaryDirectory() as directory:
                root=pathlib.Path(directory);cfg=self.fixture(root);state={'active':0,'peak':0,'finished':0,'puts':0};lock=threading.Lock()
                class Store:
                    def __init__(self,*_):self.pacer=p.Pacer(180)
                    def guard(self):pass
                    def get(self,key):
                        with lock:state['active']+=1;state['peak']=max(state['peak'],state['active'])
                        try:
                            time.sleep(.02 if key=='0' else .06)
                            if fail and key=='0':raise p.IntegrityHold('injected conflict')
                            if fail:return None
                            body=('fixture '+key).encode();return hashlib.sha256(body).hexdigest(),len(body)
                        finally:
                            with lock:state['active']-=1;state['finished']+=1
                    def put(self,*_):state['puts']+=1
                with patch.object(p,'Store',Store):
                    if fail:
                        with self.assertRaises(p.IntegrityHold):p.run(cfg)
                    else:self.assertEqual(p.run(cfg)['completed'],12)
                self.assertEqual(state['active'],0);self.assertLessEqual(state['peak'],4);self.assertGreater(state['peak'],1);self.assertEqual(state['puts'],0)
                if fail:self.assertLessEqual(state['finished'],4,'no next wave after a failure')
    def test_changed_manifest_or_target_config_fails_before_store_or_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory);cfg=self.fixture(root);(root/'config.json').write_text('{"changed":true}')
            with patch.object(p,'Store',side_effect=AssertionError('must not touch network')):
                with self.assertRaises(p.IntegrityHold):p.run(cfg)


class ProviderBoundaries(unittest.TestCase):
    def setUp(self):p.CANCELLED=False
    def store(self,responses):
        class Pace:
            def wait(self):pass
            def cooldown(self,n):self.delay=n
        class Response:
            def __init__(self,item):self.status,self.body=item
            def read(self,n):return self.body[:n]
            def getheader(self,*_):return '2'
        class Connection:
            def request(self,*args,**kwargs):pass
            def getresponse(self):return Response(responses.pop(0))
            def close(self):pass
        store=p.Store({'mode':'remote','bucket':'synthetic','accountId':'a'},Pace());store.auth.token=lambda:'synthetic-token'
        return store,Connection
    def test_http_500_is_retried_not_interpreted_as_missing(self):
        store,Connection=self.store([(500,b'provider error'),(200,b'exact')])
        with patch.object(p.http.client,'HTTPSConnection',side_effect=lambda *a,**kw:Connection()):
            self.assertEqual(p.retry_read(store,'synthetic',sleep=lambda _:None),(hashlib.sha256(b'exact').hexdigest(),5))
    def test_only_positive_404_is_missing_and_permission_errors_hold(self):
        for status,expected in [(404,None),(403,'hold')]:
            store,Connection=self.store([(status,b'error')])
            with patch.object(p.http.client,'HTTPSConnection',side_effect=lambda *a,**kw:Connection()):
                if expected is None:self.assertIsNone(p.retry_read(store,'synthetic',sleep=lambda _:None))
                else:
                    with self.assertRaises(p.TransportHold):p.retry_read(store,'synthetic',sleep=lambda _:None)
    def test_http_429_sets_shared_cooldown(self):
        store,Connection=self.store([(429,b'limited'),(200,b'exact')])
        with patch.object(p.http.client,'HTTPSConnection',side_effect=lambda *a,**kw:Connection()):p.retry_read(store,'synthetic',sleep=lambda _:None)
        self.assertEqual(store.pacer.delay,2)
    def test_byte_bound_failure_does_not_return_an_object(self):
        store,Connection=self.store([(200,b'x'*20)])
        with patch.object(p.http.client,'HTTPSConnection',side_effect=lambda *a,**kw:Connection()):
            with self.assertRaises(p.IntegrityHold):store.raw('/synthetic',limit=10)
    def test_claim_and_bucket_privacy_are_mandatory(self):
        for invalid in ['claim','public','domain',None]:
            store,_=self.store([]);store.cfg.update(databaseId='db',runId='run',bundleSha256='b',targetSha256='t')
            expected={'run_id':'run','bundle_sha256':'b','target_sha256':'t'}
            def api(path,payload=None):
                if path.endswith('/query'):return [{'success':True,'results':[] if invalid=='claim' else [expected]}]
                if path.endswith('/managed'):return {'enabled':invalid=='public'}
                return {'domains':['synthetic.invalid'] if invalid=='domain' else []}
            store.api=api
            if invalid:
                with self.assertRaises(p.IntegrityHold):store.guard()
            else:store.guard()
    def test_concurrent_workers_use_one_existing_session_refresh(self):
        with tempfile.TemporaryDirectory() as directory,patch.dict(os.environ,{},clear=True):
            root=pathlib.Path(directory);file=root/'session.toml';clock=[1000.0]
            def write(token,expiry):file.write_text('oauth_token = "'+token+'"\nexpiration_time = "'+datetime.datetime.fromtimestamp(expiry,datetime.timezone.utc).isoformat()+'"\n')
            write('synthetic-old',1001)
            cfg={'oauthFile':str(file),'logDir':str(root),'node':'node','wrangler':'wrangler','config':'synthetic','apiRoot':str(root)};auth=p.Credentials(cfg)
            def refresh(*args,**kwargs):write('synthetic-new',5000)
            with patch.object(p.time,'time',side_effect=lambda:clock[0]),patch.object(p.time,'sleep',side_effect=lambda n:clock.__setitem__(0,clock[0]+n)),patch.object(p.subprocess,'run',side_effect=refresh) as call:
                with p.concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:tokens=list(pool.map(lambda _:auth.token(),range(4)))
            self.assertEqual(tokens,['synthetic-new']*4);self.assertEqual(call.call_count,1)


if __name__=='__main__':unittest.main()
