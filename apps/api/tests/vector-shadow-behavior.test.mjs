import test from 'node:test';
import assert from 'node:assert/strict';
import {compareVectorReleases} from '../scripts/lib/vector-shadow.mjs';
test('shadow comparison records profiles, latency and identities without inventing relevance judgments',async()=>{
  const calls=[];
  const report=await compareVectorReleases({baselineUrl:'https://baseline.example',candidateUrl:'https://candidate.example',queries:['repair notice'],request:async(url,input)=>{
    calls.push({url,input});
    if(!input)return {profile:{id:new URL(url).hostname},coverage:{eligibleSources:3,verifiedLiveSources:3},ready:true};
    assert.equal(input.corpusMode,'trusted_only');return {results:(url.includes('baseline')?['a','b']:['b','c']).map(documentId=>({documentId,snippet:'private source text'}))};
  }});
  assert.equal(report.comparisons[0].jaccard,1/3);assert.equal(report.comparisons[0].overlapCount,1);assert.equal(calls.length,4);assert.doesNotMatch(JSON.stringify(report),/private source text/);assert.match(report.limitation,/not independent relevance judgments/);
  await assert.rejects(compareVectorReleases({baselineUrl:'https://baseline.example',candidateUrl:'https://candidate.example',queries:Array(21).fill('query')}),/1–20/);
});
