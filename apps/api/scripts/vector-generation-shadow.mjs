import fs from 'node:fs/promises';
import {compareVectorReleases} from './lib/vector-shadow.mjs';
const {VECTOR_SHADOW_BASELINE_URL:baselineUrl,VECTOR_SHADOW_CANDIDATE_URL:candidateUrl,VECTOR_SHADOW_QUERIES_FILE:queriesFile,VECTOR_SHADOW_REPORT_FILE:reportFile}=process.env;
if(!baselineUrl||!candidateUrl||!queriesFile||!reportFile)throw Error('Set explicit VECTOR_SHADOW_BASELINE_URL, VECTOR_SHADOW_CANDIDATE_URL, VECTOR_SHADOW_QUERIES_FILE and VECTOR_SHADOW_REPORT_FILE');
if((await fs.stat(queriesFile)).size>128*1024)throw Error('Shadow query file exceeds 128 KiB');
const queries=JSON.parse(await fs.readFile(queriesFile,'utf8'));
const report=await compareVectorReleases({baselineUrl,candidateUrl,queries});
await fs.writeFile(reportFile,JSON.stringify(report,null,2)+'\n',{flag:'wx'});
console.log(`Wrote shadow comparison to ${reportFile}; retain the baseline and obtain independent relevance review before promotion.`);
