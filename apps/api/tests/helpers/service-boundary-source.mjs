// Source guards follow an explicit implementation boundary, rather than a facade's filename.
// Behavioral tests remain the authority for results, state transitions and query budgets.
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const boundaries={
 'search.ts':['search-pipeline.ts','search.ts'],
 'search-scoring.ts':['search-query-plan.ts','search-evidence-policy.ts','search-snippets.ts','search-ranking.ts','search-scope-repository.ts','search-candidates.ts','search-scoring.ts'],
 'search-query-analysis.ts':['search-query-analysis.ts','search-filter-policy.ts'],
 'admin-ingestion.ts':['reviewer-policy.ts','reviewer-read-model.ts','admin-ingestion.ts','reviewer-contracts.ts']
};
function files(input){const file=input instanceof URL?fileURLToPath(input):input;return (boundaries[path.basename(file)]||[path.basename(file)]).map(name=>path.join(path.dirname(file),name));}
export async function readServiceBoundary(input){return (await Promise.all(files(input).map(file=>fsp.readFile(file,'utf8')))).join('\n');}
export function readServiceBoundarySync(input){return files(input).map(file=>fs.readFileSync(file,'utf8')).join('\n');}
