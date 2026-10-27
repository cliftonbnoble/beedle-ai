import { readServiceBoundary } from './helpers/service-boundary-source.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';

test('reviewer list reads versioned materialized policy and uses bounded keyset pages',async()=>{
 const queue=await fs.readFile(new URL('../src/services/reviewer-queue.ts',import.meta.url),'utf8');
 const rows=await readServiceBoundary(new URL('../src/services/admin-ingestion.ts',import.meta.url));
 const index=await fs.readFile(new URL('../src/services/reviewer-queue-index.ts',import.meta.url),'utf8');
 assert.match(queue,/FROM reviewer_queue_state/);assert.match(queue,/limit\+1/);assert.match(queue,/epoch!==state.epoch/);
 assert.match(queue,/scope:'matching_queue'/);assert.match(queue,/pageSummary/);
 assert.doesNotMatch(queue,/FROM document_reference_issues|FROM document_reference_links|OFFSET|canBatchReviewWith|DERIVED_FILTER/);
 assert.match(rows,/WITH issue_counts AS/);assert.match(rows,/link_counts AS/);
 assert.match(index,/LIMIT 25/);assert.match(index,/reviewer_queue_guards/);assert.match(index,/64\*1024/);
});
