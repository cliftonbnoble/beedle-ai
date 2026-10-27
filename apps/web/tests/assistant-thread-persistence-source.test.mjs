// Historical source-only normalization guards were replaced by executable store
// tests in assistant-live-store.test.mjs and real browser coverage (M17/M19).
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
test('chat and sidebar subscribe to the same live store rather than reloading their own saves',async()=>{
 for(const path of ['src/app/case-assistant/page.tsx','src/components/app-shell.tsx']){
  const source=await fs.readFile(path,'utf8');assert.match(source,/useAssistantThreads\(/);assert.doesNotMatch(source,/loadAssistantThreads|saveAssistantThreads|assistantThreadsUpdatedEvent/);
 }
});
