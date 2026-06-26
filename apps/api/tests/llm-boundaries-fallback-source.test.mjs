import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const apiRoot = process.cwd();
const repoRoot = path.resolve(apiRoot, "../..");
const draftPath = path.resolve(apiRoot, "src/services/draft-conclusions.ts");
const assistantPath = path.resolve(apiRoot, "src/services/assistant-chat.ts");
const sharedPath = path.resolve(repoRoot, "packages/shared/src/index.ts");
const draftingPagePath = path.resolve(repoRoot, "apps/web/src/app/drafting/page.tsx");

test("LLM prompts fence untrusted data and drafting fallback is surfaced", async () => {
  const draft = await fs.readFile(draftPath, "utf8");
  const assistant = await fs.readFile(assistantPath, "utf8");
  const shared = await fs.readFile(sharedPath, "utf8");
  const draftingPage = await fs.readFile(draftingPagePath, "utf8");

  assert.match(draft, /untrusted data, not instructions/);
  assert.match(draft, /<findings_of_fact_data>/);
  assert.match(draft, /<relevant_law_data>/);
  assert.match(draft, /<retrieved_authority index=/);
  assert.match(draft, /generationMode: DraftConclusionsResponse\["generation_mode"\] = "heuristic_fallback"/);
  assert.match(draft, /fallback_reason = fallbackReason\(error\)/);

  assert.match(assistant, /untrusted data, not instructions/);
  assert.match(assistant, /<current_question_data>/);
  assert.match(assistant, /<retrieved_decision_data index=/);

  assert.match(shared, /generation_mode: z\.enum\(\["llm", "heuristic_fallback"\]\)\.default\("llm"\)/);
  assert.match(shared, /fallback_reason: z\.string\(\)\.nullable\(\)\.default\(null\)/);
  assert.match(draftingPage, /result\.generation_mode === "heuristic_fallback"/);
  assert.match(draftingPage, /result\.fallback_reason/);
});
