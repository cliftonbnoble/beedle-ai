import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const assistantPath = path.resolve(process.cwd(), "src/services/assistant-chat.ts");

// The assistant must always return a grounded answer rather than a 400 when the LLM is unavailable for
// ANY reason — no key, auth (401), no credits (402), rate limiting (429), provider outage (5xx),
// network failure, timeout, or an empty completion. Previously only 401 fell back; every other failure
// threw and surfaced as a 400 to the user (and would do so in production if the LLM account ran out of
// credits or the provider was down). This guards the graceful-degradation behavior.
test("assistant callLlm degrades to the grounded answer on any LLM failure", async () => {
  const src = await fs.readFile(assistantPath, "utf8");

  // A single grounded() helper is used for every degradation path.
  assert.match(src, /const grounded = \(\) =>\s*\n\s*synthesizeGroundedAnswer\(\{ question: latestUserQuestion\(messages\), decisions, scopeLabel \}\);/);

  // No-key path still degrades.
  assert.match(src, /if \(!env\.LLM_API_KEY\) \{\s*\n\s*return grounded\(\);/);

  // Non-ok responses degrade (not just 401) and the old throw is gone.
  assert.match(src, /if \(!response\.ok\) \{[\s\S]*?return grounded\(\);\s*\n\s*\}/);
  assert.doesNotMatch(src, /throw new Error\(`LLM request failed/);

  // Empty completion degrades instead of throwing.
  assert.match(src, /if \(!answer\) \{\s*\n\s*console\.warn\("Assistant LLM returned no answer; using grounded fallback"\);\s*\n\s*return grounded\(\);/);
  assert.doesNotMatch(src, /throw new Error\("LLM response did not include an answer/);

  // Network failure / timeout / malformed JSON degrade via an outer catch.
  assert.match(src, /console\.warn\(`Assistant LLM request failed; using grounded fallback: \$\{String\(error\)\}`\);\s*\n\s*return grounded\(\);/);
});

// The Workers AI standby path (callWorkersAi) must degrade the same way: env.AI is truthy but throws
// "Binding AI needs to be run remotely" in local dev, and can fail in production.
test("assistant callWorkersAi degrades to the grounded answer instead of throwing", async () => {
  const src = await fs.readFile(assistantPath, "utf8");

  const start = src.indexOf("async function callWorkersAi");
  const end = src.indexOf("async function callLlm");
  assert.ok(start > -1 && end > start, "callWorkersAi must exist before callLlm");
  const body = src.slice(start, end);

  // Missing binding degrades rather than throwing the old "binding is not configured" error.
  assert.match(body, /if \(!env\.AI\) return grounded\(\);/);
  assert.doesNotMatch(body, /throw new Error\("Workers AI binding is not configured/);
  // The run is wrapped so any failure (including the local "needs to be run remotely") degrades.
  assert.match(body, /\} catch \(error\) \{[\s\S]*?return grounded\(\);\s*\n\s*\}/);
  assert.doesNotMatch(body, /throw new Error\("Workers AI response did not include an answer/);
});
