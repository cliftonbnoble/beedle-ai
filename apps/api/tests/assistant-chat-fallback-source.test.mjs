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

  // CONF-04: ANY missing LLM config (key, model, or base URL) degrades — never a silent cross-provider
  // default that would send the configured key to the wrong provider.
  assert.match(src, /if \(!env\.LLM_API_KEY \|\| !env\.LLM_MODEL \|\| !env\.LLM_BASE_URL\) \{\s*\n\s*return grounded\(\);/);
  assert.doesNotMatch(src, /\|\| "https:\/\/api\.openai\.com/);
  assert.doesNotMatch(src, /\|\| "gpt-4\.1-mini"/);

  // Non-ok responses degrade (not just 401) and the old throw is gone.
  assert.match(src, /if \(!response\.ok\) \{[\s\S]*?return grounded\(\);\s*\n\s*\}/);
  assert.doesNotMatch(src, /throw new Error\(`LLM request failed/);

  // Empty completion degrades instead of throwing.
  assert.match(src, /if \(!answer\) \{\s*\n\s*console\.warn\("Assistant LLM returned no answer; using grounded fallback"\);\s*\n\s*return grounded\(\);/);
  assert.doesNotMatch(src, /throw new Error\("LLM response did not include an answer/);

  // Network failure / timeout / malformed JSON degrade via an outer catch.
  assert.match(src, /console\.warn\(`Assistant LLM request failed; using grounded fallback: \$\{String\(error\)\}`\);\s*\n\s*return grounded\(\);/);
});
