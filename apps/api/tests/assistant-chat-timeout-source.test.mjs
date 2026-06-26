import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const assistantChatPath = path.resolve(process.cwd(), "src/services/assistant-chat.ts");

test("assistant chat model calls have explicit timeouts", async () => {
  const src = await fs.readFile(assistantChatPath, "utf8");

  assert.match(src, /const assistantChatModelTimeoutMs = 18000/);
  assert.match(src, /function withAssistantTimeout/);
  assert.match(src, /Promise\.race\(\[operation, timeoutPromise\]\)/);
  assert.match(src, /withAssistantTimeout\(\s*env\.AI\.run/);
  assert.match(src, /const controller = new AbortController\(\)/);
  assert.match(src, /controller\.abort\("assistant-llm-timeout"\)/);
  assert.match(src, /signal: controller\.signal/);
  assert.match(src, /clearTimeout\(timeout\)/);
});
