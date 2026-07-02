import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const httpPath = path.resolve(process.cwd(), "src/lib/http.ts");
const sourceRoutePath = path.resolve(process.cwd(), "src/routes/source.ts");
const draftPath = path.resolve(process.cwd(), "src/services/draft-conclusions.ts");

// API-02: server faults must not masquerade as client errors, and internals must not leak.
test("toErrorResponse separates client errors (400) from infrastructure faults (500 generic)", async () => {
  const src = await fs.readFile(httpPath, "utf8");

  // Zod validation failures stay 400, with a compact issue summary instead of the raw JSON dump.
  assert.match(src, /error\.name === "ZodError"/);
  assert.match(src, /zodIssueSummary/);

  // Platform failures (D1/AI bindings/network/Vectorize/R2) are classified and returned as a generic 500
  // — never the raw message — and logged server-side.
  assert.match(src, /INFRASTRUCTURE_ERROR_PATTERN/);
  assert.match(src, /D1_\|too many SQL variables\|no such table/);
  assert.match(src, /needs to be run remotely/);
  assert.match(src, /console\.error\("Unhandled service error:", error\)/);
  assert.match(src, /json\(\{ error: "Internal error" \}, \{ status: 500 \}\)/);

  // Crafted domain/validation messages remain client-actionable 400s.
  assert.match(src, /return json\(\{ error: message \}, \{ status: 400 \}\)/);
});

test("handleGetSource routes its failures through the shared error helper", async () => {
  const src = await fs.readFile(sourceRoutePath, "utf8");
  assert.match(src, /import \{ json, toErrorResponse \} from "\.\.\/lib\/http"/);
  assert.match(src, /catch \(error\) \{[\s\S]*?return toErrorResponse\(error\);/);
});

// The thrown message becomes the client-visible fallback_reason — upstream provider bodies (request ids,
// org/quota details) must stay server-side. Mirrors the assistant-chat convention.
test("draft LLM failures never echo the upstream response body to the client", async () => {
  const src = await fs.readFile(draftPath, "utf8");
  assert.doesNotMatch(src, /throw new Error\(`LLM request failed \(\$\{response\.status\}\): \$\{text\}`\)/);
  assert.match(src, /console\.warn\(`Draft LLM request failed \(\$\{response\.status\}\): \$\{compactWhitespace\(text\)\.slice\(0, 500\)\}`\)/);
  assert.match(src, /throw new Error\(`LLM request failed \(\$\{response\.status\}\)`\)/);
});
