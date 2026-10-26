import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const httpPath = path.resolve(process.cwd(), "src/lib/http.ts");
const sourceRoutePath = path.resolve(process.cwd(), "src/routes/source.ts");
const draftPath = path.resolve(process.cwd(), "src/services/draft-conclusions.ts");

// API-02: only explicitly classified validation errors can reach a client. This is safer than
// attempting to recognize every possible D1/AI/R2/provider error string.
test("toErrorResponse exposes only classified validation errors and hides all service faults", async () => {
  const src = await fs.readFile(httpPath, "utf8");

  // Zod validation failures stay 400, with a compact issue summary instead of the raw JSON dump.
  assert.match(src, /error\.name === "ZodError"/);
  assert.match(src, /zodIssueSummary/);

  // Request parsing errors are intentionally crafted and safe to return. All other errors are
  // logged and receive the same generic response, regardless of their message text.
  assert.match(src, /export class RequestValidationError extends Error/);
  assert.match(src, /error instanceof RequestValidationError/);
  assert.match(src, /console\.error\("Unhandled service error:", error\)/);
  assert.match(src, /json\(\{ error: "Internal error" \}, \{ status: 500 \}\)/);
  assert.doesNotMatch(src, /INFRASTRUCTURE_ERROR_PATTERN/);
  assert.doesNotMatch(src, /const message = error instanceof Error \? error\.message/);
});

test("handleGetSource routes its failures through the shared error helper", async () => {
  const src = await fs.readFile(sourceRoutePath, "utf8");
  assert.match(src, /import \{ json, toErrorResponse \} from "\.\.\/lib\/http"/);
  assert.match(src, /catch \(error\) \{[\s\S]*?return toErrorResponse\(error\);/);
});

// Provider behavior tests exercise malformed/error bodies; neither LLM client
// may bypass that shared reader and return arbitrary upstream content.
test("draft LLM failures never echo the upstream response body to the client", async () => {
  const src = await fs.readFile(draftPath, "utf8");
  assert.match(src, /await boundedCompletion\(/);
  assert.doesNotMatch(src, /response\.(text|json)\(/);
});
