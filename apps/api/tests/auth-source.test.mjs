import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

test("AUTH-01 gates non-public Worker routes before routing", async () => {
  const root = process.cwd();
  const [worker, auth, types, config] = await Promise.all([
    fs.readFile(path.resolve(root, "src/index.ts"), "utf8"),
    fs.readFile(path.resolve(root, "src/lib/auth.ts"), "utf8"),
    fs.readFile(path.resolve(root, "src/lib/types.ts"), "utf8"),
    fs.readFile(path.resolve(root, "wrangler.toml"), "utf8")
  ]);

  assert.match(worker, /const auth = await authorizeRequest\(request, env\)/);
  assert.match(worker, /if \(!auth\.ok\) return withCors\(auth\.response, request, env\)/);
  assert.match(worker, /router\.post\("\/auth\/login"/);
  assert.match(worker, /router\.get\("\/auth\/session"/);
  assert.match(auth, /path === "\/health"/);
  assert.match(auth, /path === "\/auth\/login" && method === "POST"/);
  assert.match(auth, /return \{ ok: false, response: unauthorizedResponse\(\) \}/);
  assert.match(types, /AUTH_USERNAME\?: string/);
  assert.match(types, /AUTH_PASSWORD_HASH\?: string/);
  assert.match(types, /AUTH_SESSION_SECRET\?: string/);
  assert.match(config, /name = "AUTH_RATE_LIMIT"/);
  assert.doesNotMatch(config, /AUTH_PASSWORD/);
});

test("auth uses secret-backed password hashes and signed HttpOnly sessions with CSRF", async () => {
  const root = process.cwd();
  const [auth, worker] = await Promise.all([
    fs.readFile(path.resolve(root, "src/lib/auth.ts"), "utf8"),
    fs.readFile(path.resolve(root, "src/index.ts"), "utf8")
  ]);

  assert.match(auth, /const PASSWORD_HASH_ALGORITHM = "pbkdf2_sha256"/);
  assert.match(auth, /MIN_PASSWORD_HASH_ITERATIONS = 100_000/);
  assert.match(auth, /crypto\.subtle\.deriveBits\(\{ name: "PBKDF2", hash: "SHA-256"/);
  assert.match(auth, /crypto\.subtle\.sign\("HMAC"/);
  assert.match(auth, /"HttpOnly"/);
  assert.match(auth, /config\.cookieSecure \? "Secure" : ""/);
  assert.match(auth, /`SameSite=\$\{config\.cookieSecure \? "None" : "Lax"\}`/);
  assert.match(auth, /request\.headers\.get\("x-beedle-csrf"\)/);
  assert.match(auth, /constantTimeBytesEqual/);
  assert.match(worker, /x-beedle-csrf/);
});
