import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

test("web API client persists CSRF and redirects protected 401s to login", async () => {
  const src = await fs.readFile(path.resolve(process.cwd(), "src/lib/api.ts"), "utf8");

  assert.match(src, /const csrfStorageKey = "beedle-auth-csrf"/);
  assert.match(src, /headers\.set\("x-beedle-csrf", csrf\)/);
  assert.match(src, /response\.status === 401 && !path\.startsWith\("\/auth\/"\)/);
  assert.match(src, /window\.location\.assign\(`\/login\?next=/);
  assert.match(src, /export async function login\(username: string, password: string\)/);
  assert.match(src, /authLoginRequestSchema\.parse/);
  assert.match(src, /export async function logout\(\)/);
  assert.match(src, /setCsrfToken\(""\)/);
});

test("app shell checks the session and exposes sign out outside the login page", async () => {
  const src = await fs.readFile(path.resolve(process.cwd(), "src/components/app-shell.tsx"), "utf8");

  assert.match(src, /getAuthSession\(\)/);
  assert.match(src, /router\.replace\(`\/login\?next=/);
  assert.match(src, /if \(isLoginPage\) return <>\{children\}<\/>/);
  assert.match(src, /await logout\(\)/);
  assert.match(src, /aria-label="Sign out"/);
});

test("login page uses the auth client and sanitizes next redirects", async () => {
  const src = await fs.readFile(path.resolve(process.cwd(), "src/app/login/page.tsx"), "utf8");

  assert.match(src, /await login\(username\.trim\(\), password\)/);
  assert.match(src, /router\.replace\(safeNextPath\(nextPath\)\)/);
  assert.match(src, /function safeNextPath\(value: string\): string/);
  assert.match(src, /if \(!value\.startsWith\("\/"\) \|\| value\.startsWith\("\/\/"\)\) return "\/search"/);
});
