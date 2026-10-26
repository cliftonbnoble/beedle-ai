import test from "node:test";
import assert from "node:assert/strict";
import { frameworkSecurityErrors } from "../scripts/check-framework-security.mjs";

const patched = { next: "15.5.28", react: "19.1.10", "react-dom": "19.1.10", sharp: "0.35.5" };

test("patched framework versions pass and each known vulnerable dependency fails", () => {
  assert.deepEqual(frameworkSecurityErrors(patched), []);
  for (const [name, version] of Object.entries({ next: "15.5.27", react: "19.1.9", sharp: "0.34.5" })) {
    assert.ok(frameworkSecurityErrors({ ...patched, [name]: version }).length, name);
  }
});

test("unknown release lines, prereleases, malformed versions and React mismatches require review", () => {
  for (const next of [undefined, "next", "15.5.28-canary.1", "15.2.99", "17.0.0"]) {
    assert.ok(frameworkSecurityErrors({ ...patched, next }).length);
  }
  assert.ok(frameworkSecurityErrors({ ...patched, "react-dom": "19.0.0" }).length);
  assert.deepEqual(frameworkSecurityErrors({ ...patched, next: "16.4.3" }), []);
});
