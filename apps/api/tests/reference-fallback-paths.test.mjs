import { test } from "node:test";
import assert from "node:assert/strict";
import path from "node:path";
import { spawnSync } from "node:child_process";

const apiDir = process.cwd();
const fixturePdf = path.resolve(apiDir, "fixtures", "law_sample.pdf");
const scriptPath = path.resolve(apiDir, "scripts", "rebuild-legal-references.mjs");

test("invalid text-export path fails clearly", () => {
  const result = spawnSync(process.execPath, [scriptPath], {
    cwd: apiDir,
    env: {
      ...process.env,
      INDEX_CODES_PDF: fixturePdf,
      ORDINANCE_PDF: fixturePdf,
      RULES_PDF: fixturePdf,
      ORDINANCE_LAYOUT_TEXT: "/definitely/missing/ordinance-export.txt",
      REFERENCES_DRY_RUN: "1"
    },
    encoding: "utf8"
  });

  assert.notEqual(result.status, 0);
  assert.match(`${result.stderr}${result.stdout}`, /ORDINANCE_LAYOUT_TEXT text export path is invalid or unreadable/i);
});
