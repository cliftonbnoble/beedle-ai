import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const root = fileURLToPath(new URL("../../", import.meta.url));
const cache = new Map();

// Execute actual service code in Node tests without changing the production
// bundler. Bindings are passed explicitly by each test; no remote runtime starts.
export function loadTs(relativePath) {
  const filename = path.resolve(root, relativePath);
  if (cache.has(filename)) return cache.get(filename).exports;
  const module = { exports: {} };
  cache.set(filename, module);
  const require = createRequire(filename);
  const localRequire = (name) => {
    if (name.startsWith(".")) {
      const resolved = path.resolve(path.dirname(filename), name);
      if (fs.existsSync(`${resolved}.ts`)) return loadTs(`${resolved}.ts`);
      if (resolved.endsWith(".ts")) return loadTs(resolved);
    }
    if (name === "@beedle/shared" || name === "@beedle/prompts") {
      return loadTs(path.resolve(root, "../../packages", name.split("/")[1], "src/index.ts"));
    }
    return require(name);
  };
  const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, esModuleInterop: true },
    fileName: filename
  }).outputText;
  new Function("require", "module", "exports", "__filename", "__dirname", code)(localRequire, module, module.exports, filename, path.dirname(filename));
  return module.exports;
}
