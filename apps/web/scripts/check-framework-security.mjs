import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

// Floors from the upstream October 14, 2026 advisories. This regression guard
// does not replace reviewing new advisories and upgrading supported versions.
const floors = {
  next: { "15.5": 28, "16.4": 1 },
  react: { "19.0": 9, "19.1": 10, "19.2": 9, "19.3": 1 },
  sharp: { "0.35": 5 }
};

export function frameworkSecurityErrors(versions) {
  const errors = [];
  for (const [name, branches] of Object.entries(floors)) {
    const value = versions[name];
    const parts = typeof value === "string" && /^(\d+)\.(\d+)\.(\d+)$/.exec(value);
    const floor = parts ? branches[`${parts[1]}.${parts[2]}`] : undefined;
    if (floor === undefined || Number(parts[3]) < floor) {
      errors.push(`${name}@${value}: below the reviewed security floor or an unreviewed release line`);
    }
  }
  if (versions.react !== versions["react-dom"]) errors.push("react and react-dom must use the same version");
  return errors;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const require = createRequire(import.meta.url);
  const nextRequire = createRequire(require.resolve("next/package.json"));
  const versions = Object.fromEntries(["next", "react", "react-dom"].map((name) => [name, require(`${name}/package.json`).version]));
  versions.sharp = nextRequire("sharp").versions.sharp;
  const errors = frameworkSecurityErrors(versions);
  if (errors.length) {
    console.error(errors.join("\n"));
    process.exitCode = 1;
  } else {
    console.log("Reviewed framework security floors satisfied:", versions);
  }
}
