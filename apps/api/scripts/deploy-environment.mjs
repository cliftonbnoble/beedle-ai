import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

function origin(value) {
  const url=new URL(value);
  if(url.protocol!=="https:" || url.username || url.password || url.pathname!=="/" || url.search || url.hash) {
    throw new Error("Deployment URLs must be HTTPS origins without credentials");
  }
  url.hostname=url.hostname.replace(/\.+$/u,"");
  return url.origin;
}

export function validateDeployment(config, environment) {
  if (!["preview", "production"].includes(environment)) throw new Error("Select preview or production explicitly");
  const selected = config.env[environment];
  if (selected.vars.APP_ENV !== environment || selected.vars.DEPLOYMENT_READY !== "true" ||
      selected.vars.VECTOR_NAMESPACE !== (environment === "preview" ? "beedle-docs-preview" : "beedle-docs") ||
      /REPLACE_|\.invalid/i.test(JSON.stringify({ ...selected, vars: { ...selected.vars, R2_PUBLIC_BASE_URL: undefined } }))) {
    throw new Error("Complete and verify this environment's resources and URLs before deploying");
  }
  const identities = (value) => [value.name, value.vars.VECTOR_NAMESPACE,
    ...value.d1_databases.flatMap(x => [x.database_name, x.database_id]),
    ...value.r2_buckets.map(x => x.bucket_name), ...value.vectorize.map(x => x.index_name),
    ...[...value.queues.producers, ...value.queues.consumers].map(x => x.queue), ...value.ratelimits.map(x => x.namespace_id)];
  if (environment === "preview" && identities(selected).some(x => identities(config.env.production).includes(x))) {
    throw new Error("Preview resources must be independent of production");
  }
  const sourceOrigin=origin(selected.vars.SOURCE_PROXY_BASE_URL);
  const allowedOrigins=selected.vars.CORS_ALLOWED_ORIGINS.split(",").map(x=>origin(x.trim()));
  if (environment === "preview" && (sourceOrigin === origin(config.env.production.vars.SOURCE_PROXY_BASE_URL) ||
      allowedOrigins.some(x => config.env.production.vars.CORS_ALLOWED_ORIGINS.split(",").map(x=>origin(x.trim())).includes(x)))) {
    throw new Error("Preview URLs must be independent of production");
  }
  return selected;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const environment = process.argv[2];
    const flags = process.argv.slice(3);
    if (flags.some(flag => flag !== "--dry-run")) throw new Error("Only --dry-run is accepted after the environment");
    validateDeployment(JSON.parse(readFileSync(new URL("../wrangler.json", import.meta.url), "utf8")), environment);
    const result = spawnSync("pnpm", ["exec", "wrangler", "deploy", "--env", environment, ...flags], { stdio: "inherit" });
    if (result.error) throw result.error;
    process.exitCode = result.status ?? 1;
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
