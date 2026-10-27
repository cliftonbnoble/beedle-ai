import type { Env } from "./types";

/** Applied to every entrypoint before any storage, queue or provider work. */
export function deploymentEnvironment(env: Env): Env {
  const namespaces = { local: "beedle-docs-local", preview: "beedle-docs-preview", production: "beedle-docs" };
  if (!env.APP_ENV || !Object.hasOwn(namespaces,env.APP_ENV) || env.DEPLOYMENT_READY !== "true" ||
      env.VECTOR_NAMESPACE !== namespaces[env.APP_ENV]) {
    throw new Error("Deployment environment is missing, unconfigured or inconsistent");
  }
  if (env.RELEASE_MAINTENANCE_ONLY === "true" && env.APP_ENV !== "preview") throw new Error("Preparation is restricted to isolated preview");
  if (env.APP_ENV === "local") {
    // Old .dev.vars files can still contain live provider secrets. Local mode must
    // remain offline even when those files or accidental remote bindings exist.
    const local = { ...env };
    delete local.AI;
    delete local.VECTOR_INDEX;
    delete local.LLM_API_KEY;
    delete local.LLM_BASE_URL;
    return local;
  }
  return env;
}
