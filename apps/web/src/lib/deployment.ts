type Configuration = { environment?: string; apiUrl?: string; pagesBranch?: string; productionBranch?: string; onPages?: string };
const productionApi = "https://beedle-api.clifton23.workers.dev";

export function resolveApiBase(config: Configuration): string {
  const { environment, apiUrl } = config;
  if (!["local", "preview", "production"].includes(environment || "")) {
    throw new Error("Set NEXT_PUBLIC_APP_ENV to local, preview or production explicitly");
  }
  if (config.onPages && (!config.productionBranch || !config.pagesBranch ||
      environment !== (config.pagesBranch === config.productionBranch ? "production" : "preview"))) {
    throw new Error("Pages branch and NEXT_PUBLIC_APP_ENV disagree; set BEEDLE_PRODUCTION_BRANCH");
  }
  const url = new URL(apiUrl || (environment === "local" ? "http://127.0.0.1:8787" : "https://missing.invalid"));
  if (url.hostname.endsWith(".")) throw new Error("API URL must use a canonical hostname");
  if (url.username || url.password || url.search || url.hash || url.pathname !== "/") throw new Error("API URL must be an origin without credentials");
  const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (environment === "local") {
    if (!loopback || !["http:", "https:"].includes(url.protocol)) throw new Error("Local web must use a loopback API");
  } else {
    if (!apiUrl || loopback || url.protocol !== "https:" || url.hostname.endsWith(".invalid")) throw new Error("An explicit HTTPS API origin is required");
    if (environment === "preview" && url.origin === productionApi) throw new Error("Preview cannot use the production API");
    if (environment === "production" && url.origin !== productionApi) throw new Error("Production API origin does not match the reviewed deployment");
  }
  return url.origin;
}
