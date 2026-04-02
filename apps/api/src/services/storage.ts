import type { Env } from "../lib/types";

export async function storeSourceFile(env: Env, key: string, bytes: Uint8Array, mimeType: string) {
  await env.SOURCE_BUCKET.put(key, bytes, {
    httpMetadata: {
      contentType: mimeType
    }
  });
}

export function sourceLink(env: Env, key: string) {
  return `${env.R2_PUBLIC_BASE_URL.replace(/\/$/, "")}/${key}`;
}

export function effectiveSourceLink(env: Env, documentId: string, persistedLink: string) {
  if (persistedLink.includes("example.invalid")) {
    const base = (env.SOURCE_PROXY_BASE_URL || "http://127.0.0.1:8787").replace(/\/$/, "");
    return `${base}/source/${documentId}`;
  }
  return persistedLink;
}
