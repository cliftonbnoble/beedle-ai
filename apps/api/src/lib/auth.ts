import { authLoginRequestSchema } from "@beedle/shared";
import { json, readJson } from "./http";
import type { Env } from "./types";

export type AuthContext = {
  username: string;
  csrfToken: string;
};

const DEFAULT_SESSION_TTL_SECONDS = 8 * 60 * 60;
const DEFAULT_COOKIE_NAME = "beedle_session";
const PASSWORD_HASH_ALGORITHM = "pbkdf2_sha256";
const MIN_PASSWORD_HASH_ITERATIONS = 100_000;

type AuthConfig = {
  username: string;
  passwordHash: string;
  sessionSecret: string;
  sessionTtlSeconds: number;
  cookieName: string;
  cookieSecure: boolean;
};

type SessionPayload = {
  v: 1;
  sub: string;
  iat: number;
  exp: number;
  csrf: string;
};

type AuthResult = { ok: true; user: AuthContext | null } | { ok: false; response: Response };

export function isPublicAuthPath(path: string, method: string): boolean {
  if (method === "OPTIONS") return true;
  if (path === "/health") return true;
  if (path === "/auth/session" && method === "GET") return true;
  if (path === "/auth/login" && method === "POST") return true;
  if (path === "/auth/logout" && method === "POST") return true;
  return false;
}

export async function authorizeRequest(request: Request, env: Env): Promise<AuthResult> {
  const { pathname } = new URL(request.url);
  if (isPublicAuthPath(pathname, request.method)) return { ok: true, user: null };

  const config = authConfig(env);
  if (!config) return { ok: false, response: authUnavailableResponse() };

  const payload = await readSessionCookie(request, config);
  if (!payload) return { ok: false, response: unauthorizedResponse() };

  if (requiresCsrf(request.method)) {
    const actual = request.headers.get("x-beedle-csrf") || "";
    if (!constantTimeStringEqual(actual, payload.csrf)) {
      return { ok: false, response: json({ error: "Invalid CSRF token" }, { status: 403 }) };
    }
  }

  return { ok: true, user: { username: payload.sub, csrfToken: payload.csrf } };
}

export async function handleAuthSession(request: Request, env: Env): Promise<Response> {
  const config = authConfig(env);
  if (!config) return authUnavailableResponse();

  const payload = await readSessionCookie(request, config);
  if (!payload) return unauthorizedResponse();

  return json({
    authenticated: true,
    username: payload.sub,
    csrfToken: payload.csrf,
    expiresAt: new Date(payload.exp * 1000).toISOString()
  });
}

export async function handleAuthLogin(request: Request, env: Env): Promise<Response> {
  const config = authConfig(env);
  if (!config) return authUnavailableResponse();

  const limited = await enforceAuthAttemptLimit(request, env);
  if (limited) return limited;

  const body = authLoginRequestSchema.parse(await readJson(request, { maxBytes: 4096 }));
  const usernameMatches = constantTimeStringEqual(body.username, config.username);
  const passwordMatches = await verifyPassword(body.password, config.passwordHash);
  if (!usernameMatches || !passwordMatches) return unauthorizedResponse();

  const session = await createSession(config, body.username);
  const headers = new Headers();
  headers.set("set-cookie", sessionCookie(config, session.token));
  return json(
    {
      authenticated: true,
      username: body.username,
      csrfToken: session.csrfToken,
      expiresAt: new Date(session.expiresAt * 1000).toISOString()
    },
    { headers }
  );
}

export function handleAuthLogout(env: Env): Response {
  const config = authConfig(env);
  if (!config) return authUnavailableResponse();
  return json(
    { authenticated: false },
    {
      headers: {
        "set-cookie": expiredSessionCookie(config)
      }
    }
  );
}

export function authActorKey(request: Request, user: AuthContext | null, bucket: string): string {
  const actor = user?.username ? `user:${user.username}` : `ip:${request.headers.get("cf-connecting-ip") || "unknown-client"}`;
  return `${bucket}:${actor}`;
}

function authConfig(env: Env): AuthConfig | null {
  const username = env.AUTH_USERNAME?.trim();
  const passwordHash = env.AUTH_PASSWORD_HASH?.trim();
  const sessionSecret = env.AUTH_SESSION_SECRET?.trim();
  if (!username || !passwordHash || !sessionSecret) return null;

  return {
    username,
    passwordHash,
    sessionSecret,
    sessionTtlSeconds: parsePositiveInt(env.AUTH_SESSION_TTL_SECONDS) ?? DEFAULT_SESSION_TTL_SECONDS,
    cookieName: env.AUTH_COOKIE_NAME?.trim() || DEFAULT_COOKIE_NAME,
    cookieSecure: env.AUTH_COOKIE_SECURE !== "false"
  };
}

async function enforceAuthAttemptLimit(request: Request, env: Env): Promise<Response | null> {
  try {
    const key = request.headers.get("cf-connecting-ip") || "unknown-client";
    const outcome = await env.AUTH_RATE_LIMIT.limit({ key: `login:${key}` });
    if (outcome.success) return null;
    return json(
      { error: "Too many login attempts. Please retry later." },
      { status: 429, headers: { "retry-after": "60" } }
    );
  } catch (error) {
    console.error("Auth rate limiter unavailable", { error });
    return json(
      { error: "Authentication is temporarily unavailable. Please retry later." },
      { status: 503, headers: { "retry-after": "60" } }
    );
  }
}

function authUnavailableResponse(): Response {
  return json({ error: "Authentication is not configured" }, { status: 503 });
}

function unauthorizedResponse(): Response {
  return json({ error: "Authentication required" }, { status: 401 });
}

function requiresCsrf(method: string): boolean {
  return !["GET", "HEAD", "OPTIONS"].includes(method.toUpperCase());
}

async function readSessionCookie(request: Request, config: AuthConfig): Promise<SessionPayload | null> {
  const token = parseCookies(request.headers.get("cookie"))[config.cookieName];
  if (!token) return null;
  const payload = await verifySessionToken(token, config.sessionSecret);
  if (!payload || payload.v !== 1 || payload.sub !== config.username) return null;
  if (payload.exp <= Math.floor(Date.now() / 1000)) return null;
  return payload;
}

async function createSession(config: AuthConfig, username: string): Promise<{ token: string; csrfToken: string; expiresAt: number }> {
  const now = Math.floor(Date.now() / 1000);
  const csrfToken = randomToken(32);
  const payload: SessionPayload = {
    v: 1,
    sub: username,
    iat: now,
    exp: now + config.sessionTtlSeconds,
    csrf: csrfToken
  };
  const payloadPart = base64UrlEncode(new TextEncoder().encode(JSON.stringify(payload)));
  const signature = await sign(payloadPart, config.sessionSecret);
  return {
    token: `${payloadPart}.${signature}`,
    csrfToken,
    expiresAt: payload.exp
  };
}

async function verifySessionToken(token: string, secret: string): Promise<SessionPayload | null> {
  const [payloadPart, signature] = token.split(".");
  if (!payloadPart || !signature) return null;
  const expected = await sign(payloadPart, secret);
  if (!constantTimeStringEqual(signature, expected)) return null;
  try {
    return JSON.parse(new TextDecoder().decode(base64UrlDecode(payloadPart))) as SessionPayload;
  } catch {
    return null;
  }
}

async function sign(value: string, secret: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(value));
  return base64UrlEncode(new Uint8Array(signature));
}

async function verifyPassword(password: string, encodedHash: string): Promise<boolean> {
  const parts = encodedHash.split("$");
  if (parts.length !== 4 || parts[0] !== PASSWORD_HASH_ALGORITHM) return false;
  const [, iterationPart, saltPart, hashPart] = parts as [string, string, string, string];
  const iterations = Number(iterationPart);
  if (!Number.isInteger(iterations) || iterations < MIN_PASSWORD_HASH_ITERATIONS) return false;

  const salt = base64UrlDecode(saltPart);
  const expected = base64UrlDecode(hashPart);
  const actual = await pbkdf2(password, salt, iterations, expected.byteLength);
  return constantTimeBytesEqual(actual, expected);
}

async function pbkdf2(password: string, salt: Uint8Array<ArrayBuffer>, iterations: number, length: number): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(password), "PBKDF2", false, ["deriveBits"]);
  const bits = await crypto.subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt, iterations }, key, length * 8);
  return new Uint8Array(bits);
}

function sessionCookie(config: AuthConfig, token: string): string {
  return [
    `${config.cookieName}=${token}`,
    "Path=/",
    "HttpOnly",
    config.cookieSecure ? "Secure" : "",
    `SameSite=${config.cookieSecure ? "None" : "Lax"}`,
    `Max-Age=${config.sessionTtlSeconds}`
  ]
    .filter(Boolean)
    .join("; ");
}

function expiredSessionCookie(config: AuthConfig): string {
  return [
    `${config.cookieName}=`,
    "Path=/",
    "HttpOnly",
    config.cookieSecure ? "Secure" : "",
    `SameSite=${config.cookieSecure ? "None" : "Lax"}`,
    "Max-Age=0"
  ]
    .filter(Boolean)
    .join("; ");
}

function parseCookies(header: string | null): Record<string, string> {
  const cookies: Record<string, string> = {};
  for (const pair of String(header || "").split(";")) {
    const [rawName, ...rawValue] = pair.trim().split("=");
    if (!rawName) continue;
    cookies[rawName] = rawValue.join("=");
  }
  return cookies;
}

function randomToken(byteLength: number): string {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return base64UrlEncode(bytes);
}

function parsePositiveInt(value: string | undefined): number | null {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function constantTimeStringEqual(a: string, b: string): boolean {
  return constantTimeBytesEqual(new TextEncoder().encode(a), new TextEncoder().encode(b));
}

function constantTimeBytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  let diff = a.byteLength ^ b.byteLength;
  const length = Math.max(a.byteLength, b.byteLength);
  for (let index = 0; index < length; index += 1) {
    diff |= (a[index] || 0) ^ (b[index] || 0);
  }
  return diff === 0;
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecode(value: string): Uint8Array<ArrayBuffer> {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(value.length / 4) * 4, "=");
  const binary = atob(padded);
  const bytes = new Uint8Array(new ArrayBuffer(binary.length));
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
}
