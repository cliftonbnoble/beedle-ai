"use client";

import { FormEvent, Suspense, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Landmark, LogIn } from "lucide-react";
import { login } from "@/lib/api";

export default function LoginPage() {
  return (
    <Suspense fallback={<LoginForm />}>
      <LoginFormWithParams />
    </Suspense>
  );
}

function LoginFormWithParams() {
  const searchParams = useSearchParams();
  return <LoginForm nextPath={searchParams.get("next") || "/search"} />;
}

function LoginForm({ nextPath = "/search" }: { nextPath?: string }) {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setLoading(true);
    try {
      await login(username.trim(), password);
      router.replace(safeNextPath(nextPath));
      router.refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Sign in failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="login-shell">
      <section className="login-panel" aria-labelledby="login-title">
        <div className="login-panel__brand">
          <div className="app-sidebar__crest" aria-hidden="true">
            <Landmark />
          </div>
          <div>
            <p className="page-eyebrow">Beedle AI Companion</p>
            <h1 id="login-title" className="login-panel__title">
              Sign in
            </h1>
          </div>
        </div>

        <form className="login-form" onSubmit={onSubmit}>
          <label className="login-form__field">
            <span>Username</span>
            <input
              autoComplete="username"
              autoFocus
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              required
            />
          </label>

          <label className="login-form__field">
            <span>Password</span>
            <input
              autoComplete="current-password"
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              required
            />
          </label>

          {error ? <p className="login-form__error">{error}</p> : null}

          <button type="submit" className="login-form__submit" disabled={loading}>
            <LogIn />
            <span>{loading ? "Signing in..." : "Sign in"}</span>
          </button>
        </form>
      </section>
    </main>
  );
}

function safeNextPath(value: string): string {
  if (!value.startsWith("/") || value.startsWith("//")) return "/search";
  return value;
}
