"use client";

import { FormEvent, useEffect, useState } from "react";
import { getTaxonomyConfig, resolveTaxonomyCaseType } from "@/lib/api";

export default function AdminConfigPage() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [config, setConfig] = useState<any>(null);
  const [query, setQuery] = useState("variance");
  const [resolveResult, setResolveResult] = useState<any>(null);

  useEffect(() => {
    getTaxonomyConfig()
      .then((next) => setConfig(next))
      .catch((err) => setError(err instanceof Error ? err.message : "Failed to load taxonomy config"))
      .finally(() => setLoading(false));
  }, []);

  async function onResolve(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    try {
      const next = await resolveTaxonomyCaseType(query);
      setResolveResult(next);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Resolve failed");
    }
  }

  return (
    <main>
      <h1 style={{ marginTop: 0 }}>Config Inspection</h1>
      <p style={{ color: "var(--muted)" }}>Read-only taxonomy/config view for case-type and template scaffolding sanity checks.</p>
      {loading ? <p>Loading config...</p> : null}
      {error ? <p style={{ color: "#8b2a2a" }}>Error: {error}</p> : null}

      {config ? (
        <section className="card" style={{ padding: "1rem" }}>
          <p style={{ margin: 0 }}>
            Version: <strong>{config.config.version}</strong> · Case types: <strong>{config.stats.case_type_count}</strong> · Canonical sections:{" "}
            <strong>{config.stats.canonical_section_count}</strong>
          </p>
          <p style={{ margin: "0.35rem 0 0" }}>
            Default case type: <strong>{config.config.default_case_type_id}</strong>
          </p>
          <h2 style={{ marginBottom: "0.35rem" }}>Configured Case Types</h2>
          {config.config.case_types.map((row: any) => (
            <article key={row.id} style={{ borderTop: "1px solid var(--border)", paddingTop: "0.6rem", marginTop: "0.6rem" }}>
              <p style={{ margin: 0 }}>
                <strong>{row.id}</strong> ({row.label})
              </p>
              <p style={{ margin: "0.2rem 0", color: "var(--muted)" }}>{row.description}</p>
            </article>
          ))}
        </section>
      ) : null}

      <section className="card" style={{ padding: "1rem", marginTop: "1rem" }}>
        <h2 style={{ marginTop: 0 }}>Fallback Resolver</h2>
        <form onSubmit={onResolve} style={{ display: "flex", gap: "0.6rem" }}>
          <input value={query} onChange={(e) => setQuery(e.target.value)} style={{ flex: 1, padding: "0.55rem", borderRadius: "8px", border: "1px solid var(--border)" }} />
          <button type="submit" style={{ border: 0, borderRadius: "8px", padding: "0.55rem 0.85rem", background: "var(--accent)", color: "#fff", cursor: "pointer" }}>
            Resolve
          </button>
        </form>
        {resolveResult ? (
          <p style={{ marginTop: "0.6rem" }}>
            {resolveResult.requested_case_type} → <strong>{resolveResult.resolved_case_type_id}</strong> ({resolveResult.match_type})
          </p>
        ) : null}
      </section>
    </main>
  );
}
