"use client";

import { FormEvent, useState } from "react";
import { type SearchDebugResponse } from "@beedle/shared";
import { runRetrievalDebug } from "@/lib/api";

export default function RetrievalDebugPage() {
  const [query, setQuery] = useState("variance");
  const [queryType, setQueryType] = useState<"keyword" | "exact_phrase" | "citation_lookup" | "party_name" | "index_code" | "rules_ordinance">("keyword");
  const [indexCode, setIndexCode] = useState("");
  const [rulesSection, setRulesSection] = useState("");
  const [ordinanceSection, setOrdinanceSection] = useState("");
  const [partyName, setPartyName] = useState("");
  const [result, setResult] = useState<SearchDebugResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const next = await runRetrievalDebug({
        query,
        queryType,
        limit: 20,
        offset: 0,
        snippetMaxLength: 260,
        corpusMode: "trusted_only",
        filters: {
          approvedOnly: false,
          indexCode: indexCode || undefined,
          rulesSection: rulesSection || undefined,
          ordinanceSection: ordinanceSection || undefined,
          partyName: partyName || undefined
        }
      });
      setResult(next);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to run retrieval debug");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main>
      <h1 style={{ marginTop: 0 }}>Retrieval Diagnostics</h1>
      <p style={{ color: "var(--muted)" }}>
        Dev-only ranking inspection. Shows lexical/vector and boost signals with transparent rerank reasons.
      </p>

      <section className="card" style={{ padding: "1rem" }}>
        <form onSubmit={onSubmit} style={{ display: "grid", gap: "0.6rem" }}>
          <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Query" style={{ padding: "0.55rem", borderRadius: "8px", border: "1px solid var(--border)" }} />
          <select value={queryType} onChange={(e) => setQueryType(e.target.value as typeof queryType)} style={{ padding: "0.55rem", borderRadius: "8px", border: "1px solid var(--border)" }}>
            <option value="keyword">keyword</option>
            <option value="exact_phrase">exact_phrase</option>
            <option value="citation_lookup">citation_lookup</option>
            <option value="party_name">party_name</option>
            <option value="index_code">index_code</option>
            <option value="rules_ordinance">rules_ordinance</option>
          </select>
          <input value={indexCode} onChange={(e) => setIndexCode(e.target.value)} placeholder="Index Code filter" style={{ padding: "0.55rem", borderRadius: "8px", border: "1px solid var(--border)" }} />
          <input value={rulesSection} onChange={(e) => setRulesSection(e.target.value)} placeholder="Rules section filter" style={{ padding: "0.55rem", borderRadius: "8px", border: "1px solid var(--border)" }} />
          <input value={ordinanceSection} onChange={(e) => setOrdinanceSection(e.target.value)} placeholder="Ordinance section filter" style={{ padding: "0.55rem", borderRadius: "8px", border: "1px solid var(--border)" }} />
          <input value={partyName} onChange={(e) => setPartyName(e.target.value)} placeholder="Party name filter" style={{ padding: "0.55rem", borderRadius: "8px", border: "1px solid var(--border)" }} />
          <button type="submit" disabled={loading} style={{ border: 0, borderRadius: "8px", padding: "0.65rem", background: "var(--accent)", color: "#fff", cursor: "pointer" }}>
            {loading ? "Running..." : "Run Diagnostics"}
          </button>
        </form>
      </section>

      {error ? <p style={{ color: "#8b2a2a" }}>{error}</p> : null}

      {result ? (
        <section className="card" style={{ marginTop: "1rem", padding: "1rem" }}>
          <h2 style={{ marginTop: 0 }}>
            {result.total} result{result.total === 1 ? "" : "s"}
          </h2>
          {result.results.map((row) => (
            <article key={row.chunkId} style={{ borderTop: "1px solid var(--border)", paddingTop: "0.65rem", marginTop: "0.65rem" }}>
              <p style={{ margin: 0 }}>
                <strong>{row.title}</strong> ({row.citation})
              </p>
              <p style={{ margin: "0.2rem 0", color: "var(--muted)" }}>
                score {row.diagnostics.rerankScore.toFixed(3)} · lexical {row.diagnostics.lexicalScore.toFixed(3)} · vector {row.diagnostics.vectorScore.toFixed(3)}
              </p>
              <p style={{ margin: "0.2rem 0", fontSize: "0.86rem", color: "var(--muted)" }}>
                decisionId <code>{row.documentId}</code> · tier <strong>{row.corpusTier}</strong> · chunkType{" "}
                <strong>{row.chunkType || row.sectionLabel}</strong>
              </p>
              <p style={{ margin: "0.2rem 0", fontSize: "0.88rem", color: "var(--muted)" }}>
                boosts: phrase {row.diagnostics.exactPhraseBoost.toFixed(2)} · citation {row.diagnostics.citationBoost.toFixed(2)} · metadata {row.diagnostics.metadataBoost.toFixed(2)} · section {row.diagnostics.sectionBoost.toFixed(2)} · party {row.diagnostics.partyNameBoost.toFixed(2)}
              </p>
              <p style={{ margin: "0.2rem 0" }}>{row.snippet}</p>
              <p style={{ margin: 0, fontSize: "0.86rem" }}>
                why: {row.diagnostics.why.join(", ") || "(no explicit boosts)"}
              </p>
              <p style={{ margin: 0, fontSize: "0.86rem" }}>
                anchor <strong>{row.citationAnchor}</strong> · section <strong>{row.sectionLabel}</strong>
              </p>
              <a href={row.sourceLink} target="_blank" rel="noreferrer">Open source</a>
            </article>
          ))}
        </section>
      ) : null}
    </main>
  );
}
