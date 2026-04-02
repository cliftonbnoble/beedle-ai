"use client";

import { useEffect, useState } from "react";
import { inspectNormalizedReferences } from "@/lib/api";

export default function AdminReferencesPage() {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    inspectNormalizedReferences()
      .then(setData)
      .catch((err) => setError(err instanceof Error ? err.message : "Failed to load references"));
  }, []);

  return (
    <main>
      <h1 style={{ marginTop: 0 }}>Normalized Legal References</h1>
      <p style={{ color: "var(--muted)" }}>Read-only view for Index Codes, Ordinance sections, Rules sections, and unmatched decision references.</p>
      {error ? <p style={{ color: "#8b2a2a" }}>Error: {error}</p> : null}
      {!data ? <p>Loading reference sets...</p> : null}
      {data ? (
        <section className="card" style={{ padding: "1rem" }}>
          <p style={{ margin: "0 0 0.4rem", fontSize: "0.9rem", color: "var(--muted)" }}>
            Sources: {data.source_trace.index_codes || "n/a"} · {data.source_trace.ordinance || "n/a"} · {data.source_trace.rules || "n/a"}
          </p>
          <p style={{ margin: 0 }}>
            Index Codes: <strong>{data.summary.index_code_count}</strong> · Ordinance: <strong>{data.summary.ordinance_section_count}</strong> · Rules:{" "}
            <strong>{data.summary.rules_section_count}</strong> · Crosswalk: <strong>{data.summary.crosswalk_count}</strong> · Unmatched:{" "}
            <strong>{data.summary.unmatched_reference_issue_count}</strong>
          </p>
          {data.coverage_report ? (
            <>
              <p style={{ margin: "0.35rem 0", color: "var(--muted)" }}>
                Ordinance parser: <strong>{data.coverage_report.ordinance?.parser_used || "n/a"}</strong> ({data.coverage_report.ordinance?.parsed_section_count || 0}/
                {data.coverage_report.ordinance?.expected_section_count || 0}, committed {data.coverage_report.ordinance?.committed_section_count || 0}) · Rules parser:{" "}
                <strong>{data.coverage_report.rules?.parser_used || "n/a"}</strong> ({data.coverage_report.rules?.parsed_section_count || 0}/
                {data.coverage_report.rules?.expected_section_count || 0}, committed {data.coverage_report.rules?.committed_section_count || 0}) · Unresolved crosswalk:{" "}
                <strong>{data.coverage_report.crosswalk?.unresolved_links || 0}</strong>
              </p>
              <p style={{ margin: "0.2rem 0", color: "var(--muted)" }}>
                Collision diagnostics: ord dup={data.coverage_report.ordinance?.duplicate_normalized_citations_encountered || 0}, ord merged=
                {data.coverage_report.ordinance?.duplicates_merged || 0}, ord dropped={data.coverage_report.ordinance?.duplicates_dropped || 0} · rules dup=
                {data.coverage_report.rules?.duplicate_normalized_citations_encountered || 0}, rules merged={data.coverage_report.rules?.duplicates_merged || 0}, rules
                dropped={data.coverage_report.rules?.duplicates_dropped || 0}
              </p>
              <p style={{ margin: "0.2rem 0", color: "var(--muted)" }}>
                Sample collisions: ord {(data.coverage_report.ordinance?.sample_collisions || []).slice(0, 3).map((row: any) => row.normalized_citation).join(", ") || "none"} ·
                rules {(data.coverage_report.rules?.sample_collisions || []).slice(0, 3).map((row: any) => row.normalized_citation).join(", ") || "none"}
              </p>
            </>
          ) : null}
          {data.readiness_status ? (
            <p style={{ margin: "0.2rem 0 0.5rem", color: "var(--muted)" }}>
              Readiness: ordinance={data.readiness_status.ordinance_coverage_ok ? "ok" : "low"} · rules={data.readiness_status.rules_coverage_ok ? "ok" : "low"} · crosswalk=
              {data.readiness_status.crosswalk_resolvable ? "ok" : "unresolved"} · consistency={data.readiness_status.counts_consistent ? "ok" : "mismatch"} · recommendation=
              {data.readiness_status.readiness_recommendation || "blocked"}
            </p>
          ) : null}
          <h2 style={{ marginBottom: "0.35rem" }}>Index Code Samples</h2>
          {data.samples.index_codes.slice(0, 10).map((row: any) => (
            <p key={row.code_identifier} style={{ margin: "0.2rem 0" }}>
              <strong>{row.code_identifier}</strong> {row.label ? `- ${row.label}` : ""} {row.reserved ? "(reserved)" : ""}
            </p>
          ))}
          <h2 style={{ marginBottom: "0.35rem", marginTop: "0.8rem" }}>Rules Citation Samples</h2>
          {(data.samples.rules_sections || []).slice(0, 12).map((row: any) => (
            <p key={`${row.citation}-${row.normalized_citation}`} style={{ margin: "0.2rem 0", fontSize: "0.9rem" }}>
              display=<strong>{row.citation}</strong> · bare=<strong>{row.canonical_bare_citation || row.section_number}</strong> · normalized=
              <code>{row.normalized_citation}</code> · bare_norm=<code>{row.normalized_bare_citation || "-"}</code>
            </p>
          ))}
          <h2 style={{ marginBottom: "0.35rem", marginTop: "0.8rem" }}>Recent Unmatched References</h2>
          {data.unmatched_reference_issues.length === 0 ? <p style={{ margin: 0 }}>No unmatched references.</p> : null}
          {data.unmatched_reference_issues.slice(0, 20).map((row: any) => (
            <p key={`${row.document_id}-${row.reference_type}-${row.normalized_value}-${row.created_at}`} style={{ margin: "0.2rem 0", fontSize: "0.9rem" }}>
              <code>{row.reference_type}</code> <strong>{row.raw_value}</strong> ({row.normalized_value}) - {row.message}
            </p>
          ))}
          <h2 style={{ marginBottom: "0.35rem", marginTop: "0.8rem" }}>Unresolved Crosswalk Links</h2>
          {data.unresolved_crosswalks.length === 0 ? <p style={{ margin: 0 }}>No unresolved crosswalk links.</p> : null}
          {data.unresolved_crosswalks.slice(0, 20).map((row: any) => (
            <p key={`${row.index_code || "na"}-${row.ordinance_citation || "na"}-${row.rules_citation || "na"}-${row.reason}`} style={{ margin: "0.2rem 0", fontSize: "0.9rem" }}>
              <strong>{row.index_code || "no-index"}</strong> · ord={row.ordinance_citation || "-"} · rule={row.rules_citation || "-"} · {row.reason}
            </p>
          ))}
          <h2 style={{ marginBottom: "0.35rem", marginTop: "0.8rem" }}>Critical Citation Checks</h2>
          {(data.critical_citation_checks || []).slice(0, 20).map((row: any) => (
            <p key={row.citation} style={{ margin: "0.2rem 0", fontSize: "0.9rem" }}>
              <strong>{row.citation}</strong> · {row.status} ({row.diagnostic || "n/a"}) · ord={row.ordinance_matches.length} · rules={row.rules_matches.length}
            </p>
          ))}
          <h2 style={{ marginBottom: "0.35rem", marginTop: "0.8rem" }}>Critical Exceptions</h2>
          {(data.critical_citation_exceptions || []).slice(0, 20).map((row: any) => (
            <p key={`${row.citation}-${row.classification}`} style={{ margin: "0.2rem 0", fontSize: "0.9rem" }}>
              <strong>{row.citation}</strong> · {row.status} · {row.classification} · {row.recommendation}
            </p>
          ))}
        </section>
      ) : null}
    </main>
  );
}
