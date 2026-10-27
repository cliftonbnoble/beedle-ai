import Link from "next/link";
import { StatusPill } from "./status-pill";

export default function AddDecisionPlaceholder() {
  return (
    <main className="page-shell page-add-decision">
      <section className="page-hero">
        <div>
          <p className="page-eyebrow">Manual Intake</p>
          <h2 className="page-title">Add Decision</h2>
          <p className="page-copy">Browser uploads are planned and are not available yet.</p>
        </div>
        <StatusPill label="Decision upload" stateLabel="PLANNED" />
      </section>

      <section className="section-card" aria-labelledby="intake-planned-title">
        <h3 id="intake-planned-title">Browser intake is not available</h3>
        <p>This page cannot select, receive or upload files. No document is being scanned, processed or added to search here.</p>
        <p>For new documents, contact your corpus operator to use the documented ingestion workflow. That workflow requires source validation and review before publication.</p>
        <p>Operators can <Link href="/admin/ingestion">review documents already ingested</Link> in the reviewer queue.</p>
      </section>
    </main>
  );
}
