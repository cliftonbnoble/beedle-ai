import { StatusPill } from "./status-pill";

export default function AddDecisionPlaceholder() {
  return (
    <main className="page-shell page-add-decision">
      <section className="page-hero">
        <div>
          <p className="page-eyebrow">Manual Intake</p>
          <h2 className="page-title">Add Decision</h2>
          <p className="page-copy">
            This is the front-end shell for the manual decision intake workflow. We are keeping the logic for a later pass.
          </p>
        </div>
        <StatusPill label="Decision upload" />
      </section>

      <section className="add-decision-grid">
        <article className="hero-card add-decision-dropzone">
          <div className="upload-format-row" aria-hidden="true">
            <div className="upload-format-card">
              <strong>.DOC</strong>
            </div>
            <div className="upload-format-card">
              <strong>.DOCX</strong>
            </div>
          </div>
          <h3>Ingest New Decision</h3>
          <p>
            Drag-and-drop and automation come later. For now this page establishes the production UI shape for manual uploads.
          </p>
          <div className="upload-zone">
            <div className="upload-zone__icon">UP</div>
            <strong>Click to select files or drop them here</strong>
            <span>Maximum file size and processing logic will be wired in later.</span>
          </div>
          <div className="upload-support-list">
            <span>Encrypted intake</span>
            <span>Metadata extraction ready</span>
            <span>Search indexing ready</span>
          </div>
        </article>

        <aside className="section-card analysis-rail">
          <div>
            <p className="page-eyebrow">Live Analysis</p>
            <h3>Processing timeline</h3>
          </div>
          <div className="timeline-step is-active">
            <strong>Scanning document</strong>
            <p>OCR and structural validation would begin here.</p>
          </div>
          <div className="timeline-step">
            <strong>Extracting metadata</strong>
            <p>Judge, parties, dates, court, and citations would be identified next.</p>
          </div>
          <div className="timeline-step">
            <strong>Preparing search chunks</strong>
            <p>Decision text would be chunked for retrieval and future drafting support.</p>
          </div>
          <div className="analysis-rail__notice">
            This page is intentionally visual only for now, so we can land the product shell before wiring upload logic.
          </div>
        </aside>
      </section>
    </main>
  );
}
