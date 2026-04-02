import fs from "node:fs/promises";
import path from "node:path";

const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const fixturesDir = path.resolve(process.cwd(), "fixtures");

async function toBase64(filePath) {
  const bytes = await fs.readFile(filePath);
  return bytes.toString("base64");
}

async function postJson(url, payload, options = {}) {
  const { allowFailure = false } = options;
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  if (!response.ok && !allowFailure) {
    throw new Error(`${response.status} ${response.statusText}: ${JSON.stringify(body)}`);
  }
  return { status: response.status, body };
}

async function ingestDecisionPass() {
  const bytesBase64 = await toBase64(path.join(fixturesDir, "decision_pass.docx.txt"));
  const response = await postJson(`${apiBase}/ingest/decision`, {
    jurisdiction: "City of Beedle",
    title: "Demo Decision (QC Pass)",
    citation: "BEE-DEC-2026-001",
    decisionDate: "2026-03-01",
    sourceFile: {
      filename: "decision_pass.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64
    }
  });
  return response.body;
}

async function ingestDecisionFail() {
  const bytesBase64 = await toBase64(path.join(fixturesDir, "decision_fail.docx.txt"));
  const response = await postJson(`${apiBase}/ingest/decision`, {
    jurisdiction: "City of Beedle",
    title: "Demo Decision (QC Fail)",
    citation: "BEE-DEC-2026-002",
    decisionDate: "2026-03-02",
    sourceFile: {
      filename: "decision_fail.docx",
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytesBase64
    }
  });
  return response.body;
}

async function ingestLaw() {
  const bytesBase64 = await toBase64(path.join(fixturesDir, "law_sample.pdf"));
  const response = await postJson(`${apiBase}/ingest/law`, {
    jurisdiction: "City of Beedle",
    title: "Demo Law (PDF)",
    citation: "BEE-LAW-2026-010",
    decisionDate: "2026-03-03",
    sourceFile: {
      filename: "law_sample.pdf",
      mimeType: "application/pdf",
      bytesBase64
    }
  });
  return response.body;
}

async function approveDecision(documentId) {
  const response = await postJson(`${apiBase}/decisions/${documentId}/approve`, {}, { allowFailure: true });
  return { status: response.status, ...response.body };
}

async function confirmDecisionMetadata(documentId) {
  const response = await postJson(`${apiBase}/admin/ingestion/documents/${documentId}/metadata`, {
    index_codes: ["IC-104"],
    rules_sections: ["Rule 3.1"],
    ordinance_sections: ["Ordinance 77-19"],
    confirm_required_metadata: true
  });
  return response.body;
}

async function runSearch(query, approvedOnly = true) {
  const response = await postJson(`${apiBase}/search`, {
    query,
    limit: 10,
    filters: {
      approvedOnly,
      jurisdiction: "City of Beedle"
    }
  });
  return response.body;
}

async function main() {
  console.log(`Seeding demo documents into ${apiBase}`);

  const passDecision = await ingestDecisionPass();
  const failDecision = await ingestDecisionFail();
  const law = await ingestLaw();
  const metadataConfirm = await confirmDecisionMetadata(passDecision.documentId);

  const passApproval = await approveDecision(passDecision.documentId);
  const failApproval = await approveDecision(failDecision.documentId);

  const approvedSearch = await runSearch("variance", true);
  const broadSearch = await runSearch("Ordinance", false);

  console.log("\nSeed results:");
  console.log(JSON.stringify({ passDecision, failDecision, law }, null, 2));

  console.log("\nApproval results:");
  console.log(JSON.stringify({ metadataConfirmed: metadataConfirm.qcRequiredConfirmed, passApproval, failApproval }, null, 2));

  console.log("\nSearch checks:");
  console.log(JSON.stringify({ approvedSearch, broadSearch }, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
