import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

const dashboardPath = path.resolve(process.cwd(), "src/components/dashboard-home.tsx");
const statusPillPath = path.resolve(process.cwd(), "src/components/status-pill.tsx");
const addDecisionPath = path.resolve(process.cwd(), "src/components/add-decision-placeholder.tsx");

test("dashboard avoids fake model readiness and placeholder upload status claims", async () => {
  const dashboard = await fs.readFile(dashboardPath, "utf8");
  const statusPill = await fs.readFile(statusPillPath, "utf8");
  const addDecision = await fs.readFile(addDecisionPath, "utf8");

  assert.doesNotMatch(dashboard, /Claude Opus|Gemini Pro|GPT-4O Legal/);
  assert.doesNotMatch(dashboard, /AI Intelligence|Model readiness|Systems operational|dashboard-chart|model-row|status-badge/);
  assert.match(dashboard, /Operational Views/);
  assert.match(dashboard, /href: "\/admin\/retrieval"/);
  assert.match(dashboard, /href: "\/admin\/ingestion"/);
  assert.match(dashboard, /href: "\/admin\/references"/);
  assert.match(statusPill, /stateLabel = "READY"/);
  assert.doesNotMatch(statusPill, /stateLabel = "ONLINE"/);
  assert.match(addDecision, /stateLabel="PLANNED"/);
});
