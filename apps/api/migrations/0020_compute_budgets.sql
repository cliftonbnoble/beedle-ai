-- One atomic counter row per application environment and provider work class.
-- Provider attempts are charged before outbound work and are not refunded on
-- failures/unknown acknowledgments. Limits live in reviewed deployment vars.
CREATE TABLE compute_budget_counters (
  scope TEXT NOT NULL,
  category TEXT NOT NULL CHECK (category IN ('llm','embedding','vector_write')),
  minute_bucket INTEGER NOT NULL,
  day_bucket INTEGER NOT NULL,
  minute_used INTEGER NOT NULL CHECK (minute_used >= 0),
  day_used INTEGER NOT NULL CHECK (day_used >= 0),
  updated_at TEXT NOT NULL,
  PRIMARY KEY (scope,category)
);
