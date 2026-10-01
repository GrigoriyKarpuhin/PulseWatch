CREATE TABLE monitors (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  interval_seconds INTEGER NOT NULL CHECK (interval_seconds BETWEEN 10 AND 3600),
  next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  failure_streak INTEGER NOT NULL DEFAULT 0,
  last_processed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX monitors_due_idx ON monitors(next_check_at);

CREATE TABLE check_results (
  id TEXT PRIMARY KEY,
  monitor_id BIGINT NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
  checked_at TIMESTAMPTZ NOT NULL,
  status_code INTEGER,
  latency_ms INTEGER NOT NULL CHECK(latency_ms >= 0),
  success BOOLEAN NOT NULL,
  error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX checks_monitor_time_idx ON check_results(monitor_id, checked_at DESC);

CREATE TABLE outbox (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE,
  monitor_id BIGINT NOT NULL,
  payload JSONB NOT NULL,
  published_at TIMESTAMPTZ
);
CREATE INDEX outbox_pending_idx ON outbox(id) WHERE published_at IS NULL;

CREATE TABLE processed_events (
  event_id TEXT PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE incidents (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  monitor_id BIGINT NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
  opened_at TIMESTAMPTZ NOT NULL,
  resolved_at TIMESTAMPTZ,
  reason TEXT NOT NULL
);
CREATE UNIQUE INDEX one_open_incident_idx ON incidents(monitor_id) WHERE resolved_at IS NULL;

CREATE TABLE anomalies (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  monitor_id BIGINT NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
  detected_at TIMESTAMPTZ NOT NULL,
  baseline_ms DOUBLE PRECISION NOT NULL,
  current_ms DOUBLE PRECISION NOT NULL,
  UNIQUE(monitor_id, detected_at)
);
