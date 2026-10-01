ALTER TABLE monitors
  ADD COLUMN kind TEXT NOT NULL DEFAULT 'http' CHECK (kind IN ('http', 'tcp')),
  ADD COLUMN timeout_ms INTEGER NOT NULL DEFAULT 5000 CHECK (timeout_ms BETWEEN 100 AND 30000),
  ADD COLUMN failure_threshold INTEGER NOT NULL DEFAULT 2 CHECK (failure_threshold BETWEEN 1 AND 10),
  ADD COLUMN expected_status INTEGER NOT NULL DEFAULT 0 CHECK (expected_status BETWEEN 0 AND 599),
  ADD COLUMN enabled BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE check_results ADD COLUMN kind TEXT NOT NULL DEFAULT 'http';
ALTER TABLE outbox ADD COLUMN topic TEXT NOT NULL DEFAULT 'check-results';

CREATE TABLE incident_state (
  monitor_id BIGINT PRIMARY KEY REFERENCES monitors(id) ON DELETE CASCADE,
  failure_streak INTEGER NOT NULL DEFAULT 0,
  last_processed_at TIMESTAMPTZ
);
INSERT INTO incident_state(monitor_id, failure_streak, last_processed_at)
SELECT id, failure_streak, last_processed_at FROM monitors;

CREATE TABLE notification_deliveries (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE,
  incident_id BIGINT NOT NULL,
  monitor_id BIGINT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('incident.opened','incident.resolved')),
  payload JSONB NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  delivered_at TIMESTAMPTZ,
  dead_letter_at TIMESTAMPTZ,
  last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX deliveries_pending_idx ON notification_deliveries(next_attempt_at)
  WHERE delivered_at IS NULL AND dead_letter_at IS NULL;

CREATE TABLE dead_letters (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  topic TEXT NOT NULL,
  partition_number INTEGER NOT NULL,
  offset_number BIGINT NOT NULL,
  payload BYTEA NOT NULL,
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(topic, partition_number, offset_number)
);
