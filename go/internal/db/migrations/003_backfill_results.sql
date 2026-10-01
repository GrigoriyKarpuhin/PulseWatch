-- Probe jobs and check results used the same outbox ID briefly during the
-- service split. Restore check events for results that were persisted then.
INSERT INTO outbox(event_id,topic,monitor_id,payload)
SELECT md5(c.id || ':check-results'), 'check-results', c.monitor_id,
       jsonb_build_object(
         'id', c.id,
         'monitor_id', c.monitor_id,
         'kind', c.kind,
         'checked_at', c.checked_at,
         'status_code', COALESCE(c.status_code,0),
         'latency_ms', c.latency_ms,
         'success', c.success,
         'error', c.error
       )
FROM check_results c
WHERE NOT EXISTS (
  SELECT 1 FROM outbox o
  WHERE o.topic = 'check-results' AND o.payload->>'id' = c.id
)
ON CONFLICT(event_id) DO NOTHING;
