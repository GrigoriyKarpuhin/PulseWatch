"""Detect sustained latency increases from persisted successful checks."""

import os
import statistics
import sys
import time

def detect(latencies: list[int]) -> tuple[float, float] | None:
    """Use older checks as baseline and five recent checks as the current window."""
    if len(latencies) < 25:
        return None
    baseline = statistics.median(latencies[-25:-5])
    current = statistics.median(latencies[-5:])
    if current >= max(baseline * 2, baseline + 100):
        return float(baseline), float(current)
    return None


def run_once(conn) -> int:
    detected = 0
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM monitors ORDER BY id")
        monitor_ids = [row[0] for row in cur.fetchall()]
        for monitor_id in monitor_ids:
            cur.execute(
                """SELECT checked_at, latency_ms FROM check_results
                   WHERE monitor_id = %s AND success = true
                   ORDER BY checked_at DESC LIMIT 30""",
                (monitor_id,),
            )
            checks = cur.fetchall()
            result = detect([row[1] for row in reversed(checks)])
            if result is None:
                continue
            baseline, current = result
            cur.execute(
                """INSERT INTO anomalies(monitor_id, detected_at, baseline_ms, current_ms)
                   SELECT %s, %s, %s, %s
                   WHERE NOT EXISTS (
                       SELECT 1 FROM anomalies
                       WHERE monitor_id = %s AND detected_at > now() - interval '30 minutes'
                   ) ON CONFLICT DO NOTHING""",
                (monitor_id, checks[0][0], baseline, current, monitor_id),
            )
            detected += cur.rowcount
    conn.commit()
    return detected


def main() -> None:
    import psycopg

    dsn = os.environ.get(
        "DATABASE_URL",
        "postgres://pulsewatch:pulsewatch@localhost:5432/pulsewatch?sslmode=disable",
    )
    once = "--once" in sys.argv
    while True:
        try:
            with psycopg.connect(dsn) as conn:
                print(f"anomalies detected: {run_once(conn)}", flush=True)
        except psycopg.Error as exc:
            print(f"analytics error: {exc}", file=sys.stderr, flush=True)
            if once:
                raise
        if once:
            return
        time.sleep(30)


if __name__ == "__main__":
    main()
