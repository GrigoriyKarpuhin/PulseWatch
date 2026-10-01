package scheduler

import (
	"context"
	"database/sql"
	"log"
	"time"

	"pulsewatch/internal/domain"
	"pulsewatch/internal/events"
)

// Run turns due monitor configurations into durable probe jobs.
func Run(ctx context.Context, conn *sql.DB) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if count, err := schedule(ctx, conn); err != nil && ctx.Err() == nil {
			log.Printf("schedule: %v", err)
		} else if count > 0 {
			log.Printf("scheduled %d checks", count)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func schedule(ctx context.Context, conn *sql.DB) (int, error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,url,kind,timeout_ms,expected_status,next_check_at FROM monitors WHERE enabled=true AND next_check_at<=now() ORDER BY next_check_at,id LIMIT 20 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return 0, err
	}
	var jobs []domain.ProbeJob
	for rows.Next() {
		var job domain.ProbeJob
		if err := rows.Scan(&job.MonitorID, &job.URL, &job.Kind, &job.TimeoutMS, &job.ExpectedStatus, &job.ScheduledAt); err != nil {
			rows.Close()
			return 0, err
		}
		job.ID, err = events.ID()
		if err != nil {
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, job)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, job := range jobs {
		if _, err := tx.ExecContext(ctx, `UPDATE monitors SET next_check_at=now()+interval_seconds*interval '1 second' WHERE id=$1`, job.MonitorID); err != nil {
			return 0, err
		}
		if err := events.Enqueue(ctx, tx, events.ProbeJobs, job.ID, job.MonitorID, job); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(jobs), nil
}
