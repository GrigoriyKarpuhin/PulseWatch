package projector

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"pulsewatch/internal/domain"
	"pulsewatch/internal/events"
)

// Run builds the incident projection from check events. The event ID and
// monitor timestamp make retries and delayed messages safe to process.
func Run(ctx context.Context, conn *sql.DB) error {
	return events.Consume(ctx, conn, events.CheckResults, "pulsewatch-incidents-v2", func(ctx context.Context, data []byte) error {
		var result domain.CheckResult
		if err := events.Decode(data, &result); err != nil {
			return err
		}
		if result.ID == "" || result.MonitorID <= 0 || result.CheckedAt.IsZero() {
			return &events.PoisonError{Err: errors.New("invalid check result fields")}
		}
		return apply(ctx, conn, result)
	})
}

func apply(ctx context.Context, conn *sql.DB, result domain.CheckResult) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var inserted string
	err = tx.QueryRowContext(ctx, `INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING RETURNING event_id`, result.ID).Scan(&inserted)
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	var streak int
	var last sql.NullTime
	err = tx.QueryRowContext(ctx, `SELECT failure_streak,last_processed_at FROM incident_state WHERE monitor_id=$1 FOR UPDATE`, result.MonitorID).Scan(&streak, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	if last.Valid && !result.CheckedAt.After(last.Time) {
		return tx.Commit()
	}
	var threshold int
	if err := tx.QueryRowContext(ctx, `SELECT failure_threshold FROM monitors WHERE id=$1`, result.MonitorID).Scan(&threshold); err != nil {
		return err
	}
	var incidentID int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM incidents WHERE monitor_id=$1 AND resolved_at IS NULL`, result.MonitorID).Scan(&incidentID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	next, open, resolve := domain.NextIncidentState(streak, threshold, err == nil, result.Success)
	if _, err := tx.ExecContext(ctx, `UPDATE incident_state SET failure_streak=$1,last_processed_at=$2 WHERE monitor_id=$3`, next, result.CheckedAt, result.MonitorID); err != nil {
		return err
	}
	if open {
		if err := tx.QueryRowContext(ctx, `INSERT INTO incidents(monitor_id,opened_at,reason) VALUES($1,$2,$3) RETURNING id`, result.MonitorID, result.CheckedAt, result.Error).Scan(&incidentID); err != nil {
			return err
		}
		if err := enqueueIncident(ctx, tx, incidentID, result.MonitorID, "incident.opened", result.CheckedAt, result.Error); err != nil {
			return err
		}
	}
	if resolve {
		if _, err := tx.ExecContext(ctx, `UPDATE incidents SET resolved_at=$1 WHERE id=$2`, result.CheckedAt, incidentID); err != nil {
			return err
		}
		if err := enqueueIncident(ctx, tx, incidentID, result.MonitorID, "incident.resolved", result.CheckedAt, "service recovered"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func enqueueIncident(ctx context.Context, tx *sql.Tx, incidentID, monitorID int64, kind string, at time.Time, reason string) error {
	id, err := events.ID()
	if err != nil {
		return err
	}
	value := domain.IncidentEvent{ID: id, IncidentID: incidentID, MonitorID: monitorID, Type: kind, OccurredAt: at, Reason: reason}
	return events.Enqueue(ctx, tx, events.IncidentEvents, id, monitorID, value)
}
