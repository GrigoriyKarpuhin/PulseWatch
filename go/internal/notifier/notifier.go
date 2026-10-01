package notifier

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"pulsewatch/internal/domain"
	"pulsewatch/internal/events"
)

func Run(ctx context.Context, conn *sql.DB) error {
	endpoint := os.Getenv("NOTIFY_WEBHOOK_URL")
	if endpoint == "" {
		return errors.New("NOTIFY_WEBHOOK_URL is required")
	}
	go deliverLoop(ctx, conn, endpoint)
	return events.Consume(ctx, conn, events.IncidentEvents, "pulsewatch-notifier", func(ctx context.Context, data []byte) error {
		var event domain.IncidentEvent
		if err := events.Decode(data, &event); err != nil {
			return err
		}
		if event.ID == "" || event.IncidentID <= 0 || event.MonitorID <= 0 || (event.Type != "incident.opened" && event.Type != "incident.resolved") {
			return &events.PoisonError{Err: errors.New("invalid incident event fields")}
		}
		_, err := conn.ExecContext(ctx, `INSERT INTO notification_deliveries(event_id,incident_id,monitor_id,event_type,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT(event_id) DO NOTHING`, event.ID, event.IncidentID, event.MonitorID, event.Type, data)
		return err
	})
}

func deliverLoop(ctx context.Context, conn *sql.DB, endpoint string) {
	client := &http.Client{Timeout: 5 * time.Second}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := deliverOne(ctx, conn, client, endpoint); err != nil && ctx.Err() == nil {
			log.Printf("notification delivery: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 5 {
		attempt = 5
	}
	return time.Duration(1<<attempt) * time.Second
}

func deliverOne(ctx context.Context, conn *sql.DB, client *http.Client, endpoint string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id int64
	var eventID string
	var payload []byte
	var attempts int
	err = tx.QueryRowContext(ctx, `SELECT id,event_id,payload,attempts FROM notification_deliveries WHERE delivered_at IS NULL AND dead_letter_at IS NULL AND next_attempt_at<=now() ORDER BY next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id, &eventID, &payload, &attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", eventID)
		var response *http.Response
		response, err = client.Do(req)
		if response != nil {
			_ = response.Body.Close()
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				err = fmt.Errorf("webhook returned HTTP %d", response.StatusCode)
			}
		}
	}
	attempts++
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE notification_deliveries SET attempts=$1,delivered_at=now(),last_error='' WHERE id=$2`, attempts, id)
	} else if attempts >= 5 {
		_, err = tx.ExecContext(ctx, `UPDATE notification_deliveries SET attempts=$1,dead_letter_at=now(),last_error=$2 WHERE id=$3`, attempts, err.Error(), id)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE notification_deliveries SET attempts=$1,next_attempt_at=now()+$2*interval '1 second',last_error=$3 WHERE id=$4`, attempts, int(retryDelay(attempts).Seconds()), err.Error(), id)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}
