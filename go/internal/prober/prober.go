package prober

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"pulsewatch/internal/domain"
	"pulsewatch/internal/events"
)

func Run(ctx context.Context, conn *sql.DB) error {
	return events.Consume(ctx, conn, events.ProbeJobs, "pulsewatch-probers", func(ctx context.Context, data []byte) error {
		var job domain.ProbeJob
		if err := events.Decode(data, &job); err != nil {
			return err
		}
		if job.ID == "" || job.MonitorID <= 0 || (job.Kind != "http" && job.Kind != "tcp") || job.TimeoutMS < 100 || job.TimeoutMS > 30000 {
			return &events.PoisonError{Err: errors.New("invalid probe job fields")}
		}
		var exists bool
		if err := conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM monitors WHERE id=$1 AND enabled=true)`, job.MonitorID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return nil
		}
		result := Probe(ctx, job)
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var inserted string
		err = tx.QueryRowContext(ctx, `INSERT INTO check_results(id,monitor_id,kind,checked_at,status_code,latency_ms,success,error) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(id) DO NOTHING RETURNING id`, result.ID, result.MonitorID, result.Kind, result.CheckedAt, result.StatusCode, result.LatencyMS, result.Success, result.Error).Scan(&inserted)
		if errors.Is(err, sql.ErrNoRows) {
			return tx.Commit()
		}
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23503" {
				return nil
			}
			return err
		}
		publishID, err := events.ID()
		if err != nil {
			return err
		}
		if err := events.Enqueue(ctx, tx, events.CheckResults, publishID, result.MonitorID, result); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func Probe(ctx context.Context, job domain.ProbeJob) domain.CheckResult {
	start := time.Now()
	result := domain.CheckResult{ID: job.ID, MonitorID: job.MonitorID, Kind: job.Kind, CheckedAt: start.UTC()}
	probeCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutMS)*time.Millisecond)
	defer cancel()
	var err error
	switch job.Kind {
	case "http":
		client := &http.Client{Timeout: time.Duration(job.TimeoutMS) * time.Millisecond, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
		var req *http.Request
		req, err = http.NewRequestWithContext(probeCtx, http.MethodGet, job.URL, nil)
		if err == nil {
			var response *http.Response
			response, err = client.Do(req)
			if response != nil {
				result.StatusCode = response.StatusCode
				_ = response.Body.Close()
			}
		}
		result.Success = err == nil && ((job.ExpectedStatus == 0 && result.StatusCode >= 200 && result.StatusCode < 400) || (job.ExpectedStatus > 0 && result.StatusCode == job.ExpectedStatus))
		if err == nil && !result.Success {
			err = fmt.Errorf("HTTP %d", result.StatusCode)
		}
	case "tcp":
		var parsed *url.URL
		parsed, err = url.Parse(job.URL)
		if err == nil {
			var socket net.Conn
			socket, err = (&net.Dialer{}).DialContext(probeCtx, "tcp", parsed.Host)
			if socket != nil {
				_ = socket.Close()
			}
		}
		result.Success = err == nil
	default:
		err = errors.New("unknown probe kind")
	}
	result.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		result.Error = err.Error()
		if len(result.Error) > 500 {
			result.Error = result.Error[:500]
		}
	}
	return result
}
