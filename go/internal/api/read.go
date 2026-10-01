package api

import (
	"database/sql"
	"net/http"
	"strconv"
	"time"

	"pulsewatch/internal/domain"
)

func (s *server) checks(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}
	limit := 100
	if value := r.URL.Query().Get("limit"); value != "" {
		number, err := strconv.Atoi(value)
		if err != nil || number < 1 || number > 500 {
			respond(w, 400, map[string]string{"error": "limit must be 1-500"})
			return
		}
		limit = number
	}
	rows, err := s.db.QueryContext(r.Context(), `SELECT id,monitor_id,kind,checked_at,COALESCE(status_code,0),latency_ms,success,error FROM check_results WHERE monitor_id=$1 ORDER BY checked_at DESC LIMIT $2`, id, limit)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	result := []domain.CheckResult{}
	for rows.Next() {
		var x domain.CheckResult
		if err := rows.Scan(&x.ID, &x.MonitorID, &x.Kind, &x.CheckedAt, &x.StatusCode, &x.LatencyMS, &x.Success, &x.Error); err != nil {
			dbError(w, err)
			return
		}
		result = append(result, x)
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	respond(w, 200, result)
}

func (s *server) stats(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}
	var exists bool
	if err := s.db.QueryRowContext(r.Context(), `SELECT EXISTS(SELECT 1 FROM monitors WHERE id=$1)`, id).Scan(&exists); err != nil {
		dbError(w, err)
		return
	}
	if !exists {
		respond(w, 404, map[string]string{"error": "monitor not found"})
		return
	}
	var total, successful int64
	var average, p95 float64
	err := s.db.QueryRowContext(r.Context(), `SELECT count(*),count(*) FILTER (WHERE success),COALESCE(avg(latency_ms) FILTER (WHERE success),0),COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE success),0) FROM check_results WHERE monitor_id=$1 AND checked_at>=now()-interval '24 hours'`, id).Scan(&total, &successful, &average, &p95)
	if err != nil {
		dbError(w, err)
		return
	}
	availability := 0.0
	if total > 0 {
		availability = float64(successful) / float64(total) * 100
	}
	respond(w, 200, map[string]any{"monitor_id": id, "window_hours": 24, "checks": total, "successful": successful, "availability_percent": availability, "average_latency_ms": average, "p95_latency_ms": p95})
}

func (s *server) incidents(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT id,monitor_id,opened_at,resolved_at,reason FROM incidents ORDER BY opened_at DESC LIMIT 100`)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		var id, monitorID int64
		var opened time.Time
		var resolved sql.NullTime
		var reason string
		if err := rows.Scan(&id, &monitorID, &opened, &resolved, &reason); err != nil {
			dbError(w, err)
			return
		}
		var resolvedAt any
		if resolved.Valid {
			resolvedAt = resolved.Time
		}
		result = append(result, map[string]any{"id": id, "monitor_id": monitorID, "opened_at": opened, "resolved_at": resolvedAt, "reason": reason})
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	respond(w, 200, result)
}

func (s *server) anomalies(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT id,monitor_id,detected_at,baseline_ms,current_ms FROM anomalies ORDER BY detected_at DESC LIMIT 100`)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		var id, monitorID int64
		var at time.Time
		var baseline, current float64
		if err := rows.Scan(&id, &monitorID, &at, &baseline, &current); err != nil {
			dbError(w, err)
			return
		}
		result = append(result, map[string]any{"id": id, "monitor_id": monitorID, "detected_at": at, "baseline_ms": baseline, "current_ms": current})
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	respond(w, 200, result)
}

func (s *server) deliveries(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT id,event_id,incident_id,monitor_id,event_type,attempts,delivered_at,dead_letter_at,last_error FROM notification_deliveries ORDER BY id DESC LIMIT 100`)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		var id, incidentID, monitorID int64
		var eventID, kind, lastError string
		var attempts int
		var delivered, dead sql.NullTime
		if err := rows.Scan(&id, &eventID, &incidentID, &monitorID, &kind, &attempts, &delivered, &dead, &lastError); err != nil {
			dbError(w, err)
			return
		}
		var deliveredAt, deadAt any
		if delivered.Valid {
			deliveredAt = delivered.Time
		}
		if dead.Valid {
			deadAt = dead.Time
		}
		result = append(result, map[string]any{"id": id, "event_id": eventID, "incident_id": incidentID, "monitor_id": monitorID, "event_type": kind, "attempts": attempts, "delivered_at": deliveredAt, "dead_letter_at": deadAt, "last_error": lastError})
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	respond(w, 200, result)
}

func (s *server) deadLetters(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT id,topic,partition_number,offset_number,reason,created_at FROM dead_letters ORDER BY id DESC LIMIT 100`)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		var id, offset int64
		var topic, reason string
		var partition int
		var at time.Time
		if err := rows.Scan(&id, &topic, &partition, &offset, &reason, &at); err != nil {
			dbError(w, err)
			return
		}
		result = append(result, map[string]any{"id": id, "topic": topic, "partition": partition, "offset": offset, "reason": reason, "created_at": at})
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	respond(w, 200, result)
}
