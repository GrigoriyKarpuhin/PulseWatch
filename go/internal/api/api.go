package api

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"pulsewatch/internal/domain"
)

type server struct{ db *sql.DB }

func Serve(ctx context.Context, conn *sql.DB) error {
	s := &server{db: conn}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /monitors", s.createMonitor)
	mux.HandleFunc("GET /monitors", s.listMonitors)
	mux.HandleFunc("GET /monitors/{id}", s.getMonitor)
	mux.HandleFunc("PUT /monitors/{id}", s.updateMonitor)
	mux.HandleFunc("DELETE /monitors/{id}", s.deleteMonitor)
	mux.HandleFunc("PATCH /monitors/{id}/enabled", s.setEnabled)
	mux.HandleFunc("POST /monitors/{id}/run", s.runNow)
	mux.HandleFunc("GET /monitors/{id}/checks", s.checks)
	mux.HandleFunc("GET /monitors/{id}/stats", s.stats)
	mux.HandleFunc("GET /incidents", s.incidents)
	mux.HandleFunc("GET /anomalies", s.anomalies)
	mux.HandleFunc("GET /deliveries", s.deliveries)
	mux.HandleFunc("GET /dead-letters", s.deadLetters)
	var handler http.Handler = mux
	if key := os.Getenv("API_KEY"); key != "" {
		handler = auth(key, mux)
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	httpServer := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(closeCtx)
	}()
	log.Printf("API listening on %s", addr)
	err := httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func auth(key string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		given := r.Header.Get("X-API-Key")
		if len(given) != len(key) || subtle.ConstantTimeCompare([]byte(given), []byte(key)) != 1 {
			respond(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func decode(w http.ResponseWriter, r *http.Request, value any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		respond(w, 400, map[string]string{"error": "invalid JSON"})
		return false
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		respond(w, 400, map[string]string{"error": "one JSON object is required"})
		return false
	}
	return true
}

func monitorID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		respond(w, 400, map[string]string{"error": "invalid monitor id"})
		return 0, false
	}
	return id, true
}

func monitorInput(w http.ResponseWriter, r *http.Request) (domain.Monitor, bool) {
	var m domain.Monitor
	if !decode(w, r, &m) {
		return m, false
	}
	if m.Kind == "" {
		m.Kind = "http"
	}
	if m.IntervalSeconds == 0 {
		m.IntervalSeconds = 30
	}
	if m.TimeoutMS == 0 {
		m.TimeoutMS = 5000
	}
	if m.FailureThreshold == 0 {
		m.FailureThreshold = 2
	}
	if err := domain.ValidateMonitor(m); err != nil {
		respond(w, 400, map[string]string{"error": err.Error()})
		return m, false
	}
	return m, true
}

const monitorColumns = `id,name,url,kind,interval_seconds,timeout_ms,failure_threshold,expected_status,enabled,next_check_at`

func scanMonitor(row interface{ Scan(...any) error }) (domain.Monitor, error) {
	var m domain.Monitor
	err := row.Scan(&m.ID, &m.Name, &m.URL, &m.Kind, &m.IntervalSeconds, &m.TimeoutMS, &m.FailureThreshold, &m.ExpectedStatus, &m.Enabled, &m.NextCheckAt)
	return m, err
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		respond(w, 503, map[string]string{"error": "database unavailable"})
		return
	}
	respond(w, 200, map[string]string{"status": "ok"})
}

func (s *server) createMonitor(w http.ResponseWriter, r *http.Request) {
	m, ok := monitorInput(w, r)
	if !ok {
		return
	}
	tx, err := s.db.BeginTx(r.Context(), nil)
	if err != nil {
		dbError(w, err)
		return
	}
	defer tx.Rollback()
	m, err = scanMonitor(tx.QueryRowContext(r.Context(), `INSERT INTO monitors(name,url,kind,interval_seconds,timeout_ms,failure_threshold,expected_status,enabled) VALUES($1,$2,$3,$4,$5,$6,$7,true) RETURNING `+monitorColumns, m.Name, m.URL, m.Kind, m.IntervalSeconds, m.TimeoutMS, m.FailureThreshold, m.ExpectedStatus))
	if err != nil {
		dbError(w, err)
		return
	}
	if _, err := tx.ExecContext(r.Context(), `INSERT INTO incident_state(monitor_id) VALUES($1)`, m.ID); err != nil {
		dbError(w, err)
		return
	}
	if err := tx.Commit(); err != nil {
		dbError(w, err)
		return
	}
	respond(w, 201, m)
}

func (s *server) listMonitors(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT `+monitorColumns+` FROM monitors ORDER BY id`)
	if err != nil {
		dbError(w, err)
		return
	}
	defer rows.Close()
	result := []domain.Monitor{}
	for rows.Next() {
		m, err := scanMonitor(rows)
		if err != nil {
			dbError(w, err)
			return
		}
		result = append(result, m)
	}
	if err := rows.Err(); err != nil {
		dbError(w, err)
		return
	}
	respond(w, 200, result)
}

func (s *server) getMonitor(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}
	m, err := scanMonitor(s.db.QueryRowContext(r.Context(), `SELECT `+monitorColumns+` FROM monitors WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		respond(w, 404, map[string]string{"error": "monitor not found"})
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	respond(w, 200, m)
}

func (s *server) updateMonitor(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}
	m, ok := monitorInput(w, r)
	if !ok {
		return
	}
	m, err := scanMonitor(s.db.QueryRowContext(r.Context(), `UPDATE monitors SET name=$1,url=$2,kind=$3,interval_seconds=$4,timeout_ms=$5,failure_threshold=$6,expected_status=$7 WHERE id=$8 RETURNING `+monitorColumns, m.Name, m.URL, m.Kind, m.IntervalSeconds, m.TimeoutMS, m.FailureThreshold, m.ExpectedStatus, id))
	if errors.Is(err, sql.ErrNoRows) {
		respond(w, 404, map[string]string{"error": "monitor not found"})
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	respond(w, 200, m)
}

func (s *server) deleteMonitor(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}
	result, err := s.db.ExecContext(r.Context(), `DELETE FROM monitors WHERE id=$1`, id)
	if err != nil {
		dbError(w, err)
		return
	}
	if count, _ := result.RowsAffected(); count == 0 {
		respond(w, 404, map[string]string{"error": "monitor not found"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) setEnabled(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}
	var input struct {
		Enabled *bool `json:"enabled"`
	}
	if !decode(w, r, &input) {
		return
	}
	if input.Enabled == nil {
		respond(w, 400, map[string]string{"error": "enabled is required"})
		return
	}
	m, err := scanMonitor(s.db.QueryRowContext(r.Context(), `UPDATE monitors SET enabled=$1,next_check_at=CASE WHEN $1 THEN now() ELSE next_check_at END WHERE id=$2 RETURNING `+monitorColumns, *input.Enabled, id))
	if errors.Is(err, sql.ErrNoRows) {
		respond(w, 404, map[string]string{"error": "monitor not found"})
		return
	}
	if err != nil {
		dbError(w, err)
		return
	}
	respond(w, 200, m)
}

func (s *server) runNow(w http.ResponseWriter, r *http.Request) {
	id, ok := monitorID(w, r)
	if !ok {
		return
	}
	result, err := s.db.ExecContext(r.Context(), `UPDATE monitors SET next_check_at=now() WHERE id=$1 AND enabled=true`, id)
	if err != nil {
		dbError(w, err)
		return
	}
	if count, _ := result.RowsAffected(); count == 0 {
		respond(w, 404, map[string]string{"error": "enabled monitor not found"})
		return
	}
	respond(w, 202, map[string]string{"status": "scheduled"})
}

func dbError(w http.ResponseWriter, err error) {
	log.Printf("database error: %v", err)
	respond(w, 500, map[string]string{"error": "database error"})
}
