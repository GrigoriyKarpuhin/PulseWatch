package domain

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Monitor struct {
	ID               int64     `json:"id"`
	Name             string    `json:"name"`
	URL              string    `json:"url"`
	Kind             string    `json:"kind"`
	IntervalSeconds  int       `json:"interval_seconds"`
	TimeoutMS        int       `json:"timeout_ms"`
	FailureThreshold int       `json:"failure_threshold"`
	ExpectedStatus   int       `json:"expected_status"`
	Enabled          bool      `json:"enabled"`
	NextCheckAt      time.Time `json:"next_check_at"`
}

type ProbeJob struct {
	ID             string    `json:"id"`
	MonitorID      int64     `json:"monitor_id"`
	URL            string    `json:"url"`
	Kind           string    `json:"kind"`
	TimeoutMS      int       `json:"timeout_ms"`
	ExpectedStatus int       `json:"expected_status"`
	ScheduledAt    time.Time `json:"scheduled_at"`
}

type CheckResult struct {
	ID         string    `json:"id"`
	MonitorID  int64     `json:"monitor_id"`
	Kind       string    `json:"kind"`
	CheckedAt  time.Time `json:"checked_at"`
	StatusCode int       `json:"status_code"`
	LatencyMS  int64     `json:"latency_ms"`
	Success    bool      `json:"success"`
	Error      string    `json:"error"`
}

type IncidentEvent struct {
	ID         string    `json:"id"`
	IncidentID int64     `json:"incident_id"`
	MonitorID  int64     `json:"monitor_id"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurred_at"`
	Reason     string    `json:"reason"`
}

func ValidateMonitor(m Monitor) error {
	if strings.TrimSpace(m.Name) == "" || len(m.Name) > 100 {
		return errors.New("name must be 1-100 characters")
	}
	if m.Kind != "http" && m.Kind != "tcp" {
		return errors.New("kind must be http or tcp")
	}
	u, err := url.Parse(m.URL)
	if err != nil || u.User != nil || u.Hostname() == "" {
		return errors.New("invalid target URL")
	}
	if m.Kind == "http" && (u.Scheme != "http" && u.Scheme != "https" || u.Fragment != "") {
		return errors.New("http monitor requires an absolute http(s) URL")
	}
	if m.Kind == "tcp" {
		if u.Scheme != "tcp" || u.Path != "" || u.RawQuery != "" {
			return errors.New("tcp monitor requires tcp://host:port")
		}
		_, portText, err := net.SplitHostPort(u.Host)
		if err != nil {
			return errors.New("tcp monitor requires host and port")
		}
		port, err := strconv.Atoi(portText)
		if err != nil || port < 1 || port > 65535 {
			return errors.New("tcp port must be 1-65535")
		}
	}
	if m.IntervalSeconds < 10 || m.IntervalSeconds > 3600 {
		return errors.New("interval_seconds must be 10-3600")
	}
	if m.TimeoutMS < 100 || m.TimeoutMS > 30000 {
		return errors.New("timeout_ms must be 100-30000")
	}
	if m.FailureThreshold < 1 || m.FailureThreshold > 10 {
		return errors.New("failure_threshold must be 1-10")
	}
	if m.ExpectedStatus != 0 && (m.Kind != "http" || m.ExpectedStatus < 100 || m.ExpectedStatus > 599) {
		return errors.New("expected_status requires an HTTP status 100-599")
	}
	return nil
}

func NextIncidentState(streak, threshold int, open, success bool) (next int, shouldOpen, shouldResolve bool) {
	if success {
		return 0, false, open
	}
	next = streak + 1
	return next, next >= threshold && !open, false
}
