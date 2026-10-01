package prober

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pulsewatch/internal/domain"
)

func TestHTTPProbe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }))
	defer server.Close()
	job := domain.ProbeJob{ID: "1", MonitorID: 1, Kind: "http", URL: server.URL, TimeoutMS: 1000, ExpectedStatus: 418}
	if result := Probe(context.Background(), job); !result.Success || result.StatusCode != 418 {
		t.Fatalf("unexpected result: %+v", result)
	}
	job.ExpectedStatus = 200
	if result := Probe(context.Background(), job); result.Success || !strings.Contains(result.Error, "418") {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestTCPProbe(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	job := domain.ProbeJob{ID: "2", MonitorID: 1, Kind: "tcp", URL: "tcp://" + listener.Addr().String(), TimeoutMS: 1000}
	if result := Probe(context.Background(), job); !result.Success {
		t.Fatalf("unexpected result: %+v", result)
	}
}
