package domain

import "testing"

func TestValidateMonitor(t *testing.T) {
	base := Monitor{Name: "api", URL: "https://example.com/health", Kind: "http", IntervalSeconds: 10, TimeoutMS: 5000, FailureThreshold: 2}
	if err := ValidateMonitor(base); err != nil {
		t.Fatal(err)
	}
	base.Kind = "tcp"
	base.URL = "tcp://localhost:5432"
	if err := ValidateMonitor(base); err != nil {
		t.Fatal(err)
	}
	base.URL = "tcp://localhost"
	if err := ValidateMonitor(base); err == nil {
		t.Fatal("expected missing port error")
	}
	base.URL = "tcp://localhost:abc"
	if err := ValidateMonitor(base); err == nil {
		t.Fatal("expected invalid port error")
	}
}

func TestIncidentThreshold(t *testing.T) {
	streak, open, resolve := NextIncidentState(1, 3, false, false)
	if streak != 2 || open || resolve {
		t.Fatalf("second failure: %d %v %v", streak, open, resolve)
	}
	streak, open, resolve = NextIncidentState(streak, 3, false, false)
	if streak != 3 || !open || resolve {
		t.Fatalf("third failure: %d %v %v", streak, open, resolve)
	}
	streak, open, resolve = NextIncidentState(streak, 3, true, true)
	if streak != 0 || open || !resolve {
		t.Fatalf("recovery: %d %v %v", streak, open, resolve)
	}
}
