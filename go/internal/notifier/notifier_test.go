package notifier

import (
	"testing"
	"time"
)

func TestRetryDelay(t *testing.T) {
	if retryDelay(1) != 2*time.Second || retryDelay(3) != 8*time.Second || retryDelay(10) != 32*time.Second {
		t.Fatal("wrong retry schedule")
	}
}
