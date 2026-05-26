package job

import (
	"testing"
	"time"
)

func TestNextRetryAt(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	base := 30

	// Jitter is ±20% of the base delay, so we check within that window.
	cases := []struct {
		attempt  int
		baseSecs int
	}{
		{0, 30},
		{1, 60},
		{2, 120},
		{3, 240},
	}

	for _, c := range cases {
		got := NextRetryAt(c.attempt, base, now)
		diff := int(got.Sub(now).Seconds())
		jitter := int(float64(c.baseSecs) * 0.2)
		lo := c.baseSecs - jitter
		hi := c.baseSecs + jitter
		if diff < lo || diff > hi {
			t.Errorf("attempt %d: got %ds, want %d–%ds", c.attempt, diff, lo, hi)
		}
	}
}

func TestNextRetryAt_Cap(t *testing.T) {
	now := time.Now()
	// At high attempt counts, delay caps at 3600s + 20% jitter = 4320s max.
	got := NextRetryAt(20, 30, now)
	diff := got.Sub(now)
	maxAllowed := time.Duration(3600*1.2+1) * time.Second
	if diff > maxAllowed {
		t.Errorf("backoff should cap near 1h (+jitter), got %v", diff)
	}
}

func TestShouldRetry(t *testing.T) {
	j := &Job{Attempt: 2, MaxRetries: 3}
	if !j.ShouldRetry() {
		t.Error("expected ShouldRetry=true")
	}
	j.Attempt = 3
	if j.ShouldRetry() {
		t.Error("expected ShouldRetry=false at max")
	}
}
