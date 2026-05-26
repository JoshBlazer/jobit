package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Limiter is a fixed-window per-second rate limiter backed by Redis.
// Each second gets its own key; TTL is 2 seconds so Redis cleans up automatically.
type Limiter struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Limiter {
	return &Limiter{rdb: rdb}
}

// Allow checks and records one submission for tenantID against limit (jobs/sec).
// Returns false if the tenant has exceeded their rate limit for the current second.
func (l *Limiter) Allow(ctx context.Context, tenantID uuid.UUID, limit int) (bool, error) {
	second := time.Now().Unix()
	key := fmt.Sprintf("ratelimit:%s:%d", tenantID, second)

	count, err := l.rdb.Incr(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("ratelimit incr: %w", err)
	}
	if count == 1 {
		l.rdb.Expire(ctx, key, 2*time.Second)
	}
	return int(count) <= limit, nil
}
