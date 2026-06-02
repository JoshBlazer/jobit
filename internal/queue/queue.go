package queue

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/sluice/internal/job"
	"github.com/redis/go-redis/v9"
)

const (
	HeartbeatTTL     = 15 * time.Second
	processingPrefix = "processing:"
	heartbeatPrefix  = "heartbeat:"
	pollInterval     = 100 * time.Millisecond
)

// Priority bucket names. Queue keys are "{bucket}:{tenantID}".
var priorityBuckets = []string{"queue:1", "queue:5", "queue:10"}

func bucketForPriority(priority int16) string {
	switch {
	case priority <= job.PriorityHigh:
		return "queue:1"
	case priority <= job.PriorityNormal:
		return "queue:5"
	default:
		return "queue:10"
	}
}

// TenantWeight pairs a tenant ID with its scheduling weight.
// Workers iterate tenants in proportion to their weight when dequeuing.
type TenantWeight struct {
	ID     uuid.UUID
	Weight int
}

type Queue struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Queue {
	return &Queue{rdb: rdb}
}

func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}

// Enqueue pushes a job to the tenant-scoped priority queue.
func (q *Queue) Enqueue(ctx context.Context, tenantID uuid.UUID, jobID uuid.UUID, priority int16) error {
	key := bucketForPriority(priority) + ":" + tenantID.String()
	if err := q.rdb.LPush(ctx, key, jobID.String()).Err(); err != nil {
		return fmt.Errorf("enqueue job %s to %s: %w", jobID, key, err)
	}
	return nil
}

// Dequeue pops the next job using weighted fair queuing across tenants.
// Within each poll cycle, tenants are visited in a weighted-random order so
// higher-weight tenants receive a proportionally larger share of worker time.
// Returns uuid.Nil with no error if nothing arrives before timeout.
func (q *Queue) Dequeue(ctx context.Context, workerID string, tenants []TenantWeight, timeout time.Duration) (uuid.UUID, error) {
	dest := processingPrefix + workerID
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return uuid.Nil, ctx.Err()
		default:
		}

		if len(tenants) > 0 {
			order := weightedShuffle(tenants)
			for _, tenantID := range order {
				for _, bucket := range priorityBuckets {
					key := bucket + ":" + tenantID.String()
					result, err := q.rdb.LMove(ctx, key, dest, "RIGHT", "LEFT").Result()
					if err == nil {
						id, parseErr := uuid.Parse(result)
						if parseErr == nil {
							return id, nil
						}
					}
					if err != nil && err != redis.Nil {
						return uuid.Nil, fmt.Errorf("lmove from %s: %w", key, err)
					}
				}
			}
		}

		select {
		case <-ctx.Done():
			return uuid.Nil, ctx.Err()
		case <-time.After(pollInterval):
		}
	}

	return uuid.Nil, nil
}

// Heartbeat refreshes the TTL-keyed heartbeat for a running job.
func (q *Queue) Heartbeat(ctx context.Context, jobID uuid.UUID, token string) error {
	key := heartbeatPrefix + jobID.String()
	if err := q.rdb.Set(ctx, key, token, HeartbeatTTL).Err(); err != nil {
		return fmt.Errorf("heartbeat job %s: %w", jobID, err)
	}
	return nil
}

// RemoveFromProcessing clears the job from the worker's in-flight list after completion.
func (q *Queue) RemoveFromProcessing(ctx context.Context, workerID string, jobID uuid.UUID) error {
	key := processingPrefix + workerID
	if err := q.rdb.LRem(ctx, key, 0, jobID.String()).Err(); err != nil {
		return fmt.Errorf("remove from processing list: %w", err)
	}
	return nil
}

// weightedShuffle returns a weighted-random permutation of tenant IDs.
// Tenants with higher weights appear in more favourable positions on average.
func weightedShuffle(tenants []TenantWeight) []uuid.UUID {
	remaining := make([]TenantWeight, len(tenants))
	copy(remaining, tenants)

	out := make([]uuid.UUID, 0, len(tenants))
	for len(remaining) > 0 {
		total := 0
		for _, t := range remaining {
			total += t.Weight
		}
		r := rand.Intn(total)
		cum := 0
		for i, t := range remaining {
			cum += t.Weight
			if r < cum {
				out = append(out, t.ID)
				remaining = append(remaining[:i], remaining[i+1:]...)
				break
			}
		}
	}
	return out
}
