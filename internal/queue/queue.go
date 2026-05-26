package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pulse/internal/job"
	"github.com/redis/go-redis/v9"
)

const (
	heartbeatTTL     = 15 * time.Second
	processingPrefix = "processing:"
	heartbeatPrefix  = "heartbeat:"
)

// Priority queues in order — workers check left to right, so high-priority jobs are found first.
var priorityQueues = []string{
	"queue:1",  // high  (priority <= 1)
	"queue:5",  // normal (priority <= 5)
	"queue:10", // low   (priority > 5)
}

func queueForPriority(priority int16) string {
	switch {
	case priority <= job.PriorityHigh:
		return "queue:1"
	case priority <= job.PriorityNormal:
		return "queue:5"
	default:
		return "queue:10"
	}
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

// Enqueue pushes a job ID to the left of the appropriate priority queue.
func (q *Queue) Enqueue(ctx context.Context, jobID uuid.UUID, priority int16) error {
	key := queueForPriority(priority)
	if err := q.rdb.LPush(ctx, key, jobID.String()).Err(); err != nil {
		return fmt.Errorf("enqueue job %s to %s: %w", jobID, key, err)
	}
	return nil
}

// Dequeue waits up to timeout for a job across all priority queues.
// Strategy: non-blocking LMOVE from each queue in priority order first,
// then fall back to BLPOP (which blocks on multiple keys simultaneously).
// Returns uuid.Nil with no error if nothing arrives before timeout.
func (q *Queue) Dequeue(ctx context.Context, workerID string, timeout time.Duration) (uuid.UUID, error) {
	dest := processingPrefix + workerID

	// Fast path: non-blocking scan in priority order.
	for _, src := range priorityQueues {
		result, err := q.rdb.LMove(ctx, src, dest, "RIGHT", "LEFT").Result()
		if err == nil {
			return uuid.Parse(result)
		}
		if err != redis.Nil {
			return uuid.Nil, fmt.Errorf("lmove from %s: %w", src, err)
		}
	}

	// Slow path: block across all queues. BLPOP honours left-to-right priority.
	vals, err := q.rdb.BLPop(ctx, timeout, priorityQueues...).Result()
	if err == redis.Nil {
		return uuid.Nil, nil
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("blpop: %w", err)
	}

	// vals = [queue-key, job-id]
	jobIDStr := vals[1]

	// Move to the worker's processing list. The tiny window between BLPOP and here is
	// acceptable: the Postgres TryClaim is the authoritative ownership check, not Redis.
	q.rdb.LPush(ctx, dest, jobIDStr) // best-effort; ignore error

	return uuid.Parse(jobIDStr)
}

// Heartbeat refreshes the TTL-keyed heartbeat for a running job.
// The 15-second TTL gives the scheduler 3 missed beats (at 5s interval) before reaping.
func (q *Queue) Heartbeat(ctx context.Context, jobID uuid.UUID, token string) error {
	key := heartbeatPrefix + jobID.String()
	if err := q.rdb.Set(ctx, key, token, heartbeatTTL).Err(); err != nil {
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
