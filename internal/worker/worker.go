package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pulse/internal/job"
	"github.com/pulse/internal/queue"
	"github.com/pulse/internal/storage"
)

const (
	dequeueTimeout    = 5 * time.Second
	visibilityTimeout = 30 * time.Second
	heartbeatInterval = 5 * time.Second
	httpTimeout       = 25 * time.Second
)

type Worker struct {
	id       string
	db       *pgxpool.Pool
	queue    *queue.Queue
	shutdown chan struct{}
	done     chan struct{}
}

func New(db *pgxpool.Pool, q *queue.Queue) *Worker {
	return &Worker{
		id:       uuid.NewString(),
		db:       db,
		queue:    q,
		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (w *Worker) Run(ctx context.Context) {
	defer close(w.done)
	slog.Info("worker started", "worker_id", w.id)

	for {
		select {
		case <-w.shutdown:
			slog.Info("worker shutting down", "worker_id", w.id)
			return
		default:
		}

		jobID, err := w.queue.Dequeue(ctx, w.id, dequeueTimeout)
		if err != nil {
			slog.Error("dequeue failed", "err", err)
			continue
		}
		if jobID == uuid.Nil {
			continue
		}

		w.process(ctx, jobID)
	}
}

func (w *Worker) Shutdown(timeout time.Duration) {
	close(w.shutdown)
	select {
	case <-w.done:
	case <-time.After(timeout):
		slog.Warn("worker shutdown timeout — in-flight job may be requeued by scheduler")
	}
}

func (w *Worker) process(ctx context.Context, jobID uuid.UUID) {
	token := uuid.New()
	deadline := time.Now().Add(visibilityTimeout + 30*time.Second)

	ok, err := storage.TryClaim(ctx, w.db, jobID, w.id, token, deadline)
	if err != nil {
		slog.Error("claim failed", "job_id", jobID, "err", err)
		w.queue.RemoveFromProcessing(ctx, w.id, jobID)
		return
	}
	if !ok {
		// Already claimed by someone else or cancelled.
		w.queue.RemoveFromProcessing(ctx, w.id, jobID)
		return
	}

	j, err := storage.GetJob(ctx, w.db, jobID)
	if err != nil {
		slog.Error("get job after claim", "job_id", jobID, "err", err)
		return
	}

	hbCtx, hbCancel := context.WithCancel(ctx)
	go w.heartbeat(hbCtx, jobID, token)

	slog.Info("executing job", "job_id", jobID, "type", j.Type, "attempt", j.Attempt)
	execErr := execute(ctx, j)

	hbCancel()

	if execErr != nil {
		slog.Warn("job failed", "job_id", jobID, "err", execErr)
		var nextRunAt *time.Time
		if j.ShouldRetry() {
			t := job.NextRetryAt(j.Attempt+1, j.BackoffSeconds, time.Now())
			nextRunAt = &t
		}
		if err := storage.FailJob(ctx, w.db, jobID, token, execErr.Error(), nextRunAt); err != nil {
			slog.Error("record job failure", "job_id", jobID, "err", err)
		}
	} else {
		slog.Info("job succeeded", "job_id", jobID)
		if err := storage.CompleteJob(ctx, w.db, jobID, token); err != nil {
			slog.Error("record job success", "job_id", jobID, "err", err)
		}
	}

	w.queue.RemoveFromProcessing(ctx, w.id, jobID)
}

func (w *Worker) heartbeat(ctx context.Context, jobID uuid.UUID, token uuid.UUID) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.queue.Heartbeat(ctx, jobID, token.String()); err != nil {
				slog.Warn("heartbeat failed", "job_id", jobID, "err", err)
			}
		}
	}
}

// execute dispatches to the correct handler by job type.
func execute(ctx context.Context, j *job.Job) error {
	switch j.Type {
	case "webhook":
		return executeWebhook(ctx, j)
	default:
		return fmt.Errorf("unknown job type: %s", j.Type)
	}
}

func executeWebhook(ctx context.Context, j *job.Job) error {
	var p job.WebhookPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return fmt.Errorf("invalid webhook payload: %w", err)
	}

	method := p.Method
	if method == "" {
		method = http.MethodPost
	}

	var body *bytes.Reader
	if len(p.Body) > 0 {
		body = bytes.NewReader(p.Body)
	} else {
		body = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(ctx, method, p.URL, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}
	if _, ok := p.Headers["Content-Type"]; !ok && len(p.Body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned %d", resp.StatusCode)
	}
	return nil
}
