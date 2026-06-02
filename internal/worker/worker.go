package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pulse/internal/job"
	"github.com/pulse/internal/metrics"
	"github.com/pulse/internal/queue"
	"github.com/pulse/internal/storage"
	"github.com/pulse/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const (
	dequeueTimeout    = 5 * time.Second
	visibilityTimeout = 30 * time.Second
	heartbeatInterval = 5 * time.Second
	httpTimeout       = 25 * time.Second
	tenantRefresh     = 60 * time.Second
)

type Worker struct {
	id       string
	db       *pgxpool.Pool
	queue    *queue.Queue
	shutdown chan struct{}
	done     chan struct{}
	reload   chan struct{}

	tenantsMu sync.RWMutex
	tenants   []queue.TenantWeight
}

func New(db *pgxpool.Pool, q *queue.Queue) *Worker {
	return &Worker{
		id:       uuid.NewString(),
		db:       db,
		queue:    q,
		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
		reload:   make(chan struct{}, 1),
	}
}

// Reload signals the worker to refresh its tenant weight cache from Postgres.
// Called on SIGHUP for hot config reload. Non-blocking.
func (w *Worker) Reload() {
	select {
	case w.reload <- struct{}{}:
	default:
	}
}

func (w *Worker) Run(ctx context.Context) {
	defer close(w.done)
	slog.Info("worker started", "worker_id", w.id)

	w.loadTenants(ctx)

	// Background goroutine refreshes the tenant list periodically and on demand.
	go func() {
		ticker := time.NewTicker(tenantRefresh)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.loadTenants(ctx)
			case <-w.reload:
				slog.Info("worker reloading tenant weights", "worker_id", w.id)
				w.loadTenants(ctx)
			}
		}
	}()

	for {
		select {
		case <-w.shutdown:
			slog.Info("worker shutting down", "worker_id", w.id)
			return
		case <-ctx.Done():
			return
		default:
		}

		w.tenantsMu.RLock()
		tenants := w.tenants
		w.tenantsMu.RUnlock()

		jobID, err := w.queue.Dequeue(ctx, w.id, tenants, dequeueTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
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

func (w *Worker) loadTenants(ctx context.Context) {
	tenants, err := storage.GetTenants(ctx, w.db)
	if err != nil {
		slog.Error("load tenant weights", "err", err)
		return
	}
	weights := make([]queue.TenantWeight, len(tenants))
	for i, t := range tenants {
		w := t.Weight
		if w <= 0 {
			w = 100
		}
		weights[i] = queue.TenantWeight{ID: t.ID, Weight: w}
	}
	w.tenantsMu.Lock()
	w.tenants = weights
	w.tenantsMu.Unlock()
	slog.Info("tenant weights loaded", "count", len(weights))
}

func (w *Worker) process(ctx context.Context, jobID uuid.UUID) {
	tracer := telemetry.Tracer("pulse/worker")
	ctx, span := tracer.Start(ctx, "worker.execute")
	span.SetAttributes(attribute.String("job.id", jobID.String()))
	defer span.End()

	token := uuid.New()
	deadline := time.Now().Add(visibilityTimeout + 30*time.Second)

	ok, runID, err := storage.TryClaim(ctx, w.db, jobID, w.id, token, deadline)
	if err != nil {
		telemetry.L(ctx).Error("claim failed", "job_id", jobID, "err", err)
		span.RecordError(err)
		w.queue.RemoveFromProcessing(ctx, w.id, jobID)
		return
	}
	if !ok {
		w.queue.RemoveFromProcessing(ctx, w.id, jobID)
		return
	}

	j, err := storage.GetJob(ctx, w.db, jobID)
	if err != nil {
		telemetry.L(ctx).Error("get job after claim", "job_id", jobID, "err", err)
		span.RecordError(err)
		return
	}

	span.SetAttributes(
		attribute.String("job.type", j.Type),
		attribute.String("job.tenant_id", j.TenantID.String()),
		attribute.Int("job.attempt", j.Attempt),
	)

	if err := storage.MarkRunning(ctx, w.db, jobID, token); err != nil {
		telemetry.L(ctx).Warn("mark running", "job_id", jobID, "err", err)
	}

	metrics.WorkerInFlight.Inc()
	hbCtx, hbCancel := context.WithCancel(ctx)
	go w.heartbeat(hbCtx, jobID, token)

	telemetry.L(ctx).Info("executing job", "job_id", jobID, "type", j.Type, "attempt", j.Attempt, "tenant_id", j.TenantID)
	start := time.Now()
	execErr := execute(ctx, j)
	dur := time.Since(start)

	hbCancel()
	metrics.WorkerInFlight.Dec()

	tenantID := j.TenantID.String()

	if execErr != nil {
		telemetry.L(ctx).Warn("job failed", "job_id", jobID, "err", execErr, "tenant_id", tenantID)
		span.RecordError(execErr)
		span.SetStatus(codes.Error, execErr.Error())

		var nextRunAt *time.Time
		if j.ShouldRetry() {
			t := job.NextRetryAt(j.Attempt+1, j.BackoffSeconds, time.Now())
			nextRunAt = &t
			metrics.JobRetriesTotal.WithLabelValues(j.Type, tenantID).Inc()
		}
		finalState := "failed"
		if nextRunAt == nil {
			finalState = "dead"
		}
		metrics.JobsTotal.WithLabelValues(j.Type, finalState, tenantID).Inc()
		metrics.JobDurationSeconds.WithLabelValues(j.Type, finalState, tenantID).Observe(dur.Seconds())

		if err := storage.FailJob(ctx, w.db, jobID, runID, token, execErr.Error(), nextRunAt); err != nil {
			telemetry.L(ctx).Error("record job failure", "job_id", jobID, "err", err)
		}
	} else {
		telemetry.L(ctx).Info("job succeeded", "job_id", jobID, "tenant_id", tenantID, "duration_ms", dur.Milliseconds())
		span.SetStatus(codes.Ok, "")
		metrics.JobsTotal.WithLabelValues(j.Type, "succeeded", tenantID).Inc()
		metrics.JobDurationSeconds.WithLabelValues(j.Type, "succeeded", tenantID).Observe(dur.Seconds())
		if err := storage.CompleteJob(ctx, w.db, jobID, runID, token); err != nil {
			telemetry.L(ctx).Error("record job success", "job_id", jobID, "err", err)
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
			// Extend the Postgres deadline so the stale-claim reaper doesn't reassign
			// a healthy job. TTL matches queue.HeartbeatTTL: 3 missed beats = reassignment.
			newDeadline := time.Now().Add(queue.HeartbeatTTL)
			if err := storage.ExtendDeadline(ctx, w.db, jobID, token, newDeadline); err != nil {
				slog.Warn("extend deadline failed", "job_id", jobID, "err", err)
			}
		}
	}
}

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
