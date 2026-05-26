package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pulse/internal/job"
	"github.com/pulse/internal/queue"
	"github.com/pulse/internal/storage"
	"github.com/robfig/cron/v3"
)

const (
	duePollInterval    = 100 * time.Millisecond
	staleReapInterval  = 5 * time.Second
	deadLetterInterval = 30 * time.Second
	cronInterval       = 60 * time.Second
	duePollBatchSize   = 500
	failedPollBatchSize = 500
	cronBatchSize      = 200
)

type Scheduler struct {
	db    *pgxpool.Pool
	queue *queue.Queue
}

func New(db *pgxpool.Pool, q *queue.Queue) *Scheduler {
	return &Scheduler{db: db, queue: q}
}

func (s *Scheduler) Run(ctx context.Context) {
	slog.Info("scheduler started")
	go s.runDuePoll(ctx)
	go s.runStaleReaper(ctx)
	go s.runDeadLetterPromoter(ctx)
	go s.runCronExpander(ctx)
	<-ctx.Done()
	slog.Info("scheduler stopped")
}

func (s *Scheduler) runDuePoll(ctx context.Context) {
	ticker := time.NewTicker(duePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pollScheduledJobs(ctx)
			s.pollFailedJobs(ctx)
		}
	}
}

// pollScheduledJobs promotes scheduled jobs whose run_at has passed to pending.
func (s *Scheduler) pollScheduledJobs(ctx context.Context) {
	jobs, err := storage.GetDueJobs(ctx, s.db, duePollBatchSize)
	if err != nil {
		slog.Error("poll scheduled jobs", "err", err)
		return
	}
	for _, j := range jobs {
		if err := storage.PromoteScheduledToPending(ctx, s.db, j.ID); err != nil {
			slog.Error("promote scheduled job", "job_id", j.ID, "err", err)
			continue
		}
		if err := s.queue.Enqueue(ctx, j.ID, j.Priority); err != nil {
			slog.Error("enqueue scheduled job", "job_id", j.ID, "err", err)
		}
	}
}

// pollFailedJobs re-enqueues failed jobs whose backoff has elapsed.
func (s *Scheduler) pollFailedJobs(ctx context.Context) {
	jobs, err := storage.GetFailedReadyJobs(ctx, s.db, failedPollBatchSize)
	if err != nil {
		slog.Error("poll failed ready jobs", "err", err)
		return
	}
	for _, j := range jobs {
		if err := storage.PromoteFailedToPending(ctx, s.db, j.ID); err != nil {
			slog.Error("promote failed job", "job_id", j.ID, "err", err)
			continue
		}
		if err := s.queue.Enqueue(ctx, j.ID, j.Priority); err != nil {
			slog.Error("enqueue retried job", "job_id", j.ID, "err", err)
		}
	}
}

func (s *Scheduler) runStaleReaper(ctx context.Context) {
	ticker := time.NewTicker(staleReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reapStaleClaims(ctx)
		}
	}
}

func (s *Scheduler) reapStaleClaims(ctx context.Context) {
	jobs, err := storage.GetStaleClaims(ctx, s.db)
	if err != nil {
		slog.Error("get stale claims", "err", err)
		return
	}
	for _, j := range jobs {
		slog.Warn("reaping stale job", "job_id", j.ID, "claimed_by", j.ClaimedBy)
		if err := storage.RequeueStaleJob(ctx, s.db, j.ID); err != nil {
			slog.Error("requeue stale job", "job_id", j.ID, "err", err)
			continue
		}
		if err := s.queue.Enqueue(ctx, j.ID, j.Priority); err != nil {
			slog.Error("enqueue reaped job", "job_id", j.ID, "err", err)
		}
	}
}

func (s *Scheduler) runDeadLetterPromoter(ctx context.Context) {
	ticker := time.NewTicker(deadLetterInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.promoteDeadJobs(ctx)
		}
	}
}

func (s *Scheduler) promoteDeadJobs(ctx context.Context) {
	deadState := storage.DeadState()
	jobs, err := storage.ListJobs(ctx, s.db, storage.ListFilter{State: deadState, Limit: 200})
	if err != nil {
		slog.Error("list dead jobs", "err", err)
		return
	}
	for _, j := range jobs {
		if err := storage.MoveToDeadLetter(ctx, s.db, j.ID); err != nil {
			slog.Error("move to dead letter", "job_id", j.ID, "err", err)
		}
	}
}

// runCronExpander fires once per minute, finds due recurring schedules,
// creates a job from each schedule's job_template, and advances next_run_at.
func (s *Scheduler) runCronExpander(ctx context.Context) {
	// Fire immediately on start so any overdue schedules are caught.
	s.expandCron(ctx)

	ticker := time.NewTicker(cronInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.expandCron(ctx)
		}
	}
}

func (s *Scheduler) expandCron(ctx context.Context) {
	schedules, err := storage.GetDueSchedules(ctx, s.db, cronBatchSize)
	if err != nil {
		slog.Error("get due schedules", "err", err)
		return
	}
	for _, sched := range schedules {
		if err := s.fireSchedule(ctx, sched); err != nil {
			slog.Error("fire schedule", "schedule_id", sched.ID, "name", sched.Name, "err", err)
		}
	}
}

func (s *Scheduler) fireSchedule(ctx context.Context, sched *storage.Schedule) error {
	// Parse the cron expression to calculate next_run_at.
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	expr, err := parser.Parse(sched.Cron)
	if err != nil {
		return fmt.Errorf("parse cron %q: %w", sched.Cron, err)
	}

	now := time.Now()
	nextRunAt := expr.Next(now)

	// Build a job from the schedule template.
	var template struct {
		Type           string          `json:"type"`
		Payload        json.RawMessage `json:"payload"`
		Priority       *int16          `json:"priority,omitempty"`
		MaxRetries     *int            `json:"max_retries,omitempty"`
		BackoffSeconds *int            `json:"backoff_seconds,omitempty"`
	}
	if err := json.Unmarshal(sched.JobTemplate, &template); err != nil {
		return fmt.Errorf("unmarshal job template: %w", err)
	}

	priority := job.PriorityNormal
	if template.Priority != nil {
		priority = *template.Priority
	}
	maxRetries := 3
	if template.MaxRetries != nil {
		maxRetries = *template.MaxRetries
	}
	backoff := 30
	if template.BackoffSeconds != nil {
		backoff = *template.BackoffSeconds
	}

	j := &job.Job{
		ID:             uuid.New(),
		TenantID:       sched.TenantID,
		Type:           template.Type,
		Payload:        template.Payload,
		Priority:       priority,
		State:          job.StatePending,
		RunAt:          now,
		Attempt:        0,
		MaxRetries:     maxRetries,
		BackoffSeconds: backoff,
		CreatedAt:      now,
	}

	if err := storage.InsertJob(ctx, s.db, j); err != nil {
		return fmt.Errorf("insert cron job: %w", err)
	}
	if err := s.queue.Enqueue(ctx, j.ID, j.Priority); err != nil {
		slog.Warn("enqueue cron job failed — job is durable in postgres", "job_id", j.ID, "err", err)
	}
	if err := storage.UpdateScheduleAfterRun(ctx, s.db, sched.ID, now, nextRunAt); err != nil {
		return fmt.Errorf("update schedule next_run_at: %w", err)
	}

	slog.Info("fired cron schedule", "schedule", sched.Name, "job_id", j.ID, "next_run_at", nextRunAt)
	return nil
}
