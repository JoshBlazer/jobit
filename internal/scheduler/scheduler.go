package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pulse/internal/job"
	"github.com/pulse/internal/leader"
	"github.com/pulse/internal/metrics"
	"github.com/pulse/internal/queue"
	"github.com/pulse/internal/storage"
	"github.com/robfig/cron/v3"
)

const (
	duePollInterval          = 100 * time.Millisecond
	staleReapInterval        = 5 * time.Second
	deadLetterInterval       = 30 * time.Second
	cronInterval             = 60 * time.Second
	pendingReconcileInterval = 30 * time.Second
	duePollBatchSize         = 500
	failedPollBatchSize      = 500
	cronBatchSize            = 200
	pendingReconcileBatchSize = 500
)

type Scheduler struct {
	db    *pgxpool.Pool
	queue *queue.Queue
}

func New(db *pgxpool.Pool, q *queue.Queue) *Scheduler {
	return &Scheduler{db: db, queue: q}
}

// Run enters the leader-election loop. It blocks until ctx is cancelled.
// The four scheduler loops only run while this instance holds the etcd lease.
// Non-leaders keep the DB connection warm and wait to take over.
func (s *Scheduler) Run(ctx context.Context, elect *leader.Election) {
	hostname := uuid.NewString() // unique identity within this election
	slog.Info("scheduler hot-standby — waiting for leader election")

	for ctx.Err() == nil {
		leaderCtx, resign, err := elect.Campaign(ctx, hostname)
		if err != nil {
			// ctx was cancelled during campaign — clean shutdown
			return
		}
		slog.Info("scheduler became leader")
		metrics.SchedulerIsLeader.Set(1)

		var wg sync.WaitGroup
		for _, fn := range []func(context.Context){
			s.runDuePoll,
			s.runStaleReaper,
			s.runDeadLetterPromoter,
			s.runCronExpander,
			s.runPendingReconciler,
		} {
			wg.Add(1)
			go func(f func(context.Context)) {
				defer wg.Done()
				f(leaderCtx)
			}(fn)
		}

		wg.Wait()
		metrics.SchedulerIsLeader.Set(0)
		resign()
		slog.Info("scheduler lost leadership — re-entering election")
	}
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
		if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
			slog.Error("enqueue scheduled job", "job_id", j.ID, "err", err)
		}
	}
}

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
		if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
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
		dead, err := storage.RequeueStaleJob(ctx, s.db, j.ID)
		if err != nil {
			slog.Error("requeue stale job", "job_id", j.ID, "err", err)
			continue
		}
		if !dead {
			if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
				slog.Error("enqueue reaped job", "job_id", j.ID, "err", err)
			}
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

func (s *Scheduler) runPendingReconciler(ctx context.Context) {
	ticker := time.NewTicker(pendingReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcilePending(ctx)
		}
	}
}

func (s *Scheduler) reconcilePending(ctx context.Context) {
	jobs, err := storage.GetPendingJobs(ctx, s.db, pendingReconcileBatchSize)
	if err != nil {
		slog.Error("reconcile pending jobs", "err", err)
		return
	}
	for _, j := range jobs {
		if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
			slog.Error("re-enqueue pending job", "job_id", j.ID, "err", err)
		}
	}
	if len(jobs) > 0 {
		slog.Info("reconciled pending jobs", "count", len(jobs))
	}
}

func (s *Scheduler) runCronExpander(ctx context.Context) {
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
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	expr, err := parser.Parse(sched.Cron)
	if err != nil {
		return fmt.Errorf("parse cron %q: %w", sched.Cron, err)
	}

	now := time.Now()
	nextRunAt := expr.Next(now)

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
	if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
		slog.Warn("enqueue cron job failed — job is durable in postgres", "job_id", j.ID, "err", err)
	}
	if err := storage.UpdateScheduleAfterRun(ctx, s.db, sched.ID, now, nextRunAt); err != nil {
		return fmt.Errorf("update schedule next_run_at: %w", err)
	}

	slog.Info("fired cron schedule", "schedule", sched.Name, "job_id", j.ID, "next_run_at", nextRunAt)
	return nil
}
