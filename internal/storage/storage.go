package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pulse/internal/job"
)

var ErrNotFound = errors.New("job not found")
var ErrClaimConflict = errors.New("job already claimed")
var ErrDuplicate = errors.New("duplicate idempotency key")

func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// InsertJob persists a new job. Returns ErrDuplicate (wrapping the existing job ID)
// if an idempotency key conflict is detected — callers should fetch the original job.
func InsertJob(ctx context.Context, db *pgxpool.Pool, j *job.Job) error {
	_, err := db.Exec(ctx, `
		INSERT INTO jobs (
			id, tenant_id, type, payload, priority, state,
			run_at, attempt, max_retries, backoff_seconds,
			idempotency_key, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10,
			$11, $12
		)`,
		j.ID, j.TenantID, j.Type, []byte(j.Payload), j.Priority, string(j.State),
		j.RunAt, j.Attempt, j.MaxRetries, j.BackoffSeconds,
		j.IdempotencyKey, j.CreatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrDuplicate
		}
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

func GetJobByIdempotencyKey(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID, key string) (*job.Job, error) {
	row := db.QueryRow(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs WHERE tenant_id = $1 AND idempotency_key = $2`, tenantID, key)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

func GetJob(ctx context.Context, db *pgxpool.Pool, id uuid.UUID) (*job.Job, error) {
	row := db.QueryRow(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs WHERE id = $1`, id)

	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// TryClaim atomically claims a job for a worker using SELECT FOR UPDATE SKIP LOCKED.
// Returns false if the job was already claimed or doesn't exist in pending state.
func TryClaim(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID, workerID string, token uuid.UUID, deadline time.Time) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var id uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT id FROM jobs
		WHERE id = $1 AND state = 'pending'
		FOR UPDATE SKIP LOCKED`, jobID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock job row: %w", err)
	}

	now := time.Now()
	_, err = tx.Exec(ctx, `
		UPDATE jobs SET
			state      = 'claimed',
			claimed_at = $1,
			claimed_by = $2,
			claim_token = $3,
			deadline   = $4
		WHERE id = $5`,
		now, workerID, token, deadline, jobID)
	if err != nil {
		return false, fmt.Errorf("update claim: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO job_runs (id, job_id, tenant_id, attempt, started_at, state)
		SELECT $1, j.id, j.tenant_id, j.attempt, $2, 'claimed'
		FROM jobs j WHERE j.id = $3`,
		uuid.New(), now, jobID)
	if err != nil {
		return false, fmt.Errorf("insert job_run: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit claim: %w", err)
	}
	return true, nil
}

// CompleteJob marks a job succeeded. If the claim token doesn't match, it's a no-op —
// the job was reassigned and a stale worker must not overwrite the new owner's state.
func CompleteJob(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID, token uuid.UUID) error {
	now := time.Now()
	tag, err := db.Exec(ctx, `
		UPDATE jobs SET
			state        = 'succeeded',
			completed_at = $1,
			claim_token  = NULL,
			deadline     = NULL
		WHERE id = $2 AND claim_token = $3 AND state IN ('claimed', 'running')`,
		now, jobID, token)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil // stale worker, silently discard
	}

	_, err = db.Exec(ctx, `
		UPDATE job_runs SET state = 'succeeded', finished_at = $1,
		    duration_ms = EXTRACT(EPOCH FROM ($1 - started_at))::INT * 1000
		WHERE job_id = $2 AND finished_at IS NULL`,
		now, jobID)
	return err
}

// FailJob records an error and either re-queues the job (after backoff) or moves it to dead state.
func FailJob(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID, token uuid.UUID, errMsg string, nextRunAt *time.Time) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	now := time.Now()

	// Verify token still matches before touching state.
	var currentAttempt int
	var maxRetries int
	err = tx.QueryRow(ctx, `
		SELECT attempt, max_retries FROM jobs
		WHERE id = $1 AND claim_token = $2 AND state IN ('claimed', 'running')
		FOR UPDATE`, jobID, token).Scan(&currentAttempt, &maxRetries)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // stale worker
	}
	if err != nil {
		return fmt.Errorf("lock for fail: %w", err)
	}

	newAttempt := currentAttempt + 1
	var newState string
	var runAt *time.Time

	if newAttempt >= maxRetries {
		newState = "dead"
	} else {
		newState = "failed"
		runAt = nextRunAt
	}

	_, err = tx.Exec(ctx, `
		UPDATE jobs SET
			state      = $1::job_state,
			attempt    = $2,
			last_error = $3,
			run_at     = COALESCE($4, run_at),
			claim_token = NULL,
			deadline   = NULL
		WHERE id = $5`,
		newState, newAttempt, errMsg, runAt, jobID)
	if err != nil {
		return fmt.Errorf("update failed job: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE job_runs SET state = $1::job_state, finished_at = $2, error = $3,
		    duration_ms = EXTRACT(EPOCH FROM ($2 - started_at))::INT * 1000
		WHERE job_id = $4 AND finished_at IS NULL`,
		newState, now, errMsg, jobID)
	if err != nil {
		return fmt.Errorf("update job_run: %w", err)
	}

	return tx.Commit(ctx)
}

// GetDueJobs returns up to limit scheduled jobs whose run_at is in the past.
func GetDueJobs(ctx context.Context, db *pgxpool.Pool, limit int) ([]*job.Job, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs
		WHERE state = 'scheduled' AND run_at <= NOW()
		ORDER BY priority, run_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("get due jobs: %w", err)
	}
	defer rows.Close()
	return collectJobs(rows)
}

// GetFailedReadyJobs returns failed jobs whose run_at is now due for retry.
func GetFailedReadyJobs(ctx context.Context, db *pgxpool.Pool, limit int) ([]*job.Job, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs
		WHERE state = 'failed' AND run_at <= NOW()
		ORDER BY priority, run_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("get failed ready jobs: %w", err)
	}
	defer rows.Close()
	return collectJobs(rows)
}

// GetStaleClaims returns claimed/running jobs whose deadline has passed.
func GetStaleClaims(ctx context.Context, db *pgxpool.Pool) ([]*job.Job, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs
		WHERE state IN ('claimed', 'running') AND deadline < NOW()`)
	if err != nil {
		return nil, fmt.Errorf("get stale claims: %w", err)
	}
	defer rows.Close()
	return collectJobs(rows)
}

// RequeueStaleJob moves a stale job back to pending and bumps attempt count.
func RequeueStaleJob(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID) error {
	_, err := db.Exec(ctx, `
		UPDATE jobs SET
			state       = 'pending',
			attempt     = attempt + 1,
			claim_token = NULL,
			claimed_at  = NULL,
			claimed_by  = NULL,
			deadline    = NULL,
			last_error  = 'worker heartbeat expired'
		WHERE id = $1 AND state IN ('claimed', 'running')`, jobID)
	if err != nil {
		return fmt.Errorf("requeue stale job %s: %w", jobID, err)
	}
	return nil
}

// MoveToDeadLetter moves a dead job into the dead_letter table.
func MoveToDeadLetter(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO dead_letter (job_id, tenant_id, final_error, attempt_count, original_job)
		SELECT id, tenant_id, last_error, attempt, to_jsonb(jobs.*)
		FROM jobs
		WHERE id = $1 AND state = 'dead'
		ON CONFLICT (job_id) DO NOTHING`, jobID)
	if err != nil {
		return fmt.Errorf("insert dead_letter: %w", err)
	}

	return tx.Commit(ctx)
}

type ListFilter struct {
	TenantID *uuid.UUID
	State    *job.State
	Limit    int
	Offset   int
}

func ListJobs(ctx context.Context, db *pgxpool.Pool, f ListFilter) ([]*job.Job, error) {
	if f.Limit == 0 {
		f.Limit = 50
	}
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs
		WHERE ($1::uuid IS NULL OR tenant_id = $1)
		  AND ($2::job_state IS NULL OR state = $2)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4`,
		f.TenantID, statePtr(f.State), f.Limit, f.Offset)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	return collectJobs(rows)
}

func CancelJob(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID) error {
	tag, err := db.Exec(ctx, `
		DELETE FROM jobs WHERE id = $1 AND state IN ('pending', 'scheduled')`, jobID)
	if err != nil {
		return fmt.Errorf("cancel job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// PromoteScheduledToPending moves a scheduled job to pending when its run_at has arrived.
func PromoteScheduledToPending(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID) error {
	_, err := db.Exec(ctx, `
		UPDATE jobs SET state = 'pending'
		WHERE id = $1 AND state = 'scheduled'`, jobID)
	if err != nil {
		return fmt.Errorf("promote scheduled job %s: %w", jobID, err)
	}
	return nil
}

// PromoteFailedToPending moves a failed job back to pending when its backoff has elapsed.
func PromoteFailedToPending(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID) error {
	_, err := db.Exec(ctx, `
		UPDATE jobs SET state = 'pending', run_at = NOW()
		WHERE id = $1 AND state = 'failed'`, jobID)
	if err != nil {
		return fmt.Errorf("promote failed job %s: %w", jobID, err)
	}
	return nil
}

func scanJob(row pgx.Row) (*job.Job, error) {
	var j job.Job
	var payload []byte
	var state string
	err := row.Scan(
		&j.ID, &j.TenantID, &j.Type, &payload, &j.Priority, &state,
		&j.RunAt, &j.ClaimedAt, &j.ClaimedBy, &j.ClaimToken, &j.Deadline,
		&j.Attempt, &j.MaxRetries, &j.BackoffSeconds, &j.IdempotencyKey,
		&j.LastError, &j.CreatedAt, &j.CompletedAt,
	)
	if err != nil {
		return nil, err
	}
	j.Payload = json.RawMessage(payload)
	j.State = job.State(state)
	return &j, nil
}

func collectJobs(rows pgx.Rows) ([]*job.Job, error) {
	var jobs []*job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job row: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// DeadState returns a pointer to job.StateDead for use in ListFilter.
func DeadState() *job.State {
	s := job.StateDead
	return &s
}

func statePtr(s *job.State) *string {
	if s == nil {
		return nil
	}
	v := string(*s)
	return &v
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "23505")
}

// ---------------------------------------------------------------------------
// Schedule CRUD
// ---------------------------------------------------------------------------

type Schedule struct {
	ID          uuid.UUID
	TenantID    uuid.UUID
	Name        string
	Cron        string
	Timezone    string
	JobTemplate json.RawMessage
	Enabled     bool
	LastRunAt   *time.Time
	NextRunAt   time.Time
}

func InsertSchedule(ctx context.Context, db *pgxpool.Pool, s *Schedule) error {
	_, err := db.Exec(ctx, `
		INSERT INTO schedules (id, tenant_id, name, cron, timezone, job_template, enabled, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		s.ID, s.TenantID, s.Name, s.Cron, s.Timezone, []byte(s.JobTemplate), s.Enabled, s.NextRunAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("schedule name already exists: %w", ErrDuplicate)
		}
		return fmt.Errorf("insert schedule: %w", err)
	}
	return nil
}

func GetSchedule(ctx context.Context, db *pgxpool.Pool, id uuid.UUID) (*Schedule, error) {
	row := db.QueryRow(ctx, `
		SELECT id, tenant_id, name, cron, timezone, job_template, enabled, last_run_at, next_run_at
		FROM schedules WHERE id = $1`, id)
	return scanSchedule(row)
}

func ListSchedules(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID) ([]*Schedule, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, name, cron, timezone, job_template, enabled, last_run_at, next_run_at
		FROM schedules WHERE tenant_id = $1 ORDER BY name`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	defer rows.Close()
	var out []*Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func DeleteSchedule(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, tenantID uuid.UUID) error {
	tag, err := db.Exec(ctx, `DELETE FROM schedules WHERE id = $1 AND tenant_id = $2`, id, tenantID)
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func GetDueSchedules(ctx context.Context, db *pgxpool.Pool, limit int) ([]*Schedule, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, name, cron, timezone, job_template, enabled, last_run_at, next_run_at
		FROM schedules
		WHERE enabled = TRUE AND next_run_at <= NOW()
		ORDER BY next_run_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("get due schedules: %w", err)
	}
	defer rows.Close()
	var out []*Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func UpdateScheduleAfterRun(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, lastRunAt, nextRunAt time.Time) error {
	_, err := db.Exec(ctx, `
		UPDATE schedules SET last_run_at = $1, next_run_at = $2
		WHERE id = $3`, lastRunAt, nextRunAt, id)
	return err
}

func scanSchedule(row pgx.Row) (*Schedule, error) {
	var s Schedule
	var tmpl []byte
	err := row.Scan(&s.ID, &s.TenantID, &s.Name, &s.Cron, &s.Timezone,
		&tmpl, &s.Enabled, &s.LastRunAt, &s.NextRunAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan schedule: %w", err)
	}
	s.JobTemplate = json.RawMessage(tmpl)
	return &s, nil
}

// ---------------------------------------------------------------------------
// Tenant lookup
// ---------------------------------------------------------------------------

type Tenant struct {
	ID          uuid.UUID
	Name        string
	APIKey      string
	RateLimit   int
	Status      string
}

func GetTenantByAPIKey(ctx context.Context, db *pgxpool.Pool, apiKey string) (*Tenant, error) {
	var t Tenant
	err := db.QueryRow(ctx, `
		SELECT id, name, api_key, rate_limit, status
		FROM tenants WHERE api_key = $1 AND status = 'active'`, apiKey).
		Scan(&t.ID, &t.Name, &t.APIKey, &t.RateLimit, &t.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	return &t, nil
}
