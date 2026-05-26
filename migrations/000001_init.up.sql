CREATE TYPE job_state AS ENUM (
    'pending',
    'scheduled',
    'claimed',
    'running',
    'succeeded',
    'failed',
    'dead'
);

CREATE TABLE jobs (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    type            TEXT NOT NULL,
    payload         JSONB NOT NULL,
    priority        SMALLINT NOT NULL DEFAULT 5,
    state           job_state NOT NULL DEFAULT 'pending',
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at      TIMESTAMPTZ,
    claimed_by      TEXT,
    claim_token     UUID,
    deadline        TIMESTAMPTZ,
    attempt         INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL DEFAULT 3,
    backoff_seconds INT NOT NULL DEFAULT 30,
    idempotency_key TEXT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,

    CONSTRAINT idempotency_unique UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX idx_jobs_scheduled
    ON jobs (run_at, priority)
    WHERE state = 'scheduled';

CREATE INDEX idx_jobs_in_flight
    ON jobs (deadline)
    WHERE state IN ('claimed', 'running');

CREATE INDEX idx_jobs_pending
    ON jobs (priority, created_at)
    WHERE state = 'pending';

CREATE INDEX idx_jobs_tenant_recent
    ON jobs (tenant_id, created_at DESC);

CREATE TABLE schedules (
    id           UUID PRIMARY KEY,
    tenant_id    UUID NOT NULL,
    name         TEXT NOT NULL,
    cron         TEXT NOT NULL,
    timezone     TEXT NOT NULL DEFAULT 'UTC',
    job_template JSONB NOT NULL,
    enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at  TIMESTAMPTZ,
    next_run_at  TIMESTAMPTZ NOT NULL,

    UNIQUE (tenant_id, name)
);

CREATE INDEX idx_schedules_due ON schedules (next_run_at) WHERE enabled = TRUE;

CREATE TABLE job_runs (
    id          UUID PRIMARY KEY,
    job_id      UUID NOT NULL REFERENCES jobs(id),
    tenant_id   UUID NOT NULL,
    attempt     INT NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    state       job_state NOT NULL,
    error       TEXT,
    duration_ms INT
);

CREATE INDEX idx_job_runs_job ON job_runs (job_id);
CREATE INDEX idx_job_runs_tenant ON job_runs (tenant_id, started_at DESC);

CREATE TABLE dead_letter (
    job_id        UUID PRIMARY KEY,
    tenant_id     UUID NOT NULL,
    moved_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    final_error   TEXT,
    attempt_count INT NOT NULL,
    original_job  JSONB NOT NULL
);

CREATE INDEX idx_dead_letter_tenant ON dead_letter (tenant_id, moved_at DESC);
