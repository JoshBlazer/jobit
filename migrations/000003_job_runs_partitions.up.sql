-- Convert job_runs to a partitioned table.
-- We rename the existing unpartitioned table, recreate it as partitioned,
-- migrate any existing rows, then drop the old table.

DROP INDEX IF EXISTS idx_job_runs_job;
DROP INDEX IF EXISTS idx_job_runs_tenant;
ALTER TABLE job_runs RENAME TO job_runs_old;

CREATE TABLE job_runs (
    id          UUID NOT NULL,
    job_id      UUID NOT NULL,
    tenant_id   UUID NOT NULL,
    attempt     INT NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    state       job_state NOT NULL,
    error       TEXT,
    duration_ms INT
) PARTITION BY RANGE (started_at);

-- Seed partitions covering the next 12 months from now.
-- The scheduler should create new partitions ahead of time (Phase 3 ops concern).
CREATE TABLE job_runs_2026_01 PARTITION OF job_runs
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE job_runs_2026_02 PARTITION OF job_runs
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE job_runs_2026_03 PARTITION OF job_runs
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE job_runs_2026_04 PARTITION OF job_runs
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE job_runs_2026_05 PARTITION OF job_runs
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE job_runs_2026_06 PARTITION OF job_runs
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE job_runs_2026_07 PARTITION OF job_runs
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE job_runs_2026_08 PARTITION OF job_runs
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE job_runs_2026_09 PARTITION OF job_runs
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE job_runs_2026_10 PARTITION OF job_runs
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE job_runs_2026_11 PARTITION OF job_runs
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE job_runs_2026_12 PARTITION OF job_runs
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
CREATE TABLE job_runs_2027_01 PARTITION OF job_runs
    FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');

CREATE INDEX idx_job_runs_job      ON job_runs (job_id);
CREATE INDEX idx_job_runs_tenant   ON job_runs (tenant_id, started_at DESC);

-- Migrate existing rows
INSERT INTO job_runs SELECT * FROM job_runs_old;
DROP TABLE job_runs_old;
