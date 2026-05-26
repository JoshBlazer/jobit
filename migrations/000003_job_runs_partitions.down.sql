-- Restore unpartitioned job_runs
CREATE TABLE job_runs_unpart (
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

INSERT INTO job_runs_unpart SELECT * FROM job_runs;

DROP TABLE job_runs;
ALTER TABLE job_runs_unpart RENAME TO job_runs;

CREATE INDEX idx_job_runs_job    ON job_runs (job_id);
CREATE INDEX idx_job_runs_tenant ON job_runs (tenant_id, started_at DESC);
