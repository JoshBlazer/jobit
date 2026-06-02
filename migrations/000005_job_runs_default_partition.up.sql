-- Backstop partition so inserts never fail with "no partition found for row"
-- if the scheduler loop hasn't created next month's partition yet.
-- Monthly partitions take precedence over this one for their date ranges.
CREATE TABLE job_runs_default PARTITION OF job_runs DEFAULT;
