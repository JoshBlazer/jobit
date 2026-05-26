CREATE TABLE tenants (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT NOT NULL,
    api_key      TEXT NOT NULL UNIQUE,
    rate_limit   INT  NOT NULL DEFAULT 100,  -- max jobs submitted per second
    status       TEXT NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dev tenant — api_key matches the README quick-start examples.
-- Remove or replace before any non-local deployment.
INSERT INTO tenants (id, name, api_key, rate_limit)
VALUES ('00000000-0000-0000-0000-000000000001', 'dev', 'dev-token', 1000);
