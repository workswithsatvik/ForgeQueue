CREATE TABLE IF NOT EXISTS fq_jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue text NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    idempotency_key text,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','running','succeeded','dead')),
    priority integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    generation bigint NOT NULL DEFAULT 0,
    owner text,
    lease_until timestamptz,
    last_error text,
    UNIQUE (queue, idempotency_key),
    CHECK ((state = 'running') = (owner IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS fq_ready ON fq_jobs(queue, priority DESC, available_at, id) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS fq_expired ON fq_jobs(queue, lease_until) WHERE state = 'running';
CREATE TABLE IF NOT EXISTS fq_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES fq_jobs(id) ON DELETE CASCADE,
    event text NOT NULL,
    generation bigint NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS fq_events_job ON fq_events(job_id);
-- Deliberately no unique job_id constraint: the test can detect duplicate effects.
CREATE TABLE IF NOT EXISTS fq_demo_effects (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES fq_jobs(id) ON DELETE CASCADE,
    value jsonb NOT NULL
);
