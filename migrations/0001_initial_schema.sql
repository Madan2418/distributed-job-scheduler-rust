-- ============================================================
-- Migration: 0001_initial_schema.sql
-- Distributed Job Scheduler — Full Schema
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- ENUMS
-- ────────────────────────────────────────────────────────────

CREATE TYPE job_status AS ENUM (
    'pending', 'queued', 'claimed', 'running', 'completed', 'scheduled', 'failed'
);

CREATE TYPE job_type AS ENUM (
    'immediate', 'delayed', 'scheduled', 'recurring', 'batch'
);

CREATE TYPE job_priority AS ENUM (
    'low', 'normal', 'high', 'critical'
);

CREATE TYPE backoff_strategy AS ENUM (
    'fixed', 'linear', 'exponential'
);

CREATE TYPE user_role AS ENUM (
    'admin', 'member', 'viewer'
);

CREATE TYPE outbox_event_type AS ENUM (
    'job_completed', 'job_failed', 'job_retry_scheduled',
    'job_moved_to_dlq', 'job_claimed', 'queue_paused', 'queue_resumed'
);

-- ────────────────────────────────────────────────────────────
-- CORE TABLES
-- ────────────────────────────────────────────────────────────

CREATE TABLE organizations (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name  TEXT,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE refresh_tokens (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash  TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE projects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(organization_id, slug)
);

CREATE TABLE project_members (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role        user_role NOT NULL DEFAULT 'member',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, user_id)
);

-- ────────────────────────────────────────────────────────────
-- RETRY POLICIES
-- ────────────────────────────────────────────────────────────

CREATE TABLE retry_policies (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 TEXT NOT NULL,
    max_attempts         INT NOT NULL DEFAULT 3,
    backoff_strategy     backoff_strategy NOT NULL DEFAULT 'exponential',
    base_delay_seconds   BIGINT NOT NULL DEFAULT 10,
    max_delay_seconds    BIGINT,
    multiplier           DOUBLE PRECISION DEFAULT 2.0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- QUEUES
-- ────────────────────────────────────────────────────────────

CREATE TABLE queues (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id        UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    description       TEXT,
    concurrency_limit INT NOT NULL DEFAULT 10,
    retry_policy_id   UUID REFERENCES retry_policies(id),
    is_paused         BOOLEAN NOT NULL DEFAULT FALSE,
    shard             INT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, name)
);

-- ────────────────────────────────────────────────────────────
-- WORKERS
-- ────────────────────────────────────────────────────────────

CREATE TABLE workers (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT NOT NULL,
    hostname         TEXT NOT NULL,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    registered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deregistered_at  TIMESTAMPTZ
);

CREATE TABLE worker_heartbeats (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_id   UUID NOT NULL REFERENCES workers(id) ON DELETE CASCADE UNIQUE,
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active_jobs INT NOT NULL DEFAULT 0
);

-- Index for stale worker detection
CREATE INDEX idx_worker_heartbeats_last_seen ON worker_heartbeats(worker_id, last_seen);

-- ────────────────────────────────────────────────────────────
-- JOBS
-- ────────────────────────────────────────────────────────────

CREATE TABLE jobs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id         UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    project_id       UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    job_type         job_type NOT NULL DEFAULT 'immediate',
    name             TEXT NOT NULL,
    payload          JSONB NOT NULL DEFAULT '{}',
    status           job_status NOT NULL DEFAULT 'queued',
    priority         job_priority NOT NULL DEFAULT 'normal',
    attempt_count    INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 3,
    scheduled_at     TIMESTAMPTZ,
    next_attempt_at  TIMESTAMPTZ,
    cron_expression  TEXT,
    worker_id        UUID REFERENCES workers(id),
    claimed_at       TIMESTAMPTZ,
    idempotency_key  TEXT,
    correlation_id   UUID NOT NULL DEFAULT gen_random_uuid(),
    batch_id         UUID,
    retry_policy_id  UUID REFERENCES retry_policies(id),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ,
    UNIQUE(queue_id, idempotency_key)
);

-- *** THE MOST IMPORTANT INDEX ***
-- Partial index covering only claimable jobs — fast polling query.
-- Ordered by priority DESC then created_at ASC for FIFO within same priority.
CREATE INDEX idx_jobs_claimable
    ON jobs(queue_id, priority DESC, created_at ASC)
    WHERE status = 'queued';

-- For re-queuing scheduled/retry jobs
CREATE INDEX idx_jobs_scheduled
    ON jobs(next_attempt_at)
    WHERE status = 'scheduled';

-- For correlation ID tracing
CREATE INDEX idx_jobs_correlation_id ON jobs(correlation_id);

-- ────────────────────────────────────────────────────────────
-- JOB EXECUTIONS
-- ────────────────────────────────────────────────────────────

CREATE TABLE job_executions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id       UUID NOT NULL,
    attempt_number  INT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    success         BOOLEAN NOT NULL,
    error_message   TEXT,
    duration_ms     BIGINT,
    correlation_id  UUID NOT NULL
);

CREATE INDEX idx_job_executions_job_id ON job_executions(job_id);
CREATE INDEX idx_job_executions_correlation ON job_executions(correlation_id);

-- ────────────────────────────────────────────────────────────
-- JOB LOGS (denormalized for fast dashboard reads)
-- ────────────────────────────────────────────────────────────

CREATE TABLE job_logs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id        UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    execution_id  UUID REFERENCES job_executions(id) ON DELETE CASCADE,
    level         TEXT NOT NULL DEFAULT 'info',
    message       TEXT NOT NULL,
    metadata      JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_job_logs_job_id ON job_logs(job_id);

-- ────────────────────────────────────────────────────────────
-- DEAD LETTER QUEUE
-- ────────────────────────────────────────────────────────────

CREATE TABLE dead_letter_queue (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    queue_id              UUID NOT NULL,
    project_id            UUID NOT NULL,
    job_name              TEXT NOT NULL,
    payload               JSONB NOT NULL,
    last_error            TEXT,
    attempt_count         INT NOT NULL,
    ai_summary            TEXT,
    moved_to_dlq_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    manually_retried_at   TIMESTAMPTZ,
    manually_retried_by   UUID REFERENCES users(id)
);

CREATE INDEX idx_dlq_project_id ON dead_letter_queue(project_id, moved_to_dlq_at DESC);

-- ────────────────────────────────────────────────────────────
-- OUTBOX (Outbox Pattern for reliable pub/sub)
-- ────────────────────────────────────────────────────────────

CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type      outbox_event_type NOT NULL,
    aggregate_id    UUID NOT NULL,
    payload         JSONB NOT NULL,
    published       BOOLEAN NOT NULL DEFAULT FALSE,
    published_at    TIMESTAMPTZ,
    correlation_id  UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Only scan unpublished events in the relay
CREATE INDEX idx_outbox_unpublished ON outbox_events(created_at) WHERE published = FALSE;

-- ────────────────────────────────────────────────────────────
-- WORKFLOW DEPENDENCIES (Bonus: DAG)
-- ────────────────────────────────────────────────────────────

CREATE TABLE job_dependencies (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dependent_job_id  UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    dependency_job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    UNIQUE(dependent_job_id, dependency_job_id)
);

CREATE INDEX idx_job_deps_dependent ON job_dependencies(dependent_job_id);
CREATE INDEX idx_job_deps_dependency ON job_dependencies(dependency_job_id);

-- ────────────────────────────────────────────────────────────
-- AUDIT LOGS (Bonus: RBAC audit trail)
-- ────────────────────────────────────────────────────────────

CREATE TABLE audit_logs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      UUID REFERENCES users(id),
    project_id   UUID REFERENCES projects(id),
    action       TEXT NOT NULL,
    resource     TEXT NOT NULL,
    resource_id  UUID,
    metadata     JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- SEED: Default retry policy
-- ────────────────────────────────────────────────────────────

INSERT INTO retry_policies (id, name, max_attempts, backoff_strategy, base_delay_seconds, max_delay_seconds, multiplier)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'Default Exponential',
    5,
    'exponential',
    10,
    300,
    2.0
);
