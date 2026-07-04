# Distributed Job Scheduler — Architecture Document

**Project:** Distributed Job Scheduling Platform
**Prepared for:** Codity.ai Intern Assignment
**Stack:** Rust (Axum + Tokio + SQLx) · PostgreSQL · Redis · React + TypeScript + Vite · Docker Compose

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architectural Style](#2-architectural-style)
3. [Folder Structure](#3-folder-structure)
4. [Layering & Domain Model](#4-layering--domain-model)
5. [Component Diagram](#5-component-diagram)
6. [Sequence Diagrams](#6-sequence-diagrams)
7. [Database Design](#7-database-design)
8. [Concurrency Model](#8-concurrency-model)
9. [Reliability Mechanisms](#9-reliability-mechanisms)
10. [Design Patterns Used](#10-design-patterns-used)
11. [API Design](#11-api-design)
12. [Security](#12-security)
13. [Observability](#13-observability)
14. [Bonus Features](#14-bonus-features)
15. [Testing Strategy](#15-testing-strategy)
16. [Deployment](#16-deployment)
17. [Design Decisions & Trade-offs](#17-design-decisions--trade-offs)
18. [Future Work](#18-future-work)

---

## 1. Overview

This system is a production-inspired distributed job scheduling platform capable of reliably executing asynchronous background jobs across multiple workers. It supports immediate, delayed, scheduled, recurring (cron), and batch jobs, with full lifecycle tracking, configurable retry strategies, a Dead Letter Queue for permanent failures, and a live dashboard for operational visibility.

The design prioritizes **engineering rigor over feature count**: correctness of concurrent job claiming, reliability under failure, and a well-reasoned database schema take precedence over UI polish or breadth of features.

---

## 2. Architectural Style

The system combines two architectural styles:

- **Distributed System Architecture** — the core structural choice. Multiple worker instances run as independent, stateless peers. They do not communicate with each other directly; instead they coordinate through a shared PostgreSQL database and Redis layer.
- **Event-Driven Architecture** — layered on top for lifecycle propagation. State transitions (a job completing, failing, or moving to the DLQ) emit events that drive downstream effects: retry scheduling, dashboard updates, and workflow continuation.

### Why master-less over master-slave

A common alternative design uses a central dispatcher (master) that assigns jobs to workers (slaves). This project deliberately avoids that model:

- A master node is a single point of failure and a scaling bottleneck.
- A master-less pool where workers **compete** for jobs (via atomic database claiming) is simpler to reason about, scales horizontally without coordination overhead, and matches how mature systems in this space (Sidekiq, River, Temporal workers) are built.

### Why a modular monolith over microservices

The system is implemented as a single Rust workspace with clearly separated crates/modules (`api`, `worker`, `scheduler`, `domain`, etc.) rather than as independently deployed microservices.

- Microservices would introduce network boundaries, service discovery, and deployment complexity disproportionate to this project's scope.
- A modular monolith still enforces separation of concerns and could be split into services later if genuinely needed — the internal boundaries are drawn as if it might be.

---

## 3. Folder Structure

```
scheduler/
├── api/
│   ├── handlers/          # HTTP request handlers (thin, delegate to services)
│   ├── middleware/         # auth, rate-limit, validation, authz (chain of responsibility)
│   └── routes/             # route registration
├── worker/
│   ├── poller/             # polls queues for claimable jobs
│   ├── executor/           # executes claimed jobs concurrently
│   └── heartbeat/          # periodic liveness signal to DB/Redis
├── scheduler/
│   ├── cron/                # cron expression parsing & scheduling
│   └── recurring/           # recurring job re-enqueue logic
├── domain/                 # Job, Queue, RetryPolicy, Worker, Project — pure entities & rules
├── services/                # orchestration logic, no HTTP/SQL awareness
├── repositories/            # SQLx queries, isolated from services
├── common/                  # errors, tracing setup, correlation ID propagation
└── infrastructure/          # DB pool, Redis client, LLM client, outbox relay
```

This structure makes the architecture legible from the file tree alone — a reviewer can see the layering without reading code.

---

## 4. Layering & Domain Model

```
        API
         │
         ▼
      Service
         │
         ▼
      Domain  ◄──────┐
         ▲            │
         │            │
    Repository ───────┘
         │
         ▼
      Database
```

The **Domain layer** sits at the center. It contains pure Rust entities and business rules with no awareness of HTTP or SQL:

- `Job` — payload, type, priority, lifecycle state
- `Queue` — concurrency limits, retry policy reference, pause/resume state
- `RetryPolicy` — backoff strategy and parameters
- `Worker` — identity, heartbeat state
- `Project` — ownership boundary for queues

Both `Service` and `Repository` depend on `Domain`, but `Domain` depends on neither. This is a dependency-inversion shape: the core business rules are insulated from infrastructure and delivery mechanisms, so either can change without touching domain logic.

---

## 5. Component Diagram

```
┌─────────────────┐
│ React Dashboard  │
└────────┬─────────┘
         │ HTTP / WebSocket
         ▼
┌─────────────────┐
│       API         │
└────────┬─────────┘
         ▼
┌─────────────────┐
│  Service Layer    │
└────────┬─────────┘
         ▼
┌─────────────────┐        ┌─────────────────┐
│   Repository      │◄─────►│    PostgreSQL      │
└─────────────────┘        └────────┬─────────┘
                                       │
                                       ▼
                             ┌─────────────────┐
                             │   Worker Pool      │
                             └────────┬─────────┘
                                       ▼
                             ┌─────────────────┐
                             │      Redis          │
                             └────────┬─────────┘
                                       ▼
                             ┌─────────────────┐
                             │   WebSockets        │
                             └─────────────────┘
```

---

## 6. Sequence Diagrams

### 6.1 Job Submission

```
Client → API: POST /queues/:id/jobs
API → Service: validate + build Job (Factory)
Service → Repository: INSERT job (status=Queued)
Repository → DB: transaction commit
API → Client: 201 Created (job_id, correlation_id)
```

### 6.2 Job Execution

```
Worker (Poller) → DB: SELECT ... FOR UPDATE SKIP LOCKED
DB → Worker: locked job row (status=Queued → Claimed)
Worker → DB: UPDATE status=Running, worker_id, claimed_at
Worker (Executor) → Job Handler: execute(payload)
Job Handler → Worker: result (success/failure)
Worker → DB: INSERT job_execution, UPDATE status=Completed
Worker → Outbox: write completion event (same transaction)
Outbox Relay → Redis Pub/Sub → WebSocket → Dashboard: live update
```

### 6.3 Retry

```
Job Execution → fails
Worker → RetryPolicy (Strategy): calculate next delay
Worker → DB: UPDATE status=Scheduled, next_attempt_at, attempt_count++
Scheduler (Recurring/Cron module) → DB: re-surfaces job when next_attempt_at reached
Worker → DB: SELECT ... FOR UPDATE SKIP LOCKED (job reclaimed)
```

### 6.4 Dead Letter Queue

```
Job Execution → fails
Worker → checks attempt_count >= max_attempts (RetryPolicy)
    if true:
        Worker → DB: UPDATE status=Failed
        Worker → DB: INSERT dead_letter_queue entry
        Worker → Outbox: write DLQ event
        Outbox Relay → WebSocket → Dashboard: DLQ alert
    if false:
        → proceed to Retry sequence (6.3)
```

### 6.5 Workflow Dependency (Saga)

```
Client → API: POST /workflows (job DAG definition)
API → Service: persist job_dependencies (edges)
Worker → completes Job A
Worker → Service: check job_dependencies for Job A
Service → DB: find dependent Job B, verify all its dependencies satisfied
    if satisfied:
        Service → DB: transition Job B: Pending → Queued
    if Job A failed and Job B has no compensation defined:
        Service → Saga Coordinator: trigger compensating actions for
                  already-completed upstream jobs in the workflow
```

---

## 7. Database Design

### Core Entities
Users, Organizations, Projects, Queues, Jobs, Job Executions, Retry Policies, Workers, Worker Heartbeats, Job Logs, Scheduled Jobs, Dead Letter Queue entries

### Bonus-Driven Entities
`job_dependencies` (workflow DAG edges), `queue_shards`, `roles` / `permissions` (RBAC), `audit_logs`, `rate_limit_buckets`, `distributed_locks`

### Key Design Choices

- **Normalization**: core entities (Users, Projects, Queues, Jobs) follow 3NF.
- **Deliberate denormalization**: `job_logs` and execution-history tables favor flatter, read-optimized structure since the dashboard's read patterns matter more than strict normalization here.
- **ACID transactions**: job claim + status update occur atomically in a single transaction.
- **Outbox Pattern**: on state change, the event to notify (WebSocket/Redis) is written in the *same transaction* as the state change itself, avoiding dual-write inconsistency between the database and the notification layer. A separate relay process reads unpublished outbox rows and publishes them.
- **Correlation IDs**: every `job_executions` row carries an indexed `correlation_id`, allowing a single job's full trace (claim → execution → retry → DLQ → notification) to be queried end-to-end.
- **Indexing**: partial index on `jobs(status, queue_id, priority, created_at)` for fast polling; index on `worker_heartbeats(worker_id, last_seen)` for liveness checks.
- **Cascading**: project deletion cascades to its queues and jobs; job deletion cascades to its executions and logs (not the reverse).
- **Referential integrity**: foreign keys enforced across all relationships; no orphaned executions or logs.

---

## 8. Concurrency Model

### Atomic Job Claiming

The single most important mechanism in the system — ensures no two workers ever execute the same job:

```sql
SELECT * FROM jobs
WHERE status = 'queued' AND queue_id = $1
ORDER BY priority DESC, created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

`FOR UPDATE SKIP LOCKED` allows one worker to lock a row while other concurrent workers simply skip it and move to the next candidate, rather than blocking. This is the same pattern used by mature Postgres-backed queue systems (pgboss, River).

### Worker Pool

- Built on **Tokio**, Rust's async runtime.
- Each worker instance runs N concurrent execution tasks, bounded by the queue's configured concurrency limit.
- **Idempotent execution**: job handlers are written so that re-execution (due to retry or crash-recovery) does not double-apply side effects.
- **Graceful shutdown**: on SIGTERM, a worker stops claiming new jobs, allows in-flight jobs to complete, and deregisters its heartbeat before exiting.

---

## 9. Reliability Mechanisms

- **Fail Fast**: job payloads are validated at submission time, not at execution time.
- **Graceful Degradation**: if the WebSocket connection drops, the dashboard falls back to polling rather than losing visibility entirely.
- **State Pattern**: the job lifecycle (`Queued → Scheduled → Claimed → Running → Completed / Failed`) is modeled as an explicit state machine rather than a loose enum with scattered conditional checks.
- **Strategy Pattern**: retry backoff calculation (fixed delay, linear backoff, exponential backoff) is implemented as interchangeable strategy objects behind a common trait.
- **Circuit Breaker**: the AI failure-summary call to an external LLM API is wrapped in a circuit breaker so that LLM downtime never blocks core job processing.
- **Dead Letter Queue**: jobs exceeding `max_attempts` move to a DLQ entry rather than retrying indefinitely, with manual retry available from the dashboard.

---

## 10. Design Patterns Used

| Pattern | Where | Why |
|---|---|---|
| Strategy | Retry backoff calculation | Interchangeable delay algorithms behind one interface |
| State | Job lifecycle | Explicit, testable state machine instead of scattered conditionals |
| Factory | Job creation | One entry point dispatching to immediate/delayed/scheduled/recurring/batch construction |
| Observer | Job state change notifications | Decouples state transitions from their consumers (logging, metrics, WebSocket) |
| Repository | Data access | Isolates SQLx queries from service/business logic |
| Service Layer | Business logic | Keeps orchestration out of HTTP handlers |
| Dependency Injection | Testability | Swap real DB/Redis for test doubles in unit tests |
| Outbox | State-change notification | Prevents dual-write bugs between DB and pub/sub layer |
| Saga | Workflow dependencies (bonus) | Multi-step job DAGs with compensation logic on failure |
| Chain of Responsibility | API middleware pipeline | Auth → rate limit → validation → authorization, each stage can short-circuit |

**Note on a pattern deliberately *not* used for retries:** Chain of Responsibility was initially considered for retry escalation but rejected — CoR is about passing a request through a sequence of handlers until one processes it, which doesn't match retry logic (a single strategy recalculating a delay, followed by a state transition). Strategy + State fully cover this. CoR is instead applied correctly to the middleware pipeline, where it fits naturally.

---

## 11. API Design

- RESTful, resource-oriented (`/projects/:id/queues`, `/queues/:id/jobs`)
- Versioned under `/v1`
- Consistent error response format across all endpoints
- Pagination and filtering as first-class query parameters
- **Idempotent job creation** via client-supplied idempotency keys, preventing duplicate submission on retry
- Every response includes the request's correlation ID in response headers for client-side traceability

---

## 12. Security

- JWT-based authentication (access + refresh tokens)
- **RBAC**: roles (`admin`, `member`, `viewer`) scoped per project, enforced via middleware
- **Least Privilege** and **Zero Trust** posture — every request is independently validated; no implicit trust between internal components
- Input validation and output encoding on all endpoints
- Secrets managed via environment variables, never hardcoded

---

## 13. Observability

### Correlation IDs
- Every incoming HTTP request receives a **Request ID**.
- Every job receives a **Correlation ID** at creation time, propagated through every execution attempt, log line, and outbox event.
- Both flow through `tracing` spans automatically, enabling full request/job tracing across API → worker → retry → DLQ.

### Health Endpoints
```
/health   — overall system health summary
/ready    — readiness probe (DB/Redis reachable, safe to receive traffic)
/live     — liveness probe (process alive, no dependency checks)
```
Mirrors standard Kubernetes probe conventions.

### Metrics
- Queue depth
- Jobs per second (throughput)
- Retry count (per job, per queue)
- Worker utilization
- Average execution time
- Dead Letter Queue size
- Heartbeat latency

### Logging
Structured logging via the `tracing` crate, with correlation IDs attached to every log line for cross-service traceability. Audit logs capture privileged actions (pausing a queue, manually retrying a job) tied to the acting user's role.

---

## 14. Bonus Features

| Bonus | Implementation |
|---|---|
| Workflow dependencies | `job_dependencies` table + Saga Pattern for DAG execution and compensation |
| Rate limiting | Redis token-bucket algorithm, enforced via Axum middleware |
| Distributed locking | Redlock pattern over Redis, for cross-worker mutual exclusion beyond row-level DB locks |
| Queue sharding | `hash(queue_id) % N`; stateless workers pick a shard range to poll |
| Event-driven execution | Extension of the existing Outbox/pub-sub event flow |
| WebSocket live updates | Axum native WebSocket support, fed by the Outbox relay via Redis pub/sub |
| RBAC | Role/permission tables, enforcement middleware, and conditional UI rendering |
| AI-generated failure summaries | Async call to an LLM API on job failure, guarded by a Circuit Breaker |

---

## 15. Testing Strategy

- **Unit tests**: retry-strategy calculations, state machine transitions, Saga step resolution
- **Integration tests**: atomic-claim correctness under concurrency — spin up a test database, fire concurrent claim attempts, assert exactly one worker wins. This is the single most direct evidence of correct concurrency handling.
- **CI pipeline**: GitHub Actions running the full test suite on every push
- **Explicitly out of scope**: formal load/stress testing — noted under Future Work rather than built, to keep effort proportionate to rubric weight

---

## 16. Deployment

- **Docker Compose** stack: PostgreSQL, Redis, API service, worker service(s), frontend — brought up with a single command
- `.env.example` provided for configuration
- Path to Kubernetes acknowledged (not built) — the `/ready` and `/live` endpoints are designed to plug directly into K8s probes if this system were deployed there

---

## 17. Design Decisions & Trade-offs

| Decision | Alternative Considered | Why This Choice |
|---|---|---|
| Master-less worker pool | Master-slave dispatcher | Avoids single point of failure; scales without central coordination |
| Modular monolith | Microservices | Avoids disproportionate network/deployment complexity for this scope |
| Postgres `FOR UPDATE SKIP LOCKED` | Message broker (e.g., RabbitMQ/Kafka) for job distribution | Keeps the system to one source of truth for job state; avoids dual-consistency problems between a broker and the DB |
| Outbox Pattern | Direct publish to Redis after DB write | Prevents dual-write inconsistency (DB write succeeds, publish fails, or vice versa) |
| Denormalized log tables | Fully normalized (3NF) logs | Read-heavy dashboard queries benefit more from flatter structure than strict normalization |
| Strategy + State for retries | Chain of Responsibility for retries | CoR models sequential handler delegation, not delay-calculation + state transition; better fit was rejected in favor of accurate modeling |
| Rust (Axum/Tokio) | Go, Python, Node | Best alignment with the project's core requirement (verifiable, explicit concurrency and reliability handling) and with existing team experience |

---

## 18. Future Work

- Multi-region deployment
- Kafka/NATS in place of Redis Pub/Sub as the event backbone, for stronger delivery guarantees at scale
- Kubernetes autoscaling, building directly on the `/ready` / `/live` probes already implemented
- Full distributed tracing (OpenTelemetry spans), extending the correlation ID work already in place
- Multi-database support
- Formal load and stress testing
