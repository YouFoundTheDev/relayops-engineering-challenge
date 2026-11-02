# SOLUTION

## 1. Overview

The repository started as a partially working event intake and webhook
delivery system: Nginx in front of a FastAPI API, PostgreSQL as storage, a
Python worker, and a mock receiver. The API accepted events but did not
implement idempotency, the worker did not implement the delivery contract, and
the Compose/Nginx configuration prevented several services from talking to each
other.

The final implementation keeps the original API and delivery contracts:

- `POST /events` is transactional and idempotent (`202` new, `200` identical,
  `409` conflicting, `422` invalid).
- `GET /events/{event_id}` returns the event plus its full delivery history.
- `GET /health` is database-aware and returns `503` when the database is
  unavailable.
- `GET /metrics` exports Prometheus text derived from PostgreSQL.
- Workers claim events atomically with `SELECT ... FOR UPDATE SKIP LOCKED`,
  deliver signed webhooks, retry transient failures up to three times, respect
  `Retry-After`, and recover expired processing leases.
- Two workers run from `docker compose up --build`, the database is only on the
  internal network, and the stack is reachable at `http://localhost:8080`.

## 2. Initial investigation

I first read the whole repository, ran the existing tests, inspected the
Compose model, and exercised the original API and worker against local
PostgreSQL and the unmodified mock receiver.

Baseline results:

- `pytest tests` → 4 passed, 2 skipped (the smoke tests need a running stack).
- `docker compose config` parsed, but showed the intentionally broken
  configuration: `receiver=192.0.2.10` in the override file, PostgreSQL
  published on the host, and only `service_started` dependencies.
- Running the original API directly:
  - two identical `POST /events` requests both returned `202`;
  - `GET /events/{id}` always returned `"delivery_attempts": []`;
  - `GET /metrics` returned `404`.
- Running the original worker against the mock receiver:
  - the receiver recorded `signature_valid: false` and answered `401`;
  - the worker still marked the event `delivered` and wrote no attempt rows.

The most important defects found:

| # | Area | Defect | Root cause |
|---|------|--------|------------|
| 1 | API | Duplicate submissions created multiple rows | No unique key on `event_id`, no idempotency logic |
| 2 | API | Shared database connection across threads | One module-global `psycopg` connection |
| 3 | API | No attempt history, metrics, or readiness | Endpoints were incomplete; health always returned `ok` |
| 4 | Worker | Receiver rejected every payload | Signature was computed over a different JSON serialization than the transmitted body |
| 5 | Worker | HTTP errors treated as success | The worker ignored response status codes |
| 6 | Worker | Two workers could deliver the same event | Plain `SELECT ... WHERE status='pending'` with no claim/lock |
| 7 | Worker | No crash recovery, no graceful shutdown | Events had no processing lease; no signal handling |
| 8 | DB | No uniqueness or lease/retry columns | Schema was incomplete |
| 9 | Networking | Worker could not resolve/reach receiver | Override mapped `receiver` to `192.0.2.10` (TEST-NET, black hole) |
| 10 | Networking | Nginx could not reach the API | Uvicorn bound to `127.0.0.1` inside the API container |
| 11 | Networking | `POST /events` bodies were dropped by Nginx | `proxy_pass_request_body off` on `location /events` |
| 12 | Networking | Payloads over 1 KB failed | `client_max_body_size 1k` |
| 13 | Networking | Database exposed on the host | Override published `127.0.0.1::5432` |
| 14 | Ops | No health checks or startup ordering | Only `depends_on` without conditions |
| 15 | CI | Pipeline was a placeholder | No lint, tests, build, or integration run |

## 3. Software engineering

### Event lifecycle

An event is always one of `pending`, `processing`, `delivered`, or `failed`.
The transitions are explicit:

- Insert → `pending`, `next_attempt_at = now()`.
- Claim → `processing`, `locked_at = now()`, `locked_by = worker_id`.
- Success (any `2xx`) → `delivered`, lock cleared.
- Permanent failure (non-`429` `4xx`) → `failed`, no retry.
- Transient failure with attempts remaining → `pending`, lock cleared,
  `next_attempt_at` moved into the future using backoff/`Retry-After`.
- Transient failure on attempt 3 → `failed`.

Every attempt is inserted into `delivery_attempts` in the same transaction as
the event state update. A worker crash can lose an in-flight HTTP response, but
it cannot lose the event itself because the event row remains in PostgreSQL.

### Idempotency

`events.event_id` has a unique index. Submission uses one transaction:

```sql
INSERT INTO events (event_id, type, payload)
VALUES (%s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id, status;
```

If the insert returns a row, the response is `202`. Otherwise the existing row
is selected in the same transaction:

- same `type` and JSON payload → `200` with the current full event state;
- different content → `409`;
- no row after a conflict (a concurrent transaction rolled back) → `503`, so
  the client can retry instead of getting a misleading answer.

The payload comparison uses PostgreSQL JSONB and Python dictionary equality,
so key order does not matter. Concurrent submissions of the same new event are
covered by a dedicated test; exactly one request returns `202` and the rest
return `200`.

### Database locking and worker coordination

Each worker claims one event at a time with:

```sql
SELECT id, status AS previous_status
FROM events
WHERE (status = 'pending' AND next_attempt_at <= now())
   OR (status = 'processing' AND locked_at <= now() - lease)
ORDER BY next_attempt_at, id
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

The claim is an `UPDATE ... RETURNING` inside a transaction, so only one worker
can own the row. `SKIP LOCKED` means other workers move on instead of waiting.

The lease is 60 seconds. A single HTTP attempt is capped at 5 seconds, so a
healthy worker always finishes well inside the lease. A crashed worker leaves
the event in `processing`; after the lease expires another worker claims it and
logs `recovered event with expired lease`.

Retries are **scheduled in the database**, not slept in the worker process.
After a transient failure the worker writes `status='pending'` and
`next_attempt_at = now() + delay`, then starts looking for other work. This
detail matters: an earlier version retried inside the worker after releasing
the row, which allowed a second worker to claim and deliver the same event
concurrently. The integration test for `429` with two workers exposed the
duplicate delivery, and the design was changed so that every attempt is a
separate atomic claim.

### Retry strategy

- Success: any `2xx`.
- Transient: network errors/timeouts, `429`, any `5xx`.
- Permanent: every other status, including `400`, `401`, `404`, and `3xx`.
- Maximum three attempts per event.
- Exponential backoff: `min(base * 2^(attempt-1), 30s)` with base `1s`.
- If `Retry-After` is present (numeric seconds or an HTTP date), the next
  attempt is no earlier than that value; the larger of backoff and
  `Retry-After` is used.
- The HTTP timeout is five seconds per attempt.

### HMAC signing

The worker builds the delivery body, serializes it once with compact JSON
encoding, signs those exact bytes, and sends the same bytes:

```python
raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
signature = "sha256=" + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
client.post(url, content=raw, headers={"X-RelayOps-Signature": signature})
```

The original code signed `json.dumps(body)` (which inserts spaces) but let
httpx serialize the body again. The receiver correctly rejected the mismatch.
Serializing once and using `content=` removes that class of bug. The mock
receiver reports `signature_valid: true` for every delivered event in the
integration tests.

### Crash recovery and graceful shutdown

- An expired lease is the crash-recovery mechanism. No in-memory queue is
  involved.
- On `SIGTERM`/`SIGINT`, the worker stops claiming new events. If a request is
  already in flight it is allowed to finish (it is bounded by the 5-second
  timeout). Because retries are already persisted as `pending` with a future
  `next_attempt_at`, there is no in-memory retry state to lose.
- If shutdown happens before an attempt starts, the claimed event is released
  back to `pending` with its attempt count intact.
- With `stop_grace_period: 20s` both workers exited with code `0` during
  verification, including while a 3-second receiver delay was in progress, and
  no event was left in `processing`.

### API implementation details

- `psycopg_pool.ConnectionPool` replaces the shared global connection; sync
  FastAPI endpoints run in a thread pool, so a single connection was unsafe.
- `/health` runs `SELECT 1` with a 2-second pool timeout and returns `503` if
  it fails.
- `/metrics` aggregates `events` by status and counts `delivery_attempts`
  directly in PostgreSQL.
- A middleware preserves or generates `X-Request-ID`, returns it on every
  response (including errors), stores it in a context variable, and emits it in
  JSON logs. Nginx does the same at the edge, so both layers agree.
- Logs include method/path/status/duration for HTTP and worker/event/attempt/
  outcome/reason/delay for delivery. Payloads and `WEBHOOK_SECRET` are never
  logged.

## 4. Networking

### Problems, diagnosis, and fixes

1. **Receiver DNS black hole.** `docker-compose.override.yml` contained
   `extra_hosts: receiver:192.0.2.10`. That address belongs to TEST-NET-1 and
   is not routable. `getent hosts receiver` inside the worker returned the
   black-hole address, so every delivery failed.
   **Fix:** removed the override file. Compose DNS now returns the receiver
   container address (`172.20.0.2` during verification).

2. **API not reachable by Nginx.** `api/Dockerfile` started uvicorn with
   `--host 127.0.0.1`. Inside a container that only listens on loopback, so
   `proxy_pass http://api:8000` failed.
   **Fix:** bind to `0.0.0.0`. Nginx now proxies successfully.

3. **Nginx dropped `POST /events` bodies.** The `/events` location used
   `proxy_pass_request_body off` and cleared `Content-Length`. Requests reached
   FastAPI without a body, so validation failed.
   **Fix:** removed the special location and the body suppression. `/` handles
   all routes with normal proxying.

4. **1 KB body limit.** `client_max_body_size 1k` rejected the required 1 MB
   payloads.
   **Fix:** set `client_max_body_size 2m`. The extra headroom covers the JSON
   envelope around a 1 MB payload. The integration suite posts a payload of
   1,000,000 characters through the gateway and receives `202`.

5. **Database exposed on the host.** The override published PostgreSQL on
   `127.0.0.1::5432`.
   **Fix:** removed the mapping. `docker inspect` now shows
   `{"5432/tcp": null}`; PostgreSQL is reachable only on the `backend` network.

6. **Request IDs were lost.** Nginx did not forward or return an ID.
   **Fix:** a `map` preserves `X-Request-ID` or falls back to Nginx's
   `$request_id`, sets it upstream, and returns it to the client. Verified with
   `curl -H 'X-Request-ID: interview-rid-123'` and a generated ID when the
   header is absent.

### Network topology

- `frontend`: gateway + API.
- `backend`: API + worker + database.
- `webhooks`: worker + receiver.

Only the gateway (`8080`) and mock receiver (`9000`) publish ports. Services
use Compose DNS names (`db`, `receiver`, `api`), never `localhost`, for other
containers. `depends_on` uses health conditions so startup order is meaningful.

## 5. DevOps and infrastructure

### Docker Compose

- `docker compose up --build` builds and starts the complete stack.
- Health checks exist for `db`, `api`, `worker`, `receiver`, and `gateway`.
- `api` waits for a healthy database; `worker` waits for a healthy database and
  receiver; `gateway` waits for a healthy API.
- `restart: unless-stopped` for long-running services.
- Named volume `pgdata` persists PostgreSQL data. Restarting `db` preserved a
  delivered event and its attempt history.
- Application images run as UID 10001; verified with `id` inside the
  containers.
- The worker runs with `deploy.replicas: 2`; Compose v2 started both replicas.
- `stop_grace_period: 20s` gives the worker time to finish a delivery.

### Configuration and security

- All application configuration comes from environment variables.
- `.env.example` documents the variables and the development-only defaults.
- `WEBHOOK_SECRET` is required by the worker and receiver. There is no empty
  default in application code; Compose supplies a development default so the
  stack works out of the box, and the file warns to replace it.
- PostgreSQL no longer uses `trust` authentication.
- Database access is limited to the internal `backend` network.
- Connection pool sizes are bounded (`DB_POOL_MAX`, worker pool size 2).
- Secret values and event payloads are not logged.

### CI

`.github/workflows/ci.yml` has two jobs:

1. **Lint and unit/database tests** — Python 3.12, dependency install,
   `ruff check .`, `docker compose config -q`, and `pytest -m "not integration"`
   against a PostgreSQL 16 service container.
2. **Docker Compose integration tests** — builds images, starts the stack with
   `--wait`, runs the end-to-end suite against `localhost:8080`/`localhost:9000`,
   collects `docker compose ps` and logs on failure, and always tears the stack
   down with `down -v`.

## 6. Testing and verification

### Automated tests

Unit and database tests (no Compose stack required):

```text
pytest -m "not integration" -v
37 passed, 25 deselected
```

This includes model validation, request-id log formatting, HTTP status
classification, `Retry-After` parsing (seconds and HTTP dates), exponential
backoff, signature correctness over exact bytes, settings validation, and
database-level worker tests:

- two concurrent claims → exactly one winner;
- an active lease is not stolen;
- an expired lease is recovered;
- a future retry is not claimed early;
- release returns an event to the queue;
- attempt history and event state are written atomically;
- simulated network failures produce three attempts and a `failed` event;
- shutdown before delivery releases the event;
- a transient failure schedules a retry in the future;
- a real worker subprocess recovers a stale processing lease, delivers the
  signed payload, and exits `0` on `SIGTERM` (process-level restart/recovery).

End-to-end tests (real Compose stack):

```text
pytest -m integration -v
25 passed
```

Coverage includes valid/invalid submissions, duplicate and conflicting
submissions (including five concurrent duplicates), missing events, request-id
propagation, full attempt history, health, metrics, a 1 MB payload through
Nginx, successful signed delivery, 429/5xx retries, `Retry-After`, permanent
4xx, maximum attempts, invalid HMAC rejection, two-worker single delivery, and
the existing smoke tests.

### Manual verification performed

- `docker compose config -q` → success.
- `docker compose build` → API, worker, and receiver images built.
- `docker compose up -d --wait` → all six services healthy, including two
  worker replicas.
- `docker compose ps` → only `8080` (gateway) and `9000` (receiver) published;
  `db` shows only an internal `5432/tcp`.
- `docker compose exec worker getent hosts db receiver` → resolved to container
  addresses.
- `docker compose exec gateway getent hosts api` → resolved.
- Restarting `db` preserved an event in `delivered` state with its attempt row.
- Simulated a stale processing lease (`locked_at` 120 seconds old); a worker
  recovered the event and logged `recovered event with expired lease`.
- `docker compose stop worker` exited both workers with code `0`, finished an
  in-flight 3-second delivery, and left zero `processing` events.
- With PostgreSQL stopped, `GET /health` through Nginx returned
  `503 {"status":"unhealthy","database":"unavailable"}` and `/metrics` returned
  `503`. After restarting PostgreSQL both returned `200` again.
- Preserved `X-Request-ID` in the gateway response and found the same ID in the
  API JSON log.
- `grep` over all Compose logs found no occurrence of `WEBHOOK_SECRET`'s value.
- The receiver's `/received` entries for successful deliveries all had
  `signature_valid: true`.

### Environment limitation and missing deliverable

The machine initially had the Docker CLI but no running Docker daemon or
Compose plugin. I installed the Compose plugin and started a local Colima VM
to run the real Docker verification above; it is genuine Docker/Compose, not a
simulation. The full stack and networking were verified this way.

`candidate/CV.pdf` is not present in the repository. The task says the CV is
provided separately; I did not create or fabricate one.

## 7. AI-assisted development

I used an AI coding assistant (opencode) for repository analysis, drafting
implementation and tests, and drafting this document. The work was not
line-by-line manual, and I do not claim it was.

What benefited from AI assistance:

- Reading a relatively large repository quickly and producing an initial defect
  inventory.
- Drafting the first version of the API, worker, schema, Compose, Nginx, and
  test files.
- Enumerating failure scenarios for the test matrix.

How the generated work was reviewed:

- Every design decision was checked against the README contract, especially the
  HTTP status codes, retry rules, HMAC definition, 1 MB limit, and the
  "do not change the receiver" rule.
- The code was run, not just read: lint, 33 unit/database tests, 25 integration
  tests, and manual Docker/Compose checks.
- The first AI-assisted worker design retried inside the worker after putting
  the event back to `pending`. The two-worker `429` integration test produced
  three deliveries (`429, 200, 200`) instead of two. I traced that to the
  unlocked retry window, changed retries to be scheduled in the database with
  `next_attempt_at`, and re-ran the suite. That is a concrete example of the AI
  proposal being wrong and the test catching it.
- I also rejected an earlier check that treated "the stack is not reachable" as
  a reason to skip all checks silently; the integration tests still skip when
  no stack exists, but CI now fails if the stack does not become healthy.

The final correctness argument rests on the automated tests and the commands in
section 6, not on the fact that code was generated.

## 8. Trade-offs and limitations

- **Development credentials remain as Compose defaults.** `docker compose up
  --build` must work without a prepared `.env` for grading. Production use
  should require real secrets and fail startup when they are missing.
- **Schema initialisation, not a migration framework.** `db/001_schema.sql` is
  idempotent and includes upgrade `ALTER`s, but PostgreSQL only runs
  `docker-entrypoint-initdb.d` on an empty data directory. An existing volume
  from an older revision would need to be recreated or migrated manually. The
  new unique index would also need duplicate cleanup first.
- **Retry latency is bounded by polling.** The worker polls every second by
  default, so a due retry can wait up to the poll interval. Backoff itself is
  persisted precisely in `next_attempt_at`.
- **One event per claim.** Each worker processes one delivery at a time.
  Throughput scales by adding workers; there is no in-process concurrency.
- **At-least-once, not exactly-once.** A crash after the receiver accepted a
  request but before the attempt was committed can cause a duplicate. This is
  the contract's accepted trade-off.
- **No authentication on the public API.** It is intended as an internal
  service behind the gateway; adding auth was outside the challenge contract.
- **No dead-letter queue UI.** Failed events are queryable and counted in
  metrics, but there is no re-drive endpoint.
- **`Retry-After` can be arbitrarily large.** It is respected rather than
  capped, so an event can remain pending for a long time. This matches the
  contract but makes queue age a metric worth monitoring.
- **Database-down health is verified manually, not in CI.** Adding it to the
  integration job would require deliberately stopping the database mid-suite;
  it was left out to keep CI deterministic.

## 9. What I would improve next

1. **Replace init-only SQL with versioned migrations.** A small migration
   runner or Alembic would make upgrades safe. The first migration would
   deduplicate legacy `event_id` rows before creating the unique index.
2. **Add a dead-letter and re-drive workflow.** An operator endpoint or CLI to
   list failed events and reset them to `pending` would turn `failed` from a
   terminal state into an actionable queue.
3. **Add queue health metrics.** Pending age (oldest `next_attempt_at`),
   processing leases close to expiry, retry counts per event, and delivery
   latency would make incidents visible before clients notice.
4. **Add an authentication/authorization layer** on the API and protect the
   receiver's `/control` endpoint outside the mock environment.
5. **Add a graceful-shutdown integration test to CI** that sends `SIGTERM`
   while the receiver is delaying a response and asserts the event is delivered
   exactly once and the worker exits `0`.
6. **Pin base images by digest and add image scanning.** The Python images are
   reasonably small, but digest pinning plus a Trivy/SBOM step would make
   builds reproducible and supply-chain checks explicit.
7. **Store request IDs with events.** Today the request ID is in logs; storing
   it on the event would connect an API submission to its worker deliveries
   during incident analysis.
8. **Batch claim or bounded in-process concurrency** if throughput becomes a
   problem, while keeping the same `SKIP LOCKED` claim semantics.