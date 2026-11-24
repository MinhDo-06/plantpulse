# ACIT3855 project: plant water sensors

Services (Docker Compose):

| Service | Port | Role |
|---|---|---|
| receiver | 8080 | Accepts precipitation and soil-moisture events, adds a trace_id, forwards to storage |
| storage | internal | Stores events in MySQL; `GET /sensor/<type>?after=` returns events recorded after a timestamp |
| processing | 8100 | Polls storage every `INTERVAL_SECONDS`, updates statistics in a JSON file, serves `GET /stats` |
| db | internal | MySQL 8 |

Run: `docker compose up -d --build`, then `run-jmeter.bat`, then check `http://localhost:8100/stats`.
Reset everything (DB and stats together): `docker compose down -v`.

## Resilience policy (Lab 10, Part 1 decisions)

Startup: every service **refuses to start** if its dependency is missing (log reason, exit non-zero);
Docker `restart: on-failure` retries it with backoff.

| Dependency down | Service affected | Behaviour during outage | Data loss | Recovery action |
|---|---|---|---|---|
| Kafka | receiver | POST endpoints return HTTP 503 (degraded mode) | Client gets 503, but aiokafka keeps the event buffered and delivers it after a short outage (tested: 20s; longer not tested); it is lost if the receiver stops first, and a client retry stores it twice (new trace_id) | aiokafka reconnects on its own; sends resume normally |
| Kafka | storage-worker | No new messages to consume (not degraded); GET endpoints unaffected | None | Consumer resumes from last committed offset |
| MySQL | storage-worker | Stops advancing Kafka offset; after 5 attempts (~30s) exits and Docker restarts it; messages stay pending in Kafka | None | Replays all pending messages to the database |
| MySQL | storage (API) | GET endpoints return HTTP 503; processing pauses (not affected otherwise) | None | Polling resumes from last `?after=` timestamp |
| Storage API | processing | Stats cycle is skipped and retried next interval; stats never reset | None | Next successful fetch picks up where it left off |
| Stats file write | processing | Cycle skipped; last stats on disk are kept | None | Next successful cycle overwrites the file |

| Decision | Choice |
|---|---|
| Degraded-mode map | Minimal 503 — only endpoints that genuinely cannot work; `/status` always returns 200 |
| Outage detection | Catch failures at use-time (exceptions on send/commit/fetch); `pool_pre_ping` on the DB engine |
| Duplicates | Unique `trace_id` on both tables; MySQL error 1062 → logged as duplicate, offset committed, skipped; any other `IntegrityError` is logged as a constraint violation and skipped |
| Retry/timeout params | Lab defaults: idempotent producer, `request_timeout_ms=10000`, `retry_backoff_ms=250`, `linger_ms=5`, `asyncio.wait_for(..., 5)` on sends, 5 DB attempts with `min(2**attempt, 30)`s backoff |
| Processing recovery | Resume from last `?after=` timestamps; never flush or reset `statistics.json` |
| Logging | Outages log one line with the reason, no stack trace (`logger.error` / `logger.critical`); only the catch-all 500 handler logs a trace, for real bugs; clients never see one |

## Dependency map (Part 2, adapted to this architecture)

| Lab row | This repo | Note |
|---|---|---|
| receiver → Kafka (Producer) | `receiver` | aiokafka, two topics |
| storage → Kafka (Consumer) | `storage-worker` | confluent_kafka, manual commits |
| storage → MySQL (Session) | `storage` (API) and `storage-worker` | Degrade independently: worker holds offset, API returns 503 |
| processing → storage API (GET) | `processing` | `?after=` polling every `INTERVAL_SECONDS` |
| processing → Redis | **N/A — struck out** | Stats persist to a JSON file on a volume, no Redis dependency |

## Implementation checklist (Part 2)

| # | Task | Where | Status |
|---|---|---|---|
| 1 | `restart: on-failure` on receiver, storage, processing | docker-compose.yml | ☑ verified: RestartCount climbs with Kafka down, self-recovers; every service logs `... refusing to start` and exits 1 (worker checks MySQL and Kafka, processing checks storage) |
| 2 | `unique=True` on `trace_id` (both models); needs `docker compose down -v` to rebuild tables | storage/models.py | ☑ verified: `SHOW INDEX` shows `trace_id, Non_unique=0` |
| 3 | Idempotent producer config; `send` → `send_and_wait` in `asyncio.wait_for(..., 5)`; `KafkaError`/`TimeoutError` → 503 | receiver/receiver.py | ☑ verified: 201 healthy, 503 in ~5s with Kafka down (/status 200), 201 again after recovery |
| 4 | `pool_pre_ping=True` on engine | storage/db.py | ☑ verified: storage keeps polling (no errors) after `docker compose restart db` |
| 5 | Catch DB errors on GET → 503 | storage/storage.py | ☑ verified: 503 with db stopped (no stack trace to client), 200 again after db restart |
| 6 | `IntegrityError` → rollback + commit + skip; retry wrapper: 5 attempts, exponential backoff capped at 30s | storage/worker.py | ☑ verified: duplicate trace_id skipped (restarts 0); db down → retry, event stored once db back; offsets committed synchronously; db down 75s → worker exits, restarts and replays with no loss |
| 7 | Skip-cycle error handling | processing/processing.py | ☑ already implemented |
| 8 | No exception escapes as a stack trace (all services) | all | ☑ verified: catch-all 500 JSON, trace in logs only; worker skips poison messages (restarts 0) |

## Tests

Run inside each service folder (`receiver`, `storage`, `processing`):

```
uv run --no-project --python 3.13 --with "connexion[swagger-ui,uvicorn]" --with httpx --with pyyaml --with rfc3339-validator --with aiokafka --with confluent-kafka --with sqlalchemy --with pymysql --with cryptography --with pytest python -m pytest -q
```
