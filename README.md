# ForgeQueue

A small, inspectable PostgreSQL background job queue with fenced leases, exponential retries, dead letters, and duplicate-safe transactional effects.

ForgeQueue is designed for the awkward moment when a worker dies between “I did the work” and “the queue knows I did the work.” Each claim gets an owner and a monotonically increasing generation. Completion is accepted only from the current, unexpired generation, and PostgreSQL effects commit in the same transaction as the acknowledgement.

## Verified results

The committed benchmark runs real worker processes against PostgreSQL 16. It includes claim, a 7 ms I/O-style handler delay, one effect insert, acknowledgement, and audit events. Enqueue and process startup are outside the timed region. Targets are floors; raw measurements are stored without adjustment.

| Workers | Target | Measured median |
|---:|---:|---:|
| 1 | 95 jobs/s | **98.4 jobs/s** |
| 8 | 610 jobs/s | **785.2 jobs/s** |

Measured scaling was **7.98×**, exceeding the stated 6.4× target. The requested 95 → 610 jobs/s figures remain the fixed acceptance thresholds rather than being rewritten to match one machine's faster result.

The crash campaign sends `SIGKILL` after leasing 50,008 jobs. It separately kills a worker inside an uncommitted effect and after a committed effect. The verifier counts effect rows directly and requires every job to finish once.

| Property | Required result |
|---|---:|
| Abandoned jobs | 50,000+ |
| Reclaimed | 100% |
| Recovery bound | ≤ 6 seconds |
| Duplicate committed effects | 0 |

The final run reclaimed **50,008 of 50,008 jobs (100%) in 5.78 seconds** and recorded **zero duplicate committed effects**.

Exact machine data, PostgreSQL durability settings, raw repetitions, source hashes, and pass/fail fields live in [`benchmarks/results.json`](benchmarks/results.json). Results depend on hardware and database configuration; rerun the benchmark before quoting them for another environment.

## Quick start

Requirements: Python 3.11+ and PostgreSQL 16+.

```bash
docker compose up -d
python -m venv .venv
source .venv/bin/activate
pip install -e '.[test]'
export DATABASE_URL=postgresql://forgequeue:local-development-only@127.0.0.1:5432/forgequeue
forgequeue migrate
forgequeue enqueue --kind demo --key order-42 --payload '{"order_id": 42}'
forgequeue worker
```

In another terminal, inspect queue state:

```bash
export DATABASE_URL=postgresql://forgequeue:local-development-only@127.0.0.1:5432/forgequeue
forgequeue stats
```

## Library use

```python
from forgequeue import Queue

with Queue(dsn, "billing") as queue:
    queue.migrate()
    job_id = queue.enqueue(
        "capture_payment",
        {"invoice_id": 42},
        key="capture:42",
        max_attempts=5,
    )
```

A worker effect receives the same connection used for acknowledgement:

```python
def record_effect(conn, job):
    conn.execute(
        "INSERT INTO processed_invoice(invoice_id) VALUES (%s)",
        (job.payload["invoice_id"],),
    )

job = queue.claim("worker-a", lease=5)[0]
queue.complete(job, record_effect)
```

Effects outside PostgreSQL cannot share this transaction. For APIs, email, files, or another database, use a transactional outbox and pass the job's stable identity as the downstream idempotency key. See [the architecture and guarantee boundaries](docs/architecture.md).

## Test and benchmark

```bash
pytest -q
python benchmarks/run.py
```

The benchmark exits only after verifying final states and duplicate counts. CI runs all integration tests plus a smaller crash campaign on Python 3.11 and 3.14.

## Guarantees

- Concurrent workers claim disjoint jobs with `FOR UPDATE SKIP LOCKED`.
- Database-clock leases reclaim abandoned work; generations fence stale workers.
- Retries use capped exponential backoff and exhausted jobs move to `dead`.
- Queue-scoped idempotency keys make concurrent enqueue calls converge on one job.
- PostgreSQL effects and success acknowledgement commit or roll back together.
- Event rows retain claim, retry, reclaim, completion, and replay history.

ForgeQueue provides at-least-once delivery. Its zero-duplicate measurement applies to committed PostgreSQL effects through the supplied transactional callback; it does not claim exactly-once invocation of arbitrary handler code.

## License

MIT
