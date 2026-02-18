import logging
import signal
import time
import uuid

from psycopg.types.json import Jsonb

from .queue import LeaseLost, Queue

log = logging.getLogger(__name__)


def demo_effect(conn, job):
    conn.execute("INSERT INTO fq_demo_effects(job_id,value) VALUES (%s,%s)",
                 (job.id, Jsonb(job.payload)))


def run(dsn, name="default", handlers=None, *, lease=5.0, poll=0.05, stop=None):
    """Handlers execute as transactional PostgreSQL effects; one job per worker.

    Each handler receives (connection, job). Use Queue directly for long-running
    computation with explicit heartbeats before submitting its final DB effect.
    """
    handlers = handlers if handlers is not None else {"demo": demo_effect}
    stopping = False

    def shutdown(*_):
        nonlocal stopping
        stopping = True

    if stop is None:
        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)
    owner = str(uuid.uuid4())
    with Queue(dsn, name) as q:
        # A stuck DB handler must not hold a row lock forever after lease expiry.
        q.conn.execute("SET statement_timeout = '4s'")
        q.conn.execute("SET idle_in_transaction_session_timeout = '4s'")
        while not stopping and not (stop and stop.is_set()):
            q.reclaim()
            jobs = q.claim(owner, lease=lease)
            if not jobs:
                time.sleep(poll)
                continue
            job = jobs[0]
            try:
                q.complete(job, handlers[job.kind])
            except LeaseLost:
                log.warning("lease_lost job=%s", job.id)
            except Exception as exc:
                log.exception("job_failed job=%s", job.id)
                try:
                    q.fail(job, str(exc))
                except LeaseLost:
                    pass
