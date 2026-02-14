"""One Queue per worker; all durable state and ownership live in PostgreSQL."""
from dataclasses import dataclass
from importlib.resources import files
from typing import Callable, Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


class LeaseLost(Exception):
    """The job is complete, expired, or owned by a newer worker generation."""


@dataclass(frozen=True)
class Job:
    id: int
    kind: str
    payload: Any
    owner: str
    generation: int
    attempts: int


class Queue:
    def __init__(self, dsn: str, name: str = "default"):
        if not name:
            raise ValueError("queue name must not be empty")
        self.name = name
        self.conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.conn.close()

    def migrate(self):
        with self.conn.transaction():
            self.conn.execute("SELECT pg_advisory_xact_lock(7467211)")
            self.conn.execute(files("forgequeue").joinpath("schema.sql").read_text())

    def enqueue(self, kind: str, payload: Any, *, key: str | None = None,
                priority: int = 0, delay: float = 0, max_attempts: int = 5) -> int:
        if delay < 0 or max_attempts < 1 or not kind:
            raise ValueError("invalid kind, delay or max_attempts")
        # The no-op update also returns the original ID under concurrent inserts.
        row = self.conn.execute("""
            INSERT INTO fq_jobs(queue,kind,payload,idempotency_key,priority,available_at,max_attempts)
            VALUES (%s,%s,%s,%s,%s,clock_timestamp()+%s*interval '1 second',%s)
            ON CONFLICT (queue,idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key
            RETURNING id
        """, (self.name, kind, Jsonb(payload), key, priority, delay, max_attempts)).fetchone()
        return row["id"]

    def claim(self, owner: str, *, lease: float = 5, limit: int = 1) -> list[Job]:
        if lease <= 0 or limit < 1 or not owner:
            raise ValueError("positive lease/limit and nonempty owner required")
        rows = self.conn.execute("""
                WITH picked AS (
                    SELECT id FROM fq_jobs WHERE queue=%s AND state='pending'
                      AND available_at <= clock_timestamp()
                    ORDER BY priority DESC,available_at,id
                    FOR UPDATE SKIP LOCKED LIMIT %s
                ), claimed AS (UPDATE fq_jobs j SET state='running',owner=%s,
                    lease_until=clock_timestamp()+%s*interval '1 second',
                    generation=generation+1,attempts=attempts+1
                  FROM picked WHERE j.id=picked.id
                RETURNING j.id,j.kind,j.payload,j.owner,j.generation,j.attempts
                ), audited AS (
                    INSERT INTO fq_events(job_id,event,generation)
                    SELECT id,'claimed',generation FROM claimed
                ) SELECT * FROM claimed
            """, (self.name, limit, owner, lease)).fetchall()
        return [Job(**r) for r in rows]

    def _lock(self, job: Job):
        row = self.conn.execute("""SELECT id FROM fq_jobs WHERE id=%s AND queue=%s
            AND state='running' AND owner=%s AND generation=%s
            AND lease_until>clock_timestamp() FOR UPDATE""",
            (job.id, self.name, job.owner, job.generation)).fetchone()
        if row is None:
            raise LeaseLost(f"Job {job.id} no longer belongs to generation {job.generation}")

    def heartbeat(self, job: Job, lease: float = 5):
        if lease <= 0:
            raise ValueError("lease must be positive")
        with self.conn.transaction():
            self._lock(job)
            self.conn.execute("UPDATE fq_jobs SET lease_until=clock_timestamp()+%s*interval '1 second' WHERE id=%s",
                              (lease, job.id))

    def complete(self, job: Job, effect: Callable | None = None):
        """Commit PostgreSQL effects and acknowledgement in one fenced transaction.

        effect(conn, job) must only use the supplied connection for database effects;
        never commit it, perform remote I/O, or mutate queue ownership inside effect.
        """
        with self.conn.transaction():
            self._lock(job)
            if effect is not None:
                effect(self.conn, job)
            self.conn.execute("""UPDATE fq_jobs SET state='succeeded',owner=NULL,lease_until=NULL,
                finished_at=clock_timestamp() WHERE id=%s""", (job.id,))
            self.conn.execute("INSERT INTO fq_events(job_id,event,generation) VALUES (%s,'succeeded',%s)",
                              (job.id, job.generation))

    def fail(self, job: Job, error: str, *, base: float = 1, cap: float = 60):
        if base < 0 or cap < base:
            raise ValueError("require 0 <= base <= cap")
        delay = min(cap, base * 2 ** min(job.attempts - 1, 30))
        with self.conn.transaction():
            self._lock(job)
            row = self.conn.execute("""UPDATE fq_jobs SET
                state=CASE WHEN attempts>=max_attempts THEN 'dead' ELSE 'pending' END,
                available_at=clock_timestamp()+%s*interval '1 second',owner=NULL,lease_until=NULL,
                last_error=%s,finished_at=CASE WHEN attempts>=max_attempts THEN clock_timestamp() ELSE NULL END
                WHERE id=%s RETURNING state""", (delay, str(error)[:4000], job.id)).fetchone()
            self.conn.execute("INSERT INTO fq_events(job_id,event,generation) VALUES (%s,%s,%s)",
                              (job.id, row["state"], job.generation))

    def reclaim(self, limit: int = 1000) -> list[int]:
        if limit < 1:
            raise ValueError("limit must be positive")
        rows = self.conn.execute("""WITH expired AS (
                SELECT id FROM fq_jobs WHERE queue=%s AND state='running'
                  AND lease_until<=clock_timestamp() ORDER BY lease_until
                  FOR UPDATE SKIP LOCKED LIMIT %s
                ), recovered AS (UPDATE fq_jobs j SET
                  state=CASE WHEN attempts>=max_attempts THEN 'dead' ELSE 'pending' END,
                  owner=NULL,lease_until=NULL,available_at=clock_timestamp(),
                  finished_at=CASE WHEN attempts>=max_attempts THEN clock_timestamp() ELSE NULL END,
                  last_error='lease expired'
                FROM expired WHERE j.id=expired.id RETURNING j.id,j.generation,j.state
                ), audited AS (
                    INSERT INTO fq_events(job_id,event,generation)
                    SELECT id,CASE WHEN state='pending' THEN 'reclaimed' ELSE 'dead' END,generation
                    FROM recovered
                ) SELECT id FROM recovered
            """, (self.name, limit)).fetchall()
        return [r["id"] for r in rows]

    def replay(self, job_id: int) -> bool:
        """Explicitly reset the retry budget of a dead job; retain its audit history."""
        with self.conn.transaction():
            row = self.conn.execute("""UPDATE fq_jobs SET state='pending',attempts=0,
                available_at=clock_timestamp(),finished_at=NULL,last_error=NULL
                WHERE id=%s AND queue=%s AND state='dead' RETURNING generation""",
                (job_id, self.name)).fetchone()
            if row:
                self.conn.execute("INSERT INTO fq_events(job_id,event,generation) VALUES (%s,'replayed',%s)",
                                  (job_id, row["generation"]))
            return row is not None

    def stats(self) -> dict[str, int]:
        return {r["state"]: r["count"] for r in self.conn.execute(
            "SELECT state,count(*) FROM fq_jobs WHERE queue=%s GROUP BY state", (self.name,))}
