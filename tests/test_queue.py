import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from forgequeue import LeaseLost, Queue
from forgequeue.worker import demo_effect


def test_idempotency_and_queue_isolation(q):
    def enqueue(_):
        with Queue(os.environ["DATABASE_URL"], q.name) as other:
            return other.enqueue("demo", {}, key="same")
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(enqueue, range(24)))
    assert len(set(ids)) == 1
    assert q.enqueue("demo", {}, key="different") != ids[0]
    with Queue(os.environ["DATABASE_URL"], q.name + "-other") as other:
        assert other.claim("worker") == []


def test_concurrent_claims_are_disjoint(q):
    for i in range(100):
        q.enqueue("demo", {"i": i})
    def claim(i):
        with Queue(os.environ["DATABASE_URL"], q.name) as other:
            return [j.id for j in other.claim(str(i), limit=20)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = sum(pool.map(claim, range(8)), [])
    assert len(ids) == len(set(ids)) == 100


def test_expiry_fences_old_worker(q):
    q.enqueue("demo", {})
    old = q.claim("old", lease=0.06)[0]
    time.sleep(0.08)
    with pytest.raises(LeaseLost):
        q.heartbeat(old)
    assert q.reclaim() == [old.id]
    new = q.claim("new")[0]
    assert new.generation == old.generation + 1
    for action in (q.complete, q.fail):
        with pytest.raises(LeaseLost):
            action(old, *(["error"] if action == q.fail else []))
    q.complete(new, demo_effect)
    with pytest.raises(LeaseLost):
        q.complete(new, demo_effect)
    assert q.conn.execute("SELECT count(*) AS n FROM fq_demo_effects WHERE job_id=%s", (new.id,)).fetchone()["n"] == 1


def test_effect_rollback_is_atomic(q):
    q.enqueue("demo", {})
    job = q.claim("w")[0]
    def broken(conn, job):
        demo_effect(conn, job)
        raise RuntimeError("after effect, before ack")
    with pytest.raises(RuntimeError):
        q.complete(job, broken)
    assert q.stats() == {"running": 1}
    assert q.conn.execute("SELECT count(*) AS n FROM fq_demo_effects WHERE job_id=%s", (job.id,)).fetchone()["n"] == 0
    q.complete(job, demo_effect)


def test_retry_dead_letter_and_replay(q):
    id_ = q.enqueue("demo", {}, max_attempts=2)
    first = q.claim("w")[0]
    q.fail(first, "transient", base=0.1)
    assert q.claim("w") == []
    time.sleep(0.12)
    second = q.claim("w")[0]
    q.fail(second, "permanent")
    assert q.stats() == {"dead": 1}
    assert q.replay(id_)
    assert not q.replay(id_)
    third = q.claim("w")[0]
    assert third.attempts == 1 and third.generation == 3
    q.complete(third)


def test_exponential_delay_and_cap(q):
    q.enqueue("demo", {})
    for expected in (2, 4, 5):
        job = q.claim("w")[0]
        q.fail(job, "retry", base=2, cap=5)
        row = q.conn.execute("SELECT extract(epoch from available_at-clock_timestamp()) AS delay FROM fq_jobs WHERE id=%s", (job.id,)).fetchone()
        assert expected - 0.2 < float(row["delay"]) <= expected
        q.conn.execute("UPDATE fq_jobs SET available_at=clock_timestamp() WHERE id=%s", (job.id,))


def test_heartbeat_and_exhausted_crash(q):
    q.enqueue("demo", {}, max_attempts=1)
    job = q.claim("w", lease=0.08)[0]
    q.heartbeat(job, lease=0.2)
    time.sleep(0.1)
    assert q.reclaim() == []
    time.sleep(0.12)
    assert q.reclaim() == [job.id]
    assert q.stats() == {"dead": 1}


def test_priority_and_delay(q):
    q.enqueue("demo", {}, priority=100, delay=60)
    low = q.enqueue("demo", {}, priority=-1)
    high = q.enqueue("demo", {}, priority=10)
    assert q.claim("w")[0].id == high
    assert q.claim("w")[0].id == low
    assert q.claim("w") == []
