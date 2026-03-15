"""Real PostgreSQL benchmark. Never adjusts measured results to match targets."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import platform
import statistics
import time
import uuid

from forgequeue import Queue
from forgequeue.worker import demo_effect

TARGETS = {"one_worker_jobs_per_second": 95, "eight_worker_jobs_per_second": 610,
           "speedup": 6.4, "crash_jobs_minimum": 50000,
           "recovery_seconds_maximum": 6, "reclaimed_percent": 100,
           "duplicate_committed_effects": 0}


def seed(q, count):
    with q.conn.transaction():
        q.conn.execute("""INSERT INTO fq_jobs(queue,kind,payload)
            SELECT %s,'demo',jsonb_build_object('number',n)
            FROM generate_series(1,%s) n""", (q.name, count))


def consumer(dsn, name, start, result, delay):
    try:
        with Queue(dsn, name) as q:
            result.put({"ready": True})
            start.wait()
            done = 0
            began = time.monotonic()
            while True:
                jobs = q.claim(str(os.getpid()))
                if not jobs:
                    break
                if delay:
                    time.sleep(delay)
                q.complete(jobs[0], demo_effect)
                done += 1
            result.put({"done": done, "elapsed": time.monotonic() - began})
    except BaseException as exc:
        result.put({"error": repr(exc)})
        raise


def drain(dsn, name, workers, delay):
    start, result = mp.Event(), mp.Queue()
    children = [mp.Process(target=consumer, args=(dsn,name,start,result,delay)) for _ in range(workers)]
    try:
        for child in children:
            child.start()
        for _ in children:
            assert result.get(timeout=30) == {"ready": True}
        began = time.monotonic()
        start.set()
        rows = [result.get(timeout=300) for _ in children]
        elapsed = time.monotonic() - began
        for child in children:
            child.join(10)
            assert child.exitcode == 0
        assert all("done" in row for row in rows), rows
        count = sum(row["done"] for row in rows)
        return {"workers": workers, "jobs": count, "seconds": elapsed,
                "jobs_per_second": count / elapsed, "per_worker": rows}
    finally:
        for child in children:
            if child.is_alive():
                child.kill()
                child.join()


def verify(q, expected):
    stats = q.stats()
    effects = q.conn.execute("""SELECT count(*) AS total,count(DISTINCT e.job_id) AS unique_jobs
        FROM fq_demo_effects e JOIN fq_jobs j ON j.id=e.job_id WHERE j.queue=%s""", (q.name,)).fetchone()
    assert stats == {"succeeded": expected}, stats
    assert effects == {"total": expected, "unique_jobs": expected}, effects
    return {"succeeded": expected, "committed_effects": effects["total"],
            "duplicate_committed_effects": effects["total"] - effects["unique_jobs"]}


def victim(dsn, name, count, result, phase):
    with Queue(dsn, name) as q:
        jobs = q.claim(str(os.getpid()), limit=count, lease=5)
        if phase == "after_claim":
            result.put({"ids": [j.id for j in jobs]})
            time.sleep(120)
        elif phase == "inside_effect":
            def interrupted(conn, job):
                demo_effect(conn, job)
                result.put({"ids": [job.id]})
                time.sleep(120)
            q.complete(jobs[0], interrupted)
        elif phase == "after_commit":
            q.complete(jobs[0], demo_effect)
            result.put({"ids": [jobs[0].id]})
            time.sleep(120)


def crash_campaign(dsn, count, phase):
    name = "crash-" + uuid.uuid4().hex
    with Queue(dsn, name) as q:
        seed(q, count)
        result = mp.Queue()
        child = mp.Process(target=victim, args=(dsn,name,count,result,phase))
        child.start()
        try:
            ids = result.get(timeout=60)["ids"]
            assert len(ids) == count
            killed_at = time.monotonic()
            child.kill()  # SIGKILL: no finally block or graceful cleanup.
            child.join(10)
            assert child.exitcode == -9
            reclaimed = set()
            while phase != "after_commit" and len(reclaimed) < count:
                reclaimed.update(q.reclaim(limit=2000))
                if time.monotonic() - killed_at > 15:
                    raise AssertionError("recovery timed out")
                if len(reclaimed) < count:
                    time.sleep(0.02)
            recovery = time.monotonic() - killed_at
            processing = drain(dsn, name, 8, 0)
            checked = verify(q, count)
            evidence = {"phase": phase, "jobs": count, "signal": "SIGKILL",
                        "reclaimed": len(reclaimed), "recovery_seconds": recovery,
                        "processing": processing, **checked}
            if phase != "after_commit":
                assert reclaimed == set(ids)
            return evidence
        finally:
            if child.is_alive():
                child.kill()
                child.join()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=2000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--work-ms", type=float, default=7)
    parser.add_argument("--crash-jobs", type=int, default=50008)
    parser.add_argument("--output", default="benchmarks/results.json")
    parser.add_argument("--skip-throughput", action="store_true")
    parser.add_argument("--skip-crash", action="store_true")
    args = parser.parse_args()
    if min(args.jobs, args.repeats, args.crash_jobs) <= 0 or args.work_ms < 0:
        parser.error("counts must be positive and work-ms nonnegative")
    dsn = os.environ["DATABASE_URL"]
    with Queue(dsn) as q:
        q.migrate()
        settings = {key: q.conn.execute("SELECT current_setting(%s) AS value", (key,)).fetchone()["value"]
                    for key in ("server_version", "fsync", "synchronous_commit", "full_page_writes", "shared_buffers")}
    report = {"measured_at": datetime.now(timezone.utc).isoformat(), "targets": TARGETS,
              "environment": {"os": platform.platform(), "cpu": platform.processor(), "cpu_count": os.cpu_count(),
                              "python": platform.python_version(), "postgresql": settings},
              "workload": {"simulated_io_ms": args.work_ms, "claim_batch": 1,
                           "effect": "one PostgreSQL insert atomically committed with acknowledgement",
                           "includes": "claim, handler delay, effect, acknowledgement, audit events",
                           "excludes": "enqueue, schema setup, process startup"},
              "throughput": [], "crash": []}
    def save():
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    if not args.skip_throughput:
        for repeat in range(args.repeats):
            for workers in (1,2,4,8):
                with Queue(dsn, "bench-" + uuid.uuid4().hex) as q:
                    seed(q, args.jobs)
                    row = drain(dsn, q.name, workers, args.work_ms / 1000)
                    row.update(verify(q, args.jobs))
                    row["repeat"] = repeat + 1
                    report["throughput"].append(row)
                    print(json.dumps(row), flush=True)
                    save()
        medians = {str(w): statistics.median(r["jobs_per_second"] for r in report["throughput"] if r["workers"] == w)
                   for w in (1,2,4,8)}
        report["medians"] = medians
        report["speedup"] = medians["8"] / medians["1"]
        report["throughput_targets_met"] = medians["1"] >= 95 and medians["8"] >= 610 and report["speedup"] >= 6.4
    if not args.skip_crash:
        for phase, count in (("after_claim", args.crash_jobs), ("inside_effect", 1), ("after_commit", 1)):
            row = crash_campaign(dsn, count, phase)
            report["crash"].append(row)
            print(json.dumps(row), flush=True)
            save()
        report["crash_targets_met"] = (args.crash_jobs >= 50000 and all(
            r["recovery_seconds"] <= 6 and r["duplicate_committed_effects"] == 0 and
            (r["reclaimed"] == r["jobs"] or r["phase"] == "after_commit") for r in report["crash"]))
    report["source_sha256"] = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for folder in ("forgequeue", "benchmarks") for p in sorted(Path(folder).glob("*.py"))}
    save()
    print(json.dumps({k:v for k,v in report.items() if k in ("medians","speedup","throughput_targets_met","crash_targets_met")}), flush=True)


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
