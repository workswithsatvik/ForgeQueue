import argparse
import json
import logging
import os

from .queue import Queue
from .worker import run


def main():
    parser = argparse.ArgumentParser(description="ForgeQueue durable PostgreSQL job queue")
    parser.add_argument("--queue", default="default")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    enqueue = sub.add_parser("enqueue")
    enqueue.add_argument("--kind", default="demo")
    enqueue.add_argument("--payload", default="{}")
    enqueue.add_argument("--key")
    sub.add_parser("worker")
    sub.add_parser("stats")
    sub.add_parser("reclaim")
    replay = sub.add_parser("replay")
    replay.add_argument("job_id", type=int)
    args = parser.parse_args()
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        parser.error("set DATABASE_URL to your PostgreSQL connection string")
    logging.basicConfig(level=logging.INFO)
    if args.command == "worker":
        run(dsn, args.queue)
        return
    with Queue(dsn, args.queue) as q:
        if args.command == "migrate":
            q.migrate()
        elif args.command == "enqueue":
            print(q.enqueue(args.kind, json.loads(args.payload), key=args.key))
        elif args.command == "stats":
            print(json.dumps(q.stats()))
        elif args.command == "reclaim":
            print(json.dumps(q.reclaim()))
        elif args.command == "replay":
            print(json.dumps({"replayed": q.replay(args.job_id)}))


if __name__ == "__main__":
    main()
