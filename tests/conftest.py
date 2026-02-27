import os
import uuid

import pytest

from forgequeue import Queue


@pytest.fixture
def q():
    dsn = os.environ["DATABASE_URL"]
    with Queue(dsn, "test-" + uuid.uuid4().hex) as queue:
        queue.migrate()
        yield queue
        queue.conn.execute("DELETE FROM fq_jobs WHERE queue=%s", (queue.name,))
