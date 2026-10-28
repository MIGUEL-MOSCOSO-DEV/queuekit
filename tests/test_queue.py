import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from queuekit.store import Queue
from queuekit.worker import handlers, run_one, work


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.queue = Queue(self.root / "queue.sqlite")

    def tearDown(self):
        self.temporary.cleanup()

    def test_priority_and_delay(self):
        low = self.queue.enqueue("task", {}, priority=0)
        high = self.queue.enqueue("task", {}, priority=10)
        self.queue.enqueue("task", {}, priority=100, delay=60)
        self.assertEqual(self.queue.claim()["id"], high)
        self.assertEqual(self.queue.claim()["id"], low)
        self.assertIsNone(self.queue.claim())

    def test_idempotency_requires_matching_job(self):
        identifier = self.queue.enqueue("task", {"a": 1}, idempotency="unique")
        self.assertEqual(self.queue.enqueue("task", {"a": 1}, idempotency="unique"), identifier)
        with self.assertRaises(ValueError):
            self.queue.enqueue("task", {"a": 2}, idempotency="unique")

    def test_concurrent_claims_are_exclusive(self):
        for _ in range(20):
            self.queue.enqueue("task", {})
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda _: self.queue.claim(), range(20)))
        self.assertEqual(len({job["id"] for job in claims}), 20)

    def test_stale_lease_cannot_complete_reclaimed_job(self):
        identifier = self.queue.enqueue("task", {})
        first = self.queue.claim(lease=5)
        with self.queue.connect() as db:
            db.execute("UPDATE jobs SET lease_until=0 WHERE id=?", (identifier,))
        second = self.queue.claim()
        self.assertEqual(second["id"], identifier)
        self.assertNotEqual(first["token"], second["token"])
        self.assertFalse(self.queue.finish(identifier, first["token"], {}))
        self.assertTrue(self.queue.finish(identifier, second["token"], {"done": True}))

    def test_exhausted_expired_job_becomes_failed(self):
        identifier = self.queue.enqueue("task", {}, max_attempts=1)
        self.queue.claim()
        with self.queue.connect() as db:
            db.execute("UPDATE jobs SET lease_until=0 WHERE id=?", (identifier,))
        self.assertIsNone(self.queue.claim())
        self.assertEqual(self.queue.get(identifier)["status"], "failed")

    def test_retry_backoff_and_attempt_limit(self):
        identifier = self.queue.enqueue("task", {}, max_attempts=2)
        job = self.queue.claim()
        self.queue.fail(identifier, job["token"], "temporary")
        self.assertIsNone(self.queue.claim())
        with self.queue.connect() as db:
            db.execute("UPDATE jobs SET available=0 WHERE id=?", (identifier,))
        job = self.queue.claim()
        self.queue.fail(identifier, job["token"], "again")
        self.assertEqual(self.queue.get(identifier)["status"], "failed")

    def test_cancellation_blocks_late_success(self):
        identifier = self.queue.enqueue("task", {})
        job = self.queue.claim()
        self.assertTrue(self.queue.cancel(identifier))
        self.assertFalse(self.queue.finish(identifier, job["token"], {}))
        self.assertEqual(self.queue.get(identifier)["status"], "cancelled")

    def test_builtin_handler_hashes_real_file(self):
        path = self.root / "document.txt"
        path.write_bytes(b"hello")
        identifier = self.queue.enqueue("sha256", {"path": str(path)})
        run_one(self.queue, handlers())
        result = Queue(self.queue.path).get(identifier)
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["result"]["bytes"], 5)
        self.assertNotIn("token", result)

    def test_invalid_payload_and_limits(self):
        for payload in [{"x": float("nan")}, {"x": "a" * 300000}]:
            with self.assertRaises(ValueError):
                self.queue.enqueue("task", payload)
        with self.assertRaises(ValueError):
            self.queue.enqueue("task", {}, max_attempts=0)

    def test_parallel_workers_stop_and_persist_results(self):
        for _ in range(8):
            self.queue.enqueue("count", {})
        stopped = threading.Event()
        thread = threading.Thread(
            target=work,
            args=(self.queue, {"count": lambda payload, context: {"ok": True}}, 3, stopped),
        )
        thread.start()
        deadline = time.monotonic() + 10
        while (
            any(row["status"] != "succeeded" for row in self.queue.list())
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        stopped.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(all(row["status"] == "succeeded" for row in self.queue.list()))
