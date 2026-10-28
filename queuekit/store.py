import json
import time
import uuid
from pathlib import Path

from .database import connection


def encoded(value):
    result = json.dumps(value, ensure_ascii=False, allow_nan=False)
    if len(result.encode()) > 256 * 1024:
        raise ValueError("El JSON supera 256 KB.")
    return result


class Queue:
    def __init__(self, path):
        self.path = str(path)
        Path(path).resolve().parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs(
                  id TEXT PRIMARY KEY,kind TEXT NOT NULL,payload TEXT NOT NULL,
                  priority INTEGER NOT NULL,status TEXT NOT NULL,attempts INTEGER DEFAULT 0,
                  max_attempts INTEGER NOT NULL,available REAL NOT NULL,created REAL NOT NULL,
                  updated REAL NOT NULL,lease_until REAL,token TEXT,result TEXT,error TEXT,
                  idempotency TEXT UNIQUE);
                CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status,available,priority);
            """)

    def connect(self):
        return connection(self.path)

    def enqueue(self, kind, payload, priority=0, max_attempts=3, delay=0, idempotency=None):
        if not isinstance(kind, str) or not kind or len(kind) > 100:
            raise ValueError("El tipo de trabajo no es válido.")
        if not -100 <= priority <= 100 or not 1 <= max_attempts <= 10 or not 0 <= delay <= 604800:
            raise ValueError("Prioridad, intentos o retraso fuera de rango.")
        if idempotency is not None and (not idempotency or len(idempotency) > 200):
            raise ValueError("Clave de idempotencia inválida.")
        payload = encoded(payload)
        now = time.time()
        identifier = uuid.uuid4().hex
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if idempotency:
                existing = db.execute(
                    "SELECT * FROM jobs WHERE idempotency=?", (idempotency,)
                ).fetchone()
                if existing:
                    if existing["kind"] != kind or existing["payload"] != payload:
                        raise ValueError("La clave de idempotencia ya pertenece a otro trabajo.")
                    return existing["id"]
            db.execute(
                "INSERT INTO jobs(id,kind,payload,priority,status,max_attempts,available,created,updated,idempotency) VALUES(?,?,?,?,'queued',?,?,?,?,?)",
                (
                    identifier,
                    kind,
                    payload,
                    priority,
                    max_attempts,
                    now + delay,
                    now,
                    now,
                    idempotency,
                ),
            )
        return identifier

    def claim(self, lease=30, now=None):
        if not 5 <= lease <= 3600:
            raise ValueError("El lease debe durar de 5 a 3600 segundos.")
        now = time.time() if now is None else now
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE jobs SET status=CASE WHEN attempts>=max_attempts THEN 'failed' ELSE 'queued' END, token=NULL,lease_until=NULL,error='Lease expired',updated=? WHERE status='running' AND lease_until<=?",
                (now, now),
            )
            row = db.execute(
                "SELECT * FROM jobs WHERE status='queued' AND available<=? ORDER BY priority DESC,created,id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            db.execute(
                "UPDATE jobs SET status='running',attempts=attempts+1,token=?,lease_until=?,updated=? WHERE id=?",
                (token, now + lease, now, row["id"]),
            )
            result = dict(row)
            result.update(token=token, attempts=row["attempts"] + 1, status="running")
            result["payload"] = json.loads(result["payload"])
            return result

    def heartbeat(self, identifier, token, lease=30):
        now = time.time()
        with self.connect() as db:
            return (
                db.execute(
                    "UPDATE jobs SET lease_until=?,updated=? WHERE id=? AND token=? AND status='running' AND lease_until>?",
                    (now + lease, now, identifier, token, now),
                ).rowcount
                == 1
            )

    def finish(self, identifier, token, result):
        result = encoded(result)
        now = time.time()
        with self.connect() as db:
            return (
                db.execute(
                    "UPDATE jobs SET status='succeeded',result=?,token=NULL,lease_until=NULL,updated=? WHERE id=? AND token=? AND status='running' AND lease_until>?",
                    (result, now, identifier, token, now),
                ).rowcount
                == 1
            )

    def fail(self, identifier, token, error):
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM jobs WHERE id=? AND token=? AND status='running' AND lease_until>?",
                (identifier, token, now),
            ).fetchone()
            if not row:
                return False
            retry = row["attempts"] < row["max_attempts"]
            db.execute(
                "UPDATE jobs SET status=?,available=?,error=?,token=NULL,lease_until=NULL,updated=? WHERE id=?",
                (
                    "queued" if retry else "failed",
                    now + min(300, 2 ** row["attempts"]),
                    str(error)[:2000],
                    now,
                    identifier,
                ),
            )
            return True

    def cancel(self, identifier):
        with self.connect() as db:
            return (
                db.execute(
                    "UPDATE jobs SET status='cancelled',token=NULL,lease_until=NULL,updated=? WHERE id=? AND status IN ('queued','running')",
                    (time.time(), identifier),
                ).rowcount
                == 1
            )

    def list(self, limit=100):
        with self.connect() as db:
            return [
                dict(row)
                for row in db.execute(
                    "SELECT id,kind,status,priority,attempts,max_attempts,created,updated,error FROM jobs ORDER BY created DESC LIMIT ?",
                    (limit,),
                )
            ]

    def get(self, identifier):
        with self.connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()
            if not row:
                raise ValueError("Trabajo no encontrado.")
            result = dict(row)
            result.pop("token", None)
            result["payload"] = json.loads(result["payload"])
            if result["result"] is not None:
                result["result"] = json.loads(result["result"])
            return result
