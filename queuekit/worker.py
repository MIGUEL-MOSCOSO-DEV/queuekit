import hashlib
import importlib
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


class Context:
    def __init__(self, stopping, lost):
        self.stopping, self.lost = stopping, lost

    def check_cancelled(self):
        if self.stopping.is_set() or self.lost.is_set():
            raise RuntimeError("Trabajo cancelado o lease perdido.")


def hash_file(payload, context):
    path = Path(payload["path"]).expanduser().resolve()
    checksum = hashlib.sha256()
    count = 0
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            context.check_cancelled()
            checksum.update(chunk)
            count += len(chunk)
    return {"sha256": checksum.hexdigest(), "bytes": count}


def directory_size(payload, context):
    root = Path(payload["path"]).expanduser().resolve()
    if not root.is_dir():
        raise ValueError("La carpeta no existe.")
    count, total = 0, 0
    for path in root.rglob("*"):
        context.check_cancelled()
        if not path.is_symlink() and path.is_file() and root in path.resolve().parents:
            total += path.stat().st_size
            count += 1
    return {"files": count, "bytes": total}


def handlers(module=None):
    result = {"sha256": hash_file, "directory-size": directory_size}
    if module:
        custom = importlib.import_module(module).HANDLERS
        if not isinstance(custom, dict) or not all(
            isinstance(name, str) and callable(fn) for name, fn in custom.items()
        ):
            raise ValueError("HANDLERS debe ser un diccionario de funciones.")
        result.update(custom)
    return result


def run_one(queue, registry, stopping=None, lease=30):
    stopping = stopping or threading.Event()
    job = queue.claim(lease)
    if not job:
        return False
    done, lost = threading.Event(), threading.Event()

    def renew():
        while not done.wait(lease / 3):
            try:
                if not queue.heartbeat(job["id"], job["token"], lease):
                    lost.set()
                    return
            except Exception:
                lost.set()
                return

    heartbeat = threading.Thread(target=renew, daemon=True)
    heartbeat.start()
    try:
        if job["kind"] not in registry:
            raise ValueError("No hay un handler registrado para " + job["kind"])
        context = Context(stopping, lost)
        context.check_cancelled()
        result = registry[job["kind"]](job["payload"], context)
        context.check_cancelled()
        queue.finish(job["id"], job["token"], result)
    except Exception as error:
        queue.fail(job["id"], job["token"], error)
    finally:
        done.set()
        heartbeat.join(timeout=2)
    return True


def work(queue, registry, concurrency=2, stopping=None):
    if not 1 <= concurrency <= 16:
        raise ValueError("Usa entre 1 y 16 workers.")
    stopping = stopping or threading.Event()

    def loop():
        while not stopping.is_set():
            if not run_one(queue, registry, stopping):
                stopping.wait(0.3)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(loop) for _ in range(concurrency)]
        try:
            while not stopping.wait(0.3):
                for future in futures:
                    if future.done():
                        future.result()
        except KeyboardInterrupt:
            stopping.set()
        finally:
            stopping.set()
