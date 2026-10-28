import argparse
import json
import sys
from pathlib import Path

from .paths import data_dir
from .store import Queue
from .worker import handlers, run_one, work


def main(argv=None):
    parser = argparse.ArgumentParser(description="QueueKit · trabajos persistentes en tu equipo")
    parser.add_argument("--db", type=Path, default=None)
    commands = parser.add_subparsers(dest="command", required=True)
    add = commands.add_parser("enqueue")
    add.add_argument("kind")
    add.add_argument(
        "--payload", type=Path, required=True, help="Archivo JSON con los datos de entrada"
    )
    add.add_argument("--priority", type=int, default=0)
    add.add_argument("--attempts", type=int, default=3)
    add.add_argument("--delay", type=int, default=0)
    add.add_argument("--key", default=None)
    worker = commands.add_parser("work")
    worker.add_argument("--concurrency", type=int, default=2)
    worker.add_argument("--handlers", help="Módulo Python de confianza con HANDLERS")
    worker.add_argument(
        "--once", action="store_true", help="Procesar como máximo un trabajo y salir"
    )
    commands.add_parser("list")
    show = commands.add_parser("show")
    show.add_argument("id")
    cancel = commands.add_parser("cancel")
    cancel.add_argument("id")
    args = parser.parse_args(argv)
    try:
        queue = Queue(args.db or data_dir() / "queue.sqlite")
        if args.command == "enqueue":
            if args.payload.stat().st_size > 256 * 1024:
                raise ValueError("El archivo JSON supera 256 KB.")
            identifier = queue.enqueue(
                args.kind,
                json.loads(args.payload.read_text(encoding="utf-8-sig")),
                args.priority,
                args.attempts,
                args.delay,
                args.key,
            )
            print(identifier)
        elif args.command == "work":
            registry = handlers(args.handlers)
            if args.once:
                print("processed" if run_one(queue, registry) else "idle")
            else:
                work(queue, registry, args.concurrency)
        elif args.command == "list":
            print(json.dumps(queue.list(), indent=2, ensure_ascii=False))
        elif args.command == "show":
            print(json.dumps(queue.get(args.id), indent=2, ensure_ascii=False))
        elif args.command == "cancel":
            if not queue.cancel(args.id):
                raise ValueError("El trabajo no existe o ya terminó.")
            print("cancelled")
        return 0
    except (OSError, ValueError, ImportError, KeyError) as error:
        print("QueueKit: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
