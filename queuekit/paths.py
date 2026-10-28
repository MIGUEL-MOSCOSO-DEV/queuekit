import os
from pathlib import Path


def data_dir():
    root = Path(os.environ.get("QUEUEKIT_HOME", Path.home() / ".queuekit"))
    root.mkdir(parents=True, exist_ok=True)
    return root
