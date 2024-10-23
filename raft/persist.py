# Persistent Storage Layer
from datetime import datetime
import os

from pydantic import BaseModel

from .logger import logger


class PersistedEntry(BaseModel):
    data: str
    timestamp: datetime


class PersistedStorage:
    def __init__(self, storage_root: str) -> None:
        logger.info(f"Storage: DB root path is {storage_root}")
        self.storage_root = storage_root

    def _ensure_dir_exists(self, path: str):
        if not os.path.exists(path):
            dir = os.path.dirname(path)
            if dir != "" and not os.path.exists(dir):
                os.makedirs(dir)

    def get(self, path: str) -> PersistedEntry | None:
        logger.debug(f"Storage: reading {path}")
        full_path = os.path.join(self.storage_root, path)
        if not os.path.exists(full_path):
            return None
        with open(full_path, mode="r", encoding="utf-8") as f:
            return PersistedEntry.model_validate_json(f.read())

    def set(self, path: str, data: str) -> PersistedEntry:
        logger.info(f"Storage: setting {path}")
        full_path = os.path.join(self.storage_root, path)
        self._ensure_dir_exists(full_path)
        entry = PersistedEntry(data=data, timestamp=datetime.now())
        with open(full_path, mode="w", encoding="utf-8") as f:
            f.write(entry.model_dump_json())
        return entry

    def remove(self, path: str) -> bool:
        logger.info(f"Storage: removing {path}")
        full_path = os.path.join(self.storage_root, path)
        if os.path.exists(full_path):
            os.remove(full_path)
            return True
        else:
            return False
