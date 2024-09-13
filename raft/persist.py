# Persistency Layer
from datetime import datetime
import json
import os

from pydantic import BaseModel

from .api import DataEntry


class Database(BaseModel):
    store: dict[str, DataEntry]
    timestamp: datetime


class PersistedStorage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

        if not os.path.exists(db_path):
            dir = os.path.dirname(db_path)
            if dir != "" and not os.path.exists(dir):
                os.makedirs(dir)
            init_db = Database(store={}, timestamp=datetime.now())
            self._write_disk(db_path, init_db)

        with open(db_path, "r") as f:
            db_dict = json.load(f)

        self.db = Database(**db_dict)

    @staticmethod
    def _write_disk(path: str, db: Database):
        with open(path, "w") as f:
            db_json = db.model_dump_json()
            f.write(db_json)

    def get_persisted(self, key: str) -> DataEntry | None:
        return self.db.store.get(key)

    def __len__(self):
        return len(self.db.store)

    def set_persisted(self, key: str, data: str | None) -> DataEntry:
        timestamp = datetime.now()

        # set to memory
        if data is None:
            entry = self.db.store[key]
            del self.db.store[key]
        else:
            entry = DataEntry(data=data, timestamp=timestamp)
            self.db.store[key] = entry
        self.db.timestamp = timestamp

        # write through to disk
        self._write_disk(self.db_path, self.db)

        return entry
