# Persistent Storage Layer
from datetime import datetime
import json
import os

from pydantic import BaseModel


class PersistedEntry(BaseModel):
    data: str
    timestamp: datetime


class DBSection(BaseModel):
    store: dict[str, PersistedEntry]
    timestamp: datetime


class Database(BaseModel):
    sections: dict[str, DBSection]
    timestamp: datetime


class PersistedStorage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

        if not os.path.exists(db_path):
            dir = os.path.dirname(db_path)
            if dir != "" and not os.path.exists(dir):
                os.makedirs(dir)
            init_db = Database(sections={}, timestamp=datetime.now())
            self._write_disk(db_path, init_db)

        with open(db_path, "r") as f:
            db_dict = json.load(f)

        self.db = Database(**db_dict)

    @staticmethod
    def _write_disk(path: str, db: Database):
        with open(path, "w") as f:
            db_json = db.model_dump_json()
            f.write(db_json)

    def get_persisted(self, section: str, key: str) -> PersistedEntry | None:
        db_section = self.db.sections.get(section)
        if db_section is None:
            return None
        return db_section.store.get(key)

    def get_db_size(self, section: str | None):
        if section is None:
            sizes = [len(x.store) for x in self.db.sections.values()]
            return sum(sizes)
        else:
            db_section = self.db.sections.get(section)
            if db_section is None:
                return 0
            else:
                return len(db_section.store)

    def set_persisted(
        self, section: str, key: str, data: str | None
    ) -> PersistedEntry | None:
        timestamp = datetime.now()

        db_section = self.db.sections.get(section)
        if db_section is None:
            db_section = DBSection(store={}, timestamp=timestamp)
            self.db.sections[section] = db_section

        # set to memory
        if data is None:
            if key in db_section.store:
                entry = db_section.store[key]
                del db_section.store[key]
            else:
                entry = None
        else:
            entry = PersistedEntry(data=data, timestamp=timestamp)
            db_section.store[key] = entry

        db_section.timestamp = timestamp
        self.db.timestamp = timestamp

        # write through to disk
        self._write_disk(self.db_path, self.db)

        return entry
