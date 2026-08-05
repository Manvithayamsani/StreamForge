import json
from pathlib import Path

from rocksdict import Rdict


DB_PATH = Path("data/streamforge_state")


class StateStore:
    def __init__(self):
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.db = Rdict(str(DB_PATH))

    def save_window(self, truck_id, window_start, stats):
        key = f"{truck_id}|{window_start.isoformat()}"

        self.db[key] = json.dumps({
            "truck_id": truck_id,
            "window_start": window_start.isoformat(),
            "temperature_sum": stats["temperature_sum"],
            "reading_count": stats["reading_count"],
        })

    def delete_window(self, truck_id, window_start):
        key = f"{truck_id}|{window_start.isoformat()}"

        if key in self.db:
            del self.db[key]

    def load_windows(self):
        recovered = []

        for _, value in self.db.items():
            recovered.append(json.loads(value))

        return recovered

    def close(self):
        self.db.close()