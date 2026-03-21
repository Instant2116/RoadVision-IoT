import asyncio
import importlib
import sys
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch


class FakeDbSession:
    def __init__(self):
        self.executed = []
        self.committed = False
        self.closed = False

    def execute(self, statement):
        self.executed.append(statement)
        return SimpleNamespace()

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def import_store_main_with_sqlite_engine():
    if "main" in sys.modules:
        del sys.modules["main"]
    from sqlalchemy import create_engine as real_create_engine

    sqlite_engine = real_create_engine("sqlite:///:memory:")
    with patch("sqlalchemy.create_engine", return_value=sqlite_engine):
        return importlib.import_module("main")


def test_create_processed_agent_data_saves_road_and_bus_data(monkeypatch):
    store_main = import_store_main_with_sqlite_engine()
    fake_db = FakeDbSession()
    notifications = []

    async def fake_notify(user_id, data):
        notifications.append((user_id, data))

    monkeypatch.setattr(store_main, "SessionLocal", lambda: fake_db)
    monkeypatch.setattr(store_main, "send_data_to_subscribers", fake_notify)

    payload = [
        store_main.IngestedData(
            road_state="normal",
            agent_data=store_main.AgentData(
                user_id=7,
                accelerometer=store_main.AccelerometerData(x=0.1, y=0.2, z=0.3),
                gps=store_main.GpsData(latitude=50.45, longitude=30.52),
                timestamp=datetime(2026, 3, 20, 12, 0, 0),
            ),
        ),
        store_main.IngestedData(
            road_state="bus_occupancy",
            bus_occupancy_data=store_main.BusOccupancyData(
                bus_id=101,
                occupancy_rate=0.75,
                gps=store_main.GpsData(latitude=50.46, longitude=30.53),
                timestamp=datetime(2026, 3, 20, 12, 0, 1),
            ),
        ),
    ]

    result = asyncio.run(store_main.create_processed_agent_data(payload))

    assert result == {"sent": 1}
    assert len(fake_db.executed) == 2
    assert fake_db.committed is True
    assert fake_db.closed is True
    assert len(notifications) == 1
    assert notifications[0][0] == 7
