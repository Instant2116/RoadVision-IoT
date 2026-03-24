import importlib
import sys
from datetime import datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient


def import_store_main_with_sqlite_engine():
    if "main" in sys.modules:
        del sys.modules["main"]

    from sqlalchemy import create_engine as real_create_engine
    from sqlalchemy.pool import StaticPool

    sqlite_engine = real_create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with patch("sqlalchemy.create_engine", return_value=sqlite_engine):
        return importlib.import_module("main")


def seed_analytics_data(store_main):
    with store_main.engine.begin() as conn:
        conn.execute(
            store_main.processed_agent_data.insert(),
            [
                {
                    "road_state": "pothole",
                    "user_id": 1,
                    "x": 0.1,
                    "y": 0.2,
                    "z": 0.3,
                    "latitude": 50.45,
                    "longitude": 30.52,
                    "timestamp": datetime(2026, 3, 20, 10, 0, 0),
                },
                {
                    "road_state": "normal",
                    "user_id": 2,
                    "x": 0.0,
                    "y": 0.1,
                    "z": 0.2,
                    "latitude": 50.46,
                    "longitude": 30.53,
                    "timestamp": datetime(2026, 3, 20, 10, 5, 0),
                },
                {
                    "road_state": "pothole",
                    "user_id": 1,
                    "x": 0.4,
                    "y": 0.5,
                    "z": 0.6,
                    "latitude": 50.47,
                    "longitude": 30.54,
                    "timestamp": datetime(2026, 3, 20, 10, 10, 0),
                },
            ],
        )
        conn.execute(
            store_main.bus_occupancy_data.insert(),
            [
                {
                    "bus_id": 101,
                    "occupancy_rate": 0.2,
                    "latitude": 50.45,
                    "longitude": 30.52,
                    "timestamp": datetime(2026, 3, 20, 10, 0, 30),
                },
                {
                    "bus_id": 102,
                    "occupancy_rate": 0.8,
                    "latitude": 50.46,
                    "longitude": 30.53,
                    "timestamp": datetime(2026, 3, 20, 10, 6, 0),
                },
            ],
        )


def test_road_state_summary_returns_counts():
    store_main = import_store_main_with_sqlite_engine()
    seed_analytics_data(store_main)
    client = TestClient(store_main.app)

    response = client.get("/analytics/road_state_summary")

    assert response.status_code == 200
    assert response.json() == [
        {"road_state": "pothole", "events_count": 2},
        {"road_state": "normal", "events_count": 1},
    ]


def test_road_state_summary_returns_empty_for_missing_period():
    store_main = import_store_main_with_sqlite_engine()
    seed_analytics_data(store_main)
    client = TestClient(store_main.app)

    response = client.get(
        "/analytics/road_state_summary",
        params={"from": "2027-01-01T00:00:00", "to": "2027-01-02T00:00:00"},
    )

    assert response.status_code == 200
    assert response.json() == []


def test_road_state_summary_rejects_invalid_date():
    store_main = import_store_main_with_sqlite_engine()
    client = TestClient(store_main.app)

    response = client.get("/analytics/road_state_summary", params={"from": "bad-date"})

    assert response.status_code == 422


def test_bus_occupancy_summary_returns_aggregates():
    store_main = import_store_main_with_sqlite_engine()
    seed_analytics_data(store_main)
    client = TestClient(store_main.app)

    response = client.get("/analytics/bus_occupancy_summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["records_count"] == 2
    assert payload["avg_occupancy_rate"] == pytest.approx(0.5)
    assert payload["min_occupancy_rate"] == pytest.approx(0.2)
    assert payload["max_occupancy_rate"] == pytest.approx(0.8)


def test_bus_occupancy_summary_returns_empty_shape_for_missing_period():
    store_main = import_store_main_with_sqlite_engine()
    seed_analytics_data(store_main)
    client = TestClient(store_main.app)

    response = client.get(
        "/analytics/bus_occupancy_summary",
        params={"from": "2027-01-01T00:00:00", "to": "2027-01-02T00:00:00"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "records_count": 0,
        "avg_occupancy_rate": None,
        "min_occupancy_rate": None,
        "max_occupancy_rate": None,
    }
