from datetime import datetime

import pytest

from app.entities.agent_data import AccelerometerData, AgentData, GpsData
from app.entities.bus_occupancy_data import BusOccupancyData
from app.entities.processed_agent_data import ProcessedAgentData


def test_processed_agent_data_accepts_agent_payload():
    data = ProcessedAgentData(
        road_state="normal",
        agent_data=AgentData(
            user_id=1,
            accelerometer=AccelerometerData(x=0.1, y=0.2, z=0.3),
            gps=GpsData(latitude=50.45, longitude=30.52),
            timestamp=datetime(2026, 3, 20, 12, 0, 0),
        ),
    )
    assert data.agent_data is not None
    assert data.bus_occupancy_data is None


def test_processed_agent_data_accepts_bus_payload():
    data = ProcessedAgentData(
        road_state="bus_occupancy",
        bus_occupancy_data=BusOccupancyData(
            bus_id=101,
            occupancy_rate=0.65,
            gps=GpsData(latitude=50.55, longitude=30.62),
            timestamp=datetime(2026, 3, 20, 12, 0, 1),
        ),
    )
    assert data.agent_data is None
    assert data.bus_occupancy_data.bus_id == 101


def test_processed_agent_data_requires_at_least_one_payload():
    with pytest.raises(ValueError):
        ProcessedAgentData(road_state="unknown")
