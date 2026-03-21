from datetime import datetime

from app.entities.agent_data import AccelerometerData, AgentData, GpsData
from app.usecases.data_processing import process_agent_data


def make_agent_data(x: float, y: float, z: float) -> AgentData:
    return AgentData(
        accelerometer=AccelerometerData(x=x, y=y, z=z),
        gps=GpsData(latitude=50.45, longitude=30.52),
        timestamp=datetime(2026, 3, 20, 12, 0, 0),
    )


def test_process_agent_data_classifies_normal():
    data = make_agent_data(16500.0, 0.0, 0.0)
    processed = process_agent_data(data)
    assert processed.road_state == "normal"
    assert processed.agent_data.gps.latitude == 50.45


def test_process_agent_data_classifies_bump():
    data = make_agent_data(13000.0, 0.0, 0.0)
    processed = process_agent_data(data)
    assert processed.road_state == "bump"


def test_process_agent_data_classifies_pothole():
    data = make_agent_data(9000.0, 0.0, 0.0)
    processed = process_agent_data(data)
    assert processed.road_state == "pothole"
