from datetime import datetime

import pytest

import main
from domain.accelerometer import Accelerometer
from domain.aggregated_data import AggregatedData
from domain.bus_occupancy import BusOccupancy
from domain.gps import Gps
from domain.parking import Parking


class FakeClient:
    def __init__(self):
        self.calls = []

    def publish(self, topic, payload):
        self.calls.append((topic, payload))
        return (0, 1)


class FakeDatasource:
    def __init__(self):
        self.started = False
        self.read_count = 0

    def startReading(self):
        self.started = True

    def read(self):
        self.read_count += 1
        if self.read_count > 1:
            raise KeyboardInterrupt()

        agg = [
            AggregatedData(
                accelerometer=Accelerometer(1, 2, 3),
                gps=Gps(30.52, 50.45),
                timestamp=datetime(2026, 3, 20, 12, 0, 0),
                user_id=7,
            )
        ]
        parking = [Parking(empty_count=5, gps=Gps(30.6, 50.4))]
        buses = [BusOccupancy(bus_id="101", occupancy_rate=0.65, gps=Gps(30.7, 50.5))]
        return agg, parking, buses


class StubMqttClient:
    def __init__(self):
        self.on_connect = None
        self.connected_to = None
        self.loop_started = False

    def connect(self, broker, port):
        self.connected_to = (broker, port)

    def loop_start(self):
        self.loop_started = True


def test_connect_mqtt_creates_and_starts_client(monkeypatch):
    created = StubMqttClient()

    monkeypatch.setattr(main.mqtt_client, "Client", lambda: created)

    client = main.connect_mqtt("broker.example", 1883)

    assert client is created
    assert client.connected_to == ("broker.example", 1883)
    assert client.loop_started is True
    assert callable(client.on_connect)


def test_publish_sends_serialized_data_to_expected_topics(monkeypatch):
    client = FakeClient()
    datasource = FakeDatasource()

    monkeypatch.setattr(main.time, "sleep", lambda _: None)

    with pytest.raises(KeyboardInterrupt):
        main.publish(client, "topic/agg", "topic/park", "topic/bus", datasource, delay=0)

    assert datasource.started is True
    assert len(client.calls) == 3
    assert client.calls[0][0] == "topic/agg"
    assert client.calls[1][0] == "topic/park"
    assert client.calls[2][0] == "topic/bus"
    assert "accelerometer" in client.calls[0][1]
    assert "empty_count" in client.calls[1][1]
    assert "occupancy_rate" in client.calls[2][1]
