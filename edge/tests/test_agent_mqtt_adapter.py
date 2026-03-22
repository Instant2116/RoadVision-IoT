import json

from app.adapters.agent_mqtt_adapter import AgentMQTTAdapter


class FakeHubGateway:
    def __init__(self):
        self.saved = []

    def save_data(self, processed_data):
        self.saved.append(processed_data)
        return True


class FakeMessage:
    def __init__(self, topic: str, payload: str):
        self.topic = topic
        self.payload = payload.encode("utf-8")


class FakeClient:
    def __init__(self):
        self.subscriptions = []

    def subscribe(self, topic):
        self.subscriptions.append(topic)


def test_on_connect_subscribes_to_agg_and_bus_topics():
    hub = FakeHubGateway()
    adapter = AgentMQTTAdapter(
        broker_host="localhost",
        broker_port=1883,
        topic="agent_data_topic/aggregated",
        bus_topic="agent_data_topic/bus",
        hub_gateway=hub,
    )
    fake_client = FakeClient()
    adapter.client = fake_client

    adapter.on_connect(fake_client, None, None, 0)

    assert "agent_data_topic/aggregated" in fake_client.subscriptions
    assert "agent_data_topic/bus" in fake_client.subscriptions


def test_on_message_processes_aggregated_array_and_sends_to_hub():
    hub = FakeHubGateway()
    adapter = AgentMQTTAdapter(
        broker_host="localhost",
        broker_port=1883,
        topic="agent_data_topic/aggregated",
        bus_topic="agent_data_topic/bus",
        hub_gateway=hub,
    )
    payload = json.dumps(
        [
            {
                "user_id": 1,
                "accelerometer": {"x": 0.1, "y": 0.2, "z": 0.3},
                "gps": {"latitude": 50.45, "longitude": 30.52},
                "timestamp": "2026-03-20T12:00:00",
            },
            {
                "user_id": 2,
                "accelerometer": {"x": 2.2, "y": 0.1, "z": 0.1},
                "gps": {"latitude": 50.46, "longitude": 30.53},
                "timestamp": "2026-03-20T12:00:01",
            },
        ]
    )
    msg = FakeMessage("agent_data_topic/aggregated", payload)

    adapter.on_message(None, None, msg)

    assert len(hub.saved) == 2
    assert all(item.agent_data is not None for item in hub.saved)


def test_on_message_processes_bus_array_and_sends_to_hub():
    hub = FakeHubGateway()
    adapter = AgentMQTTAdapter(
        broker_host="localhost",
        broker_port=1883,
        topic="agent_data_topic/aggregated",
        bus_topic="agent_data_topic/bus",
        hub_gateway=hub,
    )
    payload = json.dumps(
        [
            {
                "bus_id": 101,
                "occupancy_rate": 0.65,
                "gps": {"latitude": 50.55, "longitude": 30.62},
            }
        ]
    )
    msg = FakeMessage("agent_data_topic/bus", payload)

    adapter.on_message(None, None, msg)

    assert len(hub.saved) == 1
    assert hub.saved[0].road_state == "bus_occupancy"
    assert hub.saved[0].bus_occupancy_data is not None
    assert hub.saved[0].bus_occupancy_data.bus_id == 101
