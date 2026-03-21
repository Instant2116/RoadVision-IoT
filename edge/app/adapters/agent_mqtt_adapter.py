import json
import logging
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from app.interfaces.agent_gateway import AgentGateway
from app.entities.agent_data import AgentData
from app.entities.bus_occupancy_data import BusOccupancyData
from app.entities.processed_agent_data import ProcessedAgentData
from app.usecases.data_processing import process_agent_data
from app.interfaces.hub_gateway import HubGateway


class AgentMQTTAdapter(AgentGateway):
    def __init__(
        self,
        broker_host,
        broker_port,
        topic,
        bus_topic,
        hub_gateway: HubGateway,
        batch_size=10,
    ):
        self.batch_size = batch_size
        # MQTT
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.bus_topic = bus_topic
        self.client = mqtt.Client()
        # Hub
        self.hub_gateway = hub_gateway

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT broker")
            self.client.subscribe(self.topic)
            self.client.subscribe(self.bus_topic)
        else:
            logging.info(f"Failed to connect to MQTT broker with code: {rc}")

    def on_message(self, client, userdata, msg):
        """Processing agent data and send it to hub gateway. Handles both single object and array payloads (agent publishes arrays)."""
        try:
            payload: str = msg.payload.decode("utf-8")
            parsed = json.loads(payload)
            items = parsed if isinstance(parsed, list) else [parsed]
            for item in items:
                if msg.topic == self.bus_topic:
                    bus_data = BusOccupancyData.model_validate(
                        {
                            **item,
                            # Agent bus payload has no timestamp; stamp when ingested by edge.
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    processed_data = ProcessedAgentData(
                        road_state="bus_occupancy", bus_occupancy_data=bus_data
                    )
                else:
                    agent_data = AgentData.model_validate(item)
                    processed_data = process_agent_data(agent_data)
                if not self.hub_gateway.save_data(processed_data):
                    logging.error("Hub is not available")
        except Exception as e:
            logging.info(f"Error processing MQTT message: {e}")

    def connect(self):
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.broker_host, self.broker_port, 60)

    def start(self):
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()


# Usage example:
if __name__ == "__main__":
    from app.adapters.hub_mqtt_adapter import HubMqttAdapter
    from config import (
        MQTT_BROKER_HOST,
        MQTT_BROKER_PORT,
        MQTT_TOPIC,
        MQTT_TOPIC_BUS,
        HUB_MQTT_BROKER_HOST,
        HUB_MQTT_BROKER_PORT,
        HUB_MQTT_TOPIC,
    )

    hub_gateway = HubMqttAdapter(
        broker=HUB_MQTT_BROKER_HOST,
        port=HUB_MQTT_BROKER_PORT,
        topic=HUB_MQTT_TOPIC,
    )
    adapter = AgentMQTTAdapter(
        broker_host=MQTT_BROKER_HOST,
        broker_port=MQTT_BROKER_PORT,
        topic=MQTT_TOPIC,
        bus_topic=MQTT_TOPIC_BUS,
        hub_gateway=hub_gateway,
    )
    adapter.connect()
    adapter.start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        adapter.stop()
        logging.info("Adapter stopped.")
