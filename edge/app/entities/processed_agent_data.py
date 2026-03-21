from pydantic import BaseModel
from pydantic import model_validator

from app.entities.agent_data import AgentData
from app.entities.bus_occupancy_data import BusOccupancyData


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData | None = None
    bus_occupancy_data: BusOccupancyData | None = None

    @model_validator(mode="after")
    def validate_payload(self):
        if self.agent_data is None and self.bus_occupancy_data is None:
            raise ValueError(
                "Either agent_data or bus_occupancy_data must be provided."
            )
        return self
