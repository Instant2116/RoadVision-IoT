from datetime import datetime
from pydantic import BaseModel

from app.entities.agent_data import GpsData


class BusOccupancyData(BaseModel):
    bus_id: int
    occupancy_rate: float
    gps: GpsData
    timestamp: datetime
