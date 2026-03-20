from dataclasses import dataclass
from domain.gps import Gps


@dataclass
class BusOccupancy:
    bus_id: int
    occupancy_rate: float
    gps: Gps
