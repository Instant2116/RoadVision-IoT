from datetime import datetime

from domain.accelerometer import Accelerometer
from domain.aggregated_data import AggregatedData
from domain.bus_occupancy import BusOccupancy
from domain.gps import Gps
from domain.parking import Parking
from schema.accelerometer_schema import AccelerometerSchema
from schema.aggregated_data_schema import AggregatedDataSchema
from schema.bus_occupancy_schema import BusOccupancySchema
from schema.gps_schema import GpsSchema
from schema.parking_schema import ParkingSchema


def test_accelerometer_schema_serializes_fields():
    dumped = AccelerometerSchema().dump(Accelerometer(x=1, y=2, z=3))

    assert dumped == {"x": 1, "y": 2, "z": 3}


def test_gps_schema_serializes_fields():
    dumped = GpsSchema().dump(Gps(longitude=30.52, latitude=50.45))

    assert dumped["longitude"] == 30.52
    assert dumped["latitude"] == 50.45


def test_parking_schema_serializes_nested_gps():
    dumped = ParkingSchema().dump(Parking(empty_count=12, gps=Gps(30.52, 50.45)))

    assert dumped["empty_count"] == 12
    assert dumped["gps"]["longitude"] == 30.52
    assert dumped["gps"]["latitude"] == 50.45


def test_bus_occupancy_schema_serializes_expected_fields():
    dumped = BusOccupancySchema().dump(
        BusOccupancy(bus_id=101, occupancy_rate=0.65, gps=Gps(30.52, 50.45))
    )

    assert dumped["bus_id"] == 101
    assert dumped["occupancy_rate"] == 0.65
    assert dumped["gps"]["longitude"] == 30.52
    assert dumped["gps"]["latitude"] == 50.45


def test_aggregated_data_schema_serializes_nested_models():
    data = AggregatedData(
        accelerometer=Accelerometer(1, 2, 3),
        gps=Gps(30.52, 50.45),
        timestamp=datetime(2026, 3, 20, 12, 0, 0),
        user_id=7,
    )

    dumped = AggregatedDataSchema().dump(data)

    assert dumped["accelerometer"] == {"x": 1, "y": 2, "z": 3}
    assert dumped["gps"]["longitude"] == 30.52
    assert dumped["gps"]["latitude"] == 50.45
    assert dumped["timestamp"].startswith("2026-03-20T12:00:00")
    assert dumped["user_id"] == 7
