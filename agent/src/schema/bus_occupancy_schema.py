from marshmallow import Schema, fields
from schema.gps_schema import GpsSchema


class BusOccupancySchema(Schema):
    bus_id = fields.Int()
    occupancy_rate = fields.Float()
    gps = fields.Nested(GpsSchema)
