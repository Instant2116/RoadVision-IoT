import csv
import os
from dataclasses import dataclass

@dataclass
class SensorRecord:
    latitude: float
    longitude: float
    x: float
    y: float
    z: float

class FileDatasource:
    def __init__(self, filepath: str, start_lat: float, start_lon: float, scale_factor: float):
        self.filepath = filepath
        self.data = []
        self.static_lat = start_lat
        self.static_lon = start_lon
        self.scale_factor = scale_factor
        self._load_data()

    def _load_data(self):
        if not os.path.exists(self.filepath):
            print(f"Error: Data file '{self.filepath}' not found.")
            return

        with open(self.filepath, mode='r') as file:
            reader = csv.DictReader(file, skipinitialspace=True)
            for row in reader:
                try:
                    # Normalize raw values by the scale factor from settings
                    self.data.append(SensorRecord(
                        latitude=self.static_lat,
                        longitude=self.static_lon,
                        x=float(row['X']) / self.scale_factor,
                        y=float(row['Y']) / self.scale_factor,
                        z=float(row['Z']) / self.scale_factor
                    ))
                except (ValueError, KeyError) as e:
                    print(f"Skipping malformed row: {e}")

    def get_next_record(self) -> SensorRecord | None:
        if self.data:
            return self.data.pop(0)
        return None