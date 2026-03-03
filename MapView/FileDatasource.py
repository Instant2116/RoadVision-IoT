import csv
import os

class FileDatasource:
    def __init__(self, filepath: str, start_lat: float, start_lon: float, scale_factor: float, gravity_base: float,
                 p_thresh: float, b_thresh: float):
        self.filepath = filepath
        self.data = []
        self.static_lat = start_lat
        self.static_lon = start_lon
        self.scale_factor = scale_factor
        self.gravity_base = gravity_base
        self.p_thresh = p_thresh
        self.b_thresh = b_thresh
        self._load_data()

    def _load_data(self):
        if not os.path.exists(self.filepath):
            return
        with open(self.filepath, mode='r') as file:
            reader = csv.DictReader(file, skipinitialspace=True)
            for row in reader:
                try:
                    z_normalized = float(row['Z']) / self.scale_factor
                    g_ratio = float(row['Z']) / self.gravity_base

                    # Determine road state based on our logic
                    state = "normal"
                    if g_ratio < self.p_thresh:
                        state = "pothole"
                    elif g_ratio > self.b_thresh:
                        state = "bump"

                    self.data.append((self.static_lat, self.static_lon, state))
                except (ValueError, KeyError):
                    continue

    def get_new_points(self):
        """Matches the original Datasource interface"""
        if self.data:
            # Return all current data as a list and clear it,
            # simulating the websocket 'buffer'
            points = self.data[:]
            self.data = []
            return points
        return []