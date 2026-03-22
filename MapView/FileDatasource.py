import csv
import os

class FileDatasource:
    def __init__(self, filepath: str, is_occupancy: bool, start_lat: float, start_lon: float, 
                 scale_factor: float = 16384.0, gravity_base: float = 16500,
                 p_thresh: float = 0.4, b_thresh: float = 1.4):
        self.filepath = filepath
        self.is_occupancy = is_occupancy
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
            if self.is_occupancy:
                
                reader = csv.reader(file)
                for row in reader:
                    if not row or len(row) < 3:
                        continue
                    try:
                        lat = float(row[0])
                        lon = float(row[1])
                        val = float(row[2])
                        self.data.append((lat, lon, f"occ:{val}"))
                    except ValueError:
                        continue
            else:
                
                reader = csv.DictReader(file, skipinitialspace=True)
                for row in reader:
                    try:
                        
                        g_ratio = float(row['Z']) / self.gravity_base

                        state = "normal"
                        if g_ratio < self.p_thresh:
                            state = "pothole"
                        elif g_ratio > self.b_thresh:
                            state = "bump"

                        
                        self.data.append((self.static_lat, self.static_lon, state))
                    except (ValueError, KeyError):
                        continue

    def get_new_points(self):
        if self.data:
            points = self.data[:]
            self.data = []
            return points
        return []