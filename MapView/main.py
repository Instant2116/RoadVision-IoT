import json
import os
from kivy.app import App
from kivy_garden.mapview import MapMarker, MapView
from kivy.clock import Clock
from FileDatasource import FileDatasource, SensorRecord


class MapViewApp(App):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.car_marker = None
        self.settings = self._load_settings()

        # Extract configuration
        start_pos = self.settings.get("default_start_position", {"latitude": 48.4647, "longitude": 35.0461})
        self.start_lat = start_pos["latitude"]
        self.start_lon = start_pos["longitude"]
        scale_factor = self.settings.get("scale_factor", 16384.0)
        data_file = self.settings.get("data_file", "data.csv")

        # Initialize datasource with scale factor
        self.datasource = FileDatasource(data_file, self.start_lat, self.start_lon, scale_factor)

    def _load_settings(self) -> dict:
        settings_file = 'settings.json'
        if os.path.exists(settings_file):
            with open(settings_file, 'r') as file:
                return json.load(file)
        return {}

    def on_start(self):
        Clock.schedule_interval(self.update, 0.1)

    def update(self, *args):
        record = self.datasource.get_next_record()
        if not record:
            return False

        point = (record.latitude, record.longitude)
        self.update_car_marker(point)
        self.check_road_quality(record)

    def check_road_quality(self, record: SensorRecord):
        # We use normalized values (after scale_factor) for thresholds
        p_thresh = self.settings.get("pothole_threshold_pct", 0.4)
        b_thresh = self.settings.get("bump_threshold_pct", 1.4)

        # Logic now uses normalized Z directly
        if record.z < p_thresh:
            self.set_pothole_marker((record.latitude, record.longitude))
        elif record.z > b_thresh:
            self.set_bump_marker((record.latitude, record.longitude))

    def update_car_marker(self, point: tuple[float, float]):
        lat, lon = point
        if self.car_marker is None:
            self.car_marker = MapMarker(lat=lat, lon=lon, source='images/car.png')
            self.mapview.add_marker(self.car_marker)
        else:
            self.car_marker.lat = lat
            self.car_marker.lon = lon

        # Keep car on top of the visual stack
        self.mapview.remove_marker(self.car_marker)
        self.mapview.add_marker(self.car_marker)
        self.mapview.center_on(lat, lon)

    def set_pothole_marker(self, point: tuple[float, float]):
        lat, lon = point[0] + 0.0001, point[1] + 0.0001
        self.mapview.add_marker(MapMarker(lat=lat, lon=lon, source='images/pothole.png'))

    def set_bump_marker(self, point: tuple[float, float]):
        lat, lon = point[0] - 0.0001, point[1] - 0.0001
        self.mapview.add_marker(MapMarker(lat=lat, lon=lon, source='images/bump.png'))

    def build(self):
        self.mapview = MapView(zoom=16, lat=self.start_lat, lon=self.start_lon)
        return self.mapview


if __name__ == "__main__":
    MapViewApp().run()