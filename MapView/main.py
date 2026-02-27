import json
from kivy.app import App
from kivy_garden.mapview import MapMarker, MapView
from kivy.clock import Clock

# Toggle these imports depending on what you are testing
from FileDatasource import FileDatasource
# from datasource import Datasource

class MapViewApp(App):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.car_marker = None
        self.settings = self._load_settings()

        # 1. Get settings
        s = self.settings
        pos = s.get("default_start_position", {"latitude": 48.4647, "longitude": 35.0461})

        # 2. Initialize the file-based datasource with all settings
        self.datasource = FileDatasource(
            filepath=s.get("data_file", "data.csv"),
            start_lat=pos["latitude"],
            start_lon=pos["longitude"],
            scale_factor=s.get("scale_factor", 16384.0),
            gravity_base=s.get("gravity_base", 16500),
            p_thresh=s.get("pothole_threshold_pct", 0.4),
            b_thresh=s.get("bump_threshold_pct", 1.4)
        )

    def _load_settings(self):
        try:
            with open('settings.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def on_start(self):
        Clock.schedule_interval(self.update, 0.1)

    def update(self, *args):
        new_points = self.datasource.get_new_points()

        for lat, lon, state in new_points:
            self.update_car_marker(lat, lon)
            if state == "pothole":
                self.set_pothole_marker(lat, lon)
            elif state == "bump":
                self.set_bump_marker(lat, lon)

    def update_car_marker(self, lat, lon):
        if self.car_marker is None:
            self.car_marker = MapMarker(lat=lat, lon=lon, source='images/car.png')
            self.mapview.add_marker(self.car_marker)
        else:
            self.car_marker.lat, self.car_marker.lon = lat, lon

        self.mapview.remove_marker(self.car_marker)
        self.mapview.add_marker(self.car_marker)
        self.mapview.center_on(lat, lon)

    def set_pothole_marker(self, lat, lon):
        self.mapview.add_marker(MapMarker(lat=lat + 0.0001, lon=lon + 0.0001, source='images/pothole.png'))

    def set_bump_marker(self, lat, lon):
        self.mapview.add_marker(MapMarker(lat=lat - 0.0001, lon=lon - 0.0001, source='images/bump.png'))

    def build(self):
        pos = self.settings.get("default_start_position", {"latitude": 48.4647, "longitude": 35.0461})
        self.mapview = MapView(zoom=16, lat=pos["latitude"], lon=pos["longitude"])
        return self.mapview


if __name__ == "__main__":
    MapViewApp().run()