import json
from kivy.app import App
from kivy_garden.mapview import MapMarker, MapView
from kivy.clock import Clock
from FileDatasource import FileDatasource

class MapViewApp(App):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.car_marker = None
        self.settings = self._load_settings()
        s = self.settings
        pos = s.get("default_start_position", {"latitude": 48.4647, "longitude": 35.0461})

        
        self.road_ds = FileDatasource(
            filepath="data.csv",
            is_occupancy=False,
            start_lat=pos["latitude"],
            start_lon=pos["longitude"],
            scale_factor=s.get("scale_factor", 16384.0),
            gravity_base=s.get("gravity_base", 16500),
            p_thresh=s.get("pothole_threshold_pct", 0.4),
            b_thresh=s.get("bump_threshold_pct", 1.4)
        )

        
        self.occ_ds = FileDatasource(
            filepath="data2.csv",
            is_occupancy=True,
            start_lat=pos["latitude"],
            start_lon=pos["longitude"]
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
        
        for lat, lon, state in self.road_ds.get_new_points():
            self.update_car_marker(lat, lon)
            if state == "pothole":
                self.set_pothole_marker(lat, lon)
            elif state == "bump":
                self.set_bump_marker(lat, lon)

        
        for lat, lon, state in self.occ_ds.get_new_points():
            if state.startswith("occ:"):
                
                self.mapview.add_marker(MapMarker(lat=lat, lon=lon))

    def update_car_marker(self, lat, lon):
        if self.car_marker is None:
            self.car_marker = MapMarker(lat=lat, lon=lon)
            self.mapview.add_marker(self.car_marker)
        else:
            self.car_marker.lat, self.car_marker.lon = lat, lon

        self.mapview.remove_marker(self.car_marker)
        self.mapview.add_marker(self.car_marker)
        self.mapview.center_on(lat, lon)

    def set_pothole_marker(self, lat, lon):
        self.mapview.add_marker(
            MapMarker(lat=lat, lon=lon, source='images/pothole.png'))

    def set_bump_marker(self, lat, lon):
        self.mapview.add_marker(
            MapMarker(lat=lat, lon=lon, source='images/bump.png'))

    def build(self):
        pos = self.settings.get("default_start_position", {"latitude": 48.4647, "longitude": 35.0461})
        self.mapview = MapView(zoom=12, lat=pos["latitude"], lon=pos["longitude"])
        return self.mapview

if __name__ == "__main__":
    MapViewApp().run()