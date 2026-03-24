import json
from kivy.app import App
from kivy_garden.mapview import MapMarker, MapView
from kivy.clock import Clock
# from FileDatasource import FileDatasource
from datasource import Datasource


class MapViewApp(App):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.car_marker = None

        self.settings = self._load_settings()
        self.ds = Datasource(user_id=1)

    def _load_settings(self):
        try:
            with open('settings.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def on_start(self):
        Clock.schedule_interval(self.update, 0.1)

    def update(self, *args):

        # for lat, lon, state in self.road_ds.get_new_points():
        for lat, lon, state in self.ds.get_new_points():
            self.update_car_marker(lat, lon)
            if state == "pothole":
                self.set_pothole_marker(lat, lon)
            elif state == "bump":
                self.set_bump_marker(lat, lon)
            # elif state == "bus":
            #     self.set_bus_marker(lat, lon)
            elif state == "bus_low":
                self.set_bus_marker(lat, lon, 'images/bus_low.png')
            elif state == "bus_medium":
                self.set_bus_marker(lat, lon, 'images/bus_medium.png')
            elif state == "bus_high":
                self.set_bus_marker(lat, lon, 'images/bus_high.png')

        # for lat, lon, state in self.occ_ds.get_new_points():
        #     if state.startswith("occ:"):

        #         self.mapview.add_marker(MapMarker(lat=lat, lon=lon))

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

    # def set_bus_marker(self, lat, lon, icon):
    #     self.mapview.add_marker(
    #         MapMarker(lat=lat, lon=lon, source=icon))
    def set_bus_marker(self, lat, lon, icon):
        marker = MapMarker(lat=lat, lon=lon, source=icon)
        marker.size = (80, 80)
        marker.anchor_x = 0.5
        marker.anchor_y = 0.5
        self.mapview.add_marker(marker)

    def build(self):
        pos = self.settings.get("default_start_position", {
                                "latitude": 48.4647, "longitude": 35.0461})
        self.mapview = MapView(
            zoom=12, lat=pos["latitude"], lon=pos["longitude"])
        return self.mapview


if __name__ == "__main__":
    MapViewApp().run()
