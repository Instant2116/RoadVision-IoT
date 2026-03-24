import json
import queue
import threading
import time
from typing import List, Tuple, Optional

import requests
import websockets

from config import STORE_HOST, STORE_PORT


Point = Tuple[float, float, str]  # lat, lon, type/state


class Datasource:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.connection_status: Optional[str] = None
        self._q: "queue.Queue[Point]" = queue.Queue()

        self._preload_points()

        t = threading.Thread(target=self._ws_thread, daemon=True)
        t.start()

    def get_new_points(self) -> List[Point]:
        points: List[Point] = []
        while True:
            try:
                points.append(self._q.get_nowait())
            except queue.Empty:
                break
        return points

    def _preload_points(self):
        url = f"http://{STORE_HOST}:{STORE_PORT}/processed_agent_data/"
        try:
            r = requests.get(url, timeout=5)
            r.raise_for_status()
            rows = r.json()

            added = 0
            for row in rows:
                added += self._extract_points(row)

            print(
                f"[Datasource] preload rows={len(rows)} queued={added} from {url}")
        except Exception as e:
            print(f"[Datasource] preload error from {url}: {e}")

    def _ws_thread(self):
        import asyncio
        asyncio.run(self._ws_loop())

    async def _ws_loop(self):
        uri = f"ws://{STORE_HOST}:{STORE_PORT}/ws/{self.user_id}"
        print(f"[Datasource] WS connecting to {uri}")

        while True:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    self.connection_status = "Connected"
                    print("[Datasource] WS connected")

                    async def keepalive():
                        while True:
                            try:
                                await ws.send("ping")
                            except Exception:
                                return
                            await __import__("asyncio").sleep(10)

                    ka_task = __import__("asyncio").create_task(keepalive())

                    try:
                        async for message in ws:
                            self._handle_message(message)
                    finally:
                        ka_task.cancel()

            except Exception as e:
                self.connection_status = "Disconnected"
                print(f"[Datasource] WS error: {e}. Reconnect in 2s...")
                time.sleep(2)

    def _handle_message(self, message: str):
        try:
            data = json.loads(message)
        except Exception:
            return

        items = data if isinstance(data, list) else [data]

        for obj in items:
            try:
                self._extract_points(obj)
            except Exception:
                continue

    def _extract_points(self, obj) -> int:
        added = 0

        if "latitude" in obj and "longitude" in obj:
            try:
                if int(obj.get("user_id", -1)) == int(self.user_id):
                    lat = obj.get("latitude")
                    lon = obj.get("longitude")
                    state = obj.get("road_state", "normal")

                    if lat is not None and lon is not None:
                        self._q.put((float(lat), float(lon), str(state)))
                        added += 1
            except Exception:
                pass

        # new road event
        agent = obj.get("agent_data")
        if agent and agent.get("gps"):
            try:
                if int(agent.get("user_id", -1)) == int(self.user_id):
                    gps = agent["gps"]
                    lat = gps.get("latitude")
                    lon = gps.get("longitude")
                    state = obj.get("road_state", "normal")

                    if lat is not None and lon is not None:
                        self._q.put((float(lat), float(lon), str(state)))
                        added += 1
            except Exception:
                pass

        # new bus event
        bus = obj.get("bus_occupancy_data")
        if bus and bus.get("gps"):
            try:
                gps = bus["gps"]
                lat = gps.get("latitude")
                lon = gps.get("longitude")

                if lat is not None and lon is not None:
                    rate = bus.get("occupancy_rate", 0)
                    if rate < 0.33:
                        state = "bus_low"
                    elif rate < 0.66:
                        state = "bus_medium"
                    else:
                        state = "bus_high"
                    self._q.put((float(lat), float(lon), str(state)))
                    added += 1
            except Exception:
                pass

        return added
