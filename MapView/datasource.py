# datasource.py
import asyncio
import json
import queue
import threading
import time
from typing import List, Tuple, Optional

import requests
import websockets

from config import STORE_HOST, STORE_PORT


Point = Tuple[float, float, str]  # lat, lon, road_state


class Datasource:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.connection_status: Optional[str] = None

        # очередь точек между потоками
        self._q: "queue.Queue[Point]" = queue.Queue()

        # 1) подтянуть историю из Store (чтобы маркеры появились сразу)
        self._preload_points()

        # 2) websocket слушаем в отдельном потоке со своим asyncio loop
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

    # ---------- HTTP preload ----------
    def _preload_points(self):
        url = f"http://{STORE_HOST}:{STORE_PORT}/processed_agent_data/"
        try:
            r = requests.get(url, timeout=5)
            r.raise_for_status()
            rows = r.json()

            added = 0
            for row in rows:
                if int(row.get("user_id", -1)) != int(self.user_id):
                    continue

                lat = row.get("latitude")
                lon = row.get("longitude")
                state = row.get("road_state", "normal")

                if lat is None or lon is None:
                    continue

                self._q.put((float(lat), float(lon), str(state)))
                added += 1

            print(
                f"[Datasource] preload rows={len(rows)} queued={added} from {url}")
        except Exception as e:
            print(f"[Datasource] preload error from {url}: {e}")

    # ---------- WebSocket ----------
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

                    # сервер ждёт receive_text() в цикле, поэтому иногда отправляем текст
                    async def keepalive():
                        while True:
                            try:
                                await ws.send("ping")
                            except Exception:
                                return
                            await asyncio.sleep(10)

                    ka_task = asyncio.create_task(keepalive())

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
        """
        Store шлёт send_json(dict), значит здесь приходит JSON-строка объекта:
        {"road_state": "...", "latitude": ..., "longitude": ..., "user_id": ...}
        """
        try:
            data = json.loads(message)
        except Exception:
            return

        # иногда удобно поддержать и формат "список"
        items = data if isinstance(data, list) else [data]

        for obj in items:
            try:
                if int(obj.get("user_id", -1)) != int(self.user_id):
                    continue
                lat = obj.get("latitude")
                lon = obj.get("longitude")
                state = obj.get("road_state", "normal")
                if lat is None or lon is None:
                    continue
                self._q.put((float(lat), float(lon), str(state)))
            except Exception:
                continue
