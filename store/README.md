# Store Service (RoadVision-IoT)

`store` — це сервіс збереження та читання даних для RoadVision-IoT:
- приймає оброблені події від `hub/edge`;
- зберігає дані у PostgreSQL;
- надає REST API та WebSocket-канали;
- віддає аналітичні агрегати для dashboard.

## Що було покращено (P1)

### 1) Аналітичні endpoint-и
Додано 2 нові endpoint-и:
- `GET /analytics/road_state_summary`
- `GET /analytics/bus_occupancy_summary`

Обидва endpoint-и підтримують опційні параметри періоду:
- `from` — початок періоду (ISO datetime)
- `to` — кінець періоду (ISO datetime)

Якщо `from/to` не передані, агрегація рахується по всіх записах.

### 2) Індекси БД для прискорення аналітики
Додано індекси в `processed_agent_data`:
- `timestamp`
- `user_id`
- `road_state`

### 3) Покриття тестами
Додано тести для нових endpoint-ів:
- позитивний сценарій;
- порожній результат за період;
- невалідна дата.

## Швидкий запуск (Docker, рекомендовано)

> Працювати з каталогу `store/docker`.

```bash
cd store/docker
docker compose up -d --build
```

Перевірити статус:
```bash
docker compose ps
```

Зупинити:
```bash
docker compose down
```

Повний reset (з volume):
```bash
docker compose down -v
```

## Доступ після запуску

- Store API: `http://localhost:8000`
- Swagger UI: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`
- pgAdmin: `http://localhost:5050`
  - login: `admin@admin.com`
  - password: `root`

Параметри PostgreSQL:
- host: `postgres_db` (всередині Docker-мережі) / `localhost` (з хоста)
- port: `5432`
- db: `test_db`
- user: `user`
- password: `pass`

## Основні API endpoint-и

### Ingest
- `POST /processed_agent_data/`  
Приймає масив подій `IngestedData` і зберігає:
- `processed_agent_data` (стан дороги);
- `bus_occupancy_data` (заповненість транспорту).

### Read
- `GET /processed_agent_data/`  
Повертає список записів `processed_agent_data`.

### Analytics
- `GET /analytics/road_state_summary`
- `GET /analytics/bus_occupancy_summary`

Приклад запиту з фільтром періоду:
```bash
curl "http://localhost:8000/analytics/road_state_summary?from=2026-03-20T00:00:00&to=2026-03-21T00:00:00"
```

Приклад відповіді:
```json
[
  {"road_state": "pothole", "events_count": 12},
  {"road_state": "normal", "events_count": 7}
]
```

Приклад запиту:
```bash
curl "http://localhost:8000/analytics/bus_occupancy_summary"
```

Приклад відповіді:
```json
{
  "records_count": 5,
  "avg_occupancy_rate": 0.53,
  "min_occupancy_rate": 0.2,
  "max_occupancy_rate": 0.9
}
```

## Запуск тестів

### Варіант 1: у контейнері
```bash
cd store/docker
docker compose run --rm store sh -lc "pip install fastapi pytest httpx >/dev/null && pytest -q -p no:cacheprovider"
```

### Варіант 2: локально
```bash
cd store
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest httpx
pytest -q -p no:cacheprovider
```

## WebSocket endpoint-и

- `ws://localhost:8000/ws/` — публічна підписка
- `ws://localhost:8000/ws/{user_id}` — підписка за конкретним `user_id`

## Файли, змінені в P1

- `store/main.py` — додані analytics endpoint-и.
- `store/docker/db/structure.sql` — додані індекси.
- `store/tests/test_analytics_endpoints.py` — додані тести.
- `store/requirements.txt` — додані runtime-залежності (`fastapi`, `uvicorn`, `psycopg2-binary`).
