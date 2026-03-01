from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    select,
    insert,
    update as sql_update,
    delete as sql_delete,
)
from datetime import datetime
from pydantic import BaseModel, field_validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}
public_subscriptions: Set[WebSocket] = set()


# FastAPI WebSocket endpoint
@app.websocket("/ws/")
async def websocket_public_endpoint(websocket: WebSocket):
    await websocket.accept()
    public_subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        public_subscriptions.discard(websocket)


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].discard(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data: Any):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id].copy():
            try:
                await websocket.send_json(data)
            except Exception:
                subscriptions[user_id].discard(websocket)
    for websocket in public_subscriptions.copy():
        try:
            await websocket.send_json(data)
        except Exception:
            public_subscriptions.discard(websocket)


def ensure_schema():
    metadata.create_all(engine)


def map_to_db_model(row_mapping) -> ProcessedAgentDataInDB:
    return ProcessedAgentDataInDB(**dict(row_mapping))


def payload_to_db_values(data: ProcessedAgentData) -> dict:
    return {
        "road_state": data.road_state,
        "user_id": data.agent_data.user_id,
        "x": data.agent_data.accelerometer.x,
        "y": data.agent_data.accelerometer.y,
        "z": data.agent_data.accelerometer.z,
        "latitude": data.agent_data.gps.latitude,
        "longitude": data.agent_data.gps.longitude,
        "timestamp": data.agent_data.timestamp,
    }


# FastAPI CRUDL endpoints


@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    if not data:
        return {"status": "ok", "inserted": 0}
    ensure_schema()
    inserted: List[ProcessedAgentDataInDB] = []
    with engine.connect() as conn:
        for item in data:
            stmt = (
                insert(processed_agent_data)
                .values(**payload_to_db_values(item))
                .returning(*processed_agent_data.c)
            )
            row = conn.execute(stmt).mappings().one()
            inserted.append(map_to_db_model(row))
        conn.commit()
    by_user: Dict[int, List[dict]] = {}
    for row in inserted:
        by_user.setdefault(row.user_id, []).append(row.model_dump(mode="json"))
    for user_id, payload in by_user.items():
        await send_data_to_subscribers(user_id, payload)
    return {"status": "ok", "inserted": len(inserted)}


@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int):
    ensure_schema()
    with engine.connect() as conn:
        stmt = select(processed_agent_data).where(
            processed_agent_data.c.id == processed_agent_data_id
        )
        row = conn.execute(stmt).mappings().first()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"ProcessedAgentData with id={processed_agent_data_id} not found",
            )
        return map_to_db_model(row)


@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    ensure_schema()
    with engine.connect() as conn:
        stmt = select(processed_agent_data).order_by(processed_agent_data.c.id.asc())
        rows = conn.execute(stmt).mappings().all()
        return [map_to_db_model(row) for row in rows]


@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    ensure_schema()
    with engine.connect() as conn:
        stmt = (
            sql_update(processed_agent_data)
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .values(**payload_to_db_values(data))
            .returning(*processed_agent_data.c)
        )
        row = conn.execute(stmt).mappings().first()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"ProcessedAgentData with id={processed_agent_data_id} not found",
            )
        conn.commit()
        return map_to_db_model(row)


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    ensure_schema()
    with engine.connect() as conn:
        stmt = (
            sql_delete(processed_agent_data)
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .returning(*processed_agent_data.c)
        )
        row = conn.execute(stmt).mappings().first()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"ProcessedAgentData with id={processed_agent_data_id} not found",
            )
        conn.commit()
        return map_to_db_model(row)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
