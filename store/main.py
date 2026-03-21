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
    insert,
    select,
)
from sqlalchemy.orm import sessionmaker
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
bus_occupancy_data = Table(
    "bus_occupancy_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("bus_id", Integer, nullable=False),
    Column("occupancy_rate", Float, nullable=False),
    Column("latitude", Float, nullable=False),
    Column("longitude", Float, nullable=False),
    Column("timestamp", DateTime, nullable=False),
)
SessionLocal = sessionmaker(bind=engine)
metadata.create_all(engine)

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


class BusOccupancyData(BaseModel):
    bus_id: int
    occupancy_rate: float
    gps: GpsData
    timestamp: datetime


class IngestedData(BaseModel):
    road_state: str
    agent_data: AgentData | None = None
    bus_occupancy_data: BusOccupancyData | None = None


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
        for websocket in subscriptions[user_id]:
            # await websocket.send_json(json.dumps(data))
            await websocket.send_json(data)


# FastAPI CRUDL endpoints


@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[IngestedData]):
    # Insert data to database
    # Send data to subscribers
    sent = 0
    db = SessionLocal()
    try:
        for item in data:
            if item.bus_occupancy_data is not None:
                db.execute(
                    insert(bus_occupancy_data).values(
                        bus_id=item.bus_occupancy_data.bus_id,
                        occupancy_rate=item.bus_occupancy_data.occupancy_rate,
                        latitude=item.bus_occupancy_data.gps.latitude,
                        longitude=item.bus_occupancy_data.gps.longitude,
                        timestamp=item.bus_occupancy_data.timestamp,
                    )
                )
                continue
            if item.agent_data is None:
                continue
            row = {
                "road_state": item.road_state,
                "user_id": item.agent_data.user_id,
                "x": item.agent_data.accelerometer.x,
                "y": item.agent_data.accelerometer.y,
                "z": item.agent_data.accelerometer.z,
                "latitude": item.agent_data.gps.latitude,
                "longitude": item.agent_data.gps.longitude,
                "timestamp": item.agent_data.timestamp,
            }
            db.execute(insert(processed_agent_data).values(**row))

            # websocket (можно оставить)
            await send_data_to_subscribers(item.agent_data.user_id, {
                "road_state": row["road_state"],
                "user_id": row["user_id"],
                "x": row["x"], "y": row["y"], "z": row["z"],
                "latitude": row["latitude"], "longitude": row["longitude"],
                "timestamp": row["timestamp"].isoformat(),
            })
            sent += 1

        db.commit()
    finally:
        db.close()

    return {"sent": sent}


# @app.get(
#     "/processed_agent_data/{processed_agent_data_id}",
#     response_model=ProcessedAgentDataInDB,
# )
@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    db = SessionLocal()
    try:
        rows = db.execute(select(processed_agent_data)).mappings().all()
        # rows = list[RowMapping], превращаем в обычные dict
        return [dict(r) for r in rows]
    finally:
        db.close()


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


# @app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
# def list_processed_agent_data():
#     # Get list of data
#     pass


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
