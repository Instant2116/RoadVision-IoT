CREATE TABLE processed_agent_data (
    id SERIAL PRIMARY KEY,
    road_state VARCHAR(255) NOT NULL,
    user_id INTEGER NOT NULL,
    x FLOAT,
    y FLOAT,
    z FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    timestamp TIMESTAMP
);

CREATE INDEX idx_processed_agent_data_timestamp
    ON processed_agent_data (timestamp);

CREATE INDEX idx_processed_agent_data_user_id
    ON processed_agent_data (user_id);

CREATE INDEX idx_processed_agent_data_road_state
    ON processed_agent_data (road_state);

CREATE TABLE bus_occupancy_data (
    id SERIAL PRIMARY KEY,
    bus_id INTEGER NOT NULL,
    occupancy_rate FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
