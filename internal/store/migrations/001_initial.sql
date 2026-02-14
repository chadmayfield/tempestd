-- +goose Up
CREATE TABLE IF NOT EXISTS stations (
    id INTEGER PRIMARY KEY,
    device_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    latitude REAL,
    longitude REAL,
    elevation REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL REFERENCES stations(id),
    timestamp TIMESTAMP NOT NULL,
    wind_lull REAL,
    wind_avg REAL,
    wind_gust REAL,
    wind_direction REAL,
    station_pressure REAL,
    air_temperature REAL,
    relative_humidity REAL,
    illuminance REAL,
    uv_index REAL,
    solar_radiation REAL,
    rain_accumulation REAL,
    precipitation_type INTEGER,
    lightning_avg_distance REAL,
    lightning_strike_count INTEGER,
    battery REAL,
    report_interval INTEGER,
    feels_like REAL,
    dew_point REAL,
    wet_bulb REAL,
    UNIQUE(station_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_observations_station_time ON observations(station_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_observations_timestamp ON observations(timestamp);

-- +goose Down
DROP TABLE IF EXISTS observations;
DROP TABLE IF EXISTS stations;
