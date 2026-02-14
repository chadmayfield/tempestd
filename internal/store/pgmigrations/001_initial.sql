-- +goose Up
CREATE TABLE IF NOT EXISTS stations (
    id INTEGER PRIMARY KEY,
    device_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    elevation DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS observations (
    id BIGSERIAL PRIMARY KEY,
    station_id INTEGER NOT NULL REFERENCES stations(id),
    timestamp TIMESTAMPTZ NOT NULL,
    wind_lull DOUBLE PRECISION,
    wind_avg DOUBLE PRECISION,
    wind_gust DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    station_pressure DOUBLE PRECISION,
    air_temperature DOUBLE PRECISION,
    relative_humidity DOUBLE PRECISION,
    illuminance DOUBLE PRECISION,
    uv_index DOUBLE PRECISION,
    solar_radiation DOUBLE PRECISION,
    rain_accumulation DOUBLE PRECISION,
    precipitation_type INTEGER,
    lightning_avg_distance DOUBLE PRECISION,
    lightning_strike_count INTEGER,
    battery DOUBLE PRECISION,
    report_interval INTEGER,
    feels_like DOUBLE PRECISION,
    dew_point DOUBLE PRECISION,
    wet_bulb DOUBLE PRECISION,
    UNIQUE(station_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_observations_station_time ON observations(station_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_observations_timestamp ON observations(timestamp);

-- +goose Down
DROP TABLE IF EXISTS observations;
DROP TABLE IF EXISTS stations;
