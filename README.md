# tempestd
Data collection daemon for WeatherFlow Tempest weather stations. Maintains persistent WebSocket connections, stores 1-minute resolution observations in SQLite or PostgreSQL,  auto-backfills data gaps, and exposes a REST API for querying historical and real-time weather data.
