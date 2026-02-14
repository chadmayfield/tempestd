# tempestd

Data collection daemon for [WeatherFlow Tempest](https://weatherflow.com/tempest-weather-system/) weather stations. Maintains persistent WebSocket connections, stores 1-minute resolution observations in SQLite or PostgreSQL, auto-backfills data gaps via the REST API, and exposes a REST API for querying historical and real-time weather data.

## Architecture

```
                    +---------------------------+
                    |   Tempest WebSocket API   |
                    |  wss://ws.weatherflow.com |
                    +------------+--------------+
                                 | obs_st every ~60s per device
                                 | evt_strike, evt_precip
                                 v
                    +-----------------------------------------------+
                    |                  tempestd                     |
                    |                                               |
                    |  +--------------+    +---------------------+  |
                    |  |  Collector   |    |   REST API Server   |  |
                    |  |              |    |                     |  |
                    |  | WebSocket ---+    | GET /api/v1/...     |  |
                    |  |  reader      |    | stations, current,  |  |
                    |  |              |    | observations,       |  |
                    |  | Backfill ----+    | summary, health     |  |
                    |  |  (REST)      |    |                     |  |
                    |  +------+-------+    +--------+-----------+   |
                    |         |                     |               |
                    |         v                     v               |
                    |   +--------------------------------------+    |
                    |   |         Storage Interface            |    |
                    |   |     SQLite  |  PostgreSQL            |    |
                    |   +--------------------------------------+    |
                    +-----------------------------------------------+
```

**Data flow:**
1. WebSocket collector receives real-time observations from all configured stations
2. Each observation is parsed, derived metrics computed (dew point, feels-like, wet bulb), and saved to the database
3. On startup, backfill detects gaps and fills them using REST API historical data
4. REST API server exposes stored data for querying by tempest-cli or other clients

## Features

- **Real-time collection** -- persistent WebSocket connections with automatic reconnection and exponential backoff
- **Gap backfilling** -- automatically detects and fills missing data from the WeatherFlow REST API on startup
- **Dual storage backends** -- SQLite for single-node deployments, PostgreSQL for production clusters
- **REST API** -- query current conditions, historical observations, daily summaries, and station metadata
- **Derived metrics** -- automatically computes dew point, feels-like temperature, and wet bulb temperature
- **Unit conversion** -- stores all data in metric; converts to imperial on API output via `?units=imperial`
- **Resolution downsampling** -- query observations at 1m, 5m, 30m, 1h, or 3h resolution
- **Auto-migration** -- database schema managed by goose; migrations run automatically on startup
- **Connection management** -- uses tempest-go ConnectionManager to track all WebSocket connections
- **Rate limiting** -- shared rate limiter (80 req/min) and circuit breaker (5 failures / 60s cooldown) for REST API calls

## Quick Start

### Prerequisites

- Go 1.24+
- [ko](https://ko.build/) (for container builds)
- SQLite (default) or PostgreSQL
- A WeatherFlow Tempest personal access token ([get one here](https://tempestwx.com/settings/tokens))

### Install

```bash
go install github.com/chadmayfield/tempestd@latest
```

Or build from source:

```bash
git clone https://github.com/chadmayfield/tempestd.git
cd tempestd
go build -o tempestd .
```

### Configure

Create `~/.config/tempestd/config.yaml`:

```yaml
listen_addr: ":8080"
log_format: json     # json or text

storage:
  driver: sqlite     # sqlite or postgres
  sqlite:
    path: /var/lib/tempestd/tempestd.db
  postgres:
    dsn: postgres://user:pass@localhost:5432/tempestd?sslmode=disable

stations:
  - token: "your-api-token"
    station_id: 12345
    device_id: 67890
    name: "Home Weather Station"

collection:
  backfill_on_startup: true    # auto-detect and fill gaps on startup
  backfill_max_days: 30        # max days to backfill on startup
```

### Quick Start with PostgreSQL

If using PostgreSQL instead of SQLite, create the database first:

```bash
createdb tempestd
```

Then update the storage section of your config:

```yaml
storage:
  driver: postgres
  postgres:
    dsn: postgres://user:pass@localhost:5432/tempestd?sslmode=disable
```

Migrations run automatically on startup for both backends.

### Run

```bash
tempestd serve --config ~/.config/tempestd/config.yaml
```

On startup, tempestd will:
1. Run database migrations
2. Backfill any gaps in observation data (up to 30 days by default)
3. Start WebSocket connections for real-time data collection
4. Start the REST API server

## Usage

### Commands

```
tempestd                          # Start daemon (default: serve)
tempestd serve                    # Start daemon explicitly
tempestd serve --listen :9090     # Override listen address
tempestd backfill --station 12345 --from 2024-01-01
tempestd backfill --station 12345 --from 2024-01-01 --to 2024-06-01
tempestd migrate                  # Run database migrations
tempestd migrate --dry-run        # Show pending migrations
tempestd status                   # Query health of running instance
tempestd status --server http://localhost:9090
tempestd version                  # Print version info
```

### Global Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | (auto-detect) | Config file path |
| `--log-format` | `json` | Log format: `text` or `json` |

### Serve Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | (from config) | HTTP listen address override |
| `--storage-driver` | (from config) | Storage driver override |

### Configuration

Config is loaded with this precedence: CLI flag > `$TEMPESTD_CONFIG` env > `~/.config/tempestd/config.yaml` > `/etc/tempestd/config.yaml`.

Environment variables use the `TEMPESTD_` prefix:

```bash
export TEMPESTD_CONFIG=/path/to/config.yaml
```

## REST API

All endpoints return JSON. Base path: `/api/v1`.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/health` | Health check with station statuses |
| GET | `/api/v1/stations` | List all stations |
| GET | `/api/v1/stations/{id}` | Get station metadata |
| GET | `/api/v1/stations/{id}/current` | Latest observation |
| GET | `/api/v1/stations/{id}/observations` | Historical observations |
| GET | `/api/v1/stations/{id}/summary` | Daily summary (high/low/avg) |
| GET | `/api/v1/stations/{id}/range` | Oldest/newest observation timestamps |

### Query Parameters

**Current** (`/current`):
- `units` -- `metric` (default) or `imperial`

**Observations** (`/observations`):
- `start` (required) -- RFC3339 or YYYY-MM-DD
- `end` (required) -- RFC3339 or YYYY-MM-DD
- `resolution` -- `1m` (default), `5m`, `30m`, `1h`, `3h`
- `units` -- `metric` (default) or `imperial`
- `limit` -- max rows (default 1000, max 10000)
- `offset` -- pagination offset

**Summary** (`/summary`):
- `date` (required) -- date `YYYY-MM-DD`
- `units` -- `metric` (default) or `imperial`

### Examples

```bash
# Current conditions
curl http://localhost:8080/api/v1/stations/12345/current

# Current conditions in imperial units
curl "http://localhost:8080/api/v1/stations/12345/current?units=imperial"

# Last 24 hours at 30-minute resolution
curl "http://localhost:8080/api/v1/stations/12345/observations?\
start=$(date -u -v-1d +%Y-%m-%dT%H:%M:%SZ)&\
end=$(date -u +%Y-%m-%dT%H:%M:%SZ)&\
resolution=30m"

# Daily summary for June 15, 2024
curl "http://localhost:8080/api/v1/stations/12345/summary?date=2024-06-15"

# Data range / availability
curl http://localhost:8080/api/v1/stations/12345/range

# Health check
curl http://localhost:8080/api/v1/health
```

### Error Responses

All errors return JSON with `error` and `code` fields:

```json
{
  "error": "station not found",
  "code": 404
}
```

## Storage

### SQLite (default)

Best for single-node deployments. Database file is created automatically with 0600 permissions. Uses WAL journal mode for concurrent reads. At ~12 MB/year/station, storage is not a concern.

```yaml
storage:
  driver: sqlite
  sqlite:
    path: /var/lib/tempestd/tempestd.db
```

### PostgreSQL

Best for production deployments. Requires an existing database.

```yaml
storage:
  driver: postgres
  postgres:
    dsn: postgres://user:pass@localhost:5432/tempestd?sslmode=disable
```

## Deployment

### systemd

```bash
sudo cp deploy/tempestd.service /etc/systemd/system/
sudo useradd -r -s /usr/sbin/nologin tempestd
sudo mkdir -p /var/lib/tempestd /etc/tempestd
sudo cp config.yaml /etc/tempestd/
sudo systemctl enable --now tempestd
```

### Kubernetes

```bash
# Edit deploy/secret.yaml with your station config
kubectl apply -f deploy/
```

### Container Image (ko)

```bash
export KO_DOCKER_REPO=ghcr.io/chadmayfield/tempestd

# Build and push image only
ko build --bare .

# Build, push, and deploy to K8s in one command
ko apply -f deploy/
```

### Docker

Build and run with ko:

```bash
KO_DOCKER_REPO=ghcr.io/chadmayfield/tempestd ko build --bare .
docker run -v /path/to/config.yaml:/etc/tempestd/config.yaml \
  -v /path/to/data:/var/lib/tempestd \
  -p 8080:8080 \
  ghcr.io/chadmayfield/tempestd
```

## Integration with tempest-cli

[tempest-cli](https://github.com/chadmayfield/tempest-cli) can query a running tempestd instance instead of the WeatherFlow API directly:

```bash
tempest-cli current --server http://localhost:8080
tempest-cli observations --server http://localhost:8080 --start 2024-06-15 --end 2024-06-16
```

This avoids hitting the WeatherFlow rate limit for read operations and provides access to all locally stored historical data.

## Data Storage

At 1-minute resolution, each station generates roughly **~12 MB/year** in SQLite. Storage is not a concern for typical deployments.

## API Rate Limits

tempestd shares the WeatherFlow REST API rate limit (100 req/min) with any other tools using the same token. To avoid lockouts:

- The built-in rate limiter is set conservatively at 80 req/min
- Backfill pacing: 1 REST request per 2 seconds
- Circuit breaker: after 5 consecutive failures, waits 60 seconds before retrying
- **Never run multiple tempestd instances against the same API token**
- WebSocket connections have a separate rate limit and are not affected

## Development

### Build

```bash
go build .
```

### Build with version info

```bash
go build -ldflags "\
  -X github.com/chadmayfield/tempestd/cmd.Version=$(git describe --tags --always --dirty) \
  -X github.com/chadmayfield/tempestd/cmd.Commit=$(git rev-parse --short HEAD) \
  -X github.com/chadmayfield/tempestd/cmd.Date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -o tempestd .
```

### Test

```bash
go test ./...              # Run all tests
go test -race ./...        # With race detector
go vet ./...               # Static analysis
```

PostgreSQL integration tests require a running PostgreSQL instance:

```bash
export TEMPESTD_TEST_POSTGRES_DSN="postgres://test:test@localhost:5432/tempestd_test?sslmode=disable"
go test ./internal/store/ -run Postgres
```

## Monitoring

- **Health endpoint:** `GET /api/v1/health` returns station connection status, observation ages, database info, and total observation counts
- **Structured logging:** JSON by default (parse with jq, ship to log aggregator). Use `--log-format text` for local development
- **Stale connection detection:** if no message received on WebSocket for 5 minutes, tempest-go forces reconnection
- NOte: If interested, there is a seperate standalone Prometheus exporter located at [chadmayfield/tempest-exporter](https://github.com/chadmayfield/tempest-exporter)

## Troubleshooting

**Rate limit errors during backfill:** The REST API has a 100 req/min limit. tempestd paces at 1 req/2s but if you're also running other tools (tempest-cli, exporters) against the same token, you may hit the limit. Stop other tools during large backfills.

**WebSocket disconnects:** tempest-go handles reconnection automatically with exponential backoff. Check the logs for `ws client disconnected` messages. Persistent disconnects usually indicate a network issue or invalid token.

**Backfill not filling all data:** By default, startup backfill looks back 30 days. Use `tempestd backfill --station ID --from YYYY-MM-DD` for longer historical fills.

**Database locked (SQLite):** SQLite uses WAL mode for concurrent reads, but only one writer at a time. Never run multiple tempestd instances against the same SQLite database.

## Project Structure

```
main.go                              Entry point
cmd/                                 CLI commands (cobra)
  root.go                            Global flags, config loading
  serve.go                           Default command -- full daemon
  backfill.go                        Manual backfill
  migrate.go                         Run migrations
  status.go                          Query health endpoint
  version.go                         Print version info
internal/
  config/config.go                   YAML config, validation
  store/
    store.go                         Store interface, types
    sqlite.go                        SQLite implementation
    postgres.go                      PostgreSQL implementation
    migrations/001_initial.sql       SQLite migration
    pgmigrations/001_initial.sql     PostgreSQL migration
  collector/
    collector.go                     WebSocket collector
    backfill.go                      REST API backfiller
  api/
    server.go                        HTTP server
    handlers.go                      REST handlers
    middleware.go                    Request logging, CORS, recovery
deploy/
  namespace.yaml                     K8s namespace
  deployment.yaml                    K8s deployment (ko://...)
  service.yaml                       K8s service
  configmap.yaml                     K8s configmap
  secret.yaml                        K8s secret template
  tempestd.service                   systemd unit file
```

## Dependencies

| Package | Purpose |
|---------|---------|
| [tempest-go](https://github.com/chadmayfield/tempest-go) | Tempest API client, types, unit conversion, derived metrics, connection manager, rate limiter, circuit breaker |
| [cobra](https://github.com/spf13/cobra) | CLI framework |
| [viper](https://github.com/spf13/viper) | Configuration management |
| [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) | Pure-Go SQLite driver |
| [pgx](https://github.com/jackc/pgx) | PostgreSQL driver |
| [goose](https://github.com/pressly/goose) | Database migrations |

## License

MIT
