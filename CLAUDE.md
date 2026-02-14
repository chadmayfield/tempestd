# tempestd

Data collection daemon for WeatherFlow Tempest weather stations. Stores observations
at 1-minute resolution in SQLite or PostgreSQL, exposes a REST API for querying.

## Build & Run

- Build: `go build -o tempestd .`
- Build image: `ko build --bare .`
- Deploy to K8s: `ko apply -f deploy/`
- Run locally: `./tempestd serve --config config.yaml --log-format text`
- Run migrations: `./tempestd migrate --config config.yaml`
- Manual backfill: `./tempestd backfill --station 12345 --from 2024-01-01 --config config.yaml`
- Run tests: `go test ./...`
- Run tests with race detector: `go test -race ./...`
- Run PostgreSQL tests: `TEMPESTD_TEST_POSTGRES_DSN=... go test ./...`
- Lint: `golangci-lint run`

## Conventions

- All station-specific config comes from config file — never hardcode tokens or IDs
- Use tempest-go shared library for all API communication, types, unit conversion, derived metrics, connection management, rate limiting, and circuit breaking
- Storage interface pattern: both SQLite and PostgreSQL implement the same Store interface
- Database migrations managed by goose in internal/store/migrations/
- All observations stored in metric units; unit conversion happens in API response layer
- Single WebSocket connection per token, multiple listen_start for multiple devices
- Backfill pacing: 1 request per 2 seconds to avoid API abuse
- Graceful shutdown: close WebSocket → drain HTTP → flush data → close DB
- Never log tokens, passwords, or secrets
- Table-driven tests with descriptive subtest names

## Critical Safety Rules

- NEVER run multiple instances against the same Tempest API token
- ALWAYS close old WebSocket connections before opening new ones
- ALWAYS use the tempest-go ConnectionManager to track connections
- ALWAYS implement graceful shutdown — leaked connections cause API lockouts
- ALWAYS pace backfill requests — rapid-fire REST calls will trigger rate limits

## Working Style

- Ask for clarification instead of guessing
- Run tests after writing or modifying code
- Never commit secrets, tokens, or real credentials
- Explain tradeoffs briefly when making design decisions
