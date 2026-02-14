package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrations embed.FS

// SQLiteStore implements Store backed by SQLite.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore opens a SQLite database, sets file permissions, and runs migrations.
func NewSQLiteStore(dsn string) (*SQLiteStore, error) {
	dir := filepath.Dir(dsn)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("creating db directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite: %w", err)
	}

	// Set pragmas for performance and safety.
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("setting pragma %q: %w", pragma, err)
		}
	}

	// Set file permissions to 0600.
	if err := os.Chmod(dsn, 0600); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("setting file permissions: %w", err)
	}

	// Run migrations.
	goose.SetBaseFS(migrations)
	if err := goose.SetDialect("sqlite3"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("setting goose dialect: %w", err)
	}
	if err := goose.Up(db, "migrations"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

// DB returns the underlying database connection for migration commands.
func (s *SQLiteStore) DB() *sql.DB {
	return s.db
}

func (s *SQLiteStore) SaveObservation(ctx context.Context, obs *tempest.Observation) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO observations (
			station_id, timestamp,
			wind_lull, wind_avg, wind_gust, wind_direction,
			station_pressure, air_temperature, relative_humidity,
			illuminance, uv_index, solar_radiation,
			rain_accumulation, precipitation_type,
			lightning_avg_distance, lightning_strike_count,
			battery, report_interval,
			feels_like, dew_point, wet_bulb
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(station_id, timestamp) DO UPDATE SET
			wind_lull=excluded.wind_lull, wind_avg=excluded.wind_avg,
			wind_gust=excluded.wind_gust, wind_direction=excluded.wind_direction,
			station_pressure=excluded.station_pressure,
			air_temperature=excluded.air_temperature,
			relative_humidity=excluded.relative_humidity,
			illuminance=excluded.illuminance, uv_index=excluded.uv_index,
			solar_radiation=excluded.solar_radiation,
			rain_accumulation=excluded.rain_accumulation,
			precipitation_type=excluded.precipitation_type,
			lightning_avg_distance=excluded.lightning_avg_distance,
			lightning_strike_count=excluded.lightning_strike_count,
			battery=excluded.battery, report_interval=excluded.report_interval,
			feels_like=excluded.feels_like, dew_point=excluded.dew_point,
			wet_bulb=excluded.wet_bulb`,
		obs.StationID, obs.Timestamp.UTC(),
		obs.WindLull, obs.WindAvg, obs.WindGust, obs.WindDirection,
		obs.StationPressure, obs.AirTemperature, obs.RelativeHumidity,
		obs.Illuminance, obs.UVIndex, obs.SolarRadiation,
		obs.RainAccumulation, obs.PrecipitationType,
		obs.LightningAvgDist, obs.LightningCount,
		obs.Battery, obs.ReportInterval,
		obs.FeelsLike, obs.DewPoint, obs.WetBulb,
	)
	if err != nil {
		return fmt.Errorf("saving observation: %w", err)
	}
	return nil
}

func (s *SQLiteStore) SaveObservations(ctx context.Context, obs []tempest.Observation) error {
	const batchSize = 100
	for i := 0; i < len(obs); i += batchSize {
		end := i + batchSize
		if end > len(obs) {
			end = len(obs)
		}
		if err := s.saveBatch(ctx, obs[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) saveBatch(ctx context.Context, obs []tempest.Observation) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is harmless

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO observations (
			station_id, timestamp,
			wind_lull, wind_avg, wind_gust, wind_direction,
			station_pressure, air_temperature, relative_humidity,
			illuminance, uv_index, solar_radiation,
			rain_accumulation, precipitation_type,
			lightning_avg_distance, lightning_strike_count,
			battery, report_interval,
			feels_like, dew_point, wet_bulb
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(station_id, timestamp) DO UPDATE SET
			wind_lull=excluded.wind_lull, wind_avg=excluded.wind_avg,
			wind_gust=excluded.wind_gust, wind_direction=excluded.wind_direction,
			station_pressure=excluded.station_pressure,
			air_temperature=excluded.air_temperature,
			relative_humidity=excluded.relative_humidity,
			illuminance=excluded.illuminance, uv_index=excluded.uv_index,
			solar_radiation=excluded.solar_radiation,
			rain_accumulation=excluded.rain_accumulation,
			precipitation_type=excluded.precipitation_type,
			lightning_avg_distance=excluded.lightning_avg_distance,
			lightning_strike_count=excluded.lightning_strike_count,
			battery=excluded.battery, report_interval=excluded.report_interval,
			feels_like=excluded.feels_like, dew_point=excluded.dew_point,
			wet_bulb=excluded.wet_bulb`)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close() //nolint:errcheck

	for _, o := range obs {
		if _, err := stmt.ExecContext(ctx,
			o.StationID, o.Timestamp.UTC(),
			o.WindLull, o.WindAvg, o.WindGust, o.WindDirection,
			o.StationPressure, o.AirTemperature, o.RelativeHumidity,
			o.Illuminance, o.UVIndex, o.SolarRadiation,
			o.RainAccumulation, o.PrecipitationType,
			o.LightningAvgDist, o.LightningCount,
			o.Battery, o.ReportInterval,
			o.FeelsLike, o.DewPoint, o.WetBulb,
		); err != nil {
			return fmt.Errorf("inserting observation: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetLatest(ctx context.Context, stationID int) (*tempest.Observation, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT station_id, timestamp,
			wind_lull, wind_avg, wind_gust, wind_direction,
			station_pressure, air_temperature, relative_humidity,
			illuminance, uv_index, solar_radiation,
			rain_accumulation, precipitation_type,
			lightning_avg_distance, lightning_strike_count,
			battery, report_interval,
			feels_like, dew_point, wet_bulb
		FROM observations
		WHERE station_id = ?
		ORDER BY timestamp DESC
		LIMIT 1`, stationID)

	obs, err := scanObservation(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting latest observation: %w", err)
	}
	return obs, nil
}

func (s *SQLiteStore) GetObservations(ctx context.Context, stationID int, start, end time.Time, resolution time.Duration) ([]tempest.Observation, error) {
	query, args := buildObservationQuery(stationID, start, end, resolution, "sqlite")
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying observations: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	return scanObservations(rows)
}

func (s *SQLiteStore) GetDailySummary(ctx context.Context, stationID int, date time.Time) (*DailySummary, error) {
	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)

	var ds DailySummary
	err := s.db.QueryRowContext(ctx, `
		SELECT
			MAX(air_temperature),
			MIN(air_temperature),
			AVG(air_temperature),
			MAX(relative_humidity),
			MIN(relative_humidity),
			AVG(relative_humidity),
			MAX(wind_gust),
			AVG(wind_avg),
			MAX(station_pressure),
			MIN(station_pressure),
			SUM(rain_accumulation),
			MAX(uv_index),
			MAX(solar_radiation),
			COALESCE(SUM(lightning_strike_count), 0),
			COUNT(*)
		FROM observations
		WHERE station_id = ? AND timestamp >= ? AND timestamp < ?`,
		stationID, dayStart, dayEnd).Scan(
		&ds.TempHigh, &ds.TempLow, &ds.TempAvg,
		&ds.HumidityHigh, &ds.HumidityLow, &ds.HumidityAvg,
		&ds.WindMax, &ds.WindAvg,
		&ds.PressureHigh, &ds.PressureLow,
		&ds.RainTotal,
		&ds.UVMax, &ds.SolarRadMax,
		&ds.LightningTotal,
		&ds.ObservationCount,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying daily summary: %w", err)
	}

	if ds.ObservationCount == 0 {
		return nil, nil
	}

	ds.StationID = stationID
	ds.Date = dayStart
	return &ds, nil
}

func (s *SQLiteStore) GetDataRange(ctx context.Context, stationID int) (oldest, newest time.Time, err error) {
	var oldestRaw, newestRaw *string
	err = s.db.QueryRowContext(ctx, `
		SELECT MIN(timestamp), MAX(timestamp)
		FROM observations
		WHERE station_id = ?`, stationID).Scan(&oldestRaw, &newestRaw)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying data range: %w", err)
	}
	if oldestRaw == nil || newestRaw == nil {
		return time.Time{}, time.Time{}, nil
	}

	oldest, err = parseTimestamp(*oldestRaw)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parsing oldest: %w", err)
	}
	newest, err = parseTimestamp(*newestRaw)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("parsing newest: %w", err)
	}
	return oldest, newest, nil
}

func (s *SQLiteStore) GetObservationCount(ctx context.Context, stationID int) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM observations WHERE station_id = ?`, stationID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting observations: %w", err)
	}
	return count, nil
}

func (s *SQLiteStore) SaveStation(ctx context.Context, station *Station) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO stations (id, device_id, name, latitude, longitude, elevation, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			device_id=excluded.device_id,
			name=excluded.name,
			latitude=excluded.latitude,
			longitude=excluded.longitude,
			elevation=excluded.elevation,
			updated_at=excluded.updated_at`,
		station.ID, station.DeviceID, station.Name, station.Latitude, station.Longitude, station.Elevation,
		station.CreatedAt.UTC(), station.UpdatedAt.UTC())
	if err != nil {
		return fmt.Errorf("saving station: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetStation(ctx context.Context, stationID int) (*Station, error) {
	var st Station
	err := s.db.QueryRowContext(ctx, `
		SELECT id, device_id, name, latitude, longitude, elevation, created_at, updated_at
		FROM stations WHERE id = ?`, stationID).Scan(
		&st.ID, &st.DeviceID, &st.Name, &st.Latitude, &st.Longitude, &st.Elevation,
		&st.CreatedAt, &st.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting station: %w", err)
	}
	return &st, nil
}

func (s *SQLiteStore) GetStations(ctx context.Context) ([]Station, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, device_id, name, latitude, longitude, elevation, created_at, updated_at
		FROM stations ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("listing stations: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var stations []Station
	for rows.Next() {
		var st Station
		if err := rows.Scan(&st.ID, &st.DeviceID, &st.Name, &st.Latitude, &st.Longitude,
			&st.Elevation, &st.CreatedAt, &st.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning station: %w", err)
		}
		stations = append(stations, st)
	}
	return stations, rows.Err()
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// --- Shared helpers ---

type scanner interface {
	Scan(dest ...any) error
}

// parseTimestamp handles both time.Time and string timestamp values from SQLite.
func parseTimestamp(v any) (time.Time, error) {
	switch t := v.(type) {
	case time.Time:
		return t, nil
	case string:
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05+00:00",
			"2006-01-02 15:04:05 +0000 UTC",
			"2006-01-02 15:04:05",
			"2006-01-02 15:04",
			"2006-01-02 15:00",
			"2006-01-02",
		} {
			if ts, err := time.Parse(layout, t); err == nil {
				return ts, nil
			}
		}
		return time.Time{}, fmt.Errorf("unable to parse timestamp: %q", t)
	default:
		return time.Time{}, fmt.Errorf("unexpected timestamp type: %T", v)
	}
}

func scanObservation(row scanner) (*tempest.Observation, error) {
	var obs tempest.Observation
	var tsRaw any
	err := row.Scan(
		&obs.StationID, &tsRaw,
		&obs.WindLull, &obs.WindAvg, &obs.WindGust, &obs.WindDirection,
		&obs.StationPressure, &obs.AirTemperature, &obs.RelativeHumidity,
		&obs.Illuminance, &obs.UVIndex, &obs.SolarRadiation,
		&obs.RainAccumulation, &obs.PrecipitationType,
		&obs.LightningAvgDist, &obs.LightningCount,
		&obs.Battery, &obs.ReportInterval,
		&obs.FeelsLike, &obs.DewPoint, &obs.WetBulb,
	)
	if err != nil {
		return nil, err
	}
	obs.Timestamp, err = parseTimestamp(tsRaw)
	if err != nil {
		return nil, fmt.Errorf("parsing timestamp: %w", err)
	}
	return &obs, nil
}

func scanObservations(rows *sql.Rows) ([]tempest.Observation, error) {
	var result []tempest.Observation
	for rows.Next() {
		var obs tempest.Observation
		var tsRaw any
		if err := rows.Scan(
			&obs.StationID, &tsRaw,
			&obs.WindLull, &obs.WindAvg, &obs.WindGust, &obs.WindDirection,
			&obs.StationPressure, &obs.AirTemperature, &obs.RelativeHumidity,
			&obs.Illuminance, &obs.UVIndex, &obs.SolarRadiation,
			&obs.RainAccumulation, &obs.PrecipitationType,
			&obs.LightningAvgDist, &obs.LightningCount,
			&obs.Battery, &obs.ReportInterval,
			&obs.FeelsLike, &obs.DewPoint, &obs.WetBulb,
		); err != nil {
			return nil, fmt.Errorf("scanning observation: %w", err)
		}
		ts, err := parseTimestamp(tsRaw)
		if err != nil {
			return nil, fmt.Errorf("parsing timestamp: %w", err)
		}
		obs.Timestamp = ts
		result = append(result, obs)
	}
	return result, rows.Err()
}

func buildObservationQuery(stationID int, start, end time.Time, resolution time.Duration, dialect string) (string, []any) {
	var timeGroup string
	minutes := int(resolution.Minutes())
	switch {
	case minutes <= 1:
		// Raw 1m data, no grouping.
	case minutes == 5:
		if dialect == "sqlite" {
			timeGroup = "substr(timestamp,1,14) || printf('%02d', (CAST(substr(timestamp,15,2) AS INTEGER) / 5) * 5)"
		} else {
			timeGroup = "date_trunc('hour', timestamp) + (EXTRACT(MINUTE FROM timestamp)::int / 5 * 5) * interval '1 minute'"
		}
	case minutes == 30:
		if dialect == "sqlite" {
			timeGroup = "substr(timestamp,1,14) || printf('%02d', (CAST(substr(timestamp,15,2) AS INTEGER) / 30) * 30)"
		} else {
			timeGroup = "date_trunc('hour', timestamp) + (EXTRACT(MINUTE FROM timestamp)::int / 30 * 30) * interval '1 minute'"
		}
	case minutes == 60:
		if dialect == "sqlite" {
			timeGroup = "substr(timestamp,1,13)"
		} else {
			timeGroup = "date_trunc('hour', timestamp)"
		}
	case minutes == 180:
		if dialect == "sqlite" {
			timeGroup = "substr(timestamp,1,11) || printf('%02d', (CAST(substr(timestamp,12,2) AS INTEGER) / 3) * 3)"
		} else {
			timeGroup = "date_trunc('hour', timestamp) - (EXTRACT(HOUR FROM timestamp)::int % 3) * interval '1 hour'"
		}
	default:
		// Unrecognized resolution — return raw rows.
	}

	args := []any{stationID, start.UTC(), end.UTC()}

	if timeGroup == "" {
		q := `SELECT station_id, timestamp,
			wind_lull, wind_avg, wind_gust, wind_direction,
			station_pressure, air_temperature, relative_humidity,
			illuminance, uv_index, solar_radiation,
			rain_accumulation, precipitation_type,
			lightning_avg_distance, lightning_strike_count,
			battery, report_interval,
			feels_like, dew_point, wet_bulb
		FROM observations
		WHERE station_id = ? AND timestamp >= ? AND timestamp < ?
		ORDER BY timestamp`

		if dialect == "postgres" {
			q = replacePlaceholders(q)
		}
		return q, args
	}

	q := fmt.Sprintf(`SELECT station_id,
		MIN(timestamp) as timestamp,
		AVG(wind_lull), AVG(wind_avg), MAX(wind_gust), AVG(wind_direction),
		AVG(station_pressure), AVG(air_temperature), AVG(relative_humidity),
		AVG(illuminance), AVG(uv_index), AVG(solar_radiation),
		SUM(rain_accumulation), MAX(precipitation_type),
		AVG(lightning_avg_distance), SUM(lightning_strike_count),
		AVG(battery), MAX(report_interval),
		AVG(feels_like), AVG(dew_point), AVG(wet_bulb)
	FROM observations
	WHERE station_id = ? AND timestamp >= ? AND timestamp < ?
	GROUP BY station_id, %s
	ORDER BY timestamp`, timeGroup)

	if dialect == "postgres" {
		q = replacePlaceholders(q)
	}

	return q, args
}

// replacePlaceholders converts ? to $1, $2, $3 etc for postgres.
func replacePlaceholders(query string) string {
	result := make([]byte, 0, len(query))
	n := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			result = append(result, fmt.Sprintf("$%d", n)...)
			n++
		} else {
			result = append(result, query[i])
		}
	}
	return string(result)
}
