package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
	"github.com/pressly/goose/v3"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed pgmigrations/*.sql
var pgMigrations embed.FS

// PostgresStore implements Store backed by PostgreSQL.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore opens a PostgreSQL connection and runs migrations.
func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening postgres: %w", err)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	goose.SetBaseFS(pgMigrations)
	if err := goose.SetDialect("postgres"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("setting goose dialect: %w", err)
	}
	if err := goose.Up(db, "pgmigrations"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

// DB returns the underlying database connection for migration commands.
func (s *PostgresStore) DB() *sql.DB {
	return s.db
}

func (s *PostgresStore) SaveObservation(ctx context.Context, obs *tempest.Observation) error {
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
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		ON CONFLICT(station_id, timestamp) DO UPDATE SET
			wind_lull=EXCLUDED.wind_lull, wind_avg=EXCLUDED.wind_avg,
			wind_gust=EXCLUDED.wind_gust, wind_direction=EXCLUDED.wind_direction,
			station_pressure=EXCLUDED.station_pressure,
			air_temperature=EXCLUDED.air_temperature,
			relative_humidity=EXCLUDED.relative_humidity,
			illuminance=EXCLUDED.illuminance, uv_index=EXCLUDED.uv_index,
			solar_radiation=EXCLUDED.solar_radiation,
			rain_accumulation=EXCLUDED.rain_accumulation,
			precipitation_type=EXCLUDED.precipitation_type,
			lightning_avg_distance=EXCLUDED.lightning_avg_distance,
			lightning_strike_count=EXCLUDED.lightning_strike_count,
			battery=EXCLUDED.battery, report_interval=EXCLUDED.report_interval,
			feels_like=EXCLUDED.feels_like, dew_point=EXCLUDED.dew_point,
			wet_bulb=EXCLUDED.wet_bulb`,
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

func (s *PostgresStore) SaveObservations(ctx context.Context, obs []tempest.Observation) error {
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

func (s *PostgresStore) saveBatch(ctx context.Context, obs []tempest.Observation) error {
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
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		ON CONFLICT(station_id, timestamp) DO UPDATE SET
			wind_lull=EXCLUDED.wind_lull, wind_avg=EXCLUDED.wind_avg,
			wind_gust=EXCLUDED.wind_gust, wind_direction=EXCLUDED.wind_direction,
			station_pressure=EXCLUDED.station_pressure,
			air_temperature=EXCLUDED.air_temperature,
			relative_humidity=EXCLUDED.relative_humidity,
			illuminance=EXCLUDED.illuminance, uv_index=EXCLUDED.uv_index,
			solar_radiation=EXCLUDED.solar_radiation,
			rain_accumulation=EXCLUDED.rain_accumulation,
			precipitation_type=EXCLUDED.precipitation_type,
			lightning_avg_distance=EXCLUDED.lightning_avg_distance,
			lightning_strike_count=EXCLUDED.lightning_strike_count,
			battery=EXCLUDED.battery, report_interval=EXCLUDED.report_interval,
			feels_like=EXCLUDED.feels_like, dew_point=EXCLUDED.dew_point,
			wet_bulb=EXCLUDED.wet_bulb`)
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

func (s *PostgresStore) GetLatest(ctx context.Context, stationID int) (*tempest.Observation, error) {
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
		WHERE station_id = $1
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

func (s *PostgresStore) GetObservations(ctx context.Context, stationID int, start, end time.Time, resolution time.Duration) ([]tempest.Observation, error) {
	query, args := buildObservationQuery(stationID, start, end, resolution, "postgres")
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying observations: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	return scanObservations(rows)
}

func (s *PostgresStore) GetDailySummary(ctx context.Context, stationID int, date time.Time) (*DailySummary, error) {
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
		WHERE station_id = $1 AND timestamp >= $2 AND timestamp < $3`,
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

func (s *PostgresStore) GetDataRange(ctx context.Context, stationID int) (oldest, newest time.Time, err error) {
	var minT, maxT *time.Time
	err = s.db.QueryRowContext(ctx, `
		SELECT MIN(timestamp), MAX(timestamp)
		FROM observations
		WHERE station_id = $1`, stationID).Scan(&minT, &maxT)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("querying data range: %w", err)
	}
	if minT == nil || maxT == nil {
		return time.Time{}, time.Time{}, nil
	}
	return *minT, *maxT, nil
}

func (s *PostgresStore) GetObservationCount(ctx context.Context, stationID int) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM observations WHERE station_id = $1`, stationID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting observations: %w", err)
	}
	return count, nil
}

func (s *PostgresStore) SaveStation(ctx context.Context, station *Station) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO stations (id, device_id, name, latitude, longitude, elevation, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT(id) DO UPDATE SET
			device_id=EXCLUDED.device_id,
			name=EXCLUDED.name,
			latitude=EXCLUDED.latitude,
			longitude=EXCLUDED.longitude,
			elevation=EXCLUDED.elevation,
			updated_at=EXCLUDED.updated_at`,
		station.ID, station.DeviceID, station.Name, station.Latitude, station.Longitude, station.Elevation,
		station.CreatedAt.UTC(), station.UpdatedAt.UTC())
	if err != nil {
		return fmt.Errorf("saving station: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetStation(ctx context.Context, stationID int) (*Station, error) {
	var st Station
	err := s.db.QueryRowContext(ctx, `
		SELECT id, device_id, name, latitude, longitude, elevation, created_at, updated_at
		FROM stations WHERE id = $1`, stationID).Scan(
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

func (s *PostgresStore) GetStations(ctx context.Context) ([]Station, error) {
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

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
