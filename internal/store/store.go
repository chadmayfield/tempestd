package store

import (
	"context"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
)

// Store defines the interface for observation storage.
// Both SQLite and PostgreSQL implementations satisfy this interface.
type Store interface {
	// SaveObservation stores a single observation. Upserts on (station_id, timestamp).
	SaveObservation(ctx context.Context, obs *tempest.Observation) error

	// SaveObservations stores multiple observations in a single transaction (batch insert).
	SaveObservations(ctx context.Context, obs []tempest.Observation) error

	// GetObservations retrieves observations for a station within a time range.
	// Resolution controls data density: if > 1m, observations are averaged/sampled.
	GetObservations(ctx context.Context, stationID int, start, end time.Time, resolution time.Duration) ([]tempest.Observation, error)

	// GetLatest retrieves the most recent observation for a station.
	GetLatest(ctx context.Context, stationID int) (*tempest.Observation, error)

	// GetStations retrieves all known stations.
	GetStations(ctx context.Context) ([]Station, error)

	// SaveStation creates or updates a station record.
	SaveStation(ctx context.Context, station *Station) error

	// GetDataRange returns the oldest and newest observation timestamps for a station.
	GetDataRange(ctx context.Context, stationID int) (oldest, newest time.Time, err error)

	// GetObservationCount returns the total number of observations for a station.
	GetObservationCount(ctx context.Context, stationID int) (int, error)

	// GetDailySummary returns aggregated stats (min/max/avg) for a specific date.
	GetDailySummary(ctx context.Context, stationID int, date time.Time) (*DailySummary, error)

	// Close closes the database connection.
	Close() error
}

// Station is the database model for station metadata.
type Station struct {
	ID        int
	DeviceID  int
	Name      string
	Latitude  float64
	Longitude float64
	Elevation float64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// DailySummary holds aggregated daily weather data for a station.
type DailySummary struct {
	StationID        int
	Date             time.Time
	TempHigh         float64
	TempLow          float64
	TempAvg          float64
	HumidityHigh     float64
	HumidityLow      float64
	HumidityAvg      float64
	WindMax          float64
	WindAvg          float64
	PressureHigh     float64
	PressureLow      float64
	RainTotal        float64
	UVMax            float64
	SolarRadMax      float64
	LightningTotal   int
	ObservationCount int
}
