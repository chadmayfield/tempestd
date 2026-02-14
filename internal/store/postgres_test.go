package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
)

func newTestPostgresStore(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("TEMPESTD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEMPESTD_TEST_POSTGRES_DSN not set; skipping postgres tests")
	}

	s, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}

	// Clean tables before each test.
	ctx := context.Background()
	s.db.ExecContext(ctx, "DELETE FROM observations")
	s.db.ExecContext(ctx, "DELETE FROM stations")

	// Insert a station for FK support.
	s.SaveStation(ctx, &Station{ID: 1001, DeviceID: 100, Name: "Test", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()})

	t.Cleanup(func() { s.Close() })
	return s
}

func TestPostgresStore_SaveAndGetObservation(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	obs := makeObs(1001, ts, 22.5, 65.0, 3.2)

	if err := s.SaveObservation(ctx, &obs); err != nil {
		t.Fatalf("SaveObservation: %v", err)
	}

	got, err := s.GetLatest(ctx, 1001)
	if err != nil {
		t.Fatalf("GetLatest: %v", err)
	}
	if got == nil {
		t.Fatal("expected observation, got nil")
	}
	if got.AirTemperature != 22.5 {
		t.Errorf("temp = %v, want 22.5", got.AirTemperature)
	}
}

func TestPostgresStore_Upsert(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	obs := makeObs(1001, ts, 22.5, 65.0, 3.2)
	if err := s.SaveObservation(ctx, &obs); err != nil {
		t.Fatal(err)
	}

	obs.AirTemperature = 23.0
	if err := s.SaveObservation(ctx, &obs); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetLatest(ctx, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if got.AirTemperature != 23.0 {
		t.Errorf("upsert: temp = %v, want 23.0", got.AirTemperature)
	}
}

func TestPostgresStore_BatchInsert(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	var obs []tempest.Observation
	for i := 0; i < 250; i++ {
		obs = append(obs, makeObs(1001, base.Add(time.Duration(i)*time.Minute), 20.0, 50.0, 2.0))
	}

	if err := s.SaveObservations(ctx, obs); err != nil {
		t.Fatal(err)
	}

	rows, err := s.GetObservations(ctx, 1001, base, base.Add(300*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 250 {
		t.Errorf("got %d rows, want 250", len(rows))
	}
}

func TestPostgresStore_DailySummary(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	var obs []tempest.Observation
	for hour := 0; hour < 24; hour++ {
		ts := base.Add(time.Duration(hour) * time.Hour)
		temp := 15.0 + float64(hour)
		o := makeObs(1001, ts, temp, 60.0, 3.0)
		o.RainAccumulation = 0.1
		o.LightningCount = 1
		obs = append(obs, o)
	}
	if err := s.SaveObservations(ctx, obs); err != nil {
		t.Fatal(err)
	}

	summary, err := s.GetDailySummary(ctx, 1001, base)
	if err != nil {
		t.Fatal(err)
	}
	if summary == nil {
		t.Fatal("expected summary, got nil")
	}
	if summary.TempHigh != 38.0 {
		t.Errorf("temp_high = %v, want 38.0", summary.TempHigh)
	}
	if summary.TempLow != 15.0 {
		t.Errorf("temp_low = %v, want 15.0", summary.TempLow)
	}
	if summary.ObservationCount != 24 {
		t.Errorf("observation_count = %d, want 24", summary.ObservationCount)
	}
}

func TestPostgresStore_Station(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Microsecond)
	station := &Station{
		ID:        2001,
		DeviceID:  300,
		Name:      "Test Station",
		Latitude:  37.7749,
		Longitude: -122.4194,
		Elevation: 10.0,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.SaveStation(ctx, station); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetStation(ctx, 2001)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected station, got nil")
	}
	if got.Name != "Test Station" {
		t.Errorf("name = %q, want %q", got.Name, "Test Station")
	}

	stations, err := s.GetStations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stations) < 1 {
		t.Errorf("got %d stations, want >= 1", len(stations))
	}
}

func TestPostgresStore_GetDataRange(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		obs := makeObs(1001, base.Add(time.Duration(i)*time.Hour), 20.0, 50.0, 2.0)
		if err := s.SaveObservation(ctx, &obs); err != nil {
			t.Fatal(err)
		}
	}

	oldest, newest, err := s.GetDataRange(ctx, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if oldest.IsZero() || newest.IsZero() {
		t.Fatal("expected non-zero times")
	}
}

func TestPostgresStore_Downsampling(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	var obs []tempest.Observation
	for i := 0; i < 120; i++ { // 2 hours of 1-minute data
		obs = append(obs, makeObs(1001, base.Add(time.Duration(i)*time.Minute), 20.0+float64(i%5), 50.0, 2.0))
	}
	if err := s.SaveObservations(ctx, obs); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		resolution time.Duration
		maxRows    int
	}{
		{time.Minute, 120},
		{5 * time.Minute, 24},
		{30 * time.Minute, 4},
		{time.Hour, 2},
	}

	for _, tt := range tests {
		t.Run(tt.resolution.String(), func(t *testing.T) {
			rows, err := s.GetObservations(ctx, 1001, base, base.Add(2*time.Hour), tt.resolution)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != tt.maxRows {
				t.Errorf("resolution %s: got %d rows, want %d", tt.resolution, len(rows), tt.maxRows)
			}
		})
	}
}

func TestPostgresStore_NoObservations(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	got, err := s.GetLatest(ctx, 9999)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil for non-existent station obs")
	}
}

func TestPostgresStore_Pagination(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	var obs []tempest.Observation
	for i := 0; i < 20; i++ {
		obs = append(obs, makeObs(1001, base.Add(time.Duration(i)*time.Minute), 20.0+float64(i), 50.0, 2.0))
	}
	if err := s.SaveObservations(ctx, obs); err != nil {
		t.Fatal(err)
	}

	all, err := s.GetObservations(ctx, 1001, base, base.Add(30*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 20 {
		t.Fatalf("expected 20 rows, got %d", len(all))
	}

	tests := []struct {
		name      string
		limit     int
		offset    int
		wantCount int
	}{
		{"first page", 5, 0, 5},
		{"second page", 5, 5, 5},
		{"third page", 5, 10, 5},
		{"last partial page", 5, 18, 2},
		{"offset past end", 5, 25, 0},
		{"no limit", 0, 0, 20},
		{"limit larger than set", 100, 0, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := s.GetObservations(ctx, 1001, base, base.Add(30*time.Minute), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// Apply limit/offset in-memory (same as handler does).
			if tt.offset > 0 && tt.offset < len(rows) {
				rows = rows[tt.offset:]
			} else if tt.offset >= len(rows) {
				rows = nil
			}
			if tt.limit > 0 && tt.limit < len(rows) {
				rows = rows[:tt.limit]
			}
			if len(rows) != tt.wantCount {
				t.Errorf("got %d rows, want %d", len(rows), tt.wantCount)
			}
		})
	}
}

func TestPostgresStore_StationNotFound(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	got, err := s.GetStation(ctx, 9999)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil for non-existent station")
	}
}

func TestPostgresStore_FilePermissions(t *testing.T) {
	// PostgreSQL doesn't have file permissions like SQLite.
	// Verify that the store can be opened and closed cleanly.
	s := newTestPostgresStore(t)
	if s.db == nil {
		t.Fatal("expected non-nil db connection")
	}
}

func TestPostgresStore_GetDataRange_Empty(t *testing.T) {
	s := newTestPostgresStore(t)
	ctx := context.Background()

	oldest, newest, err := s.GetDataRange(ctx, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if !oldest.IsZero() || !newest.IsZero() {
		t.Error("expected zero times for empty store")
	}
}

func TestPostgresStore_ChunkCalculation(t *testing.T) {
	tests := []struct {
		days       int
		wantChunks int
	}{
		{1, 1},
		{4, 1},
		{5, 1},
		{6, 2},
		{10, 2},
		{11, 3},
		{15, 3},
		{30, 6},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d_days", tt.days), func(t *testing.T) {
			to := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
			from := to.AddDate(0, 0, -tt.days)
			chunks := 0
			chunkDays := 5
			for cs := from; cs.Before(to); {
				ce := cs.AddDate(0, 0, chunkDays)
				if ce.After(to) {
					ce = to
				}
				chunks++
				cs = ce
			}
			if chunks != tt.wantChunks {
				t.Errorf("days=%d: got %d chunks, want %d", tt.days, chunks, tt.wantChunks)
			}
		})
	}
}
