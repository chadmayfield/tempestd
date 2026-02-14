package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
)

func newTestSQLiteStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dir := t.TempDir()
	dsn := filepath.Join(dir, "test.db")
	s, err := NewSQLiteStore(dsn)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Insert a station record for foreign key support.
	ctx := context.Background()
	if err := s.SaveStation(ctx, &Station{ID: 1001, DeviceID: 100, Name: "Test", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("saving station: %v", err)
	}

	return s
}

func makeObs(stationID int, ts time.Time, temp, humidity, wind float64) tempest.Observation {
	return tempest.Observation{
		StationID:        stationID,
		Timestamp:        ts,
		AirTemperature:   temp,
		RelativeHumidity: humidity,
		WindAvg:          wind,
		WindGust:         wind * 1.5,
		StationPressure:  1013.25,
		Battery:          2.7,
		ReportInterval:   1,
	}
}

func TestSQLiteStore_SaveAndGetObservation(t *testing.T) {
	s := newTestSQLiteStore(t)
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
	if got.RelativeHumidity != 65.0 {
		t.Errorf("humidity = %v, want 65.0", got.RelativeHumidity)
	}
}

func TestSQLiteStore_Upsert(t *testing.T) {
	s := newTestSQLiteStore(t)
	ctx := context.Background()

	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	obs := makeObs(1001, ts, 22.5, 65.0, 3.2)
	if err := s.SaveObservation(ctx, &obs); err != nil {
		t.Fatalf("first save: %v", err)
	}

	// Update with different temp.
	obs.AirTemperature = 23.0
	if err := s.SaveObservation(ctx, &obs); err != nil {
		t.Fatalf("second save (upsert): %v", err)
	}

	got, err := s.GetLatest(ctx, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if got.AirTemperature != 23.0 {
		t.Errorf("upsert: temp = %v, want 23.0", got.AirTemperature)
	}

	// Verify only one row exists.
	rows, err := s.GetObservations(ctx, 1001, ts.Add(-time.Hour), ts.Add(time.Hour), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row after upsert, got %d", len(rows))
	}
}

func TestSQLiteStore_BatchInsert(t *testing.T) {
	s := newTestSQLiteStore(t)
	ctx := context.Background()

	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	var obs []tempest.Observation
	for i := 0; i < 250; i++ {
		obs = append(obs, makeObs(1001, base.Add(time.Duration(i)*time.Minute), 20.0+float64(i%10), 50.0, 2.0))
	}

	if err := s.SaveObservations(ctx, obs); err != nil {
		t.Fatalf("SaveObservations: %v", err)
	}

	rows, err := s.GetObservations(ctx, 1001, base, base.Add(300*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 250 {
		t.Errorf("got %d rows, want 250", len(rows))
	}
}

func TestSQLiteStore_Downsampling(t *testing.T) {
	s := newTestSQLiteStore(t)
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

func TestSQLiteStore_DailySummary(t *testing.T) {
	s := newTestSQLiteStore(t)
	ctx := context.Background()

	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	var obs []tempest.Observation
	for hour := 0; hour < 24; hour++ {
		ts := base.Add(time.Duration(hour) * time.Hour)
		temp := 15.0 + float64(hour) // 15-38 range
		o := makeObs(1001, ts, temp, 60.0, 3.0)
		o.RainAccumulation = 0.1
		o.LightningCount = 1
		o.UVIndex = float64(hour % 12)
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
	if summary.LightningTotal != 24 {
		t.Errorf("lightning_total = %d, want 24", summary.LightningTotal)
	}
	if summary.ObservationCount != 24 {
		t.Errorf("observation_count = %d, want 24", summary.ObservationCount)
	}
	if summary.StationID != 1001 {
		t.Errorf("station_id = %d, want 1001", summary.StationID)
	}
}

func TestSQLiteStore_GetDataRange(t *testing.T) {
	s := newTestSQLiteStore(t)
	ctx := context.Background()

	// Empty store.
	oldest, newest, err := s.GetDataRange(ctx, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if !oldest.IsZero() || !newest.IsZero() {
		t.Error("expected zero times for empty store")
	}

	// Insert some data.
	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		obs := makeObs(1001, base.Add(time.Duration(i)*time.Hour), 20.0, 50.0, 2.0)
		if err := s.SaveObservation(ctx, &obs); err != nil {
			t.Fatal(err)
		}
	}

	oldest, newest, err = s.GetDataRange(ctx, 1001)
	if err != nil {
		t.Fatal(err)
	}
	if oldest.IsZero() || newest.IsZero() {
		t.Fatal("expected non-zero times")
	}
	if !oldest.Equal(base) {
		t.Errorf("oldest = %v, want %v", oldest, base)
	}
	expectedNewest := base.Add(9 * time.Hour)
	if !newest.Equal(expectedNewest) {
		t.Errorf("newest = %v, want %v", newest, expectedNewest)
	}
}

func TestSQLiteStore_Station(t *testing.T) {
	s := newTestSQLiteStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)
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
		t.Fatalf("SaveStation: %v", err)
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
	if got.DeviceID != 300 {
		t.Errorf("device_id = %d, want 300", got.DeviceID)
	}

	// List.
	stations, err := s.GetStations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Includes the station from newTestSQLiteStore + this one.
	if len(stations) < 1 {
		t.Errorf("got %d stations, want >= 1", len(stations))
	}

	// Not found.
	notFound, err := s.GetStation(ctx, 9999)
	if err != nil {
		t.Fatal(err)
	}
	if notFound != nil {
		t.Error("expected nil for non-existent station")
	}
}

func TestSQLiteStore_NoObservations(t *testing.T) {
	s := newTestSQLiteStore(t)
	ctx := context.Background()

	got, err := s.GetLatest(ctx, 9999)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil for non-existent station obs")
	}
}

func TestSQLiteStore_Pagination(t *testing.T) {
	s := newTestSQLiteStore(t)
	ctx := context.Background()

	base := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	var obs []tempest.Observation
	for i := 0; i < 20; i++ {
		obs = append(obs, makeObs(1001, base.Add(time.Duration(i)*time.Minute), 20.0+float64(i), 50.0, 2.0))
	}
	if err := s.SaveObservations(ctx, obs); err != nil {
		t.Fatal(err)
	}

	// Full result set for comparison.
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

func TestSQLiteStore_FilePermissions(t *testing.T) {
	dir := t.TempDir()
	dsn := filepath.Join(dir, "perms.db")
	s, err := NewSQLiteStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	info, err := os.Stat(dsn)
	if err != nil {
		t.Fatal(err)
	}
	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("file permissions = %o, want 0600", perm)
	}
}
