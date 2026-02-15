package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
	"github.com/chadmayfield/tempestd/internal/store"
)

// mockStore implements store.Store for testing.
type mockStore struct {
	observations []tempest.Observation
	stations     map[int]*store.Station
}

func newMockStore() *mockStore {
	return &mockStore{stations: make(map[int]*store.Station)}
}

func (m *mockStore) SaveObservation(_ context.Context, obs *tempest.Observation) error {
	m.observations = append(m.observations, *obs)
	return nil
}

func (m *mockStore) SaveObservations(_ context.Context, obs []tempest.Observation) error {
	m.observations = append(m.observations, obs...)
	return nil
}

func (m *mockStore) GetLatest(_ context.Context, stationID int) (*tempest.Observation, error) {
	var latest *tempest.Observation
	for i := range m.observations {
		if m.observations[i].StationID == stationID {
			if latest == nil || m.observations[i].Timestamp.After(latest.Timestamp) {
				latest = &m.observations[i]
			}
		}
	}
	return latest, nil
}

func (m *mockStore) GetObservations(_ context.Context, stationID int, start, end time.Time, _ time.Duration) ([]tempest.Observation, error) {
	var result []tempest.Observation
	for _, o := range m.observations {
		if o.StationID == stationID && !o.Timestamp.Before(start) && o.Timestamp.Before(end) {
			result = append(result, o)
		}
	}
	return result, nil
}

func (m *mockStore) GetDailySummary(_ context.Context, stationID int, date time.Time) (*store.DailySummary, error) {
	return nil, nil
}

func (m *mockStore) GetDataRange(_ context.Context, stationID int) (time.Time, time.Time, error) {
	var oldest, newest time.Time
	for _, o := range m.observations {
		if o.StationID == stationID {
			if oldest.IsZero() || o.Timestamp.Before(oldest) {
				oldest = o.Timestamp
			}
			if newest.IsZero() || o.Timestamp.After(newest) {
				newest = o.Timestamp
			}
		}
	}
	return oldest, newest, nil
}

func (m *mockStore) SaveStation(_ context.Context, s *store.Station) error {
	m.stations[s.ID] = s
	return nil
}

func (m *mockStore) GetStations(_ context.Context) ([]store.Station, error) {
	var result []store.Station
	for _, s := range m.stations {
		result = append(result, *s)
	}
	return result, nil
}

func (m *mockStore) GetObservationCount(_ context.Context, stationID int) (int, error) {
	count := 0
	for _, o := range m.observations {
		if o.StationID == stationID {
			count++
		}
	}
	return count, nil
}

func (m *mockStore) Close() error { return nil }

func setupTestServer(ms *mockStore) *httptest.Server {
	h := &Handlers{Store: ms, StartTime: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/stations", h.ListStations)
	mux.HandleFunc("GET /api/v1/stations/{station_id}", h.GetStation)
	mux.HandleFunc("GET /api/v1/stations/{station_id}/current", h.GetCurrentObservation)
	mux.HandleFunc("GET /api/v1/stations/{station_id}/observations", h.GetObservations)
	mux.HandleFunc("GET /api/v1/stations/{station_id}/summary", h.GetDailySummary)
	mux.HandleFunc("GET /api/v1/stations/{station_id}/range", h.GetObservationRange)
	mux.HandleFunc("GET /api/v1/health", h.Health)
	return httptest.NewServer(ContentType(mux))
}

func TestHandlers_Health(t *testing.T) {
	ms := newMockStore()
	srv := setupTestServer(ms)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "healthy" {
		t.Errorf("status = %v, want 'healthy'", body["status"])
	}
}

func TestHandlers_ListStations(t *testing.T) {
	ms := newMockStore()
	now := time.Now().UTC()
	_ = ms.SaveStation(context.Background(), &store.Station{
		ID:        1001,
		DeviceID:  100,
		Name:      "Station 1",
		CreatedAt: now,
		UpdatedAt: now,
	})

	srv := setupTestServer(ms)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/stations")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var stations []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&stations)
	if len(stations) != 1 {
		t.Errorf("got %d stations, want 1", len(stations))
	}
}

func TestHandlers_GetStation(t *testing.T) {
	ms := newMockStore()
	now := time.Now().UTC()
	_ = ms.SaveStation(context.Background(), &store.Station{
		ID:        1001,
		DeviceID:  100,
		Name:      "Station 1",
		CreatedAt: now,
		UpdatedAt: now,
	})

	srv := setupTestServer(ms)
	defer srv.Close()

	t.Run("found", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/api/v1/stations/1001")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}

		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		if body["name"] != "Station 1" {
			t.Errorf("name = %v, want %q", body["name"], "Station 1")
		}
		if body["station_id"] != float64(1001) {
			t.Errorf("station_id = %v, want 1001", body["station_id"])
		}
	})

	t.Run("not found", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/api/v1/stations/9999")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
		}
	})

	t.Run("invalid id", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/api/v1/stations/abc")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})
}

func TestHandlers_GetCurrentObservation(t *testing.T) {
	ms := newMockStore()
	ts := time.Now().UTC()
	ms.observations = append(ms.observations, tempest.Observation{
		StationID:      1001,
		Timestamp:      ts,
		AirTemperature: 22.5,
		WindDirection:  22.5,
	})

	srv := setupTestServer(ms)
	defer srv.Close()

	t.Run("found", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/api/v1/stations/1001/current")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}

		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		if body["air_temperature"] != 22.5 {
			t.Errorf("temp = %v, want 22.5", body["air_temperature"])
		}
		if body["wind_direction_cardinal"] == nil {
			t.Error("expected wind_direction_cardinal field")
		}
		if body["units"] != "metric" {
			t.Errorf("units = %v, want 'metric'", body["units"])
		}
	})

	t.Run("not found", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/api/v1/stations/9999/current")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
		}
	})
}

func TestHandlers_GetObservations(t *testing.T) {
	ms := newMockStore()
	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	for i := range 10 {
		ms.observations = append(ms.observations, tempest.Observation{
			StationID:      1001,
			Timestamp:      base.Add(time.Duration(i) * time.Minute),
			AirTemperature: 20.0 + float64(i),
		})
	}

	srv := setupTestServer(ms)
	defer srv.Close()

	t.Run("valid range", func(t *testing.T) {
		url := srv.URL + "/api/v1/stations/1001/observations?start=2024-06-15T12:00:00Z&end=2024-06-15T12:10:00Z"
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}

		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		obs, ok := body["observations"].([]any)
		if !ok {
			t.Fatal("expected observations array in envelope response")
		}
		if len(obs) != 10 {
			t.Errorf("got %d observations, want 10", len(obs))
		}
		if body["station_id"] != float64(1001) {
			t.Errorf("station_id = %v, want 1001", body["station_id"])
		}
	})

	t.Run("with YYYY-MM-DD dates", func(t *testing.T) {
		url := srv.URL + "/api/v1/stations/1001/observations?start=2024-06-15&end=2024-06-16"
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}
	})

	t.Run("with Unix epoch timestamps", func(t *testing.T) {
		// 1718452800 = 2024-06-15T12:00:00Z, 1718453400 = 2024-06-15T12:10:00Z
		url := srv.URL + "/api/v1/stations/1001/observations?start=1718452800&end=1718453400"
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}

		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		obs, ok := body["observations"].([]any)
		if !ok {
			t.Fatal("expected observations array in envelope response")
		}
		if len(obs) != 10 {
			t.Errorf("got %d observations, want 10", len(obs))
		}
	})

	t.Run("invalid time format", func(t *testing.T) {
		url := srv.URL + "/api/v1/stations/1001/observations?start=not-a-date&end=2024-06-15T12:10:00Z"
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})

	t.Run("missing start", func(t *testing.T) {
		url := srv.URL + "/api/v1/stations/1001/observations?end=2024-06-15T12:10:00Z"
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})

	t.Run("missing end", func(t *testing.T) {
		url := srv.URL + "/api/v1/stations/1001/observations?start=2024-06-15T12:00:00Z"
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})
}

func TestHandlers_GetDailySummary(t *testing.T) {
	ms := newMockStore()
	srv := setupTestServer(ms)
	defer srv.Close()

	t.Run("valid with single date", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/api/v1/stations/1001/summary?date=2024-06-15")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		// Should be 404 since no data.
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("status = %d, want %d (no data)", resp.StatusCode, http.StatusNotFound)
		}
	})

	t.Run("missing date", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/api/v1/stations/1001/summary")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})

	t.Run("invalid date", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/api/v1/stations/1001/summary?date=bad")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
		}
	})
}

func TestHandlers_Pagination(t *testing.T) {
	ms := newMockStore()
	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	for i := range 20 {
		ms.observations = append(ms.observations, tempest.Observation{
			StationID:      1001,
			Timestamp:      base.Add(time.Duration(i) * time.Minute),
			AirTemperature: 20.0 + float64(i),
		})
	}

	srv := setupTestServer(ms)
	defer srv.Close()

	tests := []struct {
		name      string
		limit     string
		offset    string
		wantCount int
		wantTotal int
	}{
		{"default pagination", "", "", 20, 20},
		{"limit 5", "5", "", 5, 20},
		{"limit 5 offset 15", "5", "15", 5, 20},
		{"limit 5 offset 18", "5", "18", 2, 20},
		{"offset past end", "5", "25", 0, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := srv.URL + "/api/v1/stations/1001/observations?start=2024-06-15T12:00:00Z&end=2024-06-15T12:20:00Z"
			if tt.limit != "" {
				url += "&limit=" + tt.limit
			}
			if tt.offset != "" {
				url += "&offset=" + tt.offset
			}

			resp, err := http.Get(url)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close() //nolint:errcheck

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
			}

			var body map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&body)

			obs, ok := body["observations"].([]any)
			if !ok {
				t.Fatal("expected observations array")
			}
			if len(obs) != tt.wantCount {
				t.Errorf("got %d observations, want %d", len(obs), tt.wantCount)
			}
			if int(body["total"].(float64)) != tt.wantTotal {
				t.Errorf("total = %v, want %d", body["total"], tt.wantTotal)
			}
		})
	}
}

func TestHandlers_UnitConversion(t *testing.T) {
	tests := []struct {
		name     string
		units    string
		wantTemp float64
		wantUnit string
	}{
		{"metric (default)", "", 0.0, "metric"},
		{"metric (explicit)", "metric", 0.0, "metric"},
		{"imperial", "imperial", 32.0, "imperial"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := newMockStore()
			ms.observations = append(ms.observations, tempest.Observation{
				StationID:      1001,
				Timestamp:      time.Now().UTC(),
				AirTemperature: 0.0, // 0°C = 32°F
				WindAvg:        1.0,
			})

			h := &Handlers{Store: ms, StartTime: time.Now()}
			mux := http.NewServeMux()
			mux.HandleFunc("GET /api/v1/stations/{station_id}/current", h.GetCurrentObservation)
			srv := httptest.NewServer(ContentType(mux))
			defer srv.Close()

			url := srv.URL + "/api/v1/stations/1001/current"
			if tt.units != "" {
				url += "?units=" + tt.units
			}
			resp, err := http.Get(url)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close() //nolint:errcheck

			var body map[string]any
			_ = json.NewDecoder(resp.Body).Decode(&body)

			if body["air_temperature"] != tt.wantTemp {
				t.Errorf("temp = %v, want %v", body["air_temperature"], tt.wantTemp)
			}
			if body["units"] != tt.wantUnit {
				t.Errorf("units = %v, want %q", body["units"], tt.wantUnit)
			}
		})
	}
}

func TestHandlers_GetRange(t *testing.T) {
	ms := newMockStore()
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	ms.observations = append(ms.observations, tempest.Observation{
		StationID: 1001,
		Timestamp: ts,
	})

	srv := setupTestServer(ms)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/stations/1001/range")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["station_id"] != float64(1001) {
		t.Errorf("station_id = %v, want 1001", body["station_id"])
	}
	if body["total_observations"] == nil {
		t.Error("expected total_observations field")
	}
}

func TestHandlers_ErrorResponseHasCode(t *testing.T) {
	ms := newMockStore()
	srv := setupTestServer(ms)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/stations/abc")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["code"] == nil {
		t.Error("expected 'code' field in error response")
	}
	if body["code"] != float64(400) {
		t.Errorf("code = %v, want 400", body["code"])
	}
}

func TestMiddleware_CORS(t *testing.T) {
	handler := CORS("https://example.com")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	t.Run("regular request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
			t.Errorf("CORS origin = %q, want %q", got, "https://example.com")
		}
	})

	t.Run("preflight", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusNoContent {
			t.Errorf("preflight status = %d, want %d", w.Code, http.StatusNoContent)
		}
	})
}

func TestMiddleware_Recovery(t *testing.T) {
	handler := Recovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("panic recovery status = %d, want %d", w.Code, http.StatusInternalServerError)
	}
}

func TestServer_GracefulShutdown(t *testing.T) {
	ms := newMockStore()
	h := &Handlers{Store: ms, StartTime: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", h.Health)

	srv := &Server{
		httpServer: &http.Server{Handler: mux},
		handlers:   h,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start the server.
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx, "127.0.0.1:0")
	}()

	// Give server time to start.
	time.Sleep(100 * time.Millisecond)

	// Cancel context to trigger shutdown.
	cancel()

	// Shutdown should complete quickly.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Errorf("shutdown error: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("ListenAndServe error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down in time")
	}
}

func TestHandlers_HealthReflectsStatus(t *testing.T) {
	ms := newMockStore()

	// Add some observations to verify total_observations is computed.
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	for i := range 5 {
		ms.observations = append(ms.observations, tempest.Observation{
			StationID: 1001,
			Timestamp: ts.Add(time.Duration(i) * time.Minute),
		})
	}
	_ = ms.SaveStation(context.Background(), &store.Station{
		ID:       1001,
		DeviceID: 100,
		Name:     "Test Station",
	})

	h := &Handlers{
		Store:         ms,
		StartTime:     time.Now().Add(-time.Hour),
		StorageDriver: "sqlite",
		StoragePath:   "/tmp/test.db",
		Version:       "v1.2.3",
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", h.Health)
	srv := httptest.NewServer(ContentType(mux))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)

	if body["status"] != "healthy" {
		t.Errorf("status = %v, want 'healthy'", body["status"])
	}

	// Verify version is populated.
	if body["version"] != "v1.2.3" {
		t.Errorf("version = %v, want 'v1.2.3'", body["version"])
	}

	// Verify uptime is populated.
	if body["uptime"] == nil || body["uptime"] == "" {
		t.Error("expected non-empty uptime")
	}

	// Verify database info.
	db, ok := body["database"].(map[string]any)
	if !ok {
		t.Fatal("expected database object in health response")
	}
	if db["driver"] != "sqlite" {
		t.Errorf("driver = %v, want 'sqlite'", db["driver"])
	}
	if db["path"] != nil {
		t.Errorf("path should not be exposed in health response, got %v", db["path"])
	}
	if db["status"] != "ok" {
		t.Errorf("db status = %v, want 'ok'", db["status"])
	}
	if db["total_observations"].(float64) != 5 {
		t.Errorf("total_observations = %v, want 5", db["total_observations"])
	}
}

func TestHandlers_InvalidResolution(t *testing.T) {
	ms := newMockStore()
	base := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	ms.observations = append(ms.observations, tempest.Observation{
		StationID: 1001,
		Timestamp: base,
	})

	srv := setupTestServer(ms)
	defer srv.Close()

	// Invalid resolution should fall back to 1m default.
	url := srv.URL + "/api/v1/stations/1001/observations?start=2024-06-15T12:00:00Z&end=2024-06-15T12:01:00Z&resolution=invalid"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	// Should default to 1m.
	if body["resolution"] != "invalid" {
		t.Errorf("resolution = %v, should preserve input string", body["resolution"])
	}
}

func TestMiddleware_RequestID(t *testing.T) {
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	id := w.Header().Get("X-Request-ID")
	if id == "" {
		t.Error("expected X-Request-ID header")
	}
}
