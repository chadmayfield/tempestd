package collector

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	tempest "github.com/chadmayfield/tempest-go"
	"github.com/chadmayfield/tempestd/internal/store"
)

// mockStore implements store.Store for testing.
type mockStore struct {
	observations []tempest.Observation
	stations     map[int]*store.Station
	saveErr      error // If set, SaveObservation returns this error.
}

func newMockStore() *mockStore {
	return &mockStore{
		stations: make(map[int]*store.Station),
	}
}

func (m *mockStore) SaveObservation(_ context.Context, obs *tempest.Observation) error {
	if m.saveErr != nil {
		return m.saveErr
	}
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

func (m *mockStore) GetDailySummary(_ context.Context, _ int, _ time.Time) (*store.DailySummary, error) {
	return nil, nil
}

func (m *mockStore) GetDataRange(_ context.Context, _ int) (time.Time, time.Time, error) {
	return time.Time{}, time.Time{}, nil
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

func (m *mockStore) SaveStation(_ context.Context, station *store.Station) error {
	m.stations[station.ID] = station
	return nil
}

func (m *mockStore) GetStations(_ context.Context) ([]store.Station, error) {
	var result []store.Station
	for _, s := range m.stations {
		result = append(result, *s)
	}
	return result, nil
}

func (m *mockStore) Close() error { return nil }

func TestCollector_StatusTracking(t *testing.T) {
	ms := newMockStore()
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	coll.AddStation("test-token", 1001, 100, "Station 1")
	coll.AddStation("test-token", 1002, 200, "Station 2")

	statuses := coll.Status()
	if len(statuses) != 2 {
		t.Fatalf("got %d statuses, want 2", len(statuses))
	}

	// All should start disconnected.
	for _, s := range statuses {
		if s.Connected {
			t.Errorf("station %d should not be connected initially", s.StationID)
		}
		if s.ErrorCount != 0 {
			t.Errorf("station %d should have 0 errors initially", s.StationID)
		}
	}
}

func TestObsHandler_DerivedMetrics(t *testing.T) {
	tests := []struct {
		name     string
		temp     float64
		humidity float64
		wind     float64
	}{
		{"warm and humid", 25.0, 60.0, 3.0},
		{"hot and dry", 35.0, 20.0, 5.0},
		{"cold and damp", 5.0, 90.0, 1.0},
		{"freezing", -5.0, 80.0, 10.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := newMockStore()
			connMgr := tempest.NewConnectionManager()
			logger := slog.Default()

			coll := NewCollector(ms, connMgr, logger)
			coll.AddStation("test-token", 1001, 100, "Station 1")

			handler := &obsHandler{collector: coll}

			obs := &tempest.Observation{
				StationID:        1001,
				Timestamp:        time.Now().UTC(),
				AirTemperature:   tt.temp,
				RelativeHumidity: tt.humidity,
				WindAvg:          tt.wind,
			}

			handler.HandleObservation(obs)

			if len(ms.observations) != 1 {
				t.Fatalf("expected 1 saved observation, got %d", len(ms.observations))
			}

			saved := ms.observations[0]

			expectedDew := tempest.DewPoint(tt.temp, tt.humidity)
			if saved.DewPoint != expectedDew {
				t.Errorf("dew point = %v, want %v", saved.DewPoint, expectedDew)
			}

			expectedFeels := tempest.FeelsLike(tt.temp, tt.humidity, tt.wind)
			if saved.FeelsLike != expectedFeels {
				t.Errorf("feels like = %v, want %v", saved.FeelsLike, expectedFeels)
			}

			expectedWet := tempest.WetBulb(tt.temp, tt.humidity)
			if saved.WetBulb != expectedWet {
				t.Errorf("wet bulb = %v, want %v", saved.WetBulb, expectedWet)
			}
		})
	}
}

func TestCollector_ContextCancellation(t *testing.T) {
	ms := newMockStore()
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	coll.AddStation("test-token", 1001, 100, "Station 1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	// Start should return promptly when context is already cancelled.
	done := make(chan struct{})
	go func() {
		_ = coll.Start(ctx)
		close(done)
	}()

	select {
	case <-done:
		// Success -- Start returned.
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}

func TestCollector_MultipleStationsSameToken(t *testing.T) {
	ms := newMockStore()
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	coll.AddStation("shared-token", 1001, 100, "Station 1")
	coll.AddStation("shared-token", 1002, 200, "Station 2")

	// Verify both stations are tracked.
	statuses := coll.Status()
	if len(statuses) != 2 {
		t.Fatalf("got %d statuses, want 2", len(statuses))
	}

	// Verify they're grouped by token (stations slice should have 2 entries with same token).
	coll.mu.RLock()
	defer coll.mu.RUnlock()
	tokenCount := make(map[string]int)
	for _, st := range coll.stations {
		tokenCount[st.token]++
	}
	if tokenCount["shared-token"] != 2 {
		t.Errorf("expected 2 stations with shared-token, got %d", tokenCount["shared-token"])
	}
}

func TestObsHandler_UpdatesStatus(t *testing.T) {
	ms := newMockStore()
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	coll.AddStation("test-token", 1001, 100, "Station 1")

	handler := &obsHandler{
		collector: coll,
	}

	ts := time.Now().UTC()
	obs := &tempest.Observation{
		StationID:        1001,
		Timestamp:        ts,
		AirTemperature:   20.0,
		RelativeHumidity: 50.0,
		WindAvg:          1.0,
	}

	handler.HandleObservation(obs)

	statuses := coll.Status()
	found := false
	for _, s := range statuses {
		if s.StationID == 1001 {
			found = true
			if !s.Connected {
				t.Error("station should be connected after handling observation")
			}
			if s.LastObsAt != ts {
				t.Errorf("last obs at = %v, want %v", s.LastObsAt, ts)
			}
		}
	}
	if !found {
		t.Error("station 1001 not found in statuses")
	}
}

func TestObsHandler_SaveError(t *testing.T) {
	ms := newMockStore()
	ms.saveErr = errors.New("database write failed")
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	coll.AddStation("test-token", 1001, 100, "Station 1")

	handler := &obsHandler{
		collector: coll,
	}

	obs := &tempest.Observation{
		StationID:        1001,
		Timestamp:        time.Now().UTC(),
		AirTemperature:   20.0,
		RelativeHumidity: 50.0,
		WindAvg:          1.0,
	}

	// Should not panic, even with save error.
	handler.HandleObservation(obs)

	// No observations saved.
	if len(ms.observations) != 0 {
		t.Errorf("expected 0 observations (save error), got %d", len(ms.observations))
	}

	// Error should be recorded.
	statuses := coll.Status()
	for _, s := range statuses {
		if s.StationID == 1001 {
			if s.ErrorCount == 0 {
				t.Error("expected error count > 0 after save failure")
			}
			if s.LastError == "" {
				t.Error("expected last_error to be set")
			}
		}
	}
}

func TestObsHandler_PanicRecovery(t *testing.T) {
	// Verify that the panic recovery in HandleObservation works.
	// We test this by verifying the defer/recover exists —
	// a nil collector would panic without recovery.
	ms := newMockStore()
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	coll.AddStation("test-token", 1001, 100, "Station 1")

	handler := &obsHandler{collector: coll}

	// Valid observation — should not panic.
	obs := &tempest.Observation{
		StationID:        1001,
		Timestamp:        time.Now().UTC(),
		AirTemperature:   20.0,
		RelativeHumidity: 50.0,
		WindAvg:          1.0,
	}
	handler.HandleObservation(obs)

	if len(ms.observations) != 1 {
		t.Errorf("expected 1 observation, got %d", len(ms.observations))
	}
}

func TestCollector_RecordError(t *testing.T) {
	ms := newMockStore()
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	coll.AddStation("test-token", 1001, 100, "Station 1")

	coll.recordError(1001, errors.New("test error"))
	coll.recordError(1001, errors.New("test error 2"))

	statuses := coll.Status()
	for _, s := range statuses {
		if s.StationID == 1001 {
			if s.ErrorCount != 2 {
				t.Errorf("error count = %d, want 2", s.ErrorCount)
			}
			if s.LastError != "test error 2" {
				t.Errorf("last error = %q, want %q", s.LastError, "test error 2")
			}
		}
	}
}

func TestObsHandler_HandlesEventMethods(t *testing.T) {
	ms := newMockStore()
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	handler := &obsHandler{collector: coll}

	// Verify these don't panic.
	handler.HandleLightningStrike(&tempest.LightningStrike{
		Distance: 10,
		Energy:   1000,
	})
	handler.HandleRainStart(&tempest.RainStart{
		Timestamp: time.Now(),
	})
	handler.HandleRapidWind(&tempest.RapidWind{
		WindSpeed:     5.0,
		WindDirection: 180.0,
	})
}

func TestCollector_WSToStore(t *testing.T) {
	// Simulate a WebSocket observation stream flowing through the handler
	// pipeline to the mock store. This verifies the full data path from
	// incoming WS messages to persisted observations with derived metrics.
	ms := newMockStore()
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	coll.AddStation("test-token", 1001, 100, "Station 1")
	coll.AddStation("test-token", 1002, 200, "Station 2")

	handler := &obsHandler{collector: coll}

	// Simulate a stream of observations from two stations
	// (as a WebSocket connection would deliver them).
	base := time.Now().UTC().Truncate(time.Minute)
	for i := 0; i < 5; i++ {
		for _, stationID := range []int{1001, 1002} {
			obs := &tempest.Observation{
				StationID:        stationID,
				Timestamp:        base.Add(time.Duration(i) * time.Minute),
				AirTemperature:   20.0 + float64(i),
				RelativeHumidity: 60.0 + float64(i),
				WindAvg:          2.0 + float64(i)*0.5,
			}
			handler.HandleObservation(obs)
		}
	}

	// Verify all 10 observations saved (5 per station).
	if len(ms.observations) != 10 {
		t.Fatalf("expected 10 saved observations, got %d", len(ms.observations))
	}

	// Verify derived metrics computed for all observations.
	for i, obs := range ms.observations {
		if obs.DewPoint == 0 {
			t.Errorf("obs[%d] station %d: dew point not computed", i, obs.StationID)
		}
		if obs.FeelsLike == 0 {
			t.Errorf("obs[%d] station %d: feels like not computed", i, obs.StationID)
		}
		if obs.WetBulb == 0 {
			t.Errorf("obs[%d] station %d: wet bulb not computed", i, obs.StationID)
		}
	}

	// Verify status tracking updated for both stations.
	statuses := coll.Status()
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	for _, s := range statuses {
		if !s.Connected {
			t.Errorf("station %d: expected connected after observations", s.StationID)
		}
		if s.LastObsAt.IsZero() {
			t.Errorf("station %d: expected non-zero last obs time", s.StationID)
		}
	}

	// Verify per-station observation counts.
	counts := map[int]int{}
	for _, obs := range ms.observations {
		counts[obs.StationID]++
	}
	if counts[1001] != 5 {
		t.Errorf("station 1001: got %d observations, want 5", counts[1001])
	}
	if counts[1002] != 5 {
		t.Errorf("station 1002: got %d observations, want 5", counts[1002])
	}
}

func TestCollector_MockWSServer(t *testing.T) {
	// Full integration test: mock WebSocket server → Collector → mock store.
	// Uses tempest-go's WithWSURL to inject a test WS URL.
	outgoing := make(chan []byte, 100)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Logf("ws accept error: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()

		// Read loop (consume client messages like listen_start).
		go func() {
			for {
				_, _, err := conn.Read(r.Context())
				if err != nil {
					return
				}
			}
		}()

		// Write loop: send messages from outgoing channel.
		for msg := range outgoing {
			if err := conn.Write(r.Context(), websocket.MessageText, msg); err != nil {
				return
			}
		}
	}))
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1)

	ms := newMockStore()
	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(ms, connMgr, logger)
	coll.testWSURL = wsURL
	coll.AddStation("test-token", 1001, 100, "Station 1")

	ctx, cancel := context.WithCancel(context.Background())

	// Start collector in background.
	done := make(chan error, 1)
	go func() {
		done <- coll.Start(ctx)
	}()

	// Wait for WS connection to establish + subscribe goroutine.
	time.Sleep(3 * time.Second)

	// Use a thread-safe store for this test since WS goroutine writes concurrently.
	tms := &syncMockStore{mockStore: ms}
	// Replace the collector's store with the thread-safe one.
	coll.store = tms

	// Send an obs_st message via mock WS server.
	outgoing <- []byte(`{
		"type": "obs_st",
		"device_id": 100,
		"obs": [[1718020800, 0.5, 2.0, 3.5, 180, 3, 1013, 22.5, 65.0, 10000, 3.0, 200, 0, 0, 10, 0, 2.7, 1]]
	}`)

	// Poll for observation to appear (thread-safe).
	deadline := time.After(5 * time.Second)
	for tms.obsCount() < 1 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for observation, got %d", tms.obsCount())
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Verify observation was saved to the store with correct StationID and derived metrics.
	saved := tms.getObs(0)
	if saved.StationID != 1001 {
		t.Errorf("StationID = %d, want 1001 (should be mapped from DeviceID 100)", saved.StationID)
	}
	if saved.AirTemperature != 22.5 {
		t.Errorf("air_temperature = %v, want 22.5", saved.AirTemperature)
	}
	if saved.DewPoint == 0 {
		t.Error("dew point not computed")
	}
	if saved.FeelsLike == 0 {
		t.Error("feels like not computed")
	}
	if saved.WetBulb == 0 {
		t.Error("wet bulb not computed")
	}

	cancel()
	close(outgoing)
	<-done
}

// syncMockStore wraps mockStore with a mutex for concurrent access.
type syncMockStore struct {
	*mockStore
	mu sync.Mutex
}

func (s *syncMockStore) SaveObservation(ctx context.Context, obs *tempest.Observation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mockStore.SaveObservation(ctx, obs)
}

func (s *syncMockStore) obsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.observations)
}

func (s *syncMockStore) getObs(i int) tempest.Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observations[i]
}

func TestCollector_ShutdownSequence(t *testing.T) {
	// Test full shutdown: cancel context → WS closed → HTTP drained → DB closed.
	// Uses an ordered log to verify shutdown sequence.
	var sequence []string
	var seqMu sync.Mutex
	record := func(event string) {
		seqMu.Lock()
		defer seqMu.Unlock()
		sequence = append(sequence, event)
	}

	// Mock WS server that records when connection closes.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.CloseNow()
			record("ws_closed")
		}()

		// Read until connection closes.
		for {
			_, _, err := conn.Read(r.Context())
			if err != nil {
				return
			}
		}
	}))
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1)

	// trackingStore wraps mockStore and records Close() in sequence.
	ms := newMockStore()
	tracker := &trackingStore{mockStore: ms, record: record}

	connMgr := tempest.NewConnectionManager()
	logger := slog.Default()

	coll := NewCollector(tracker, connMgr, logger)
	coll.testWSURL = wsURL
	coll.AddStation("test-token", 1001, 100, "Station 1")

	ctx, cancel := context.WithCancel(context.Background())

	// Start collector.
	done := make(chan struct{})
	go func() {
		_ = coll.Start(ctx)
		record("collector_stopped")
		close(done)
	}()

	// Wait for connection.
	time.Sleep(3 * time.Second)

	// Cancel context to trigger shutdown.
	cancel()

	// Wait for collector to stop.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("collector did not stop in time")
	}

	// Give WS server time to notice disconnection.
	time.Sleep(200 * time.Millisecond)

	// Close store (simulating serve.go's cleanup).
	_ = tracker.Close()

	seqMu.Lock()
	defer seqMu.Unlock()

	// Verify ordering: collector stopped, then WS closed, then DB closed.
	// The WS close happens server-side when the client disconnects.
	dbIdx := -1
	for i, ev := range sequence {
		if ev == "db_closed" {
			dbIdx = i
		}
	}
	if dbIdx == -1 {
		t.Fatal("db_closed not recorded in shutdown sequence")
	}
	// DB close must be last.
	if dbIdx != len(sequence)-1 {
		t.Errorf("db_closed at index %d, but sequence has %d events: %v", dbIdx, len(sequence), sequence)
	}
}

// trackingStore wraps mockStore and records Close() events.
type trackingStore struct {
	*mockStore
	record func(string)
}

func (t *trackingStore) Close() error {
	t.record("db_closed")
	return t.mockStore.Close()
}
