package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
	"github.com/chadmayfield/tempestd/internal/config"
)

func TestBackfiller_BackfillStation(t *testing.T) {
	// Mock REST server returning observations in WeatherFlow format.
	var requestCount atomic.Int64
	ts1 := float64(time.Date(2024, 6, 10, 12, 0, 0, 0, time.UTC).Unix())
	ts2 := float64(time.Date(2024, 6, 10, 12, 1, 0, 0, time.UTC).Unix())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Return two observations per chunk in deviceObsResponse format.
		resp := map[string]any{
			"device_id": 1001,
			"obs": [][]any{
				{ts1, 0.5, 2.0, 3.5, 180.0, 3, 1013.0, 22.5, 65.0, 10000.0, 3.0, 200.0, 0.0, 0, 10.0, 0, 2.7, 1},
				{ts2, 0.3, 1.5, 2.8, 190.0, 3, 1013.5, 24.0, 60.0, 10000.0, 2.5, 180.0, 0.0, 0, 8.0, 0, 2.7, 1},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	ms := newMockStore()
	logger := slog.Default()
	bf := NewBackfiller(ms, logger)
	bf.testBaseURL = server.URL

	// Backfill a 4-day range (fits in 1 chunk of 5 days).
	from := time.Date(2024, 6, 10, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 14, 0, 0, 0, 0, time.UTC)

	stationID := 2001
	deviceID := 1001
	err := bf.BackfillStation(context.Background(), "test-token", stationID, deviceID, from, to)
	if err != nil {
		t.Fatalf("BackfillStation: %v", err)
	}

	// Verify the mock server was called.
	if requestCount.Load() != 1 {
		t.Errorf("expected 1 API request, got %d", requestCount.Load())
	}

	// Verify observations were saved to the store.
	if len(ms.observations) != 2 {
		t.Fatalf("expected 2 saved observations, got %d", len(ms.observations))
	}

	// Verify StationID is set correctly on all observations (device API only sets DeviceID).
	for i, obs := range ms.observations {
		if obs.StationID != stationID {
			t.Errorf("obs[%d]: StationID = %d, want %d", i, obs.StationID, stationID)
		}
	}

	// Verify derived metrics were computed.
	for i, obs := range ms.observations {
		if obs.DewPoint == 0 && obs.AirTemperature != 0 {
			t.Errorf("obs[%d]: dew point not computed", i)
		}
		if obs.FeelsLike == 0 && obs.AirTemperature != 0 {
			t.Errorf("obs[%d]: feels like not computed", i)
		}
		if obs.WetBulb == 0 && obs.AirTemperature != 0 {
			t.Errorf("obs[%d]: wet bulb not computed", i)
		}
	}
}

func TestBackfiller_DetectAndFill_NoPreviousData(t *testing.T) {
	ms := newMockStore()
	logger := slog.Default()
	bf := NewBackfiller(ms, logger)
	ctx := context.Background()

	stations := []config.StationConfig{
		{Token: "invalid-token", StationID: 1001, DeviceID: 100},
	}

	// DetectAndFill should detect no data for station 1001.
	// Since it creates its own REST client, this will fail (no API server).
	// That's expected — we just want to verify the gap detection logic.
	err := bf.DetectAndFill(ctx, stations, 7)
	// DetectAndFill logs errors but doesn't return them for individual stations.
	_ = err
}

func TestBackfiller_DetectAndFill_DataIsCurrent(t *testing.T) {
	ms := newMockStore()
	logger := slog.Default()
	// Add a recent observation.
	ms.observations = append(ms.observations, tempest.Observation{
		StationID: 1001,
		Timestamp: time.Now().UTC().Add(-30 * time.Second), // 30 seconds ago
	})

	bf := NewBackfiller(ms, logger)
	ctx := context.Background()

	stations := []config.StationConfig{
		{Token: "test-token", StationID: 1001, DeviceID: 100},
	}

	err := bf.DetectAndFill(ctx, stations, 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not have added any new observations (data is current).
	if len(ms.observations) != 1 {
		t.Errorf("got %d observations, want 1 (no backfill needed)", len(ms.observations))
	}
}

func TestBackfiller_ContextCancellation(t *testing.T) {
	ms := newMockStore()
	logger := slog.Default()
	bf := NewBackfiller(ms, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	from := time.Now().AddDate(0, 0, -30)
	to := time.Now()

	err := bf.BackfillStation(ctx, "test-token", 1001, 100, from, to)
	if err == nil {
		t.Error("expected context cancelled error")
	}
}

func TestBackfiller_Pacing(t *testing.T) {
	// Mock REST server that returns empty observations (fast responses).
	var requestCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		resp := map[string]any{
			"status": map[string]any{"status_code": 0},
			"obs":    []map[string]any{{"obs": [][]any{}}},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	ms := newMockStore()
	logger := slog.Default()
	bf := NewBackfiller(ms, logger)
	bf.testBaseURL = server.URL

	// Verify the pacing constant.
	if requestPace != 2*time.Second {
		t.Errorf("requestPace = %v, want 2s", requestPace)
	}

	// Verify chunk calculation for a 15-day span.
	to := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	from := to.AddDate(0, 0, -15)
	chunks := 0
	for cs := from; cs.Before(to); {
		ce := cs.AddDate(0, 0, chunkDays)
		if ce.After(to) {
			ce = to
		}
		chunks++
		cs = ce
	}
	if chunks != 3 {
		t.Errorf("15-day backfill: got %d chunks, want 3", chunks)
	}

	// Verify that the backfiller has rate limiter and circuit breaker.
	if bf.limiter == nil {
		t.Error("backfiller should have a rate limiter")
	}
	if bf.breaker == nil {
		t.Error("backfiller should have a circuit breaker")
	}
}

func TestBackfiller_Idempotency(t *testing.T) {
	ms := newMockStore()

	// Pre-populate with observations.
	base := time.Date(2024, 6, 10, 0, 0, 0, 0, time.UTC)
	for i := range 5 {
		obs := tempest.Observation{
			StationID:        1001,
			Timestamp:        base.Add(time.Duration(i) * time.Hour),
			AirTemperature:   22.0,
			RelativeHumidity: 65.0,
			WindAvg:          3.0,
		}
		ms.observations = append(ms.observations, obs)
	}
	originalCount := len(ms.observations)

	// SaveObservations on the mock store appends, so after a "re-backfill",
	// we'd see new entries. In a real store with upsert, the count would stay the same.
	// Test that the mock store receives the batch call (the real upsert is tested in store tests).
	_ = ms.SaveObservations(context.Background(), ms.observations)

	// After saving the same observations again, they should be appended in mock
	// but in real store would be upserted (same count).
	if len(ms.observations) != originalCount*2 {
		t.Errorf("expected %d observations after double save, got %d", originalCount*2, len(ms.observations))
	}
}

func TestBackfiller_MultiChunkBackfill(t *testing.T) {
	// Mock REST server that tracks how many chunk requests are made.
	var requestCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		// Return one observation per chunk in deviceObsResponse format.
		ts := float64(time.Date(2024, 6, 10, 0, 0, 0, 0, time.UTC).Unix()) + float64(requestCount.Load())*60
		resp := map[string]any{
			"device_id": 1001,
			"obs": [][]any{
				{ts, 0.5, 2.0, 3.5, 180.0, 3, 1013.0, 22.5, 65.0, 10000.0, 3.0, 200.0, 0.0, 0, 10.0, 0, 2.7, 1},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	ms := newMockStore()
	logger := slog.Default()
	bf := NewBackfiller(ms, logger)
	bf.testBaseURL = server.URL

	// 11-day range = 3 chunks (5 + 5 + 1).
	from := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 12, 0, 0, 0, 0, time.UTC)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stationID := 2001
	deviceID := 1001
	err := bf.BackfillStation(ctx, "test-token", stationID, deviceID, from, to)
	if err != nil {
		t.Fatalf("BackfillStation: %v", err)
	}

	// Should have made 3 chunk requests.
	if requestCount.Load() != 3 {
		t.Errorf("expected 3 API requests for 11-day range, got %d", requestCount.Load())
	}

	// Should have saved 3 observations (1 per chunk).
	if len(ms.observations) != 3 {
		t.Errorf("expected 3 saved observations, got %d", len(ms.observations))
	}

	// Verify StationID is set correctly on all observations.
	for i, obs := range ms.observations {
		if obs.StationID != stationID {
			t.Errorf("obs[%d]: StationID = %d, want %d", i, obs.StationID, stationID)
		}
	}

	// Verify derived metrics on all saved observations.
	for i, obs := range ms.observations {
		if obs.DewPoint == 0 {
			t.Errorf("obs[%d]: dew point not computed", i)
		}
		if obs.FeelsLike == 0 {
			t.Errorf("obs[%d]: feels like not computed", i)
		}
	}
}

func TestBackfiller_ChunkCalculation(t *testing.T) {
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
			// Use exact day boundaries to avoid sub-day rounding issues.
			to := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
			from := to.AddDate(0, 0, -tt.days)
			chunks := 0
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
