package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
	"github.com/chadmayfield/tempestd/internal/store"
)

// StationStatus tracks the state of a station's WebSocket connection.
type StationStatus struct {
	StationID             int       `json:"station_id"`
	Name                  string    `json:"name"`
	Connected             bool      `json:"connected"`
	LastObsAt             time.Time `json:"last_obs_at,omitempty"`
	ObservationAgeSeconds float64   `json:"observation_age_seconds,omitempty"`
	ErrorCount            int       `json:"error_count"`
	LastError             string    `json:"last_error,omitempty"`
}

// Collector manages WebSocket connections for all configured stations.
type Collector struct {
	store   store.Store
	connMgr *tempest.ConnectionManager
	logger  *slog.Logger

	mu              sync.RWMutex
	statuses        map[int]*StationStatus
	stations        []stationEntry
	deviceToStation map[int]int    // Maps device IDs to station IDs.
	ctx             context.Context // Parent context for observation saves.
	testWSURL       string          // For testing only; empty in production.
}

type stationEntry struct {
	token     string
	stationID int
	deviceID  int
	name      string
}

// NewCollector creates a new collector.
func NewCollector(s store.Store, connMgr *tempest.ConnectionManager, logger *slog.Logger) *Collector {
	return &Collector{
		store:           s,
		connMgr:         connMgr,
		logger:          logger,
		statuses:        make(map[int]*StationStatus),
		deviceToStation: make(map[int]int),
	}
}

// AddStation adds a station to collect from.
func (c *Collector) AddStation(token string, stationID, deviceID int, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stations = append(c.stations, stationEntry{
		token:     token,
		stationID: stationID,
		deviceID:  deviceID,
		name:      name,
	})
	c.statuses[stationID] = &StationStatus{
		StationID: stationID,
		Name:      name,
	}
	c.deviceToStation[deviceID] = stationID
}

const (
	reconnectMin = 2 * time.Second
	reconnectMax = 5 * time.Minute
)

// Start connects to all stations and blocks until the context is cancelled.
func (c *Collector) Start(ctx context.Context) error {
	c.ctx = ctx

	// Group stations by token to share WebSocket connections.
	byToken := make(map[string][]stationEntry)
	c.mu.RLock()
	for _, st := range c.stations {
		byToken[st.token] = append(byToken[st.token], st)
	}
	c.mu.RUnlock()

	var wg sync.WaitGroup
	for token, stations := range byToken {
		wg.Add(1)
		go func(token string, stations []stationEntry) {
			defer wg.Done()
			c.runWSClientWithRetry(ctx, token, stations)
		}(token, stations)
	}

	wg.Wait()
	return nil
}

func (c *Collector) runWSClientWithRetry(ctx context.Context, token string, stations []stationEntry) {
	backoff := reconnectMin
	for {
		start := time.Now()
		c.runWSClient(ctx, token, stations)
		if ctx.Err() != nil {
			return
		}
		// Reset backoff if connection was stable for a while.
		if time.Since(start) > reconnectMax {
			backoff = reconnectMin
		}
		c.logger.Warn("ws client disconnected, reconnecting",
			"backoff", backoff,
			"stations", len(stations),
		)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, reconnectMax)
	}
}

// Status returns a snapshot of all station statuses.
func (c *Collector) Status() []StationStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now().UTC()
	result := make([]StationStatus, 0, len(c.statuses))
	for _, s := range c.statuses {
		status := *s
		if !s.LastObsAt.IsZero() {
			status.ObservationAgeSeconds = now.Sub(s.LastObsAt).Seconds()
		}
		result = append(result, status)
	}
	return result
}

func (c *Collector) runWSClient(ctx context.Context, token string, stations []stationEntry) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("panic in ws client goroutine", "error", r)
			for _, st := range stations {
				c.recordError(st.stationID, fmt.Errorf("panic: %v", r))
			}
		}
	}()

	opts := []tempest.WSClientOption{tempest.WithConnectionManager(c.connMgr)}
	if c.testWSURL != "" {
		opts = append(opts, tempest.WithWSURL(c.testWSURL))
	}
	ws, err := tempest.NewWSClient(token, opts...)
	if err != nil {
		c.logger.Error("failed to create ws client", "error", err)
		for _, st := range stations {
			c.recordError(st.stationID, err)
		}
		return
	}

	handler := &obsHandler{
		collector: c,
	}

	// Subscribe to each station's devices after connect.
	var subWg sync.WaitGroup
	subWg.Add(1)
	go func() {
		defer subWg.Done()
		// Give the WS client time to establish the initial connection.
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}

		for _, st := range stations {
			if err := ws.Subscribe(ctx, st.deviceID); err != nil {
				c.logger.Error("failed to subscribe", "device_id", st.deviceID, "error", err)
			} else {
				c.logger.Info("subscribed to device", "station_id", st.stationID, "device_id", st.deviceID)
			}

			c.mu.Lock()
			if s, ok := c.statuses[st.stationID]; ok {
				s.Connected = true
			}
			c.mu.Unlock()
		}
	}()

	if err := ws.Connect(ctx, handler); err != nil && ctx.Err() == nil {
		c.logger.Error("ws client disconnected", "error", err)
		for _, st := range stations {
			c.mu.Lock()
			if s, ok := c.statuses[st.stationID]; ok {
				s.Connected = false
			}
			c.mu.Unlock()
			c.recordError(st.stationID, err)
		}
	}

	// Wait for subscribe goroutine to finish before returning.
	subWg.Wait()
}

func (c *Collector) recordError(stationID int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.statuses[stationID]
	if !ok {
		return
	}
	s.ErrorCount++
	s.LastError = err.Error()
}

// obsHandler implements tempest.MessageHandler.
type obsHandler struct {
	collector *Collector
}

func (h *obsHandler) HandleObservation(obs *tempest.Observation) {
	defer func() {
		if r := recover(); r != nil {
			h.collector.logger.Error("panic in observation handler",
				"error", r, "station_id", obs.StationID)
		}
	}()

	// Map DeviceID to StationID for WS messages that only set DeviceID.
	if obs.StationID == 0 && obs.DeviceID != 0 {
		h.collector.mu.RLock()
		if sid, ok := h.collector.deviceToStation[obs.DeviceID]; ok {
			obs.StationID = sid
		}
		h.collector.mu.RUnlock()
	}

	if obs.StationID == 0 {
		h.collector.logger.Warn("dropping observation with unknown station",
			"device_id", obs.DeviceID)
		return
	}

	// Compute derived metrics before saving.
	obs.DewPoint = tempest.DewPoint(obs.AirTemperature, obs.RelativeHumidity)
	obs.FeelsLike = tempest.FeelsLike(obs.AirTemperature, obs.RelativeHumidity, obs.WindAvg)
	obs.WetBulb = tempest.WetBulb(obs.AirTemperature, obs.RelativeHumidity)

	parentCtx := h.collector.ctx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	if err := h.collector.store.SaveObservation(ctx, obs); err != nil {
		h.collector.logger.Error("failed to save observation", "station_id", obs.StationID, "error", err)
		h.collector.recordError(obs.StationID, err)
		return
	}

	h.collector.mu.Lock()
	if s, ok := h.collector.statuses[obs.StationID]; ok {
		s.LastObsAt = obs.Timestamp
		s.Connected = true
	}
	h.collector.mu.Unlock()

	h.collector.logger.Info("saved observation",
		"station_id", obs.StationID,
		"timestamp", obs.Timestamp.Format(time.RFC3339),
		"temp", fmt.Sprintf("%.1f°C", obs.AirTemperature),
	)
}

func (h *obsHandler) HandleLightningStrike(evt *tempest.LightningStrike) {
	h.collector.logger.Debug("lightning strike", "distance_km", evt.Distance, "energy", evt.Energy)
}

func (h *obsHandler) HandleRainStart(evt *tempest.RainStart) {
	h.collector.logger.Debug("rain start", "timestamp", evt.Timestamp)
}

func (h *obsHandler) HandleRapidWind(evt *tempest.RapidWind) {
	// Rapid wind events are high frequency (3s); don't persist.
	h.collector.logger.Debug("rapid wind", "speed", evt.WindSpeed, "direction", evt.WindDirection)
}
