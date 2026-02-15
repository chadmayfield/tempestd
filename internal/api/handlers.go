package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
	"github.com/chadmayfield/tempestd/internal/collector"
	"github.com/chadmayfield/tempestd/internal/store"
)

// Handlers holds dependencies for HTTP handlers.
type Handlers struct {
	Store         store.Store
	Collector     *collector.Collector
	Logger        *slog.Logger
	StartTime     time.Time
	StorageDriver string
	StoragePath   string
	Version       string
}

// apiError is a JSON error response.
type apiError struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("failed to encode JSON response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, apiError{Error: msg, Code: status})
}

func parseStationID(r *http.Request) (int, bool) {
	s := r.PathValue("station_id")
	id, err := strconv.Atoi(s)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func parseUnits(r *http.Request) tempest.UnitSystem {
	if r.URL.Query().Get("units") == "imperial" {
		return tempest.Imperial
	}
	return tempest.Metric
}

func unitSystemString(u tempest.UnitSystem) string {
	if u == tempest.Imperial {
		return "imperial"
	}
	return "metric"
}

func parseTime(s string) (time.Time, error) {
	// Try RFC3339 first, then YYYY-MM-DD, then Unix epoch.
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.DateOnly, s); err == nil {
		return t, nil
	}
	if epoch, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(epoch, 0).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid time format: %q (expected RFC3339, YYYY-MM-DD, or Unix epoch)", s)
}

func parseResolution(s string) time.Duration {
	switch s {
	case "5m":
		return 5 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "3h":
		return 3 * time.Hour
	default:
		return time.Minute
	}
}

func formatUptime(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}

// ListStations handles GET /api/v1/stations
func (h *Handlers) ListStations(w http.ResponseWriter, r *http.Request) {
	stations, err := h.Store.GetStations(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list stations")
		return
	}

	type stationResponse struct {
		StationID       int        `json:"station_id"`
		DeviceID        int        `json:"device_id"`
		Name            string     `json:"name"`
		Latitude        float64    `json:"latitude"`
		Longitude       float64    `json:"longitude"`
		Elevation       float64    `json:"elevation"`
		Status          string     `json:"status"`
		LastObservation *time.Time `json:"last_observation,omitempty"`
	}

	result := make([]stationResponse, 0, len(stations))
	for _, st := range stations {
		sr := stationResponse{
			StationID: st.ID,
			DeviceID:  st.DeviceID,
			Name:      st.Name,
			Latitude:  st.Latitude,
			Longitude: st.Longitude,
			Elevation: st.Elevation,
			Status:    "unknown",
		}

		// Check collector status for online/offline.
		if h.Collector != nil {
			for _, cs := range h.Collector.Status() {
				if cs.StationID == st.ID {
					if cs.Connected {
						sr.Status = "online"
					} else {
						sr.Status = "offline"
					}
					if !cs.LastObsAt.IsZero() {
						t := cs.LastObsAt
						sr.LastObservation = &t
					}
					break
				}
			}
		}

		// If no collector status, try to get last observation from store.
		if sr.LastObservation == nil {
			if obs, err := h.Store.GetLatest(r.Context(), st.ID); err == nil && obs != nil {
				sr.LastObservation = &obs.Timestamp
			}
		}

		result = append(result, sr)
	}

	writeJSON(w, http.StatusOK, result)
}

// GetStation handles GET /api/v1/stations/{station_id}
func (h *Handlers) GetStation(w http.ResponseWriter, r *http.Request) {
	id, ok := parseStationID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid station_id")
		return
	}

	stations, err := h.Store.GetStations(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get station")
		return
	}

	for _, st := range stations {
		if st.ID == id {
			writeJSON(w, http.StatusOK, stationToMap(&st))
			return
		}
	}

	writeError(w, http.StatusNotFound, "station not found")
}

// GetCurrentObservation handles GET /api/v1/stations/{station_id}/current
func (h *Handlers) GetCurrentObservation(w http.ResponseWriter, r *http.Request) {
	id, ok := parseStationID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid station_id")
		return
	}

	obs, err := h.Store.GetLatest(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get observation")
		return
	}
	if obs == nil {
		writeError(w, http.StatusNotFound, "no observations found")
		return
	}

	units := parseUnits(r)
	converted := convertObs(obs, units)

	// Build response map with snake_case keys plus extra fields.
	resp := obsToMap(converted)
	resp["wind_direction_cardinal"] = tempest.WindDirectionToCompass(obs.WindDirection)
	resp["units"] = unitSystemString(units)

	writeJSON(w, http.StatusOK, resp)
}

// GetObservations handles GET /api/v1/stations/{station_id}/observations
func (h *Handlers) GetObservations(w http.ResponseWriter, r *http.Request) {
	id, ok := parseStationID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid station_id")
		return
	}

	q := r.URL.Query()
	startStr := q.Get("start")
	if startStr == "" {
		writeError(w, http.StatusBadRequest, "missing 'start' parameter")
		return
	}
	start, err := parseTime(startStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid 'start' parameter (RFC3339 or YYYY-MM-DD)")
		return
	}

	endStr := q.Get("end")
	if endStr == "" {
		writeError(w, http.StatusBadRequest, "missing 'end' parameter")
		return
	}
	end, err := parseTime(endStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid 'end' parameter (RFC3339 or YYYY-MM-DD)")
		return
	}

	if !start.Before(end) {
		writeError(w, http.StatusBadRequest, "'start' must be before 'end'")
		return
	}

	resolutionStr := q.Get("resolution")
	if resolutionStr == "" {
		resolutionStr = "1m"
	}
	resolution := parseResolution(resolutionStr)

	units := parseUnits(r)

	// Parse limit and offset.
	limit := 1000
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 10000 {
			limit = n
		}
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	obs, err := h.Store.GetObservations(r.Context(), id, start, end, resolution)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get observations")
		return
	}

	total := len(obs)

	// Apply limit/offset.
	if offset > 0 && offset < len(obs) {
		obs = obs[offset:]
	} else if offset >= len(obs) {
		obs = nil
	}
	if limit > 0 && limit < len(obs) {
		obs = obs[:limit]
	}

	// Convert units and map to snake_case.
	result := make([]map[string]any, len(obs))
	for i := range obs {
		result[i] = obsToMap(convertObs(&obs[i], units))
	}

	// Envelope response.
	type obsResponse struct {
		StationID    int              `json:"station_id"`
		Start        string           `json:"start"`
		End          string           `json:"end"`
		Resolution   string           `json:"resolution"`
		Units        string           `json:"units"`
		Total        int              `json:"total"`
		Limit        int              `json:"limit"`
		Offset       int              `json:"offset"`
		Observations []map[string]any `json:"observations"`
	}

	writeJSON(w, http.StatusOK, obsResponse{
		StationID:    id,
		Start:        start.Format(time.RFC3339),
		End:          end.Format(time.RFC3339),
		Resolution:   resolutionStr,
		Units:        unitSystemString(units),
		Total:        total,
		Limit:        limit,
		Offset:       offset,
		Observations: result,
	})
}

// GetDailySummary handles GET /api/v1/stations/{station_id}/summary
func (h *Handlers) GetDailySummary(w http.ResponseWriter, r *http.Request) {
	id, ok := parseStationID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid station_id")
		return
	}

	dateStr := r.URL.Query().Get("date")
	if dateStr == "" {
		writeError(w, http.StatusBadRequest, "missing 'date' parameter (YYYY-MM-DD)")
		return
	}
	date, err := time.Parse(time.DateOnly, dateStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid 'date' parameter (YYYY-MM-DD)")
		return
	}

	units := parseUnits(r)

	summary, err := h.Store.GetDailySummary(r.Context(), id, date)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get daily summary")
		return
	}
	if summary == nil {
		writeError(w, http.StatusNotFound, "no data for this date")
		return
	}

	// Build nested response per spec.
	type tempRange struct {
		High float64 `json:"high"`
		Low  float64 `json:"low"`
		Avg  float64 `json:"avg"`
	}
	type humRange struct {
		High float64 `json:"high"`
		Low  float64 `json:"low"`
		Avg  float64 `json:"avg"`
	}
	type windRange struct {
		Max float64 `json:"max"`
		Avg float64 `json:"avg"`
	}
	type pressRange struct {
		High float64 `json:"high"`
		Low  float64 `json:"low"`
	}
	type summaryResponse struct {
		StationID        int       `json:"station_id"`
		Date             string    `json:"date"`
		Units            string    `json:"units"`
		Temperature      tempRange `json:"temperature"`
		Humidity         humRange  `json:"humidity"`
		Wind             windRange `json:"wind"`
		Pressure         pressRange `json:"pressure"`
		RainTotal        float64   `json:"rain_total"`
		UVMax            float64   `json:"uv_max"`
		SolarRadiationMax float64  `json:"solar_radiation_max"`
		LightningTotal   int       `json:"lightning_total"`
		ObservationCount int       `json:"observation_count"`
	}

	// Convert summary values if imperial.
	tempHigh, tempLow, tempAvg := summary.TempHigh, summary.TempLow, summary.TempAvg
	windMax, windAvg := summary.WindMax, summary.WindAvg
	pressHigh, pressLow := summary.PressureHigh, summary.PressureLow
	rainTotal := summary.RainTotal

	if units == tempest.Imperial {
		// Temperature: C → F
		tempHigh = tempHigh*9.0/5.0 + 32
		tempLow = tempLow*9.0/5.0 + 32
		tempAvg = tempAvg*9.0/5.0 + 32
		// Wind: m/s → mph
		windMax = windMax * 2.23694
		windAvg = windAvg * 2.23694
		// Pressure: mb → inHg
		pressHigh = pressHigh * 0.02953
		pressLow = pressLow * 0.02953
		// Rain: mm → in
		rainTotal = rainTotal * 0.03937
	}

	writeJSON(w, http.StatusOK, summaryResponse{
		StationID: id,
		Date:      dateStr,
		Units:     unitSystemString(units),
		Temperature: tempRange{
			High: tempHigh,
			Low:  tempLow,
			Avg:  tempAvg,
		},
		Humidity: humRange{
			High: summary.HumidityHigh,
			Low:  summary.HumidityLow,
			Avg:  summary.HumidityAvg,
		},
		Wind: windRange{
			Max: windMax,
			Avg: windAvg,
		},
		Pressure: pressRange{
			High: pressHigh,
			Low:  pressLow,
		},
		RainTotal:         rainTotal,
		UVMax:             summary.UVMax,
		SolarRadiationMax: summary.SolarRadMax,
		LightningTotal:    summary.LightningTotal,
		ObservationCount:  summary.ObservationCount,
	})
}

// GetObservationRange handles GET /api/v1/stations/{station_id}/range
func (h *Handlers) GetObservationRange(w http.ResponseWriter, r *http.Request) {
	id, ok := parseStationID(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid station_id")
		return
	}

	oldest, newest, err := h.Store.GetDataRange(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to query data range")
		return
	}

	type rangeResponse struct {
		StationID         int        `json:"station_id"`
		Oldest            *time.Time `json:"oldest"`
		Newest            *time.Time `json:"newest"`
		TotalObservations int        `json:"total_observations"`
	}

	resp := rangeResponse{StationID: id}
	if !oldest.IsZero() {
		resp.Oldest = &oldest
	}
	if !newest.IsZero() {
		resp.Newest = &newest
	}

	// Count total observations.
	if !oldest.IsZero() {
		if count, err := h.Store.GetObservationCount(r.Context(), id); err == nil {
			resp.TotalObservations = count
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

// Health handles GET /api/v1/health
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	type stationHealth struct {
		StationID             int     `json:"station_id"`
		Name                  string  `json:"name"`
		WebSocket             string  `json:"websocket"`
		LastObservation       string  `json:"last_observation,omitempty"`
		ObservationAgeSeconds float64 `json:"observation_age_seconds,omitempty"`
		DataRangeOldest       string  `json:"data_range_oldest,omitempty"`
		DataRangeNewest       string  `json:"data_range_newest,omitempty"`
	}
	type dbHealth struct {
		Driver            string `json:"driver"`
		Path              string `json:"path,omitempty"`
		Status            string `json:"status"`
		SizeBytes         int64  `json:"size_bytes,omitempty"`
		TotalObservations int    `json:"total_observations"`
	}
	type healthResponse struct {
		Status   string          `json:"status"`
		Version  string          `json:"version"`
		Uptime   string          `json:"uptime"`
		Stations []stationHealth `json:"stations"`
		Database dbHealth        `json:"database"`
	}

	resp := healthResponse{
		Status:  "healthy",
		Version: h.Version,
		Uptime:  formatUptime(time.Since(h.StartTime)),
	}

	// Build a map of data ranges per station for enrichment.
	type dataRange struct {
		oldest, newest time.Time
		count          int
	}
	dataRanges := make(map[int]dataRange)
	if stations, err := h.Store.GetStations(r.Context()); err == nil {
		for _, st := range stations {
			oldest, newest, err := h.Store.GetDataRange(r.Context(), st.ID)
			if err == nil && !oldest.IsZero() {
				dr := dataRange{oldest: oldest, newest: newest}
				if count, err := h.Store.GetObservationCount(r.Context(), st.ID); err == nil {
					dr.count = count
				}
				dataRanges[st.ID] = dr
			}
		}
	}

	if h.Collector != nil {
		for _, cs := range h.Collector.Status() {
			sh := stationHealth{
				StationID: cs.StationID,
				Name:      cs.Name,
				WebSocket: "disconnected",
			}
			if cs.Connected {
				sh.WebSocket = "connected"
			}
			if !cs.LastObsAt.IsZero() {
				sh.LastObservation = cs.LastObsAt.Format(time.RFC3339)
				sh.ObservationAgeSeconds = cs.ObservationAgeSeconds
			}
			if dr, ok := dataRanges[cs.StationID]; ok {
				sh.DataRangeOldest = dr.oldest.Format(time.DateOnly)
				sh.DataRangeNewest = dr.newest.Format(time.DateOnly)
			}
			resp.Stations = append(resp.Stations, sh)
		}
	}

	// Database health (path omitted to avoid exposing filesystem details).
	resp.Database = dbHealth{
		Driver: h.StorageDriver,
		Status: "ok",
	}
	if h.StorageDriver == "sqlite" && h.StoragePath != "" {
		if info, err := os.Stat(h.StoragePath); err == nil {
			resp.Database.SizeBytes = info.Size()
		}
	}
	for _, dr := range dataRanges {
		resp.Database.TotalObservations += dr.count
	}

	writeJSON(w, http.StatusOK, resp)
}

func convertObs(obs *tempest.Observation, units tempest.UnitSystem) *tempest.Observation {
	if units == tempest.Imperial {
		return tempest.ConvertObservation(obs, tempest.Imperial)
	}
	return obs
}

// obsToMap converts an Observation to a map with snake_case keys for JSON responses.
// tempest.Observation has no JSON tags, so we must map fields explicitly.
func obsToMap(obs *tempest.Observation) map[string]any {
	return map[string]any{
		"timestamp":              obs.Timestamp,
		"station_id":             obs.StationID,
		"wind_lull":              obs.WindLull,
		"wind_avg":               obs.WindAvg,
		"wind_gust":              obs.WindGust,
		"wind_direction":         obs.WindDirection,
		"station_pressure":       obs.StationPressure,
		"air_temperature":        obs.AirTemperature,
		"relative_humidity":      obs.RelativeHumidity,
		"uv_index":               obs.UVIndex,
		"solar_radiation":        obs.SolarRadiation,
		"rain_accumulation":      obs.RainAccumulation,
		"precipitation_type":     obs.PrecipitationType,
		"lightning_avg_distance": obs.LightningAvgDist,
		"lightning_strike_count": obs.LightningCount,
		"battery":                obs.Battery,
		"feels_like":             obs.FeelsLike,
		"dew_point":              obs.DewPoint,
	}
}

// stationToMap converts a Station to a map with snake_case keys for JSON responses.
func stationToMap(st *store.Station) map[string]any {
	return map[string]any{
		"station_id": st.ID,
		"device_id":  st.DeviceID,
		"name":       st.Name,
		"latitude":   st.Latitude,
		"longitude":  st.Longitude,
		"elevation":  st.Elevation,
		"created_at": st.CreatedAt,
		"updated_at": st.UpdatedAt,
	}
}
