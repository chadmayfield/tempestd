package collector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
	"github.com/chadmayfield/tempestd/internal/config"
	"github.com/chadmayfield/tempestd/internal/store"
)

const (
	chunkDays      = 5
	requestPace    = 2 * time.Second
	defaultMaxDays = 30
)

// Backfiller fills gaps in observation data using the REST API.
type Backfiller struct {
	store       store.Store
	logger      *slog.Logger
	limiter     *tempest.RateLimiter
	breaker     *tempest.CircuitBreaker
	testBaseURL string // For testing only; empty in production.
}

// NewBackfiller creates a new Backfiller with a shared rate limiter (80 req/min)
// and circuit breaker (5 failures, 60s cooldown) for all REST API calls.
func NewBackfiller(s store.Store, logger *slog.Logger) *Backfiller {
	return &Backfiller{
		store:   s,
		logger:  logger,
		limiter: tempest.NewRateLimiter(80),
		breaker: tempest.NewCircuitBreaker(5, 60*time.Second),
	}
}

// BackfillStation fetches observations from the REST API for the given time range
// in 5-day chunks with 1 req/2s pacing, and batch-inserts them.
// It uses the device observations endpoint (which returns raw arrays) rather than
// the station observations endpoint (which returns named-field objects that don't
// match the library's parser).
func (b *Backfiller) BackfillStation(ctx context.Context, token string, stationID, deviceID int, from, to time.Time) error {
	var client *tempest.Client
	var err error
	if b.testBaseURL != "" {
		client, err = tempest.NewClient(token,
			tempest.WithRateLimiter(b.limiter),
			tempest.WithCircuitBreaker(b.breaker),
			tempest.WithBaseURL(b.testBaseURL),
		)
	} else {
		client, err = tempest.NewClient(token,
			tempest.WithRateLimiter(b.limiter),
			tempest.WithCircuitBreaker(b.breaker),
		)
	}
	if err != nil {
		return fmt.Errorf("creating rest client: %w", err)
	}

	// Calculate total chunks for progress logging.
	totalChunks := 0
	for cs := from; cs.Before(to); {
		ce := cs.AddDate(0, 0, chunkDays)
		if ce.After(to) {
			ce = to
		}
		totalChunks++
		cs = ce
	}

	chunkNum := 0
	for chunkStart := from; chunkStart.Before(to); {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		chunkEnd := chunkStart.AddDate(0, 0, chunkDays)
		if chunkEnd.After(to) {
			chunkEnd = to
		}
		chunkNum++

		b.logger.Info("backfilling station",
			"station_id", stationID,
			"from", chunkStart.Format(time.DateOnly),
			"to", chunkEnd.Format(time.DateOnly),
			"chunk", fmt.Sprintf("%d/%d", chunkNum, totalChunks),
		)

		obs, err := client.GetDeviceObservations(ctx, deviceID, chunkStart, chunkEnd)
		if err != nil {
			return fmt.Errorf("fetching observations for %s to %s: %w",
				chunkStart.Format(time.DateOnly), chunkEnd.Format(time.DateOnly), err)
		}

		// GetDeviceObservations sets DeviceID but not StationID; fill it in.
		// Also compute derived metrics for each observation.
		for i := range obs {
			obs[i].StationID = stationID
			obs[i].DewPoint = tempest.DewPoint(obs[i].AirTemperature, obs[i].RelativeHumidity)
			obs[i].FeelsLike = tempest.FeelsLike(obs[i].AirTemperature, obs[i].RelativeHumidity, obs[i].WindAvg)
			obs[i].WetBulb = tempest.WetBulb(obs[i].AirTemperature, obs[i].RelativeHumidity)
		}

		if len(obs) > 0 {
			if err := b.store.SaveObservations(ctx, obs); err != nil {
				return fmt.Errorf("saving observations: %w", err)
			}
			b.logger.Info("backfilled chunk",
				"station_id", stationID,
				"observations", len(obs),
				"chunk", fmt.Sprintf("%d/%d", chunkNum, totalChunks),
			)
		}

		chunkStart = chunkEnd

		// Pace requests to avoid rate limiting.
		if chunkStart.Before(to) {
			ticker := time.NewTicker(requestPace)
			select {
			case <-ctx.Done():
				ticker.Stop()
				return ctx.Err()
			case <-ticker.C:
			}
			ticker.Stop()
		}
	}

	b.logger.Info("backfill complete", "station_id", stationID)
	return nil
}

// DetectAndFill finds gaps in stored data and fills them automatically.
// maxDays limits how far back to look.
func (b *Backfiller) DetectAndFill(ctx context.Context, stations []config.StationConfig, maxDays int) error {
	if maxDays <= 0 {
		maxDays = defaultMaxDays
	}

	for _, st := range stations {
		if err := b.detectAndFillStation(ctx, st.Token, st.StationID, st.DeviceID, maxDays); err != nil {
			b.logger.Error("backfill failed", "station_id", st.StationID, "error", err)
		}
	}
	return nil
}

func (b *Backfiller) detectAndFillStation(ctx context.Context, token string, stationID, deviceID int, maxDays int) error {
	latest, err := b.store.GetLatest(ctx, stationID)
	if err != nil {
		return fmt.Errorf("getting latest observation: %w", err)
	}

	now := time.Now().UTC()
	var from time.Time

	if latest == nil {
		// No data at all — backfill maxDays.
		from = now.AddDate(0, 0, -maxDays)
		b.logger.Info("no existing data, backfilling from scratch",
			"station_id", stationID,
			"days", maxDays,
		)
	} else if now.Sub(latest.Timestamp) > 2*time.Minute {
		from = latest.Timestamp
		b.logger.Info("gap detected, backfilling",
			"station_id", stationID,
			"gap_since", latest.Timestamp.Format(time.RFC3339),
		)
	} else {
		b.logger.Info("data is current, no backfill needed", "station_id", stationID)
		return nil
	}

	return b.BackfillStation(ctx, token, stationID, deviceID, from, now)
}
