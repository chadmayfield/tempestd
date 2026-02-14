package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/chadmayfield/tempestd/internal/collector"
	"github.com/chadmayfield/tempestd/internal/config"
	"github.com/chadmayfield/tempestd/internal/store"
	"github.com/spf13/cobra"
)

var (
	bfStation int
	bfFrom    string
	bfTo      string
)

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "Manually backfill observation data from the REST API",
	RunE:  runBackfill,
}

func init() {
	backfillCmd.Flags().IntVar(&bfStation, "station", 0, "station ID to backfill")
	backfillCmd.Flags().StringVar(&bfFrom, "from", "", "start date (YYYY-MM-DD)")
	backfillCmd.Flags().StringVar(&bfTo, "to", "", "end date (YYYY-MM-DD, default: now)")
	_ = backfillCmd.MarkFlagRequired("station")
	_ = backfillCmd.MarkFlagRequired("from")
	rootCmd.AddCommand(backfillCmd)
}

func runBackfill(cmd *cobra.Command, args []string) error {
	setupLogging()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return err
	}

	var s store.Store
	switch cfg.Storage.Driver {
	case "sqlite":
		s, err = store.NewSQLiteStore(cfg.DSN())
	case "postgres":
		s, err = store.NewPostgresStore(cfg.DSN())
	}
	if err != nil {
		return err
	}
	defer s.Close()

	// Find the station in config.
	var station *config.StationConfig
	for _, st := range cfg.Stations {
		if st.StationID == bfStation {
			station = &st
			break
		}
	}
	if station == nil {
		return fmt.Errorf("station %d not found in config", bfStation)
	}

	from, err := time.Parse(time.DateOnly, bfFrom)
	if err != nil {
		return fmt.Errorf("invalid --from date: %w", err)
	}

	to := time.Now().UTC()
	if bfTo != "" {
		to, err = time.Parse(time.DateOnly, bfTo)
		if err != nil {
			return fmt.Errorf("invalid --to date: %w", err)
		}
	}

	if !from.Before(to) {
		return fmt.Errorf("--from date must be before --to date")
	}

	// Support context cancellation via signals.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	bf := collector.NewBackfiller(s, slog.Default())

	slog.Info("backfilling station",
		"station_id", station.StationID,
		"device_id", station.DeviceID,
		"from", from.Format(time.DateOnly),
		"to", to.Format(time.DateOnly),
	)

	return bf.BackfillStation(ctx, station.Token, station.StationID, station.DeviceID, from, to)
}
