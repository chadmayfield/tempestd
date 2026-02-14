package cmd

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	tempest "github.com/chadmayfield/tempest-go"
	"github.com/chadmayfield/tempestd/internal/api"
	"github.com/chadmayfield/tempestd/internal/collector"
	"github.com/chadmayfield/tempestd/internal/config"
	"github.com/chadmayfield/tempestd/internal/store"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var (
	listenAddr    string
	storageDriver string
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the tempestd daemon (default command)",
	RunE:  runServe,
}

func init() {
	serveCmd.Flags().StringVar(&listenAddr, "listen", "", "HTTP listen address (overrides config)")
	serveCmd.Flags().StringVar(&storageDriver, "storage-driver", "", "storage driver (overrides config)")
	rootCmd.AddCommand(serveCmd)

	// Make serve the default command.
	rootCmd.RunE = runServe
}

func runServe(cmd *cobra.Command, args []string) error {
	setupLogging()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return err
	}

	// Apply flag overrides.
	if listenAddr != "" {
		cfg.ListenAddr = listenAddr
	}
	if storageDriver != "" {
		cfg.Storage.Driver = storageDriver
	}

	slog.Info("starting tempestd",
		"listen_addr", cfg.ListenAddr,
		"storage_driver", cfg.Storage.Driver,
		"stations", len(cfg.Stations),
	)

	// Open store.
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
	defer s.Close() //nolint:errcheck

	slog.Info("database ready", "driver", cfg.Storage.Driver)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Save/update station records in the database.
	for _, st := range cfg.Stations {
		now := time.Now().UTC()
		if err := s.SaveStation(ctx, &store.Station{
			ID:        st.StationID,
			DeviceID:  st.DeviceID,
			Name:      st.Name,
			CreatedAt: now,
			UpdatedAt: now,
		}); err != nil {
			slog.Error("failed to save station", "station_id", st.StationID, "error", err)
		}
	}

	// Backfill gaps on startup.
	if cfg.Collection.BackfillOnStartup {
		bf := collector.NewBackfiller(s, slog.Default())
		if err := bf.DetectAndFill(ctx, cfg.Stations, cfg.Collection.BackfillMaxDays); err != nil {
			slog.Error("backfill failed", "error", err)
		}
	}

	// Create connection manager and collector.
	connMgr := tempest.NewConnectionManager()
	coll := collector.NewCollector(s, connMgr, slog.Default())
	for _, st := range cfg.Stations {
		coll.AddStation(st.Token, st.StationID, st.DeviceID, st.Name)
	}

	// Create API server.
	srv := api.NewServer(s, coll, slog.Default())
	srv.SetVersion(Version)
	storagePath := cfg.DSN()
	if cfg.Storage.Driver == "postgres" {
		storagePath = redactDSN(storagePath)
	}
	srv.SetStorageInfo(cfg.Storage.Driver, storagePath)

	slog.Info("tempestd ready", "addr", cfg.ListenAddr)

	// Start collector and server using errgroup.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return coll.Start(gctx) })
	g.Go(func() error { return srv.ListenAndServe(gctx, cfg.ListenAddr) })

	waitErr := g.Wait()
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		slog.Error("tempestd exited with error", "error", waitErr)
	}

	// Always run graceful cleanup, even on error.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	_ = srv.Shutdown(shutdownCtx)
	_ = connMgr.Shutdown(shutdownCtx)
	_ = s.Close()

	slog.Info("tempestd shutdown complete")
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		return waitErr
	}
	return nil
}

func setupLogging() {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}

	if logFormat == "text" {
		handler = slog.NewTextHandler(os.Stderr, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(handler))
}

// redactDSN masks the password in a PostgreSQL DSN for safe display.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	if u.User != nil {
		u.User = url.UserPassword(u.User.Username(), "***")
	}
	return u.String()
}
