package cmd

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/chadmayfield/tempestd/internal/config"
	"github.com/chadmayfield/tempestd/internal/store"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
)

var dryRun bool

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run database migrations",
	RunE:  runMigrate,
}

func init() {
	migrateCmd.Flags().BoolVar(&dryRun, "dry-run", false, "show pending migrations without applying")
	rootCmd.AddCommand(migrateCmd)
}

// dbOpener is satisfied by both SQLiteStore and PostgresStore.
type dbOpener interface {
	DB() *sql.DB
	Close() error
}

func runMigrate(cmd *cobra.Command, args []string) error {
	setupLogging()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return err
	}

	if dryRun {
		slog.Info("dry run mode — showing pending migrations")
		return showPendingMigrations(cfg)
	}

	// Opening the store automatically runs migrations.
	var s dbOpener
	switch cfg.Storage.Driver {
	case "sqlite":
		s, err = store.NewSQLiteStore(cfg.DSN())
	case "postgres":
		s, err = store.NewPostgresStore(cfg.DSN())
	default:
		return fmt.Errorf("unknown storage driver: %s", cfg.Storage.Driver)
	}
	if err != nil {
		return err
	}
	defer s.Close()

	slog.Info("migrations complete")
	return nil
}

func showPendingMigrations(cfg *config.Config) error {
	var db *sql.DB
	var err error
	var dialect string

	switch cfg.Storage.Driver {
	case "sqlite":
		db, err = sql.Open("sqlite", cfg.DSN())
		dialect = "sqlite3"
	case "postgres":
		db, err = sql.Open("pgx", cfg.DSN())
		dialect = "postgres"
	default:
		return fmt.Errorf("unknown storage driver: %s", cfg.Storage.Driver)
	}
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	if err := goose.SetDialect(dialect); err != nil {
		return err
	}

	current, err := goose.GetDBVersion(db)
	if err != nil {
		current = 0
	}

	slog.Info("migration status", "current_version", current, "driver", cfg.Storage.Driver)
	return nil
}
