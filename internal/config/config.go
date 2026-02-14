package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

// Config is the top-level configuration for tempestd.
type Config struct {
	ListenAddr string          `mapstructure:"listen_addr"`
	LogFormat  string          `mapstructure:"log_format"`
	Storage    StorageConfig   `mapstructure:"storage"`
	Stations   []StationConfig `mapstructure:"stations"`
	Collection CollectionConfig `mapstructure:"collection"`
}

// StationConfig defines a station to collect data from.
type StationConfig struct {
	Token     string `mapstructure:"token"`
	StationID int    `mapstructure:"station_id"`
	DeviceID  int    `mapstructure:"device_id"`
	Name      string `mapstructure:"name"`
}

// StorageConfig defines the database backend.
type StorageConfig struct {
	Driver   string         `mapstructure:"driver"` // "sqlite" or "postgres"
	SQLite   SQLiteConfig   `mapstructure:"sqlite"`
	Postgres PostgresConfig `mapstructure:"postgres"`
}

// SQLiteConfig holds SQLite-specific configuration.
type SQLiteConfig struct {
	Path string `mapstructure:"path"`
}

// PostgresConfig holds PostgreSQL-specific configuration.
type PostgresConfig struct {
	DSN string `mapstructure:"dsn"`
}

// CollectionConfig defines collection behavior.
type CollectionConfig struct {
	BackfillOnStartup bool `mapstructure:"backfill_on_startup"`
	BackfillMaxDays   int  `mapstructure:"backfill_max_days"`
}

// Load reads configuration from flag path, env vars, then default file paths.
// Precedence: flag → $TEMPESTD_CONFIG env → ~/.config/tempestd/config.yaml → /etc/tempestd/config.yaml
func Load(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	// Defaults
	v.SetDefault("listen_addr", ":8080")
	v.SetDefault("log_format", "json")
	v.SetDefault("storage.driver", "sqlite")
	v.SetDefault("collection.backfill_on_startup", true)
	v.SetDefault("collection.backfill_max_days", 30)

	// Env var support
	v.SetEnvPrefix("TEMPESTD")
	v.AutomaticEnv()

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else if envPath := os.Getenv("TEMPESTD_CONFIG"); envPath != "" {
		v.SetConfigFile(envPath)
	} else {
		// Try ~/.config/tempestd/config.yaml first
		if home, err := os.UserHomeDir(); err == nil {
			v.AddConfigPath(filepath.Join(home, ".config", "tempestd"))
		}
		// Fall back to /etc/tempestd/config.yaml
		v.AddConfigPath("/etc/tempestd")
		v.SetConfigName("config")
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("reading config: %w", err)
		}
	} else {
		// Warn if config file is world-readable.
		if cfgPath := v.ConfigFileUsed(); cfgPath != "" {
			if info, err := os.Stat(cfgPath); err == nil {
				perm := info.Mode().Perm()
				if perm&0004 != 0 {
					slog.Warn("config file is world-readable", "path", cfgPath, "permissions", fmt.Sprintf("%04o", perm))
				}
			}
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	// Inject station tokens from env vars (TEMPESTD_STATIONS_0_TOKEN, etc.).
	// Viper's AutomaticEnv cannot map env vars to array element fields,
	// so we handle this explicitly for K8s secret injection.
	for i := range cfg.Stations {
		envKey := fmt.Sprintf("TEMPESTD_STATIONS_%d_TOKEN", i)
		if tok := os.Getenv(envKey); tok != "" {
			cfg.Stations[i].Token = tok
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

// Validate checks that the configuration is complete and correct.
func (c *Config) Validate() error {
	if len(c.Stations) == 0 {
		return fmt.Errorf("at least one station is required")
	}

	for i, s := range c.Stations {
		if s.StationID == 0 {
			return fmt.Errorf("station[%d]: station_id is required", i)
		}
		if s.Token == "" {
			return fmt.Errorf("station[%d]: token is required", i)
		}
		if s.DeviceID == 0 {
			return fmt.Errorf("station[%d]: device_id is required", i)
		}
	}

	switch c.Storage.Driver {
	case "sqlite":
		if c.Storage.SQLite.Path == "" {
			return fmt.Errorf("storage.sqlite.path is required for sqlite driver")
		}
		dir := filepath.Dir(c.Storage.SQLite.Path)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0700); err != nil {
				return fmt.Errorf("creating storage directory %q: %w", dir, err)
			}
		}
	case "postgres":
		if c.Storage.Postgres.DSN == "" {
			return fmt.Errorf("storage.postgres.dsn is required for postgres driver")
		}
	default:
		return fmt.Errorf("storage.driver must be 'sqlite' or 'postgres', got %q", c.Storage.Driver)
	}

	// Validate listen_addr.
	if _, _, err := net.SplitHostPort(c.ListenAddr); err != nil {
		return fmt.Errorf("listen_addr %q is not a valid address: %w", c.ListenAddr, err)
	}

	return nil
}

// DSN returns the appropriate DSN for the configured storage driver.
func (c *Config) DSN() string {
	switch c.Storage.Driver {
	case "sqlite":
		return c.Storage.SQLite.Path
	case "postgres":
		return c.Storage.Postgres.DSN
	default:
		return ""
	}
}
