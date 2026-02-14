package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:    "no stations",
			config:  Config{ListenAddr: ":8080", Storage: StorageConfig{Driver: "sqlite", SQLite: SQLiteConfig{Path: "/tmp/test.db"}}},
			wantErr: true,
		},
		{
			name: "missing station_id",
			config: Config{
				ListenAddr: ":8080",
				Stations:   []StationConfig{{Token: "tok", DeviceID: 1}},
				Storage:    StorageConfig{Driver: "sqlite", SQLite: SQLiteConfig{Path: "/tmp/test.db"}},
			},
			wantErr: true,
		},
		{
			name: "missing token",
			config: Config{
				ListenAddr: ":8080",
				Stations:   []StationConfig{{StationID: 1, DeviceID: 1}},
				Storage:    StorageConfig{Driver: "sqlite", SQLite: SQLiteConfig{Path: "/tmp/test.db"}},
			},
			wantErr: true,
		},
		{
			name: "missing device_id",
			config: Config{
				ListenAddr: ":8080",
				Stations:   []StationConfig{{StationID: 1, Token: "tok"}},
				Storage:    StorageConfig{Driver: "sqlite", SQLite: SQLiteConfig{Path: "/tmp/test.db"}},
			},
			wantErr: true,
		},
		{
			name: "invalid driver",
			config: Config{
				ListenAddr: ":8080",
				Stations:   []StationConfig{{StationID: 1, Token: "tok", DeviceID: 1}},
				Storage:    StorageConfig{Driver: "mysql"},
			},
			wantErr: true,
		},
		{
			name: "sqlite missing path",
			config: Config{
				ListenAddr: ":8080",
				Stations:   []StationConfig{{StationID: 1, Token: "tok", DeviceID: 1}},
				Storage:    StorageConfig{Driver: "sqlite"},
			},
			wantErr: true,
		},
		{
			name: "postgres missing dsn",
			config: Config{
				ListenAddr: ":8080",
				Stations:   []StationConfig{{StationID: 1, Token: "tok", DeviceID: 1}},
				Storage:    StorageConfig{Driver: "postgres"},
			},
			wantErr: true,
		},
		{
			name: "valid sqlite config",
			config: Config{
				ListenAddr: ":8080",
				Stations:   []StationConfig{{StationID: 1, Token: "tok", DeviceID: 1}},
				Storage:    StorageConfig{Driver: "sqlite", SQLite: SQLiteConfig{Path: "test.db"}},
			},
			wantErr: false,
		},
		{
			name: "valid postgres config",
			config: Config{
				ListenAddr: ":8080",
				Stations:   []StationConfig{{StationID: 1, Token: "tok", DeviceID: 1}},
				Storage:    StorageConfig{Driver: "postgres", Postgres: PostgresConfig{DSN: "postgres://localhost/db"}},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConfig_ValidateSQLiteDirCheck(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		ListenAddr: ":8080",
		Stations:   []StationConfig{{StationID: 1, Token: "tok", DeviceID: 1}},
		Storage:    StorageConfig{Driver: "sqlite", SQLite: SQLiteConfig{Path: filepath.Join(dir, "test.db")}},
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid dir should not error: %v", err)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for missing config file")
	}
}

func TestLoad_ValidFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	content := `
listen_addr: ":9090"
log_format: text

stations:
  - station_id: 1001
    token: "test-token"
    device_id: 2001
    name: "Test"

storage:
  driver: sqlite
  sqlite:
    path: test.db
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(cfg.Stations) != 1 {
		t.Errorf("stations = %d, want 1", len(cfg.Stations))
	}
	if cfg.Stations[0].StationID != 1001 {
		t.Errorf("station_id = %d, want 1001", cfg.Stations[0].StationID)
	}
	if cfg.ListenAddr != ":9090" {
		t.Errorf("listen_addr = %q, want %q", cfg.ListenAddr, ":9090")
	}
	if cfg.Stations[0].DeviceID != 2001 {
		t.Errorf("device_id = %d, want 2001", cfg.Stations[0].DeviceID)
	}
}

func TestLoad_EnvVarTokenInjection(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	// Config without token — token comes from env var.
	content := `
listen_addr: ":9090"
stations:
  - station_id: 1001
    token: "placeholder"
    device_id: 2001
    name: "Test"
storage:
  driver: sqlite
  sqlite:
    path: test.db
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("TEMPESTD_STATIONS_0_TOKEN", "secret-from-env")

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Stations[0].Token != "secret-from-env" {
		t.Errorf("token = %q, want %q", cfg.Stations[0].Token, "secret-from-env")
	}
}

func TestConfig_DSN(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		cfg := Config{Storage: StorageConfig{Driver: "sqlite", SQLite: SQLiteConfig{Path: "/tmp/test.db"}}}
		if dsn := cfg.DSN(); dsn != "/tmp/test.db" {
			t.Errorf("DSN() = %q, want %q", dsn, "/tmp/test.db")
		}
	})

	t.Run("postgres", func(t *testing.T) {
		cfg := Config{Storage: StorageConfig{Driver: "postgres", Postgres: PostgresConfig{DSN: "postgres://localhost/db"}}}
		if dsn := cfg.DSN(); dsn != "postgres://localhost/db" {
			t.Errorf("DSN() = %q, want %q", dsn, "postgres://localhost/db")
		}
	})
}
