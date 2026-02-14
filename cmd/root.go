package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	cfgFile   string
	logFormat string
)

var rootCmd = &cobra.Command{
	Use:   "tempestd",
	Short: "Data collection daemon for WeatherFlow Tempest weather stations",
	Long: `tempestd maintains persistent WebSocket connections to WeatherFlow Tempest
weather stations, stores 1-minute resolution observations in SQLite or PostgreSQL,
auto-backfills data gaps via the REST API, and exposes a REST API for querying
historical and real-time weather data.`,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file path")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "json", "log format (text or json)")
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
