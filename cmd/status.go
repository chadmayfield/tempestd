package cmd

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

var statusServer string

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Query the health endpoint of a running tempestd instance",
	RunE:  runStatus,
}

func init() {
	statusCmd.Flags().StringVar(&statusServer, "server", "http://localhost:8080", "tempestd server URL")
	rootCmd.AddCommand(statusCmd)
}

func runStatus(cmd *cobra.Command, args []string) error {
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
	resp, err := client.Get(statusServer + "/api/v1/health")
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", statusServer, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	var health struct {
		Status   string `json:"status"`
		Version  string `json:"version"`
		Uptime   string `json:"uptime"`
		Stations []struct {
			StationID             int     `json:"station_id"`
			Name                  string  `json:"name"`
			WebSocket             string  `json:"websocket"`
			LastObservation       string  `json:"last_observation"`
			ObservationAgeSeconds float64 `json:"observation_age_seconds"`
			DataRangeOldest       string  `json:"data_range_oldest"`
			DataRangeNewest       string  `json:"data_range_newest"`
		} `json:"stations"`
		Database struct {
			Driver            string `json:"driver"`
			Path              string `json:"path"`
			Status            string `json:"status"`
			SizeBytes         int64  `json:"size_bytes"`
			TotalObservations int    `json:"total_observations"`
		} `json:"database"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 10<<20)).Decode(&health); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}

	// Human-readable output.
	fmt.Printf("tempestd %s\n", health.Version)
	fmt.Printf("Status: %s\n", health.Status)
	fmt.Printf("Uptime: %s\n", health.Uptime)
	fmt.Println()

	if len(health.Stations) > 0 {
		fmt.Println("Stations:")
		for _, s := range health.Stations {
			fmt.Printf("  %s (%d)\n", s.Name, s.StationID)
			fmt.Printf("    WebSocket: %s\n", s.WebSocket)
			if s.LastObservation != "" {
				fmt.Printf("    Last observation: %s (%.0fs ago)\n", s.LastObservation, s.ObservationAgeSeconds)
			}
			if s.DataRangeOldest != "" {
				fmt.Printf("    Data range: %s to %s\n", s.DataRangeOldest, s.DataRangeNewest)
			}
		}
		fmt.Println()
	}

	if health.Database.Path != "" {
		fmt.Printf("Database: %s (%s)\n", health.Database.Driver, health.Database.Path)
	} else {
		fmt.Printf("Database: %s\n", health.Database.Driver)
	}
	if health.Database.SizeBytes > 0 {
		fmt.Printf("  Size: %s\n", formatBytes(health.Database.SizeBytes))
	}
	if health.Database.TotalObservations > 0 {
		fmt.Printf("  Observations: %s\n", formatNumber(health.Database.TotalObservations))
	}

	return nil
}

// formatNumber formats an integer with comma separators (e.g., 1,247,832).
func formatNumber(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func formatBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
