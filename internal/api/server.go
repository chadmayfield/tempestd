package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/chadmayfield/tempestd/internal/collector"
	"github.com/chadmayfield/tempestd/internal/store"
)

// Server is the REST API server.
type Server struct {
	httpServer *http.Server
	handlers   *Handlers
}

// NewServer creates a new API server with all routes registered.
func NewServer(s store.Store, c *collector.Collector, logger *slog.Logger) *Server {
	h := &Handlers{
		Store:     s,
		Collector: c,
		Logger:    logger,
		StartTime: time.Now(),
	}

	mux := http.NewServeMux()

	// API routes.
	mux.HandleFunc("GET /api/v1/stations", h.ListStations)
	mux.HandleFunc("GET /api/v1/stations/{station_id}", h.GetStation)
	mux.HandleFunc("GET /api/v1/stations/{station_id}/current", h.GetCurrentObservation)
	mux.HandleFunc("GET /api/v1/stations/{station_id}/observations", h.GetObservations)
	mux.HandleFunc("GET /api/v1/stations/{station_id}/summary", h.GetDailySummary)
	mux.HandleFunc("GET /api/v1/stations/{station_id}/range", h.GetObservationRange)
	mux.HandleFunc("GET /api/v1/health", h.Health)

	// Apply middleware (outermost runs first).
	var handler http.Handler = mux
	handler = ContentType(handler)
	handler = SecurityHeaders(handler)
	handler = CORS("")(handler) // Empty string disables CORS headers.
	handler = Logger(handler)
	handler = RequestID(handler)
	handler = Recovery(handler)

	srv := &http.Server{
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &Server{httpServer: srv, handlers: h}
}

// ListenAndServe starts the HTTP server. Blocks until context is cancelled.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	s.httpServer.Addr = addr
	slog.Info("api server starting", "addr", addr)

	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("api server: %w", err)
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// SetVersion sets the version string for the health endpoint.
func (s *Server) SetVersion(v string) { s.handlers.Version = v }

// SetStorageInfo sets storage driver and path for the health endpoint.
func (s *Server) SetStorageInfo(driver, path string) {
	s.handlers.StorageDriver = driver
	s.handlers.StoragePath = path
}
