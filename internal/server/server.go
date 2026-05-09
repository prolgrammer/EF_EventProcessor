package server

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/eventflow/event-processor/internal/storage"
)

// Server is the HTTP server exposing operational endpoints:
//   - GET /healthz  — liveness probe (always 200 if the process is alive)
//   - GET /readyz   — readiness probe (200 only when ClickHouse is reachable)
//   - GET /metrics  — Prometheus metrics
//
// The server is intentionally started before ClickHouse is connected so that
// Docker's liveness probe succeeds immediately and does not mark the container
// as unhealthy during the startup retry window.  Call SetClickHouse once the
// connection is established; until then /readyz returns 503.
type Server struct {
	srv *http.Server
	log *zap.Logger

	chMu sync.RWMutex
	ch   *storage.ClickHouse // nil until SetClickHouse is called
}

// New creates a new Server bound to addr.
// ch is not accepted here; wire it later with SetClickHouse so the HTTP server
// can start (and answer liveness probes) before ClickHouse is ready.
func New(addr string, log *zap.Logger) *Server {
	s := &Server{log: log}

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(requestLogger(log))

	r.Get("/healthz", s.handleLiveness)
	r.Get("/readyz", s.handleReadiness)
	r.Handle("/metrics", promhttp.Handler())

	s.srv = &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

// SetClickHouse wires the ClickHouse client into the readiness probe.
// Safe to call concurrently. Typically called once after storage.New succeeds.
func (s *Server) SetClickHouse(ch *storage.ClickHouse) {
	s.chMu.Lock()
	s.ch = ch
	s.chMu.Unlock()
}

// Start runs the HTTP server in the background. Returns immediately.
func (s *Server) Start() {
	go func() {
		s.log.Info("http server listening", zap.String("addr", s.srv.Addr))
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("http server error", zap.Error(err))
		}
	}()
}

// Shutdown gracefully drains active connections within the given timeout.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// handleLiveness is the Kubernetes liveness probe endpoint.
// Always returns 200 OK as long as the process is running.
func (s *Server) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleReadiness checks ClickHouse reachability.
// Returns 503 during startup (ch == nil) or when ClickHouse is down.
func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	s.chMu.RLock()
	ch := s.ch
	s.chMu.RUnlock()

	if ch == nil {
		// Still starting up — ClickHouse not connected yet.
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready","reason":"starting up"}`))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	if err := ch.Ping(ctx); err != nil {
		s.log.Warn("readiness check failed: clickhouse unreachable", zap.Error(err))
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready","reason":"clickhouse unreachable"}`))
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

// requestLogger is a minimal chi middleware that logs each request.
func requestLogger(log *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			log.Info("http request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", ww.Status()),
				zap.Duration("duration", time.Since(start)),
			)
		})
	}
}
