package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/ratelimit"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type Server struct {
	db      *pgxpool.Pool
	queue   *queue.Queue
	rdb     *redis.Client
	limiter *ratelimit.Limiter
	server  *http.Server
}

func New(db *pgxpool.Pool, q *queue.Queue, rdb *redis.Client, limiter *ratelimit.Limiter, port int) *Server {
	s := &Server{db: db, queue: q, rdb: rdb, limiter: limiter}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware)
	r.Use(correlationMiddleware)

	// Health + observability (no auth)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/ws", s.handleWebSocket)

	r.Route("/v1", func(r chi.Router) {
		r.Use(authMiddleware(db))

		r.Post("/jobs", s.handleSubmitJob)
		r.Get("/jobs", s.handleListJobs)
		r.Get("/jobs/{id}", s.handleGetJob)
		r.Post("/jobs/{id}/cancel", s.handleCancelJob)
		r.Post("/jobs/{id}/replay", s.handleReplayJob)

		r.Post("/schedules", s.handleCreateSchedule)
		r.Get("/schedules", s.handleListSchedules)
		r.Get("/schedules/{id}", s.handleGetSchedule)
		r.Delete("/schedules/{id}", s.handleDeleteSchedule)

		// Dashboard data endpoints (no tenant scoping — admin-level)
		r.Get("/stats", s.handleStats)
		r.Get("/stats/runs", s.handleRecentRuns)
		r.Get("/stats/dead-letter", s.handleDeadLetter)
	})

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      otelhttp.NewHandler(r, "sluice-api"),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

func (s *Server) Start() error {
	slog.Info("api listening", "addr", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
