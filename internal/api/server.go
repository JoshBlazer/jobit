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
	"github.com/pulse/internal/queue"
	"github.com/pulse/internal/ratelimit"
)

type Server struct {
	db      *pgxpool.Pool
	queue   *queue.Queue
	limiter *ratelimit.Limiter
	server  *http.Server
}

func New(db *pgxpool.Pool, q *queue.Queue, limiter *ratelimit.Limiter, port int) *Server {
	s := &Server{db: db, queue: q, limiter: limiter}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Route("/v1", func(r chi.Router) {
		r.Use(authMiddleware(db))

		r.Post("/jobs", s.handleSubmitJob)
		r.Get("/jobs", s.handleListJobs)
		r.Get("/jobs/{id}", s.handleGetJob)
		r.Post("/jobs/{id}/cancel", s.handleCancelJob)

		r.Post("/schedules", s.handleCreateSchedule)
		r.Get("/schedules", s.handleListSchedules)
		r.Get("/schedules/{id}", s.handleGetSchedule)
		r.Delete("/schedules/{id}", s.handleDeleteSchedule)
	})

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      r,
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
