package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pulse/internal/api"
	"github.com/pulse/internal/queue"
	"github.com/pulse/internal/ratelimit"
	"github.com/pulse/internal/scheduler"
	"github.com/pulse/internal/storage"
	"github.com/pulse/internal/worker"
)

type config struct {
	role            string
	postgresURL     string
	redisAddr       string
	httpPort        int
	shutdownTimeout time.Duration
}

func loadConfig() config {
	var c config
	flag.StringVar(&c.role, "role", env("PULSE_ROLE", ""), "role to run: api | scheduler | worker")
	flag.StringVar(&c.postgresURL, "postgres-url", env("PULSE_POSTGRES_URL", "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable"), "postgres connection string")
	flag.StringVar(&c.redisAddr, "redis-addr", env("PULSE_REDIS_ADDR", "localhost:6379"), "redis address")
	flag.IntVar(&c.httpPort, "port", envInt("PULSE_PORT", 8080), "http port (api role only)")
	flag.DurationVar(&c.shutdownTimeout, "shutdown-timeout", 30*time.Second, "graceful shutdown timeout")
	flag.Parse()
	return c
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	c := loadConfig()
	if c.role == "" {
		fmt.Fprintln(os.Stderr, "usage: pulse --role <api|scheduler|worker>")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := storage.NewPool(ctx, c.postgresURL)
	if err != nil {
		slog.Error("connect to postgres", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	rdb := queue.NewClient(c.redisAddr)
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("connect to redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	q := queue.New(rdb)
	limiter := ratelimit.New(rdb)

	switch c.role {
	case "api":
		runAPI(ctx, c, db, q, limiter)
	case "scheduler":
		runScheduler(ctx, db, q)
	case "worker":
		runWorker(ctx, c, db, q)
	default:
		fmt.Fprintf(os.Stderr, "unknown role %q — must be api, scheduler, or worker\n", c.role)
		os.Exit(1)
	}
}

func runAPI(ctx context.Context, c config, db *pgxpool.Pool, q *queue.Queue, limiter *ratelimit.Limiter) {
	srv := api.New(db, q, limiter, c.httpPort)

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		slog.Error("api server error", "err", err)
		return
	}

	slog.Info("shutting down api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), c.shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("api shutdown error", "err", err)
	}
}

func runScheduler(ctx context.Context, db *pgxpool.Pool, q *queue.Queue) {
	s := scheduler.New(db, q)
	s.Run(ctx)
}

func runWorker(ctx context.Context, c config, db *pgxpool.Pool, q *queue.Queue) {
	w := worker.New(db, q)
	go w.Run(ctx)
	<-ctx.Done()
	w.Shutdown(c.shutdownTimeout)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		fmt.Sscanf(v, "%d", &n)
		return n
	}
	return fallback
}
