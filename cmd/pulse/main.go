package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/pulse/internal/api"
	"github.com/pulse/internal/leader"
	"github.com/pulse/internal/queue"
	"github.com/pulse/internal/ratelimit"
	"github.com/pulse/internal/scheduler"
	"github.com/pulse/internal/storage"
	"github.com/pulse/internal/telemetry"
	"github.com/pulse/internal/worker"
	"github.com/redis/go-redis/v9"
)

type config struct {
	role            string
	postgresURL     string
	redisAddr       string
	etcdEndpoints   string
	otlpEndpoint    string
	httpPort        int
	metricsPort     int
	shutdownTimeout time.Duration
}

func loadConfig() config {
	var c config
	flag.StringVar(&c.role, "role", env("PULSE_ROLE", ""), "role to run: api | scheduler | worker")
	flag.StringVar(&c.postgresURL, "postgres-url", env("PULSE_POSTGRES_URL", "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable"), "postgres connection string")
	flag.StringVar(&c.redisAddr, "redis-addr", env("PULSE_REDIS_ADDR", "localhost:6379"), "redis address")
	flag.StringVar(&c.etcdEndpoints, "etcd-endpoints", env("PULSE_ETCD_ENDPOINTS", "localhost:2379"), "comma-separated etcd endpoints")
	flag.StringVar(&c.otlpEndpoint, "otlp-endpoint", env("PULSE_OTLP_ENDPOINT", "localhost:4318"), "OTLP HTTP trace endpoint")
	flag.IntVar(&c.httpPort, "port", envInt("PULSE_PORT", 8080), "http port (api role only)")
	flag.IntVar(&c.metricsPort, "metrics-port", envInt("PULSE_METRICS_PORT", 0), "prometheus metrics port (scheduler=9091, worker=9092 by default)")
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

	// Initialise OTel tracing. Non-fatal if Jaeger is not available.
	otelShutdown, err := telemetry.Init(ctx, "pulse-"+c.role, c.otlpEndpoint)
	if err != nil {
		slog.Warn("OTel init failed — tracing disabled", "err", err)
	} else {
		defer otelShutdown(context.Background()) //nolint:errcheck
	}

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
		runAPI(ctx, c, db, q, rdb, limiter)
	case "scheduler":
		startMetricsServer(c.metricsPort, 9091)
		runScheduler(ctx, c, db, q)
	case "worker":
		startMetricsServer(c.metricsPort, 9092)
		runWorker(ctx, c, db, q, rdb)
	default:
		fmt.Fprintf(os.Stderr, "unknown role %q — must be api, scheduler, or worker\n", c.role)
		os.Exit(1)
	}
}

// startMetricsServer launches a tiny HTTP server serving only /metrics and /healthz.
// port overrides the default if non-zero.
func startMetricsServer(port, defaultPort int) {
	if port == 0 {
		port = defaultPort
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	addr := fmt.Sprintf(":%d", port)
	go func() {
		slog.Info("metrics server listening", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("metrics server error", "err", err)
		}
	}()
}

func runAPI(ctx context.Context, c config, db *pgxpool.Pool, q *queue.Queue, rdb *redis.Client, limiter *ratelimit.Limiter) {
	srv := api.New(db, q, rdb, limiter, c.httpPort)

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

func runScheduler(ctx context.Context, c config, db *pgxpool.Pool, q *queue.Queue) {
	endpoints := strings.Split(c.etcdEndpoints, ",")
	etcdClient, err := leader.NewClient(endpoints)
	if err != nil {
		slog.Error("connect to etcd", "err", err)
		os.Exit(1)
	}
	defer etcdClient.Close()

	elect := leader.New(etcdClient, "/pulse/scheduler/leader", 5)
	s := scheduler.New(db, q)
	s.Run(ctx, elect)
}

func runWorker(ctx context.Context, c config, db *pgxpool.Pool, q *queue.Queue, rdb *redis.Client) {
	_ = rdb // reserved for future worker-side Redis ops
	w := worker.New(db, q)
	go w.Run(ctx)

	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)
	go func() {
		for range sighupCh {
			slog.Info("SIGHUP received — reloading tenant weights")
			w.Reload()
		}
	}()

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
