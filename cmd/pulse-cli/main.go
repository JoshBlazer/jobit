package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pulse/internal/job"
	"github.com/pulse/internal/queue"
	"github.com/pulse/internal/storage"
	"github.com/redis/go-redis/v9"
)

func main() {
	postgresURL := flag.String("postgres-url", env("PULSE_POSTGRES_URL", "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable"), "postgres DSN")
	redisAddr := flag.String("redis-addr", env("PULSE_REDIS_ADDR", "localhost:6379"), "redis address")
	flag.Parse()

	if flag.NArg() == 0 {
		usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, *postgresURL)
	if err != nil {
		fatalf("connect postgres: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()

	q := queue.New(rdb)

	switch flag.Arg(0) {
	case "replay":
		cmdReplay(ctx, db, q, flag.Args()[1:])
	case "force-fail":
		cmdForceFail(ctx, db, flag.Args()[1:])
	case "drain":
		cmdDrain(ctx, rdb)
	case "dump-scheduler":
		cmdDumpScheduler(ctx, db, rdb)
	case "list-dead":
		cmdListDead(ctx, db)
	case "queue-depth":
		cmdQueueDepth(ctx, rdb)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", flag.Arg(0))
		usage()
		os.Exit(1)
	}
}

func cmdReplay(ctx context.Context, db *pgxpool.Pool, q *queue.Queue, args []string) {
	if len(args) == 0 {
		fatalf("usage: pulse-cli replay <job-id>")
	}
	jobID, err := uuid.Parse(args[0])
	if err != nil {
		fatalf("invalid job id: %v", err)
	}

	j, err := storage.GetJob(ctx, db, jobID)
	if err != nil {
		fatalf("get job: %v", err)
	}
	if j.State != job.StateDead {
		fatalf("job %s is in state %s, not dead — only dead jobs can be replayed", jobID, j.State)
	}

	// Move back to pending and re-enqueue.
	_, err = db.Exec(ctx, `
		UPDATE jobs SET state = 'pending', attempt = 0, last_error = NULL,
		    claim_token = NULL, claimed_at = NULL, claimed_by = NULL, deadline = NULL
		WHERE id = $1 AND state = 'dead'`, jobID)
	if err != nil {
		fatalf("reset job state: %v", err)
	}
	if err := q.Enqueue(ctx, j.TenantID, jobID, j.Priority); err != nil {
		fatalf("enqueue: %v", err)
	}
	fmt.Printf("replayed job %s\n", jobID)
}

func cmdForceFail(ctx context.Context, db *pgxpool.Pool, args []string) {
	if len(args) == 0 {
		fatalf("usage: pulse-cli force-fail <job-id>")
	}
	jobID, err := uuid.Parse(args[0])
	if err != nil {
		fatalf("invalid job id: %v", err)
	}

	tag, err := db.Exec(ctx, `
		UPDATE jobs SET state = 'dead', last_error = 'force-failed by operator',
		    claim_token = NULL, deadline = NULL
		WHERE id = $1 AND state IN ('claimed', 'running', 'pending', 'failed')`, jobID)
	if err != nil {
		fatalf("force-fail: %v", err)
	}
	if tag.RowsAffected() == 0 {
		fatalf("job %s not found or already terminal", jobID)
	}
	fmt.Printf("force-failed job %s\n", jobID)
}

func cmdListDead(ctx context.Context, db *pgxpool.Pool) {
	rows, err := db.Query(ctx, `
		SELECT job_id, tenant_id, moved_at, final_error, attempt_count
		FROM dead_letter
		ORDER BY moved_at DESC
		LIMIT 50`)
	if err != nil {
		fatalf("query dead_letter: %v", err)
	}
	defer rows.Close()

	fmt.Printf("%-36s  %-36s  %-25s  %s\n", "JOB ID", "TENANT", "MOVED AT", "ERROR")
	fmt.Println(repeat("-", 130))
	count := 0
	for rows.Next() {
		var jobID, tenantID uuid.UUID
		var movedAt time.Time
		var finalError *string
		var attempts int
		if err := rows.Scan(&jobID, &tenantID, &movedAt, &finalError, &attempts); err != nil {
			fatalf("scan: %v", err)
		}
		errStr := ""
		if finalError != nil {
			errStr = *finalError
			if len(errStr) > 50 {
				errStr = errStr[:50] + "…"
			}
		}
		fmt.Printf("%-36s  %-36s  %-25s  %s\n", jobID, tenantID, movedAt.Format(time.RFC3339), errStr)
		count++
	}
	if count == 0 {
		fmt.Println("(no dead-letter jobs)")
	}
}

func cmdQueueDepth(ctx context.Context, rdb *redis.Client) {
	queues := []string{"queue:1", "queue:5", "queue:10"}
	labels := []string{"high", "normal", "low"}

	fmt.Printf("%-12s  %s\n", "PRIORITY", "DEPTH")
	fmt.Println(repeat("-", 24))
	for i, key := range queues {
		n, err := rdb.LLen(ctx, key).Result()
		if err != nil {
			n = -1
		}
		fmt.Printf("%-12s  %d\n", labels[i], n)
	}
}

func cmdDrain(ctx context.Context, rdb *redis.Client) {
	var cursor uint64
	var totalJobs int64
	var queueCount int

	for {
		keys, next, err := rdb.Scan(ctx, cursor, "queue:*", 100).Result()
		if err != nil {
			fatalf("scan redis: %v", err)
		}
		for _, key := range keys {
			n, err := rdb.LLen(ctx, key).Result()
			if err != nil || n == 0 {
				continue
			}
			if err := rdb.Del(ctx, key).Err(); err != nil {
				fmt.Fprintf(os.Stderr, "warn: del %s: %v\n", key, err)
				continue
			}
			totalJobs += n
			queueCount++
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	if totalJobs == 0 {
		fmt.Println("queues already empty")
		return
	}
	fmt.Printf("removed %d jobs from %d queues\n", totalJobs, queueCount)
	fmt.Println("jobs are preserved in postgres and will be re-enqueued by the reconciler when ready")
}

func cmdDumpScheduler(ctx context.Context, db *pgxpool.Pool, rdb *redis.Client) {
	fmt.Println("JOB COUNTS")
	rows, err := db.Query(ctx, `SELECT state::text, COUNT(*) FROM jobs GROUP BY state ORDER BY state`)
	if err != nil {
		fatalf("query job counts: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			fatalf("scan job counts: %v", err)
		}
		fmt.Printf("  %-12s %d\n", state, count)
	}

	fmt.Println("\nACTIVE WORKERS")
	rows2, err := db.Query(ctx, `
		SELECT id, claimed_by, state::text, claimed_at
		FROM jobs WHERE state IN ('claimed', 'running')
		ORDER BY claimed_at`)
	if err != nil {
		fatalf("query active workers: %v", err)
	}
	defer rows2.Close()
	activeCount := 0
	for rows2.Next() {
		var jobID uuid.UUID
		var claimedBy *string
		var state string
		var claimedAt *time.Time
		if err := rows2.Scan(&jobID, &claimedBy, &state, &claimedAt); err != nil {
			fatalf("scan active worker: %v", err)
		}
		worker := "(unknown)"
		if claimedBy != nil {
			worker = *claimedBy
		}
		elapsed := ""
		if claimedAt != nil {
			elapsed = fmt.Sprintf("(%s ago)", time.Since(*claimedAt).Round(time.Second))
		}
		fmt.Printf("  %-10s  %-36s  worker %.8s…  %s\n", state, jobID, worker, elapsed)
		activeCount++
	}
	if activeCount == 0 {
		fmt.Println("  (none)")
	}

	fmt.Println("\nUPCOMING SCHEDULES")
	rows3, err := db.Query(ctx, `
		SELECT name, cron, next_run_at FROM schedules
		WHERE enabled = TRUE ORDER BY next_run_at LIMIT 10`)
	if err != nil {
		fatalf("query schedules: %v", err)
	}
	defer rows3.Close()
	schedCount := 0
	for rows3.Next() {
		var name, cron string
		var nextRun time.Time
		if err := rows3.Scan(&name, &cron, &nextRun); err != nil {
			fatalf("scan schedule: %v", err)
		}
		in := time.Until(nextRun).Round(time.Second)
		fmt.Printf("  %-24s  %-20s  %s  (in %s)\n", name, cron, nextRun.UTC().Format(time.RFC3339), in)
		schedCount++
	}
	if schedCount == 0 {
		fmt.Println("  (no enabled schedules)")
	}

	fmt.Println("\nREDIS QUEUE DEPTHS")
	var cursor uint64
	type qd struct{ key string; n int64 }
	var depths []qd
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "queue:*", 100).Result()
		if err != nil {
			break
		}
		for _, key := range keys {
			n, err := rdb.LLen(ctx, key).Result()
			if err != nil || n == 0 {
				continue
			}
			depths = append(depths, qd{key, n})
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(depths) == 0 {
		fmt.Println("  (all queues empty)")
	}
	for _, d := range depths {
		fmt.Printf("  %-44s  %d\n", d.key, d.n)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `pulse-cli — Pulse admin tool

Commands:
  replay <job-id>     re-enqueue a dead-letter job from the beginning
  force-fail <job-id> mark a job dead immediately
  drain               flush all Redis queues (jobs stay in postgres)
  dump-scheduler      show job counts, active workers, schedules, queue depths
  list-dead           list dead-letter jobs (most recent 50)
  queue-depth         show pending job counts per priority queue

Flags:`)
	flag.PrintDefaults()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}
