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
	if err := q.Enqueue(ctx, jobID, j.Priority); err != nil {
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

func usage() {
	fmt.Fprintln(os.Stderr, `pulse-cli — Pulse admin tool

Commands:
  replay <job-id>     re-enqueue a dead-letter job from the beginning
  force-fail <job-id> mark a job dead immediately
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
