package api

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/pulse/internal/storage"
)

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Queue depths from Redis — one LLEN per tenant per priority.
	depths, err := s.queueDepths(ctx)
	if err != nil {
		slog.Error("queue depths", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to read queue depths")
		return
	}

	// Job state counts from Postgres.
	counts, err := storage.CountJobsByState(ctx, s.db)
	if err != nil {
		slog.Error("job state counts", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to read job counts")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"queues":        depths,
		"jobs_by_state": counts,
	})
}

func (s *Server) handleRecentRuns(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	runs, err := storage.ListRecentRuns(r.Context(), s.db, limit)
	if err != nil {
		slog.Error("list recent runs", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list runs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs, "count": len(runs)})
}

func (s *Server) handleDeadLetter(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	entries, err := storage.ListDeadLetter(r.Context(), s.db, limit)
	if err != nil {
		slog.Error("list dead letter", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list dead letter")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "count": len(entries)})
}
