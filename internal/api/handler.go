package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/pulse/internal/job"
	"github.com/pulse/internal/storage"
	"github.com/pulse/internal/tenant"
)

type submitJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int16          `json:"priority,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	MaxRetries     *int            `json:"max_retries,omitempty"`
	BackoffSeconds *int            `json:"backoff_seconds,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())

	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if len(req.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	// Rate limit check.
	if s.limiter != nil {
		ok, err := s.limiter.Allow(r.Context(), t.ID, t.RateLimit)
		if err != nil {
			slog.Error("rate limit check", "tenant_id", t.ID, "err", err)
		} else if !ok {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
	}

	priority := job.PriorityNormal
	if req.Priority != nil {
		priority = *req.Priority
	}
	maxRetries := 3
	if req.MaxRetries != nil {
		maxRetries = *req.MaxRetries
	}
	backoff := 30
	if req.BackoffSeconds != nil {
		backoff = *req.BackoffSeconds
	}

	now := time.Now()
	runAt := now
	state := job.StatePending
	if req.RunAt != nil && req.RunAt.After(now) {
		runAt = *req.RunAt
		state = job.StateScheduled
	}

	j := &job.Job{
		ID:             uuid.New(),
		TenantID:       t.ID,
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       priority,
		State:          state,
		RunAt:          runAt,
		Attempt:        0,
		MaxRetries:     maxRetries,
		BackoffSeconds: backoff,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      now,
	}

	err := storage.InsertJob(r.Context(), s.db, j)
	if errors.Is(err, storage.ErrDuplicate) {
		// Idempotency key conflict — return the original job with 200.
		if req.IdempotencyKey != nil {
			existing, fetchErr := storage.GetJobByIdempotencyKey(r.Context(), s.db, t.ID, *req.IdempotencyKey)
			if fetchErr == nil {
				writeJSON(w, http.StatusOK, existing)
				return
			}
		}
		writeError(w, http.StatusConflict, "duplicate idempotency key")
		return
	}
	if err != nil {
		slog.Error("insert job", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	if state == job.StatePending {
		if err := s.queue.Enqueue(r.Context(), j.ID, j.Priority); err != nil {
			slog.Error("enqueue job", "job_id", j.ID, "err", err)
		}
	}

	writeJSON(w, http.StatusCreated, j)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	j, err := storage.GetJob(r.Context(), s.db, id)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		slog.Error("get job", "job_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	filter := storage.ListFilter{TenantID: &t.ID, Limit: 50}

	if v := r.URL.Query().Get("state"); v != "" {
		st := job.State(v)
		filter.State = &st
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			filter.Limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			filter.Offset = n
		}
	}

	jobs, err := storage.ListJobs(r.Context(), s.db, filter)
	if err != nil {
		slog.Error("list jobs", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "count": len(jobs)})
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	if err := storage.CancelJob(r.Context(), s.db, id); errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found or not cancellable")
		return
	} else if err != nil {
		slog.Error("cancel job", "job_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
