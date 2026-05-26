package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/pulse/internal/storage"
	"github.com/pulse/internal/tenant"
	"github.com/robfig/cron/v3"
)

type createScheduleRequest struct {
	Name        string          `json:"name"`
	Cron        string          `json:"cron"`
	Timezone    string          `json:"timezone,omitempty"`
	JobTemplate json.RawMessage `json:"job_template"`
}

func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())

	var req createScheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Cron == "" {
		writeError(w, http.StatusBadRequest, "cron is required")
		return
	}
	if len(req.JobTemplate) == 0 {
		writeError(w, http.StatusBadRequest, "job_template is required")
		return
	}

	tz := req.Timezone
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid timezone")
		return
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	expr, err := parser.Parse(req.Cron)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid cron expression: "+err.Error())
		return
	}

	nextRunAt := expr.Next(time.Now().In(loc))

	sched := &storage.Schedule{
		ID:          uuid.New(),
		TenantID:    t.ID,
		Name:        req.Name,
		Cron:        req.Cron,
		Timezone:    tz,
		JobTemplate: req.JobTemplate,
		Enabled:     true,
		NextRunAt:   nextRunAt,
	}

	if err := storage.InsertSchedule(r.Context(), s.db, sched); err != nil {
		if errors.Is(err, storage.ErrDuplicate) {
			writeError(w, http.StatusConflict, "schedule name already exists")
			return
		}
		slog.Error("insert schedule", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to create schedule")
		return
	}

	writeJSON(w, http.StatusCreated, sched)
}

func (s *Server) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	schedules, err := storage.ListSchedules(r.Context(), s.db, t.ID)
	if err != nil {
		slog.Error("list schedules", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list schedules")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": schedules, "count": len(schedules)})
}

func (s *Server) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid schedule id")
		return
	}
	sched, err := storage.GetSchedule(r.Context(), s.db, id)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	if err != nil {
		slog.Error("get schedule", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get schedule")
		return
	}
	writeJSON(w, http.StatusOK, sched)
}

func (s *Server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	t, _ := tenant.FromContext(r.Context())
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid schedule id")
		return
	}
	if err := storage.DeleteSchedule(r.Context(), s.db, id, t.ID); errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	} else if err != nil {
		slog.Error("delete schedule", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to delete schedule")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
