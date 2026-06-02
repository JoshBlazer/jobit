package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sluice/internal/storage"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // permissive for local dev
}

// handleWebSocket upgrades the connection and streams a live snapshot every second.
// The snapshot carries queue depths and job-by-state counts — enough for the dashboard.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("ws upgrade", "err", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Close ctx when the client disconnects.
	go func() {
		for {
			if _, _, err := conn.NextReader(); err != nil {
				cancel()
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depths, err := s.queueDepths(ctx)
			if err != nil {
				slog.Warn("ws queue depths", "err", err)
				continue
			}
			counts, err := storage.CountJobsByState(ctx, s.db)
			if err != nil {
				slog.Warn("ws job counts", "err", err)
				continue
			}
			msg, _ := json.Marshal(map[string]any{
				"queues":        depths,
				"jobs_by_state": counts,
				"timestamp":     time.Now().UTC(),
			})
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}
}

// queueDepths returns Redis LLEN for every known priority+tenant combination.
func (s *Server) queueDepths(ctx context.Context) ([]map[string]any, error) {
	tenants, err := storage.GetTenants(ctx, s.db)
	if err != nil {
		return nil, err
	}

	priorities := []struct {
		label string
		key   string
	}{
		{"high", "queue:1"},
		{"normal", "queue:5"},
		{"low", "queue:10"},
	}

	var out []map[string]any
	for _, t := range tenants {
		for _, p := range priorities {
			key := p.key + ":" + t.ID.String()
			n, err := s.rdb.LLen(ctx, key).Result()
			if err != nil {
				n = 0
			}
			out = append(out, map[string]any{
				"tenant_id": t.ID,
				"tenant":    t.Name,
				"priority":  p.label,
				"depth":     n,
			})
		}
	}
	return out, nil
}
