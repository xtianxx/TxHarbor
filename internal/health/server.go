package health

import (
	"encoding/json"
	"net/http"
)

// Server exposes the probes and the metrics endpoint on the standard library
// mux (contracts/http.md).
type Server struct {
	agg     *Aggregate
	metrics http.Handler
}

// NewServer builds the HTTP handler set. metrics may be nil.
func NewServer(agg *Aggregate, metrics http.Handler) *Server {
	return &Server{agg: agg, metrics: metrics}
}

// Handler returns the mux. Only GET is accepted (method patterns).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	if s.metrics != nil {
		mux.Handle("GET /metrics", s.metrics)
	}
	return mux
}

func (s *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	rep := s.agg.Report()
	if rep.Ready {
		checks := make(map[string]string, len(s.agg.Names()))
		for _, name := range s.agg.Names() {
			checks[name] = "ok"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ready",
			"checks": checks,
		})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{
		"status": "not-ready",
		"failed": rep.Failed,
		"detail": rep.Detail,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
