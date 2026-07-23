// Package control exposes an HTTP API for inspecting proxy status and
// injecting faults or scenarios at runtime, so faults can be triggered from
// a script, a CI job, or a dashboard rather than only at proxy startup.
package control

import (
	"crypto/subtle"
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/jimroxodezi/dbfailsim/internal/check"
	"github.com/jimroxodezi/dbfailsim/internal/config"
	"github.com/jimroxodezi/dbfailsim/internal/proxy"
	"github.com/jimroxodezi/dbfailsim/internal/scenario"
)

//go:embed static/*
var staticFS embed.FS

type Server struct {
	cfg     *config.Config
	proxies map[string]*proxy.Proxy
	engine  *scenario.Engine
}

func NewServer(cfg *config.Config, proxies map[string]*proxy.Proxy, engine *scenario.Engine) *Server {
	return &Server{cfg: cfg, proxies: proxies, engine: engine}
}

// ListenAndServe starts the control API and the web dashboard. Blocks; run
// in its own goroutine.
func (s *Server) ListenAndServe(addr string) error {
	h, err := s.Handler()
	if err != nil {
		return err
	}
	if s.cfg.ControlToken == "" {
		log.Printf("WARNING: control API is UNAUTHENTICATED — anyone who can reach %s can inject faults and run check commands. Set control_token in the config (or DBFAILSIM_CONTROL_TOKEN) before exposing it beyond localhost.", addr)
	}
	log.Printf("dashboard + control API at http://%s (GET /status /scenarios /check, POST /nodes/{node}/fault /scenarios/{name}/run /heal)", addr)
	return http.ListenAndServe(addr, h)
}

// requireBearerToken rejects requests that don't carry
// "Authorization: Bearer <token>" with a 401. Comparison is constant-time.
func requireBearerToken(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			const prefix = "Bearer "
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, prefix) ||
				subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(auth, prefix)), []byte(token)) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="dbfailsim"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Handler returns the control API and dashboard routes, so they can be
// mounted on any server (including httptest servers). Method enforcement is
// handled by the router: a wrong-method request gets a 405 automatically.
func (s *Server) Handler() (http.Handler, error) {
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, err
	}
	fileServer := http.FileServer(http.FS(staticSub))

	r := chi.NewRouter()
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Dashboard shell and assets are public; they contain nothing sensitive
	// and the page itself needs to load before it can ask for a token.
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		b, _ := staticFS.ReadFile("static/index.html")
		w.Write(b)
	})
	r.Handle("/static/*", http.StripPrefix("/static/", fileServer))

	r.Group(func(api chi.Router) {
		if s.cfg.ControlToken != "" {
			api.Use(requireBearerToken(s.cfg.ControlToken))
		}
		api.Get("/status", s.handleStatus)
		api.Post("/nodes/{node}/fault", s.handleFault)
		api.Get("/scenarios", s.handleScenarios)
		api.Post("/scenarios/{name}/run", s.handleScenarioRun)
		api.Post("/heal", s.handleHeal)
		api.Get("/check", s.handleCheck)
	})
	return r, nil
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	var statuses []proxy.Status
	for _, p := range s.proxies {
		statuses = append(statuses, p.Status())
	}
	writeJSON(w, http.StatusOK, statuses)
}

// faultRequest is the body of POST /nodes/{node}/fault, e.g.:
//
//	POST /nodes/replica-1/fault {"kind": "latency", "latency_ms": 3000}
type faultRequest struct {
	Kind        string `json:"kind"`
	LatencyMs   int    `json:"latency_ms"`
	DropPercent int    `json:"drop_percent"`
}

func (s *Server) handleFault(w http.ResponseWriter, r *http.Request) {
	node := chi.URLParam(r, "node")
	p, ok := s.proxies[node]
	if !ok {
		http.Error(w, "unknown node: "+node, http.StatusNotFound)
		return
	}
	var req faultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch req.Kind {
	case "latency":
		p.State.SetLatency(req.LatencyMs)
	case "drop":
		p.State.SetDrop(req.DropPercent)
	case "partition":
		p.State.SetPartitioned(true)
	case "crash":
		p.State.SetCrashed(true)
	case "heal":
		p.State.Heal()
	default:
		http.Error(w, "unknown fault kind: "+req.Kind, http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, p.Status())
}

func (s *Server) handleScenarioRun(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	sc := s.cfg.FindScenario(name)
	if sc == nil {
		http.Error(w, "unknown scenario: "+name, http.StatusNotFound)
		return
	}
	go func() {
		if err := s.engine.Run(sc); err != nil {
			log.Printf("scenario %q failed: %v", name, err)
		}
	}()
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "started", "scenario": name})
}

func (s *Server) handleHeal(w http.ResponseWriter, r *http.Request) {
	s.engine.HealAll()
	writeJSON(w, http.StatusOK, map[string]string{"status": "healed"})
}

func (s *Server) handleScenarios(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.cfg.Scenarios)
}

// checkResultJSON mirrors check.Result but adds a per-row divergence flag
// and an error string (net/http can't serialize an `error` field directly).
type checkResultJSON struct {
	Node     string `json:"node"`
	Output   string `json:"output"`
	Error    string `json:"error,omitempty"`
	Diverges bool   `json:"diverges"`
}

type checkResponse struct {
	Results []checkResultJSON `json:"results"`
	Agree   bool              `json:"agree"`
}

func (s *Server) handleCheck(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	if query == "" {
		http.Error(w, "missing query parameter", http.StatusBadRequest)
		return
	}
	results := check.Run(s.cfg, query)

	resp := checkResponse{Agree: true}
	var first string
	for i, res := range results {
		if i == 0 {
			first = res.Output
		} else if res.Output != first {
			resp.Agree = false
		}
	}
	for i, res := range results {
		errStr := ""
		if res.Err != nil {
			errStr = res.Err.Error()
		}
		resp.Results = append(resp.Results, checkResultJSON{
			Node:     res.Node,
			Output:   res.Output,
			Error:    errStr,
			Diverges: i > 0 && res.Output != first,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
