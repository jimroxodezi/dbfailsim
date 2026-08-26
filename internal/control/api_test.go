package control

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jimroxodezi/dbfailsim/internal/config"
	"github.com/jimroxodezi/dbfailsim/internal/proxy"
	"github.com/jimroxodezi/dbfailsim/internal/scenario"
)

func newTestHandler(t *testing.T) (http.Handler, map[string]*proxy.Proxy) {
	t.Helper()
	cfg := &config.Config{
		Nodes: []config.Node{{Name: "n1", ListenAddr: "127.0.0.1:0", UpstreamAddr: "127.0.0.1:1"}},
		Scenarios: []config.Scenario{{
			Name:  "crash-n1",
			Steps: []config.FaultStep{{Node: "n1", Kind: "crash", AfterMs: 0}},
		}},
	}
	proxies := map[string]*proxy.Proxy{
		"n1": proxy.New("n1", "127.0.0.1:0", "127.0.0.1:1"),
	}
	eng := scenario.New(proxies, nil)
	h, err := NewServer(cfg, proxies, eng).Handler()
	if err != nil {
		t.Fatal(err)
	}
	return h, proxies
}

func newAuthedHandler(t *testing.T, token string) http.Handler {
	t.Helper()
	cfg := &config.Config{
		Nodes:        []config.Node{{Name: "n1", ListenAddr: "127.0.0.1:0", UpstreamAddr: "127.0.0.1:1"}},
		ControlToken: token,
	}
	proxies := map[string]*proxy.Proxy{
		"n1": proxy.New("n1", "127.0.0.1:0", "127.0.0.1:1"),
	}
	h, err := NewServer(cfg, proxies, scenario.New(proxies, nil)).Handler()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func do(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, target, nil)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestAuth(t *testing.T) {
	h := newAuthedHandler(t, "sekret")

	doAuth := func(method, target, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}

	if w := doAuth(http.MethodGet, "/status", ""); w.Code != http.StatusUnauthorized {
		t.Errorf("no token: status = %d, want 401", w.Code)
	}
	if w := doAuth(http.MethodGet, "/status", "wrong"); w.Code != http.StatusUnauthorized {
		t.Errorf("wrong token: status = %d, want 401", w.Code)
	}
	if w := doAuth(http.MethodPost, "/heal", "wrong"); w.Code != http.StatusUnauthorized {
		t.Errorf("wrong token on /heal: status = %d, want 401", w.Code)
	}
	if w := doAuth(http.MethodGet, "/status", "sekret"); w.Code != http.StatusOK {
		t.Errorf("valid token: status = %d, want 200", w.Code)
	}
	if w := doAuth(http.MethodPost, "/heal", "sekret"); w.Code != http.StatusOK {
		t.Errorf("valid token on /heal: status = %d, want 200", w.Code)
	}

	// Dashboard shell and assets stay public so the page can load and
	// prompt for the token.
	if w := doAuth(http.MethodGet, "/", ""); w.Code != http.StatusOK {
		t.Errorf("dashboard: status = %d, want 200 without token", w.Code)
	}
	if w := doAuth(http.MethodGet, "/static/app.js", ""); w.Code != http.StatusOK {
		t.Errorf("static asset: status = %d, want 200 without token", w.Code)
	}
}

func TestNoTokenConfiguredMeansOpen(t *testing.T) {
	h := newAuthedHandler(t, "")
	if w := do(h, http.MethodGet, "/status", ""); w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 when no token configured", w.Code)
	}
}

func TestStatusEndpoint(t *testing.T) {
	h, _ := newTestHandler(t)
	w := do(h, http.MethodGet, "/status", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var statuses []proxy.Status
	if err := json.NewDecoder(w.Body).Decode(&statuses); err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || statuses[0].Name != "n1" {
		t.Fatalf("unexpected statuses: %+v", statuses)
	}
}

func TestFaultEndpoint(t *testing.T) {
	h, proxies := newTestHandler(t)

	w := do(h, http.MethodPost, "/nodes/n1/fault", `{"kind":"latency","latency_ms":1234}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body)
	}
	if st := proxies["n1"].Status(); st.LatencyMs != 1234 {
		t.Fatalf("latency not applied: %+v", st)
	}

	if w := do(h, http.MethodGet, "/nodes/n1/fault", ""); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /nodes/n1/fault status = %d, want 405", w.Code)
	}
	if w := do(h, http.MethodPost, "/nodes/nope/fault", `{"kind":"crash"}`); w.Code != http.StatusNotFound {
		t.Errorf("unknown node status = %d, want 404", w.Code)
	}
	if w := do(h, http.MethodPost, "/nodes/n1/fault", `{"kind":"meteor"}`); w.Code != http.StatusBadRequest {
		t.Errorf("unknown kind status = %d, want 400", w.Code)
	}
}

func TestHealEndpointIsPostOnly(t *testing.T) {
	h, proxies := newTestHandler(t)
	proxies["n1"].State.SetLatency(500)

	if w := do(h, http.MethodGet, "/heal", ""); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /heal status = %d, want 405", w.Code)
	}
	if st := proxies["n1"].Status(); st.LatencyMs != 500 {
		t.Fatal("GET /heal must not change fault state")
	}

	if w := do(h, http.MethodPost, "/heal", ""); w.Code != http.StatusOK {
		t.Errorf("POST /heal status = %d, want 200", w.Code)
	}
	if st := proxies["n1"].Status(); st.LatencyMs != 0 {
		t.Fatalf("heal did not clear faults: %+v", st)
	}
}

func TestScenarioEndpoint(t *testing.T) {
	h, _ := newTestHandler(t)

	if w := do(h, http.MethodPost, "/scenarios/unknown/run", ""); w.Code != http.StatusNotFound {
		t.Errorf("unknown scenario status = %d, want 404", w.Code)
	}
	if w := do(h, http.MethodGet, "/scenarios/crash-n1/run", ""); w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /scenarios/crash-n1/run status = %d, want 405", w.Code)
	}
	if w := do(h, http.MethodPost, "/scenarios/crash-n1/run", ""); w.Code != http.StatusAccepted {
		t.Errorf("POST /scenarios/crash-n1/run status = %d, want 202", w.Code)
	}
}

func TestScenariosEndpoint(t *testing.T) {
	h, _ := newTestHandler(t)
	w := do(h, http.MethodGet, "/scenarios", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var scenarios []config.Scenario
	if err := json.NewDecoder(w.Body).Decode(&scenarios); err != nil {
		t.Fatal(err)
	}
	if len(scenarios) != 1 || scenarios[0].Name != "crash-n1" {
		t.Fatalf("unexpected scenarios: %+v", scenarios)
	}
}

func TestFaultEndpointGeneralForm(t *testing.T) {
	h, proxies := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	body := `{"kind":"reorder","params":{"buffer_size":4,"probability":0.5},"for_ms":150}`
	resp, err := http.Post(srv.URL+"/nodes/n1/fault", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var st proxy.Status
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if len(st.ActiveFaults) != 1 || st.ActiveFaults[0] != "reorder" {
		t.Fatalf("active_faults = %v", st.ActiveFaults)
	}
	time.Sleep(250 * time.Millisecond)
	if n := proxies["n1"].Registry.Names(); len(n) != 0 {
		t.Fatalf("for_ms did not expire fault: %v", n)
	}

	resp2, err := http.Post(srv.URL+"/nodes/n1/fault", "application/json", strings.NewReader(`{"kind":"nope"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown kind status = %d", resp2.StatusCode)
	}
}
