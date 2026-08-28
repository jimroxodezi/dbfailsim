package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad(t *testing.T) {
	path := writeConfig(t, `
control_addr: 127.0.0.1:9999
nodes:
  - name: primary
    listen_addr: 127.0.0.1:6432
    upstream_addr: 127.0.0.1:5432
scenarios:
  - name: s1
    steps:
      - {node: primary, kind: crash, after_ms: 100}
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ControlAddr != "127.0.0.1:9999" {
		t.Errorf("ControlAddr = %q", cfg.ControlAddr)
	}
	if len(cfg.Nodes) != 1 || cfg.Nodes[0].Name != "primary" {
		t.Errorf("unexpected nodes: %+v", cfg.Nodes)
	}
	if len(cfg.Scenarios) != 1 || cfg.Scenarios[0].Steps[0].AfterMs != 100 {
		t.Errorf("unexpected scenarios: %+v", cfg.Scenarios)
	}
}

func TestLoadDefaultsControlAddr(t *testing.T) {
	path := writeConfig(t, "nodes:\n  - {name: n, listen_addr: a, upstream_addr: b}\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ControlAddr != "127.0.0.1:8080" {
		t.Errorf("ControlAddr = %q, want default 127.0.0.1:8080", cfg.ControlAddr)
	}
}

func TestLoadRejectsEmptyNodes(t *testing.T) {
	path := writeConfig(t, "nodes: []\n")
	if _, err := Load(path); err == nil {
		t.Fatal("want error for config with no nodes")
	}
}

func TestLoadRejectsMalformedYAML(t *testing.T) {
	path := writeConfig(t, "nodes:\n  - name: a\n   listen_addr: bad indent\n")
	if _, err := Load(path); err == nil {
		t.Fatal("want error for malformed YAML")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("want error for missing file")
	}
}

func TestFindNodeAndScenario(t *testing.T) {
	cfg := &Config{
		Nodes:     []Node{{Name: "a"}, {Name: "b"}},
		Scenarios: []Scenario{{Name: "s"}},
	}
	if n := cfg.FindNode("b"); n == nil || n.Name != "b" {
		t.Errorf("FindNode(b) = %+v", n)
	}
	if cfg.FindNode("missing") != nil {
		t.Error("FindNode(missing) should be nil")
	}
	if s := cfg.FindScenario("s"); s == nil || s.Name != "s" {
		t.Errorf("FindScenario(s) = %+v", s)
	}
	if cfg.FindScenario("missing") != nil {
		t.Error("FindScenario(missing) should be nil")
	}
}

func TestLoadNodeExtrasAndValidation(t *testing.T) {
	path := writeConfig(t, `
nodes:
  - name: primary
    listen_addr: a
    upstream_addr: b
    role: primary
    target: {type: ssh, host: db1, inner: {type: docker, container: dbfailsim-primary, network: dbnet}}
  - {name: primary-repl, listen_addr: c, upstream_addr: d, stream: replication}
scenarios:
  - name: s
    steps:
      - node: primary-repl
        kind: replica_lag
        for_ms: 2000
        params: {delay: 1500ms, buffer_size: 3, probability: 0.5}
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	tgt := cfg.Nodes[0].Target
	if cfg.Nodes[0].Role != "primary" || tgt == nil || tgt.Type != "ssh" || tgt.Host != "db1" ||
		tgt.Inner == nil || tgt.Inner.Type != "docker" || tgt.Inner.Container != "dbfailsim-primary" || tgt.Inner.Network != "dbnet" {
		t.Errorf("node target not parsed: %+v", cfg.Nodes[0])
	}
	if cfg.Nodes[1].Target != nil {
		t.Error("node without target should have nil Target")
	}
	if !cfg.Nodes[1].IsReplicationStream() || cfg.Nodes[0].IsReplicationStream() {
		t.Error("stream classification wrong")
	}
	st := cfg.Scenarios[0].Steps[0]
	if st.For() != 2*time.Second || st.DurationParam("delay", 0) != 1500*time.Millisecond {
		t.Errorf("step timing/params wrong: %+v", st)
	}
	// YAML decodes 3 as int and 0.5 as float64; both must come through.
	if st.IntParam("buffer_size", 0) != 3 || st.FloatParam("probability", 0) != 0.5 {
		t.Errorf("yaml numeric params wrong: %+v", st.Params)
	}
	for _, bad := range []string{
		"nodes:\n  - {name: a, listen_addr: x, upstream_addr: y}\n  - {name: a, listen_addr: x, upstream_addr: y}\n",
		"nodes:\n  - {name: a, listen_addr: x, upstream_addr: y, stream: sideways}\n",
		"nodes:\n  - {name: a, listen_addr: x, upstream_addr: y}\nscenarios:\n  - name: s\n    steps: [{node: ghost, kind: crash}]\n",
		"nodes:\n  - {name: a, listen_addr: x, upstream_addr: y}\nscenarios:\n  - name: s\n    steps: [{node: a}]\n",
		"nodes: [\n",
		"nodes:\n  - {name: a, listen_addr: x, upstream_addr: y, target: {type: docker}}\n",
		"nodes:\n  - {name: a, listen_addr: x, upstream_addr: y, target: {type: process}}\n",
		"nodes:\n  - {name: a, listen_addr: x, upstream_addr: y, target: {type: lxc, container: c}}\n",
		"nodes:\n  - {name: a, listen_addr: x, upstream_addr: y, target: {type: ssh, host: h, inner: {type: ssh, host: h2}}}\n",
	} {
		if _, err := Load(writeConfig(t, bad)); err == nil {
			t.Errorf("Load accepted invalid config: %s", bad)
		}
	}
}

func TestFaultStepParamAccessors(t *testing.T) {
	st := FaultStep{Params: map[string]any{
		"n": float64(5), "f": 0.25, "s": "abc", "d": "750ms", "dms": float64(200), "big": float64(50 * 1024 * 1024),
	}}
	if st.IntParam("n", 0) != 5 || st.IntParam("missing", 7) != 7 || st.IntParam("f", 9) != 9 {
		t.Error("IntParam")
	}
	if st.FloatParam("f", 0) != 0.25 || st.FloatParam("n", 0) != 5 {
		t.Error("FloatParam")
	}
	if st.StringParam("s", "") != "abc" || st.StringParam("n", "def") != "def" {
		t.Error("StringParam")
	}
	if st.DurationParam("d", 0) != 750*time.Millisecond || st.DurationParam("dms", 0) != 200*time.Millisecond || st.DurationParam("missing", time.Second) != time.Second {
		t.Error("DurationParam")
	}
	if st.Int64Param("big", 0) != 50*1024*1024 {
		t.Error("Int64Param")
	}
}
