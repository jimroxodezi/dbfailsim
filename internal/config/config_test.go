package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad(t *testing.T) {
	path := writeConfig(t, `{
		"control_addr": "127.0.0.1:9999",
		"nodes": [
			{"name": "primary", "listen_addr": "127.0.0.1:6432", "upstream_addr": "127.0.0.1:5432"}
		],
		"scenarios": [
			{"name": "s1", "steps": [{"node": "primary", "kind": "crash", "after_ms": 100}]}
		]
	}`)

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
	path := writeConfig(t, `{"nodes": [{"name": "n", "listen_addr": "a", "upstream_addr": "b"}]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ControlAddr != "127.0.0.1:8080" {
		t.Errorf("ControlAddr = %q, want default 127.0.0.1:8080", cfg.ControlAddr)
	}
}

func TestLoadRejectsEmptyNodes(t *testing.T) {
	path := writeConfig(t, `{"nodes": []}`)
	if _, err := Load(path); err == nil {
		t.Fatal("want error for config with no nodes")
	}
}

func TestLoadRejectsBadJSON(t *testing.T) {
	path := writeConfig(t, `{not json`)
	if _, err := Load(path); err == nil {
		t.Fatal("want error for malformed JSON")
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
