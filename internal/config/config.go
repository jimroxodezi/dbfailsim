// Package config defines the JSON configuration format for dbfailsim.
//
// A config describes the real database nodes you want to run chaos
// experiments against. Each node gets a local TCP proxy that sits between
// your application and the real database, so you point your app at the
// proxy instead of the database directly. dbfailsim can then inject faults
// (latency, drops, partitions, crashes) on that connection without touching
// the real database process.
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Node describes one database node (e.g. a Postgres primary or a replica)
// and the proxy that fronts it.
type Node struct {
	// Name is a short identifier used in scenarios and CLI commands, e.g. "primary", "replica-1".
	Name string `json:"name"`

	// ListenAddr is where dbfailsim's proxy listens, e.g. "127.0.0.1:6432".
	// Point your application's connection string at this address instead of
	// the real database.
	ListenAddr string `json:"listen_addr"`

	// UpstreamAddr is the real database address, e.g. "127.0.0.1:5432".
	UpstreamAddr string `json:"upstream_addr"`

	// CheckCommand is a shell command template used by `dbfailsim check` to
	// query this node directly (bypassing the proxy) and see what data a
	// client connected to this node would actually observe. Use {query} as
	// a placeholder for the query text passed on the CLI.
	//
	// Example for Postgres:
	//   "psql postgres://user:pass@127.0.0.1:5432/mydb -t -c \"{query}\""
	CheckCommand string `json:"check_command,omitempty"`
}

// FaultStep is one timed fault change applied to a named node during a scenario.
type FaultStep struct {
	Node string `json:"node"`

	// Kind is one of: "latency", "drop", "partition", "crash", "heal".
	Kind string `json:"kind"`

	// LatencyMs is used when Kind == "latency": added round-trip delay.
	LatencyMs int `json:"latency_ms,omitempty"`

	// DropPercent is used when Kind == "drop": percent chance (0-100) a
	// given read/write is dropped, simulating lossy replication links.
	DropPercent int `json:"drop_percent,omitempty"`

	// AfterMs delays this step relative to scenario start.
	AfterMs int `json:"after_ms"`
}

// Scenario is a named, reproducible failure pattern made of ordered fault steps.
type Scenario struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Steps       []FaultStep `json:"steps"`
}

// Config is the full dbfailsim configuration.
type Config struct {
	Nodes     []Node     `json:"nodes"`
	Scenarios []Scenario `json:"scenarios"`
	// ControlAddr is where the HTTP control/status API listens, e.g. "127.0.0.1:8080".
	ControlAddr string `json:"control_addr"`
	// ControlToken, when set, requires every control API request to carry
	// "Authorization: Bearer <token>". The CLI subcommands read it from the
	// same config file. The DBFAILSIM_CONTROL_TOKEN environment variable
	// overrides this field on both server and CLI, so the token can be kept
	// out of the config file entirely. Empty means no authentication.
	ControlToken string `json:"control_token,omitempty"`
}

// Load reads and parses a Config from a JSON file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if c.ControlAddr == "" {
		c.ControlAddr = "127.0.0.1:8080"
	}
	if v := os.Getenv("DBFAILSIM_CONTROL_TOKEN"); v != "" {
		c.ControlToken = v
	}
	if len(c.Nodes) == 0 {
		return nil, fmt.Errorf("config has no nodes defined")
	}
	return &c, nil
}

// FindNode returns the node with the given name, or nil.
func (c *Config) FindNode(name string) *Node {
	for i := range c.Nodes {
		if c.Nodes[i].Name == name {
			return &c.Nodes[i]
		}
	}
	return nil
}

// FindScenario returns the scenario with the given name, or nil.
func (c *Config) FindScenario(name string) *Scenario {
	for i := range c.Scenarios {
		if c.Scenarios[i].Name == name {
			return &c.Scenarios[i]
		}
	}
	return nil
}
