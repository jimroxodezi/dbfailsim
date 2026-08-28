// Package config defines the single YAML configuration format for dbfailsim.
//
// A config describes the real database nodes you want to run chaos
// experiments against. Each node gets a local TCP proxy that sits between
// your application and the real database, so you point your app at the
// proxy instead of the database directly. dbfailsim can then inject faults
// on that connection without touching the real database process, and — for
// nodes that declare a target — act on the node itself (kill, freeze,
// throttle) through the matching driver: a local process, a systemd unit,
// a docker container, or any of those over ssh.
//
// Scenarios are named, timed sequences of FaultSteps. A FaultStep names a
// node, a fault kind, an offset, an optional duration, and kind-specific
// params; the scenario engine resolves the kind through the faults package.
package config

import (
	"fmt"
	"math"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Node describes one database node (e.g. a Postgres primary or a replica)
// and the proxy that fronts it.
type Node struct {
	// Name is a short identifier used in scenarios and CLI commands, e.g. "primary", "replica-1".
	Name string `yaml:"name" json:"name"`

	// ListenAddr is where dbfailsim's proxy listens, e.g. "127.0.0.1:6432".
	// Point your application's connection string at this address instead of
	// the real database.
	ListenAddr string `yaml:"listen_addr" json:"listen_addr"`

	// UpstreamAddr is the real database address, e.g. "127.0.0.1:5432".
	UpstreamAddr string `yaml:"upstream_addr" json:"upstream_addr"`

	// CheckCommand is a shell command template used by `dbfailsim check` to
	// query this node directly (bypassing the proxy) and see what data a
	// client connected to this node would actually observe. Use {query} as
	// a placeholder for the query text passed on the CLI.
	//
	// Example for Postgres:
	//   "psql postgres://user:pass@127.0.0.1:5432/mydb -t -c \"{query}\""
	CheckCommand string `yaml:"check_command,omitempty" json:"check_command,omitempty"`

	// Target says how to reach this node's process for node-level faults
	// (node_crash, zombie, cpu_throttle, oom, clock_skew). Nil means the
	// node is only reachable through its proxy — a managed service, for
	// example — and node-level faults are unavailable for it.
	Target *NodeTarget `yaml:"target,omitempty" json:"target,omitempty"`

	// Role is a free-form label ("primary", "replica", "voter") surfaced
	// to topology-aware faults and the dashboard. Optional.
	Role string `yaml:"role,omitempty" json:"role,omitempty"`

	// Stream classifies the traffic this proxy carries: "query" (default)
	// or "replication". Packet-level faults such as replica_lag and
	// wal_delay only act on a replication-stream proxy.
	Stream string `yaml:"stream,omitempty" json:"stream,omitempty"`
}

// NodeTarget describes the deployment behind a node so faults can act on
// the process itself. Exactly one backend is selected by Type:
//
//	target: {type: process, pid_file: /var/run/postgresql/16-main.pid, start_command: "pg_ctlcluster 16 main start"}
//	target: {type: systemd, unit: postgresql}
//	target: {type: docker, container: dbfailsim-primary, network: docker_default}
//	target: {type: ssh, host: db2.internal, inner: {type: systemd, unit: postgresql}}
//
// Not every fault is possible on every backend: a bare process cannot be
// CPU- or memory-limited (use systemd or docker), and a process without
// start_command cannot be restarted after node_crash. The engine reports
// those as errors rather than pretending.
type NodeTarget struct {
	Type string `yaml:"type" json:"type"` // process | docker | systemd | ssh

	// process
	PID          int    `yaml:"pid,omitempty" json:"pid,omitempty"`
	PIDFile      string `yaml:"pid_file,omitempty" json:"pid_file,omitempty"`
	StartCommand string `yaml:"start_command,omitempty" json:"start_command,omitempty"`

	// docker
	Container string `yaml:"container,omitempty" json:"container,omitempty"`
	Network   string `yaml:"network,omitempty" json:"network,omitempty"`

	// systemd
	Unit string `yaml:"unit,omitempty" json:"unit,omitempty"`

	// ssh: run the inner target's commands on Host via the local ssh client
	Host  string      `yaml:"host,omitempty" json:"host,omitempty"`
	Inner *NodeTarget `yaml:"inner,omitempty" json:"inner,omitempty"`
}

// Validate checks that the selected backend has what it needs.
func (t *NodeTarget) Validate() error {
	switch t.Type {
	case "process":
		if t.PID <= 0 && t.PIDFile == "" {
			return fmt.Errorf("target process: need pid or pid_file")
		}
	case "docker":
		if t.Container == "" {
			return fmt.Errorf("target docker: need container")
		}
	case "systemd":
		if t.Unit == "" {
			return fmt.Errorf("target systemd: need unit")
		}
	case "ssh":
		if t.Host == "" || t.Inner == nil {
			return fmt.Errorf("target ssh: need host and inner")
		}
		if t.Inner.Type == "ssh" {
			return fmt.Errorf("target ssh: inner target cannot be ssh")
		}
		return t.Inner.Validate()
	case "":
		return fmt.Errorf("target: type is required (process|docker|systemd|ssh)")
	default:
		return fmt.Errorf("target: unknown type %q (want process|docker|systemd|ssh)", t.Type)
	}
	return nil
}

// IsReplicationStream reports whether the node's proxy carries replication traffic.
func (n *Node) IsReplicationStream() bool { return n.Stream == "replication" }

// FaultStep is one timed fault change applied to a named node during a scenario.
//
// Two ways to describe a fault coexist:
//
//   - the original short form: kind "latency" with latency_ms, kind "drop"
//     with drop_percent, and the parameterless kinds "partition", "crash",
//     "heal";
//
//   - the general form: any kind known to the scenario engine (see
//     `dbfailsim fault -h` or the README fault catalogue) with kind-specific
//     params, e.g.
//
//   - node: replica-1
//     kind: reorder
//     params: {buffer_size: 5}
//
// Both forms may set for_ms, after which the engine unregisters or reverts
// the fault automatically.
type FaultStep struct {
	Node string `yaml:"node" json:"node"`

	// Kind is the fault kind, e.g. "latency", "drop", "partition", "crash",
	// "heal", "reorder", "replica_lag", "node_crash", ...
	Kind string `yaml:"kind" json:"kind"`

	// LatencyMs is the short form for Kind == "latency": added delay.
	LatencyMs int `yaml:"latency_ms,omitempty" json:"latency_ms,omitempty"`

	// DropPercent is the short form for Kind == "drop": percent chance
	// (0-100) a given chunk is dropped, simulating lossy links.
	DropPercent int `yaml:"drop_percent,omitempty" json:"drop_percent,omitempty"`

	// Params carries kind-specific parameters for the general form.
	// Durations may be given as strings ("500ms", "2s") or as numbers of
	// milliseconds. YAML decodes numbers as int or float64; the accessors
	// below accept either.
	Params map[string]any `yaml:"params,omitempty" json:"params,omitempty"`

	// AfterMs delays this step relative to scenario start.
	AfterMs int `yaml:"after_ms" json:"after_ms"`

	// ForMs, when > 0, is how long the fault stays active before the
	// engine removes it (unregister for proxy faults, Revert for node
	// faults). 0 means until healed.
	ForMs int `yaml:"for_ms,omitempty" json:"for_ms,omitempty"`
}

// After returns the step's offset from scenario start.
func (s FaultStep) After() time.Duration { return time.Duration(s.AfterMs) * time.Millisecond }

// For returns how long the fault stays active; 0 means until healed.
func (s FaultStep) For() time.Duration { return time.Duration(s.ForMs) * time.Millisecond }

// Param accessors. Each returns def when the key is absent or of the wrong
// type. YAML decodes integers as int and decimals as float64 (and the
// control API's JSON body decodes every number as float64), so the numeric
// accessors accept both.

func (s FaultStep) IntParam(key string, def int) int {
	switch v := s.Params[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		if v == math.Trunc(v) {
			return int(v)
		}
	}
	return def
}

func (s FaultStep) Int64Param(key string, def int64) int64 {
	switch v := s.Params[key].(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	}
	return def
}

func (s FaultStep) FloatParam(key string, def float64) float64 {
	switch v := s.Params[key].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	}
	return def
}

func (s FaultStep) StringParam(key, def string) string {
	if v, ok := s.Params[key].(string); ok {
		return v
	}
	return def
}

// DurationParam accepts "500ms"-style strings or a number of milliseconds.
func (s FaultStep) DurationParam(key string, def time.Duration) time.Duration {
	switch v := s.Params[key].(type) {
	case string:
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	case float64:
		return time.Duration(v) * time.Millisecond
	case int:
		return time.Duration(v) * time.Millisecond
	case int64:
		return time.Duration(v) * time.Millisecond
	}
	return def
}

// Scenario is a named, reproducible failure pattern made of ordered fault steps.
type Scenario struct {
	Name        string      `yaml:"name" json:"name"`
	Description string      `yaml:"description" json:"description"`
	Steps       []FaultStep `yaml:"steps" json:"steps"`
}

// Config is the full dbfailsim configuration.
type Config struct {
	Nodes     []Node     `yaml:"nodes" json:"nodes"`
	Scenarios []Scenario `yaml:"scenarios" json:"scenarios"`
	// ControlAddr is where the HTTP control/status API listens, e.g. "127.0.0.1:8080".
	ControlAddr string `yaml:"control_addr" json:"control_addr"`
	// ControlToken, when set, requires every control API request to carry
	// "Authorization: Bearer <token>". The CLI subcommands read it from the
	// same config file. The DBFAILSIM_CONTROL_TOKEN environment variable
	// overrides this field on both server and CLI, so the token can be kept
	// out of the config file entirely. Empty means no authentication.
	ControlToken string `yaml:"control_token,omitempty" json:"control_token,omitempty"`
}

// Load reads and parses a Config from a YAML file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.ControlAddr == "" {
		c.ControlAddr = "127.0.0.1:8080"
	}
	if v := os.Getenv("DBFAILSIM_CONTROL_TOKEN"); v != "" {
		c.ControlToken = v
	}
	return &c, nil
}

// Validate checks structural invariants: at least one node, unique node
// names, known stream values, and that scenario steps reference known nodes.
func (c *Config) Validate() error {
	if len(c.Nodes) == 0 {
		return fmt.Errorf("config has no nodes defined")
	}
	seen := make(map[string]bool, len(c.Nodes))
	for _, n := range c.Nodes {
		if n.Name == "" {
			return fmt.Errorf("config: node with empty name")
		}
		if seen[n.Name] {
			return fmt.Errorf("config: duplicate node name %q", n.Name)
		}
		seen[n.Name] = true
		switch n.Stream {
		case "", "query", "replication":
		default:
			return fmt.Errorf("config: node %q has unknown stream %q (want query|replication)", n.Name, n.Stream)
		}
		if n.Target != nil {
			if err := n.Target.Validate(); err != nil {
				return fmt.Errorf("config: node %q: %w", n.Name, err)
			}
		}
	}
	for _, sc := range c.Scenarios {
		for i, st := range sc.Steps {
			if st.Node != "*" && !seen[st.Node] {
				return fmt.Errorf("config: scenario %q step %d references unknown node %q", sc.Name, i, st.Node)
			}
			if st.Kind == "" {
				return fmt.Errorf("config: scenario %q step %d has no kind", sc.Name, i)
			}
		}
	}
	return nil
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
