package scenario

import "time"

// Config is the top-level YAML document.
type Config struct {
	Listen   string          `yaml:"listen"`
	Upstream UpstreamConfig  `yaml:"upstream"`
	Nodes    []NodeConfig    `yaml:"nodes,omitempty"`
	Scenarios []ScenarioSpec `yaml:"scenarios"`
}

type UpstreamConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// NodeConfig maps a logical node name to its container ID/name, used by
// node-level and DB-aware faults that act via `docker exec`/`docker kill`.
type NodeConfig struct {
	Name        string `yaml:"name"`
	ContainerID string `yaml:"container_id"`
	Role        string `yaml:"role,omitempty"` // "primary", "replica", "voter", etc.
}

// ScenarioSpec is a named, timed sequence of faults.
type ScenarioSpec struct {
	Name     string       `yaml:"name"`
	Duration Duration     `yaml:"duration"`
	Faults   []FaultSpec  `yaml:"faults"`
}

// FaultSpec describes one fault instance: its type, when it starts/ends
// relative to scenario start, and type-specific parameters.
type FaultSpec struct {
	Type   string                 `yaml:"type"`
	At     Duration               `yaml:"at"`               // offset from scenario start
	For    Duration               `yaml:"for,omitempty"`    // duration the fault stays active
	Target string                 `yaml:"target,omitempty"` // node name, "*" for all
	Params map[string]interface{} `yaml:"params,omitempty"`
}

// Duration wraps time.Duration to parse plain strings like "500ms" or "2m"
// directly from YAML.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}