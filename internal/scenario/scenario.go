// Package scenario runs named, timed sequences of fault injections across a
// set of proxies, so a failure pattern like "split brain" or "replica lag"
// is reproducible with one command instead of manual fault toggling.
package scenario

import (
	"fmt"
	"log"
	"time"

	"github.com/jimroxodezi/dbfailsim/internal/config"
	"github.com/jimroxodezi/dbfailsim/internal/proxy"
)

// Engine runs scenarios against a set of live proxies, keyed by node name.
type Engine struct {
	Proxies map[string]*proxy.Proxy
}

// New creates an Engine over the given proxies.
func New(proxies map[string]*proxy.Proxy) *Engine {
	return &Engine{Proxies: proxies}
}

// Run executes a scenario's steps in order, honoring each step's AfterMs
// offset from scenario start. It blocks until all steps have been applied.
// Faults are NOT automatically healed at the end — call Heal explicitly (or
// include "heal" steps in the scenario) so you can inspect the broken state
// with `dbfailsim check` before recovering.
func (e *Engine) Run(s *config.Scenario) error {
	log.Printf("running scenario %q: %s", s.Name, s.Description)
	start := time.Now()
	for _, step := range s.Steps {
		target := time.Duration(step.AfterMs) * time.Millisecond
		if wait := target - time.Since(start); wait > 0 {
			time.Sleep(wait)
		}
		if err := e.applyStep(step); err != nil {
			return err
		}
	}
	log.Printf("scenario %q complete", s.Name)
	return nil
}

func (e *Engine) applyStep(step config.FaultStep) error {
	p, ok := e.Proxies[step.Node]
	if !ok {
		return fmt.Errorf("scenario references unknown node %q", step.Node)
	}
	switch step.Kind {
	case "latency":
		log.Printf("[%s] injecting %dms latency", step.Node, step.LatencyMs)
		p.State.SetLatency(step.LatencyMs)
	case "drop":
		log.Printf("[%s] injecting %d%% packet drop", step.Node, step.DropPercent)
		p.State.SetDrop(step.DropPercent)
	case "partition":
		log.Printf("[%s] partitioning node (unreachable)", step.Node)
		p.State.SetPartitioned(true)
	case "crash":
		log.Printf("[%s] crashing node", step.Node)
		p.State.SetCrashed(true)
	case "heal":
		log.Printf("[%s] healing node", step.Node)
		p.State.Heal()
	default:
		return fmt.Errorf("unknown fault kind %q", step.Kind)
	}
	return nil
}

// HealAll clears fault state on every proxy, restoring normal operation.
func (e *Engine) HealAll() {
	for name, p := range e.Proxies {
		p.State.Heal()
		log.Printf("[%s] healed", name)
	}
}

// BuiltinScenarios returns a small library of common failure patterns,
// parameterized over the given node names, useful as a starting point when
// a config doesn't define its own scenarios. primary/replica should be
// existing node names from the config.
func BuiltinScenarios(primary string, replicas []string) []config.Scenario {
	var scenarios []config.Scenario

	if len(replicas) > 0 {
		steps := []config.FaultStep{
			{Node: replicas[0], Kind: "latency", LatencyMs: 2000, AfterMs: 0},
			{Node: replicas[0], Kind: "drop", DropPercent: 15, AfterMs: 0},
		}
		scenarios = append(scenarios, config.Scenario{
			Name:        "replica-lag",
			Description: "Replica falls behind: added latency + intermittent drops on the replication path.",
			Steps:       steps,
		})
	}

	if len(replicas) >= 2 {
		steps := []config.FaultStep{
			{Node: primary, Kind: "partition", AfterMs: 0},
			{Node: replicas[0], Kind: "partition", AfterMs: 0},
			// replicas[1] and beyond remain reachable, forming the other side
			// of the split — clients that land there see a different
			// (stale or divergent) view of the data.
		}
		scenarios = append(scenarios, config.Scenario{
			Name:        "split-brain",
			Description: "Cluster splits into two partitions: primary + one replica isolated from the rest.",
			Steps:       steps,
		})
	}

	scenarios = append(scenarios, config.Scenario{
		Name:        "node-crash",
		Description: fmt.Sprintf("Node %q crashes outright: all connections severed, new ones refused.", primary),
		Steps: []config.FaultStep{
			{Node: primary, Kind: "crash", AfterMs: 0},
		},
	})

	return scenarios
}
