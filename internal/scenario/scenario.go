// Package scenario runs named, timed sequences of fault injections across a
// set of proxies, so a failure pattern like "split brain" or "replica lag"
// is reproducible with one command instead of manual fault toggling.
//
// The Engine is the single place that turns a config.FaultStep into an
// action: proxy-level state flags (partition, crash, heal), connection /
// packet / dial / read-hook faults registered on a proxy's Registry, or
// node-level faults executed against a container. The control API's
// one-off fault endpoint goes through the same Apply path as scenarios.
package scenario

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/jimroxodezi/dbfailsim/internal/config"
	"github.com/jimroxodezi/dbfailsim/internal/faults"
	"github.com/jimroxodezi/dbfailsim/internal/proxy"
)

// Engine runs scenarios against a set of live proxies, keyed by node name.
type Engine struct {
	Proxies map[string]*proxy.Proxy
	cfg     *config.Config // may be nil: node-level faults then unavailable

	mu       sync.Mutex
	injected []injectedNodeFault // outstanding node faults needing Revert on heal
}

type injectedNodeFault struct {
	fault  faults.NodeFault
	nodeID string
}

// New creates an Engine over the given proxies. cfg supplies container IDs
// for node-level faults; pass nil when only proxy-level faults are needed.
func New(proxies map[string]*proxy.Proxy, cfg *config.Config) *Engine {
	return &Engine{Proxies: proxies, cfg: cfg}
}

// Kinds lists every fault kind Apply understands, for help text.
func Kinds() []string {
	return []string{
		"latency", "drop", "partition", "crash", "heal",
		"bandwidth_throttle", "bursty_loss", "reorder", "duplication", "asymmetric_partition", "tcp_rst",
		"dns_failure",
		"replica_lag", "wal_delay", "wal_corruption", "query_corruption", "stale_read",
		"node_crash", "clock_skew", "cpu_throttle", "oom", "zombie",
	}
}

// Run executes a scenario's steps in order, honoring each step's AfterMs
// offset from scenario start. It blocks until all steps have been applied
// (not until their for_ms windows expire — those are handled by timers).
// Faults are NOT automatically healed at the end — call HealAll explicitly
// (or include "heal" steps in the scenario) so you can inspect the broken
// state with `dbfailsim check` before recovering.
func (e *Engine) Run(ctx context.Context, s *config.Scenario) error {
	log.Printf("running scenario %q: %s", s.Name, s.Description)
	steps := append([]config.FaultStep(nil), s.Steps...)
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].AfterMs < steps[j].AfterMs })

	start := time.Now()
	for _, step := range steps {
		if wait := step.After() - time.Since(start); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := e.Apply(ctx, step); err != nil {
			return err
		}
	}
	log.Printf("scenario %q complete", s.Name)
	return nil
}

// Apply performs one fault step immediately. It is the shared entry point
// for scenario steps and the control API's POST /nodes/{node}/fault.
func (e *Engine) Apply(ctx context.Context, step config.FaultStep) error {
	targets, err := e.targets(step)
	if err != nil {
		return err
	}
	switch step.Kind {

	// --- proxy state flags -------------------------------------------
	case "partition":
		for _, p := range targets {
			log.Printf("[%s] partitioning node (unreachable)", p.Name)
			p.State.SetPartitioned(true)
			e.expireState(step, p, func() { p.State.SetPartitioned(false) })
		}
	case "crash":
		for _, p := range targets {
			log.Printf("[%s] crashing node (proxy-level)", p.Name)
			p.State.SetCrashed(true)
			e.expireState(step, p, func() { p.State.SetCrashed(false) })
		}
	case "heal":
		for _, p := range targets {
			log.Printf("[%s] healing node", p.Name)
			p.State.Heal()
		}
		e.revertNodeFaults(ctx, func(nodeID string) bool { return e.ownsNode(step, nodeID) })

	// --- connection-level faults -------------------------------------
	case "latency":
		delay := step.DurationParam("delay", time.Duration(step.LatencyMs)*time.Millisecond)
		f := faults.NewLatencyInjectionFault(delay, step.DurationParam("jitter", 0))
		f.RampTo = step.DurationParam("ramp_to", 0)
		f.RampOver = step.DurationParam("ramp_over", 0)
		log.Printf("[%s] injecting latency %v", step.Node, delay)
		return e.registerConn(step, targets, f)

	case "drop":
		percent := step.IntParam("drop_percent", step.DropPercent)
		if percent < 0 || percent > 100 {
			return fmt.Errorf("drop_percent must be 0-100, got %d", percent)
		}
		log.Printf("[%s] injecting %d%% packet drop", step.Node, percent)
		if percent == 0 {
			for _, p := range targets {
				p.Registry.Unregister("bursty_loss")
			}
			return nil
		}
		rate := float64(percent) / 100
		return e.registerConn(step, targets, &faults.BurstyLossFault{BaselineLoss: rate, BurstLoss: rate})

	case "bursty_loss":
		return e.registerConn(step, targets, &faults.BurstyLossFault{
			BaselineLoss: step.FloatParam("baseline_loss", 0.01),
			BurstLoss:    step.FloatParam("burst_loss", 0.4),
			BurstEvery:   step.DurationParam("burst_every", 30*time.Second),
			BurstFor:     step.DurationParam("burst_for", 5*time.Second),
		})

	case "bandwidth_throttle":
		return e.registerConn(step, targets, faults.NewBandwidthThrottleFault(
			step.Int64Param("rate_bytes_per_sec", 64*1024),
			step.Int64Param("burst_bytes", 64*1024),
		))

	case "reorder":
		return e.registerConn(step, targets, &faults.ReorderFault{
			BufferSize:  step.IntParam("buffer_size", 5),
			Probability: step.FloatParam("probability", 0.1),
		})

	case "duplication":
		return e.registerConn(step, targets, &faults.DuplicationFault{
			Probability:    step.FloatParam("probability", 0.05),
			MaxExtraCopies: step.IntParam("max_extra_copies", 1),
		})

	case "asymmetric_partition":
		dir := faults.DirectionClientToUpstream
		switch d := step.StringParam("block_direction", "client_to_upstream"); d {
		case "client_to_upstream", "outbound":
		case "upstream_to_client", "inbound":
			dir = faults.DirectionUpstreamToClient
		default:
			return fmt.Errorf("unknown block_direction %q", d)
		}
		return e.registerConn(step, targets, &faults.AsymmetricPartitionFault{BlockDirection: dir})

	case "tcp_rst":
		return e.registerConn(step, targets, &faults.RSTFault{})

	// --- dial-level ----------------------------------------------------
	case "dns_failure":
		for _, p := range targets {
			host, _, err := net.SplitHostPort(p.UpstreamAddr)
			if err != nil {
				host = p.UpstreamAddr
			}
			f := &faults.DNSFailureFault{ServiceName: step.StringParam("service", host)}
			window := step.For()
			if window <= 0 {
				window = 24 * time.Hour // until healed
			}
			f.Trigger(window)
			p.Registry.RegisterDialFault(f)
			e.expireRegistry(step, p.Registry, f.Name())
		}

	// --- packet-level (gated by the proxy's Stream) ----------------------
	case "replica_lag":
		return e.registerPacket(step, targets, &faults.ReplicaLagFault{
			Delay:  step.DurationParam("delay", 2*time.Second),
			Jitter: step.DurationParam("jitter", 0),
		})
	case "wal_delay":
		return e.registerPacket(step, targets, &faults.WALDelayFault{FlushDelay: step.DurationParam("flush_delay", time.Second)})
	case "wal_corruption":
		return e.registerPacket(step, targets, &faults.WALCorruptionFault{
			Probability: step.FloatParam("probability", 0.01),
			BytesToFlip: step.IntParam("bytes_to_flip", 1),
		})
	case "query_corruption":
		return e.registerPacket(step, targets, &faults.QueryCorruptionFault{
			Probability: step.FloatParam("probability", 0.01),
			BytesToFlip: step.IntParam("bytes_to_flip", 1),
		})

	// --- read hook -------------------------------------------------------
	case "stale_read":
		f := faults.NewStaleReadFault(step.FloatParam("probability", 0.3), step.DurationParam("max_age", 10*time.Second))
		for _, p := range targets {
			p.Registry.RegisterReadHook(f)
			e.expireRegistry(step, p.Registry, f.Name())
		}

	// --- node-level (docker) ----------------------------------------------
	case "node_crash":
		sig := syscall.SIGKILL
		switch s := step.StringParam("signal", "SIGKILL"); s {
		case "SIGKILL", "KILL":
		case "SIGTERM", "TERM":
			sig = syscall.SIGTERM
		default:
			return fmt.Errorf("unknown signal %q (want SIGKILL|SIGTERM)", s)
		}
		return e.runNodeFault(ctx, step, &faults.CrashFault{Signal: sig})
	case "clock_skew":
		return e.runNodeFault(ctx, step, &faults.ClockSkewFault{Offset: step.DurationParam("offset", time.Minute)})
	case "cpu_throttle":
		return e.runNodeFault(ctx, step, &faults.CPUThrottleFault{CPUQuota: step.FloatParam("cpu_quota", 0.1)})
	case "oom":
		return e.runNodeFault(ctx, step, &faults.OOMFault{LimitBytes: step.Int64Param("limit_bytes", 50*1024*1024)})
	case "zombie":
		return e.runNodeFault(ctx, step, &faults.ZombieFault{})

	default:
		return fmt.Errorf("unknown fault kind %q (known: %v)", step.Kind, Kinds())
	}
	return nil
}

// HealAll clears fault state on every proxy and reverts every outstanding
// node-level fault, restoring normal operation.
func (e *Engine) HealAll() {
	for name, p := range e.Proxies {
		p.State.Heal()
		log.Printf("[%s] healed", name)
	}
	e.revertNodeFaults(context.Background(), func(string) bool { return true })
}

// targets resolves step.Node to proxies: "*" means every proxy.
func (e *Engine) targets(step config.FaultStep) ([]*proxy.Proxy, error) {
	if step.Node == "*" {
		out := make([]*proxy.Proxy, 0, len(e.Proxies))
		for _, p := range e.Proxies {
			out = append(out, p)
		}
		return out, nil
	}
	p, ok := e.Proxies[step.Node]
	if !ok {
		return nil, fmt.Errorf("scenario references unknown node %q", step.Node)
	}
	return []*proxy.Proxy{p}, nil
}

func (e *Engine) registerConn(step config.FaultStep, targets []*proxy.Proxy, f faults.Fault) error {
	for _, p := range targets {
		p.Registry.RegisterConnFault(f)
		e.expireRegistry(step, p.Registry, f.Name())
	}
	return nil
}

func (e *Engine) registerPacket(step config.FaultStep, targets []*proxy.Proxy, f faults.PacketFault) error {
	for _, p := range targets {
		p.Registry.RegisterPacketFault(f)
		e.expireRegistry(step, p.Registry, f.Name())
	}
	return nil
}

// expireRegistry unregisters name after the step's for_ms window.
func (e *Engine) expireRegistry(step config.FaultStep, reg *proxy.Registry, name string) {
	if d := step.For(); d > 0 {
		time.AfterFunc(d, func() { reg.Unregister(name) })
	}
}

// expireState undoes a partition/crash flag after the step's for_ms window.
func (e *Engine) expireState(step config.FaultStep, p *proxy.Proxy, undo func()) {
	if d := step.For(); d > 0 {
		time.AfterFunc(d, func() {
			log.Printf("[%s] %s window elapsed, clearing", p.Name, step.Kind)
			undo()
		})
	}
}

// runNodeFault injects nf against the step's node container now and
// schedules Revert after for_ms. The revert uses its own context so a
// finished scenario cannot strand a container in the faulted state.
func (e *Engine) runNodeFault(ctx context.Context, step config.FaultStep, nf faults.NodeFault) error {
	if e.cfg == nil {
		return fmt.Errorf("fault %s: node-level faults need a config with container_id", step.Kind)
	}
	var nodes []*config.Node
	if step.Node == "*" {
		for i := range e.cfg.Nodes {
			nodes = append(nodes, &e.cfg.Nodes[i])
		}
	} else if n := e.cfg.FindNode(step.Node); n != nil {
		nodes = append(nodes, n)
	} else {
		return fmt.Errorf("fault %s: unknown node %q", step.Kind, step.Node)
	}
	for _, n := range nodes {
		if n.ContainerID == "" {
			return fmt.Errorf("fault %s: node %q has no container_id", step.Kind, n.Name)
		}
		log.Printf("[%s] injecting node fault %s on container %s", n.Name, nf.Name(), n.ContainerID)
		if err := nf.Inject(ctx, n.ContainerID); err != nil {
			return err
		}
		e.mu.Lock()
		e.injected = append(e.injected, injectedNodeFault{fault: nf, nodeID: n.ContainerID})
		e.mu.Unlock()
		if d := step.For(); d > 0 {
			id := n.ContainerID
			time.AfterFunc(d, func() {
				e.revertNodeFaults(context.Background(), func(nodeID string) bool { return nodeID == id })
			})
		}
	}
	return nil
}

// revertNodeFaults reverts (and forgets) every outstanding node fault whose
// container matches.
func (e *Engine) revertNodeFaults(ctx context.Context, match func(nodeID string) bool) {
	e.mu.Lock()
	var keep, revert []injectedNodeFault
	for _, inj := range e.injected {
		if match(inj.nodeID) {
			revert = append(revert, inj)
		} else {
			keep = append(keep, inj)
		}
	}
	e.injected = keep
	e.mu.Unlock()
	for _, inj := range revert {
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := inj.fault.Revert(rctx, inj.nodeID); err != nil {
			log.Printf("revert %s on %s failed: %v", inj.fault.Name(), inj.nodeID, err)
		}
		cancel()
	}
}

// ownsNode reports whether a heal step for step.Node covers the container.
func (e *Engine) ownsNode(step config.FaultStep, nodeID string) bool {
	if step.Node == "*" || e.cfg == nil {
		return true
	}
	n := e.cfg.FindNode(step.Node)
	return n != nil && n.ContainerID == nodeID
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
