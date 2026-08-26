package scenario

import (
	"context"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/jimroxodezi/dbfailsim/internal/faults"
	"github.com/jimroxodezi/dbfailsim/internal/proxy"
	"gopkg.in/yaml.v3"
)

// Load reads and parses a scenario config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	return &cfg, nil
}

// Runner executes a single ScenarioSpec, firing each FaultSpec at its
// configured offset and reverting node-level faults after their `for`
// window elapses.
type Runner struct {
	cfg     *Config
	nodes   map[string]NodeConfig
	proxies map[string]*proxy.Proxy // keyed by node name; each owns its fault Registry
}

// NewRunner creates a Runner over the live proxies. Connection-, packet-,
// dial-, and read-hook faults are registered on the proxy named by each
// FaultSpec.Target ("*" = every proxy); node faults use the container ID
// from cfg.Nodes.
func NewRunner(cfg *Config, proxies map[string]*proxy.Proxy) *Runner {
	nodes := make(map[string]NodeConfig, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		nodes[n.Name] = n
	}
	return &Runner{cfg: cfg, nodes: nodes, proxies: proxies}
}

// Run executes the named scenario to completion (blocking).
func (r *Runner) Run(ctx context.Context, scenarioName string) error {
	var spec *ScenarioSpec
	for i := range r.cfg.Scenarios {
		if r.cfg.Scenarios[i].Name == scenarioName {
			spec = &r.cfg.Scenarios[i]
			break
		}
	}
	if spec == nil {
		return fmt.Errorf("scenario %q not found", scenarioName)
	}

	scenarioCtx, cancel := context.WithTimeout(ctx, spec.Duration.Duration)
	defer cancel()

	for _, fs := range spec.Faults {
		fs := fs
		go func() {
			select {
			case <-time.After(fs.At.Duration):
				if err := r.fireFault(scenarioCtx, fs); err != nil {
					fmt.Fprintf(os.Stderr, "fault %s failed: %v\n", fs.Type, err)
				}
			case <-scenarioCtx.Done():
			}
		}()
	}

	<-scenarioCtx.Done()
	return nil
}

// fireFault instantiates and injects/applies one fault based on its spec.
// Connection- and packet-level faults (network.go, db.go's PacketFault
// implementations) get registered with the live proxy's fault chain;
// NodeFault implementations run Inject now and Revert after `for`.
func (r *Runner) fireFault(ctx context.Context, fs FaultSpec) error {
	switch fs.Type {
	case "reorder":
		f := &faults.ReorderFault{
			BufferSize:  intParam(fs.Params, "buffer_size", 5),
			Probability: floatParam(fs.Params, "probability", 0.1),
		}
		return r.registerConn(fs, f)

	case "duplication":
		f := &faults.DuplicationFault{
			Probability:    floatParam(fs.Params, "probability", 0.05),
			MaxExtraCopies: intParam(fs.Params, "max_extra_copies", 1),
		}
		return r.registerConn(fs, f)

	case "asymmetric_partition":
		dir := faults.DirectionClientToUpstream
		if strParam(fs.Params, "block_direction", "client_to_upstream") == "upstream_to_client" {
			dir = faults.DirectionUpstreamToClient
		}
		f := &faults.AsymmetricPartitionFault{BlockDirection: dir}
		return r.registerConn(fs, f)

	case "tcp_rst":
		f := &faults.RSTFault{}
		return r.registerConn(fs, f)

	case "bursty_loss":
		f := &faults.BurstyLossFault{
			BaselineLoss: floatParam(fs.Params, "baseline_loss", 0.01),
			BurstLoss:    floatParam(fs.Params, "burst_loss", 0.4),
			BurstEvery:   durationParam(fs.Params, "burst_every", 30*time.Second),
			BurstFor:     durationParam(fs.Params, "burst_for", 5*time.Second),
		}
		return r.registerConn(fs, f)

	case "crash":
		sig := syscall.SIGKILL
		if strParam(fs.Params, "signal", "SIGKILL") == "SIGTERM" {
			sig = syscall.SIGTERM
		}
		return r.runNodeFault(ctx, &faults.CrashFault{Signal: sig}, fs)

	case "clock_skew":
		offset := durationParam(fs.Params, "offset", time.Minute)
		return r.runNodeFault(ctx, &faults.ClockSkewFault{Offset: offset}, fs)

	case "cpu_throttle":
		f := &faults.CPUThrottleFault{CPUQuota: floatParam(fs.Params, "cpu_quota", 0.1)}
		return r.runNodeFault(ctx, f, fs)

	case "oom":
		f := &faults.OOMFault{LimitBytes: int64(intParam(fs.Params, "limit_bytes", 50*1024*1024))}
		return r.runNodeFault(ctx, f, fs)

	case "zombie":
		return r.runNodeFault(ctx, &faults.ZombieFault{}, fs)

	case "replica_lag":
		f := &faults.ReplicaLagFault{
			Delay:  durationParam(fs.Params, "delay", 2*time.Second),
			Jitter: durationParam(fs.Params, "jitter", 0),
		}
		return r.registerPacket(fs, f)

	case "stale_read":
		f := faults.NewStaleReadFault(
			floatParam(fs.Params, "probability", 0.3),
			durationParam(fs.Params, "max_age", 10*time.Second),
		)
		return r.registerReadHook(fs, f)

	case "latency":
		f := faults.NewLatencyInjectionFault(
			durationParam(fs.Params, "delay", 500*time.Millisecond),
			durationParam(fs.Params, "jitter", 0),
		)
		f.RampTo = durationParam(fs.Params, "ramp_to", 0)
		f.RampOver = durationParam(fs.Params, "ramp_over", 0)
		return r.registerConn(fs, f)

	case "bandwidth_throttle":
		f := faults.NewBandwidthThrottleFault(
			int64(intParam(fs.Params, "rate_bytes_per_sec", 64*1024)),
			int64(intParam(fs.Params, "burst_bytes", 64*1024)),
		)
		return r.registerConn(fs, f)

	case "dns_failure":
		// ServiceName must equal the host part of the proxy's upstream
		// address, which is what the proxy passes to BeforeDial.
		for _, p := range r.targets(fs) {
			host, _, err := net.SplitHostPort(p.UpstreamAddr)
			if err != nil {
				host = p.UpstreamAddr
			}
			f := &faults.DNSFailureFault{ServiceName: strParam(fs.Params, "service", host)}
			window := fs.For.Duration
			if window <= 0 {
				window = 24 * time.Hour // until healed
			}
			f.Trigger(window)
			p.Registry.RegisterDialFault(f)
			r.expire(ctx, fs, p.Registry, f.Name())
		}
		return nil

	case "wal_delay":
		f := &faults.WALDelayFault{FlushDelay: durationParam(fs.Params, "flush_delay", time.Second)}
		return r.registerPacket(fs, f)

	case "wal_corruption":
		f := &faults.WALCorruptionFault{
			Probability: floatParam(fs.Params, "probability", 0.01),
			BytesToFlip: intParam(fs.Params, "bytes_to_flip", 1),
		}
		return r.registerPacket(fs, f)

	case "query_corruption":
		f := &faults.QueryCorruptionFault{
			Probability: floatParam(fs.Params, "probability", 0.01),
			BytesToFlip: intParam(fs.Params, "bytes_to_flip", 1),
		}
		return r.registerPacket(fs, f)

	default:
		return fmt.Errorf("unknown fault type %q", fs.Type)
	}
}

// runNodeFault injects a NodeFault against fs.Target now, and schedules
// Revert after fs.For (if set).
func (r *Runner) runNodeFault(ctx context.Context, nf faults.NodeFault, fs FaultSpec) error {
	node, ok := r.nodes[fs.Target]
	if !ok {
		return fmt.Errorf("unknown target node %q", fs.Target)
	}
	if err := nf.Inject(ctx, node.ContainerID); err != nil {
		return err
	}
	if fs.For.Duration > 0 {
		go func() {
			select {
			case <-time.After(fs.For.Duration):
				_ = nf.Revert(context.Background(), node.ContainerID)
			case <-ctx.Done():
			}
		}()
	}
	return nil
}

// targets resolves fs.Target to proxies: "*" (or empty) means all.
func (r *Runner) targets(fs FaultSpec) []*proxy.Proxy {
	if fs.Target == "*" || fs.Target == "" {
		out := make([]*proxy.Proxy, 0, len(r.proxies))
		for _, p := range r.proxies {
			out = append(out, p)
		}
		return out
	}
	if p, ok := r.proxies[fs.Target]; ok {
		return []*proxy.Proxy{p}
	}
	return nil
}

func (r *Runner) checkTargets(fs FaultSpec) ([]*proxy.Proxy, error) {
	ps := r.targets(fs)
	if len(ps) == 0 {
		return nil, fmt.Errorf("fault %s: unknown target proxy %q", fs.Type, fs.Target)
	}
	return ps, nil
}

func (r *Runner) registerConn(fs FaultSpec, f faults.Fault) error {
	ps, err := r.checkTargets(fs)
	if err != nil {
		return err
	}
	for _, p := range ps {
		p.Registry.RegisterConnFault(f)
		r.expire(context.Background(), fs, p.Registry, f.Name())
	}
	return nil
}

func (r *Runner) registerPacket(fs FaultSpec, f faults.PacketFault) error {
	ps, err := r.checkTargets(fs)
	if err != nil {
		return err
	}
	for _, p := range ps {
		p.Registry.RegisterPacketFault(f)
		r.expire(context.Background(), fs, p.Registry, f.Name())
	}
	return nil
}

func (r *Runner) registerReadHook(fs FaultSpec, f *faults.StaleReadFault) error {
	ps, err := r.checkTargets(fs)
	if err != nil {
		return err
	}
	for _, p := range ps {
		p.Registry.RegisterReadHook(f)
		r.expire(context.Background(), fs, p.Registry, f.Name())
	}
	return nil
}

// expire unregisters name from reg after fs.For, mirroring runNodeFault's
// Revert scheduling. A fault with no `for` stays until healed.
func (r *Runner) expire(ctx context.Context, fs FaultSpec, reg *proxy.Registry, name string) {
	if fs.For.Duration <= 0 {
		return
	}
	go func() {
		select {
		case <-time.After(fs.For.Duration):
			reg.Unregister(name)
		case <-ctx.Done():
		}
	}()
}

// HealAll clears every proxy's registry and partition/crash flags.
func (r *Runner) HealAll() {
	for _, p := range r.proxies {
		p.State.Heal()
	}
}

func intParam(m map[string]any, key string, def int) int {
	if v, ok := m[key]; ok {
		if i, ok := v.(int); ok {
			return i
		}
	}
	return def
}

func floatParam(m map[string]any, key string, def float64) float64 {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return n
		case int:
			return float64(n)
		}
	}
	return def
}

func strParam(m map[string]any, key, def string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return def
}

func durationParam(m map[string]any, key string, def time.Duration) time.Duration {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			if d, err := time.ParseDuration(s); err == nil {
				return d
			}
		}
	}
	return def
}
