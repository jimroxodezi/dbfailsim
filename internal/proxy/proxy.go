// Package proxy implements a protocol-agnostic TCP fault-injection proxy.
//
// It works by byte-copying between a client connection and the real
// upstream database, so it doesn't need to understand Postgres wire
// protocol, MySQL wire protocol, or anything else — it works with any
// TCP-based database. Faults come from the faults package and plug in at
// three points:
//
//   - DialFault:   consulted before every upstream dial (DNS failure)
//   - Fault:       wraps each pump's destination conn (latency, loss,
//     throttle, reorder, duplication, RST, one-way partition)
//   - PacketFault: runs on every chunk in the pump, gated by the proxy's
//     StreamType (replica lag, WAL delay/corruption, query corruption)
//
// plus StaleReadFault as a read hook on the reply leg. Which faults are
// live is held per node in a Registry. Full partition and crash are not
// faults on the wire but on the accept loop, so they stay in State.
package proxy

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/jimroxodezi/dbfailsim/internal/faults"
)

// State keeps only what a conn wrapper cannot express: refusing accepts
// and severing live conns. All fields are guarded by mu.
type State struct {
	mu          sync.RWMutex
	partitioned bool
	crashed     bool

	// onSever, when set, is called after the node becomes partitioned or
	// crashed, so the owning proxy can tear down existing connections
	// immediately instead of waiting for pumps to notice.
	onSever func()

	// reg is the owning proxy's registry; SetLatency/SetDrop/Heal are
	// thin shims over it so the control API and old Engine keep working.
	reg *Registry
}

func (s *State) severed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.partitioned || s.crashed
}

// SetLatency installs (or retunes) a fixed latency fault of ms on both
// directions. 0 removes it.
func (s *State) SetLatency(ms int) {
	if ms <= 0 {
		s.reg.Unregister("latency")
		return
	}
	s.reg.RegisterConnFault(faults.NewLatencyInjectionFault(time.Duration(ms)*time.Millisecond, 0))
}

// SetDrop installs (or retunes) a uniform loss fault of percent. 0
// removes it. Uniform loss is BurstyLossFault with baseline == burst.
func (s *State) SetDrop(percent int) {
	if percent <= 0 {
		s.reg.Unregister("bursty_loss")
		return
	}
	rate := float64(percent) / 100
	s.reg.RegisterConnFault(&faults.BurstyLossFault{BaselineLoss: rate, BurstLoss: rate})
}

// SetPartitioned marks the node as network-partitioned: existing connections
// are severed and new connections are refused until healed.
func (s *State) SetPartitioned(v bool) {
	s.mu.Lock()
	s.partitioned = v
	sever := v && s.onSever != nil
	fn := s.onSever
	s.mu.Unlock()
	if sever {
		fn()
	}
}

// SetCrashed marks the node as crashed: behaves like partitioned but is
// reported separately for clarity in status output.
func (s *State) SetCrashed(v bool) {
	s.mu.Lock()
	s.crashed = v
	sever := v && s.onSever != nil
	fn := s.onSever
	s.mu.Unlock()
	if sever {
		fn()
	}
}

// Heal clears all fault conditions — the flags here and every fault in
// the registry — returning the node to normal operation.
func (s *State) Heal() {
	s.mu.Lock()
	s.partitioned = false
	s.crashed = false
	s.mu.Unlock()
	s.reg.Clear()
}

// Status is a point-in-time snapshot of a node's health, safe to serialize.
// LatencyMs/DropPercent report the "latency"/"bursty_loss" faults when
// present, for the dashboard and CLI; ActiveFaults lists everything.
type Status struct {
	Name          string   `json:"name"`
	ListenAddr    string   `json:"listen_addr"`
	UpstreamAddr  string   `json:"upstream_addr"`
	Stream        string   `json:"stream"`
	LatencyMs     int      `json:"latency_ms"`
	DropPercent   int      `json:"drop_percent"`
	Partitioned   bool     `json:"partitioned"`
	Crashed       bool     `json:"crashed"`
	ActiveFaults  []string `json:"active_faults"`
	ActiveClients int      `json:"active_clients"`
}

// Proxy fronts a single upstream database node.
type Proxy struct {
	Name         string
	ListenAddr   string
	UpstreamAddr string
	State        *State
	Registry     *Registry

	// Stream classifies every byte through this proxy. There is no
	// protocol parser: the proxy fronting the primary's replication port
	// is configured as ReplicationStream, everything else is query
	// traffic. PacketFaults gate on this.
	Stream faults.StreamType

	// Optional topology hookup for inter-node proxies (e.g. a replication
	// proxy: FromNode="replica-1", ToNode="primary"). Empty FromNode means
	// client-facing: the ClusterView is not consulted.
	View     faults.ClusterView
	FromNode string
	ToNode   string

	listener net.Listener
	active   int
	activeMu sync.Mutex

	// conns maps each live conn to the cancel func of its session, so
	// severConns both closes sockets and ends any PacketFault sleep.
	conns   map[net.Conn]context.CancelFunc
	connsMu sync.Mutex
}

// New creates a Proxy for the given node. Call Start to begin listening.
func New(name, listenAddr, upstreamAddr string) *Proxy {
	p := &Proxy{
		Name:         name,
		ListenAddr:   listenAddr,
		UpstreamAddr: upstreamAddr,
		State:        &State{},
		Registry:     NewRegistry(),
		conns:        make(map[net.Conn]context.CancelFunc),
	}
	p.State.onSever = p.severConns
	p.State.reg = p.Registry
	return p
}

// Start begins listening and proxying connections. It runs until the
// listener is closed (via Close) or the process exits. Intended to be run
// in its own goroutine.
func (p *Proxy) Start() error {
	if err := p.Listen(); err != nil {
		return err
	}
	return p.Serve()
}

// Listen binds the proxy's listen address without serving yet. Useful when
// the caller needs the bound address (see Addr) before traffic starts.
func (p *Proxy) Listen() error {
	l, err := net.Listen("tcp", p.ListenAddr)
	if err != nil {
		return err
	}
	p.listener = l
	return nil
}

// Addr returns the actual bound listen address once Listen has succeeded
// (which resolves ":0" to the assigned port), or ListenAddr before then.
func (p *Proxy) Addr() string {
	if p.listener == nil {
		return p.ListenAddr
	}
	return p.listener.Addr().String()
}

// allowed is the single reachability check used by Serve and pump: the
// node's own partition/crash flags plus, for inter-node proxies, the
// cluster topology.
func (p *Proxy) allowed() bool {
	if p.State.severed() {
		return false
	}
	if p.View != nil && p.FromNode != "" && !p.View.Allowed(p.FromNode, p.ToNode) {
		return false
	}
	return true
}

// Serve accepts and proxies connections until the listener is closed.
func (p *Proxy) Serve() error {
	l := p.listener
	log.Printf("[%s] proxy listening on %s -> %s", p.Name, p.ListenAddr, p.UpstreamAddr)

	for {
		conn, err := l.Accept()
		if err != nil {
			return err // listener closed
		}
		if !p.allowed() {
			// Simulate an unreachable node: refuse the connection outright.
			conn.Close()
			continue
		}
		go p.handle(conn)
	}
}

// Close stops the proxy from accepting new connections.
func (p *Proxy) Close() error {
	if p.listener != nil {
		return p.listener.Close()
	}
	return nil
}

// Status returns a snapshot of this proxy's current health.
func (p *Proxy) Status() Status {
	p.State.mu.RLock()
	partitioned, crashed := p.State.partitioned, p.State.crashed
	p.State.mu.RUnlock()
	p.activeMu.Lock()
	active := p.active
	p.activeMu.Unlock()

	st := Status{
		Name:          p.Name,
		ListenAddr:    p.ListenAddr,
		UpstreamAddr:  p.UpstreamAddr,
		Stream:        "query",
		Partitioned:   partitioned,
		Crashed:       crashed,
		ActiveFaults:  p.Registry.Names(),
		ActiveClients: active,
	}
	if p.Stream == faults.ReplicationStream {
		st.Stream = "replication"
	}
	if f, ok := p.Registry.Get("latency").(*faults.LatencyInjectionFault); ok {
		st.LatencyMs = int(f.CurrentDelay() / time.Millisecond)
	}
	if f, ok := p.Registry.Get("bursty_loss").(*faults.BurstyLossFault); ok {
		st.DropPercent = int(f.CurrentLossRate()*100 + 0.5)
	}
	return st
}

// SeverConns closes every live connection and cancels its session. It is
// exported so the scenario runner can call it after a TopologyFault
// changes ClusterView — the view has no hook of its own.
func (p *Proxy) SeverConns() { p.severConns() }

func (p *Proxy) severConns() {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	for c, cancel := range p.conns {
		cancel()
		c.Close()
	}
}

func (p *Proxy) track(c net.Conn, cancel context.CancelFunc) {
	p.connsMu.Lock()
	p.conns[c] = cancel
	p.connsMu.Unlock()
}

func (p *Proxy) untrack(c net.Conn) {
	p.connsMu.Lock()
	delete(p.conns, c)
	p.connsMu.Unlock()
}

// session is the per-connection state shared by the two pumps.
type session struct {
	ctx context.Context

	// lastQuery is the most recent client->upstream chunk, so the reply
	// pump can pair a response with its query for StaleReadFault.
	// Chunk-granular: right for a single-chunk SELECT, approximate for
	// multi-chunk result sets (needs protocol framing to do better).
	mu        sync.Mutex
	lastQuery []byte
}

func (p *Proxy) handle(client net.Conn) {
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// DialFault hook: fail resolution before any socket exists.
	host, _, err := net.SplitHostPort(p.UpstreamAddr)
	if err != nil {
		host = p.UpstreamAddr
	}
	for _, d := range p.Registry.DialFaults() {
		if err := d.BeforeDial(ctx, host); err != nil {
			log.Printf("[%s] dial fault %s: %v", p.Name, d.Name(), err)
			return
		}
	}
	upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.UpstreamAddr)
	if err != nil {
		log.Printf("[%s] failed to dial upstream %s: %v", p.Name, p.UpstreamAddr, err)
		return
	}
	defer upstream.Close()

	p.track(client, cancel)
	p.track(upstream, cancel)
	defer p.untrack(client)
	defer p.untrack(upstream)

	// Re-check after tracking: a partition/crash set between Accept and
	// track would otherwise leave these connections open.
	if !p.allowed() {
		return
	}

	p.activeMu.Lock()
	p.active++
	p.activeMu.Unlock()
	defer func() {
		p.activeMu.Lock()
		p.active--
		p.activeMu.Unlock()
	}()

	// Conn faults wrap each pump's DESTINATION, tagged with the direction
	// of the bytes written into it (the faults package acts on Write).
	toUpstream := p.Registry.Wrap(upstream, faults.DirectionClientToUpstream)
	toClient := p.Registry.Wrap(client, faults.DirectionUpstreamToClient)

	s := &session{ctx: ctx}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer cancel() // one leg dying ends any fault sleep on the other
		p.pump(s, client, toUpstream, faults.DirectionClientToUpstream)
	}()
	go func() {
		defer wg.Done()
		defer cancel()
		p.pump(s, upstream, toClient, faults.DirectionUpstreamToClient)
	}()
	wg.Wait()
}

// pump copies src -> dst. Per chunk: reachability re-check, PacketFaults
// (ctx-aware, may sleep, mutate, or drop), read hooks on the reply leg,
// then the write — where the wrapped dst applies conn-level faults. It
// stops when either side closes; a partition or crash closes both
// connections (see severConns), which unblocks the read here.
func (p *Proxy) pump(s *session, src, dst net.Conn, dir faults.Direction) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !p.allowed() {
				return // severed mid-flight: partition, crash, or topology
			}
			chunk := buf[:n]

			for _, pf := range p.Registry.PacketFaults() {
				chunk = pf.Apply(s.ctx, chunk, p.Stream)
				if s.ctx.Err() != nil {
					return
				}
				if len(chunk) == 0 {
					break // dropped
				}
			}

			switch dir {
			case faults.DirectionClientToUpstream:
				s.mu.Lock()
				s.lastQuery = append(s.lastQuery[:0], chunk...)
				s.mu.Unlock()
			case faults.DirectionUpstreamToClient:
				if hooks := p.Registry.ReadHooks(); len(hooks) > 0 {
					s.mu.Lock()
					q := append([]byte(nil), s.lastQuery...)
					s.mu.Unlock()
					// Hooks cache what they are given; hand them a copy,
					// not a window onto buf that the next Read overwrites.
					chunk = append([]byte(nil), chunk...)
					for _, h := range hooks {
						chunk = h.MaybeServeStale(q, chunk)
					}
				}
			}

			if len(chunk) > 0 {
				if _, werr := dst.Write(chunk); werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}
