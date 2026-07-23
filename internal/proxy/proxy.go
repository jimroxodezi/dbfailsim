// Package proxy implements a protocol-agnostic TCP fault-injection proxy.
//
// It works by byte-copying between a client connection and the real
// upstream database, so it doesn't need to understand Postgres wire
// protocol, MySQL wire protocol, or anything else — it works with any
// TCP-based database. Faults are applied to the copy loops:
//
//   - latency:   sleep before forwarding each chunk (simulates replication/network delay)
//   - drop:      probabilistically discard a chunk instead of forwarding it (simulates packet loss / lag spikes)
//   - partition: refuse all new connections and hang up existing ones (simulates a network partition / split brain)
//   - crash:     immediately close all connections and refuse new ones (simulates a node crash)
package proxy

import (
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

// State is the current fault-injection state for one node's proxy.
// All fields are guarded by mu and safe for concurrent read/update.
type State struct {
	mu          sync.RWMutex
	latencyMs   int
	dropPercent int
	partitioned bool
	crashed     bool

	// onSever, when set, is called after the node becomes partitioned or
	// crashed, so the owning proxy can tear down existing connections
	// immediately instead of waiting for pumps to notice.
	onSever func()
}

func (s *State) snapshot() (latencyMs, dropPercent int, partitioned, crashed bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latencyMs, s.dropPercent, s.partitioned, s.crashed
}

// SetLatency sets an added delay (ms) applied to every forwarded chunk in both directions.
func (s *State) SetLatency(ms int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latencyMs = ms
}

// SetDrop sets the percent chance (0-100) that a chunk is silently dropped.
func (s *State) SetDrop(percent int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropPercent = percent
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

// Heal clears all fault conditions, returning the node to normal operation.
func (s *State) Heal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latencyMs = 0
	s.dropPercent = 0
	s.partitioned = false
	s.crashed = false
}

// Status is a point-in-time snapshot of a node's health, safe to serialize.
type Status struct {
	Name          string `json:"name"`
	ListenAddr    string `json:"listen_addr"`
	UpstreamAddr  string `json:"upstream_addr"`
	LatencyMs     int    `json:"latency_ms"`
	DropPercent   int    `json:"drop_percent"`
	Partitioned   bool   `json:"partitioned"`
	Crashed       bool   `json:"crashed"`
	ActiveClients int    `json:"active_clients"`
}

// Proxy fronts a single upstream database node.
type Proxy struct {
	Name         string
	ListenAddr   string
	UpstreamAddr string
	State        *State

	listener net.Listener
	active   int
	activeMu sync.Mutex
	conns    map[net.Conn]struct{}
	connsMu  sync.Mutex
}

// New creates a Proxy for the given node. Call Start to begin listening.
func New(name, listenAddr, upstreamAddr string) *Proxy {
	p := &Proxy{
		Name:         name,
		ListenAddr:   listenAddr,
		UpstreamAddr: upstreamAddr,
		State:        &State{},
		conns:        make(map[net.Conn]struct{}),
	}
	p.State.onSever = p.severConns
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

// Serve accepts and proxies connections until the listener is closed.
func (p *Proxy) Serve() error {
	l := p.listener
	log.Printf("[%s] proxy listening on %s -> %s", p.Name, p.ListenAddr, p.UpstreamAddr)

	for {
		conn, err := l.Accept()
		if err != nil {
			return err // listener closed
		}
		_, _, partitioned, crashed := p.State.snapshot()
		if partitioned || crashed {
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
	latencyMs, dropPercent, partitioned, crashed := p.State.snapshot()
	p.activeMu.Lock()
	active := p.active
	p.activeMu.Unlock()
	return Status{
		Name:          p.Name,
		ListenAddr:    p.ListenAddr,
		UpstreamAddr:  p.UpstreamAddr,
		LatencyMs:     latencyMs,
		DropPercent:   dropPercent,
		Partitioned:   partitioned,
		Crashed:       crashed,
		ActiveClients: active,
	}
}

// severConns closes every live connection so blocked pump reads unblock and
// exit as soon as the node becomes unreachable.
func (p *Proxy) severConns() {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	for c := range p.conns {
		c.Close()
	}
}

func (p *Proxy) track(c net.Conn) {
	p.connsMu.Lock()
	p.conns[c] = struct{}{}
	p.connsMu.Unlock()
}

func (p *Proxy) untrack(c net.Conn) {
	p.connsMu.Lock()
	delete(p.conns, c)
	p.connsMu.Unlock()
}

func (p *Proxy) handle(client net.Conn) {
	defer client.Close()

	upstream, err := net.Dial("tcp", p.UpstreamAddr)
	if err != nil {
		log.Printf("[%s] failed to dial upstream %s: %v", p.Name, p.UpstreamAddr, err)
		return
	}
	defer upstream.Close()

	p.track(client)
	p.track(upstream)
	defer p.untrack(client)
	defer p.untrack(upstream)

	// Re-check after tracking: a partition/crash set between Accept and
	// track would otherwise leave these connections open.
	if _, _, partitioned, crashed := p.State.snapshot(); partitioned || crashed {
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

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.pump(client, upstream)
	}()
	go func() {
		defer wg.Done()
		p.pump(upstream, client)
	}()
	wg.Wait()
}

// pump copies bytes from src to dst, applying the current fault state to
// each chunk. It stops when either side closes; a partition or crash closes
// both connections (see severConns), which unblocks the read here.
func (p *Proxy) pump(src, dst net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			latencyMs, dropPercent, partitioned, crashed := p.State.snapshot()
			if partitioned || crashed {
				// Sever the connection to simulate the node becoming unreachable.
				return
			}
			if dropPercent > 0 && rand.Intn(100) < dropPercent {
				// Silently discard this chunk: simulates a lost packet /
				// replication delay that never catches up.
			} else {
				if latencyMs > 0 {
					time.Sleep(time.Duration(latencyMs) * time.Millisecond)
				}
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
		}
		if err != nil {
			return
		}
	}
}
