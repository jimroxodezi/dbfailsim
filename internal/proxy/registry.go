package proxy

import (
	"net"
	"sync"
	"time"

	"github.com/jimroxodezi/dbfailsim/internal/faults"
)

// Registry holds the live faults for ONE node's proxy. The accept loop,
// dialer, and pumps consult it; the scenario runner and control API
// mutate it. It lives in this package (not scenario) because scenario
// imports proxy and Go forbids the cycle.
//
// Conn faults are applied lazily: Wrap returns a conn that rebuilds its
// wrapper chain whenever the registry changes, so a fault registered
// after a connection was accepted still takes effect on it — the same
// live-update behavior the old State knobs had.
type Registry struct {
	mu           sync.RWMutex
	gen          uint64 // bumped on every mutation; liveConn re-wraps when it changes
	connFaults   []faults.Fault
	packetFaults []faults.PacketFault
	dialFaults   []faults.DialFault
	readHooks    []*faults.StaleReadFault

	// tickers stops the goroutine driving a TimeVaryingFault, keyed by
	// fault name. Only the registry ever calls Tick.
	tickers map[string]chan struct{}
}

func NewRegistry() *Registry {
	return &Registry{tickers: make(map[string]chan struct{})}
}

// RegisterConnFault installs f, replacing any conn fault with the same
// Name so re-injecting "latency" retunes rather than stacks.
func (r *Registry) RegisterConnFault(f faults.Fault) {
	r.mu.Lock()
	r.stopTickerLocked(f.Name())
	r.connFaults = replaceOrAppend(r.connFaults, f)
	r.gen++
	if tv, ok := f.(faults.TimeVaryingFault); ok {
		r.startTickerLocked(tv)
	}
	r.mu.Unlock()
}

func (r *Registry) RegisterPacketFault(f faults.PacketFault) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.packetFaults = replaceOrAppend(r.packetFaults, f)
	r.gen++
}

func (r *Registry) RegisterDialFault(f faults.DialFault) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dialFaults = replaceOrAppend(r.dialFaults, f)
	r.gen++
}

func (r *Registry) RegisterReadHook(f *faults.StaleReadFault) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readHooks = replaceOrAppend(r.readHooks, f)
	r.gen++
}

// Unregister removes every fault with the given name across all lists,
// stopping its ticker if it had one. The runner calls this when a
// FaultSpec's `for` window elapses.
func (r *Registry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	found := false
	r.connFaults = filter(r.connFaults, name, &found)
	r.packetFaults = filter(r.packetFaults, name, &found)
	r.dialFaults = filter(r.dialFaults, name, &found)
	r.readHooks = filter(r.readHooks, name, &found)
	r.stopTickerLocked(name)
	if found {
		r.gen++
	}
	return found
}

// Clear drops everything — the registry half of "heal".
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connFaults, r.packetFaults, r.dialFaults, r.readHooks = nil, nil, nil, nil
	for name := range r.tickers {
		r.stopTickerLocked(name)
	}
	r.gen++
}

// Get returns the conn fault with the given name, or nil.
func (r *Registry) Get(name string) faults.Fault {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, f := range r.connFaults {
		if f.Name() == name {
			return f
		}
	}
	return nil
}

// Names lists every active fault for /status.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.connFaults)+len(r.packetFaults)+len(r.dialFaults)+len(r.readHooks))
	for _, f := range r.connFaults {
		out = append(out, f.Name())
	}
	for _, f := range r.packetFaults {
		out = append(out, f.Name())
	}
	for _, f := range r.dialFaults {
		out = append(out, f.Name())
	}
	for _, f := range r.readHooks {
		out = append(out, f.Name())
	}
	return out
}

// Snapshots for the hot path — copied under RLock so a pump never holds
// the lock while sleeping inside a fault.

func (r *Registry) PacketFaults() []faults.PacketFault {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]faults.PacketFault(nil), r.packetFaults...)
}

func (r *Registry) DialFaults() []faults.DialFault {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]faults.DialFault(nil), r.dialFaults...)
}

func (r *Registry) ReadHooks() []*faults.StaleReadFault {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]*faults.StaleReadFault(nil), r.readHooks...)
}

// Wrap returns a conn that applies the registry's conn faults to every
// Write. dir is the direction of the bytes WRITTEN into conn — the
// faults package wrappers act on Write, so the proxy wraps each pump's
// destination.
func (r *Registry) Wrap(conn net.Conn, dir faults.Direction) net.Conn {
	return &liveConn{Conn: conn, raw: conn, reg: r, dir: dir}
}

// wrap folds the current conn faults over raw. RSTFault goes first, on
// the raw conn: its Apply does conn.(*net.TCPConn), which fails silently
// through any other wrapper.
func (r *Registry) wrap(raw net.Conn, dir faults.Direction) (net.Conn, uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conn := raw
	for _, f := range r.connFaults {
		if _, isRST := f.(*faults.RSTFault); isRST {
			conn = f.Apply(conn, dir)
		}
	}
	for _, f := range r.connFaults {
		if _, isRST := f.(*faults.RSTFault); !isRST {
			conn = f.Apply(conn, dir)
		}
	}
	return conn, r.gen
}

// liveConn is the conn handed to a pump. Its Write goes through the
// wrapper chain built from the registry's current faults, rebuilt when
// the registry generation changes. Reads, Close, deadlines go straight
// to the raw conn.
type liveConn struct {
	net.Conn // raw, for everything except Write
	raw      net.Conn
	reg      *Registry
	dir      faults.Direction

	mu      sync.Mutex
	gen     uint64
	wrapped net.Conn
}

func (c *liveConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.reg.mu.RLock()
	stale := c.wrapped == nil || c.gen != c.reg.gen
	c.reg.mu.RUnlock()
	if stale {
		c.wrapped, c.gen = c.reg.wrap(c.raw, c.dir)
	}
	w := c.wrapped
	c.mu.Unlock()
	return w.Write(p)
}

// startTickerLocked drives a TimeVaryingFault with elapsed-since-
// registration, the contract LatencyInjectionFault.Tick expects.
func (r *Registry) startTickerLocked(tv faults.TimeVaryingFault) {
	stop := make(chan struct{})
	r.tickers[tv.Name()] = stop
	go func() {
		start := time.Now()
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				tv.Tick(time.Since(start))
			}
		}
	}()
}

func (r *Registry) stopTickerLocked(name string) {
	if stop, ok := r.tickers[name]; ok {
		close(stop)
		delete(r.tickers, name)
	}
}

type named interface{ Name() string }

func replaceOrAppend[T named](list []T, f T) []T {
	for i, existing := range list {
		if existing.Name() == f.Name() {
			list[i] = f
			return list
		}
	}
	return append(list, f)
}

func filter[T named](list []T, name string, found *bool) []T {
	out := make([]T, 0, len(list))
	for _, f := range list {
		if f.Name() == name {
			*found = true
			continue
		}
		out = append(out, f)
	}
	return out
}
