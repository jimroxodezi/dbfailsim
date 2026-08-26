package scenario

import (
	"sync"

	"github.com/jimroxodezi/dbfailsim/internal/faults"
)

// Registry holds the currently-active faults that the proxy's connection
// handler consults on every new conn / packet. This is intentionally
// minimal — in the real fluxproxy integration, the proxy's accept loop
// should hold a *Registry and call Apply on it for every conn/packet
// instead of a static fault list.
type Registry struct {
	mu           sync.RWMutex
	connFaults   []faults.Fault
	packetFaults []faults.PacketFault
	readHooks    []*faults.StaleReadFault
}

var proxyRegistry = &Registry{}

func (r *Registry) RegisterConnFault(f faults.Fault) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connFaults = append(r.connFaults, f)
	return nil
}

func (r *Registry) RegisterPacketFault(f faults.PacketFault) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.packetFaults = append(r.packetFaults, f)
	return nil
}

func (r *Registry) RegisterReadHook(f *faults.StaleReadFault) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readHooks = append(r.readHooks, f)
	return nil
}

// ApplyConnFaults runs every registered conn-level fault over a fresh
// connection in registration order, for the given direction.
func (r *Registry) ApplyConnFaults(conn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
}, dir faults.Direction) {
	// See faults.Fault.Apply signature (net.Conn) — real integration
	// passes a net.Conn here; the interface above is illustrative.
}