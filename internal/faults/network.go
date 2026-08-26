package faults

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Latency injection — fixed, jittered, and gradually increasing
// 
// LatencyInjectionFault delays every Write by a base delay plus optional
// jitter. Implements LatencyFault. Also implements TimeVaryingFault when
// RampTo/RampOver are set: Tick grows the current delay linearly from
// BaseDelay toward RampTo over RampOver, then holds.
type LatencyInjectionFault struct {
	BaseDelay time.Duration
	Jitter    time.Duration
	RampTo    time.Duration // zero means no ramping — fixed/jittered only
	RampOver  time.Duration

	mu      sync.Mutex
	current time.Duration
}

func NewLatencyInjectionFault(base, jitter time.Duration) *LatencyInjectionFault {
	return &LatencyInjectionFault{BaseDelay: base, Jitter: jitter, current: base}
}

func (f *LatencyInjectionFault) Name() string { return "latency" }

func (f *LatencyInjectionFault) CurrentDelay() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.current
	if f.Jitter > 0 {
		d += time.Duration(rand.Int63n(int64(f.Jitter)))
	}
	return d
}

// Tick advances the ramp. Safe to call even when no ramp is configured
// (it's then a no-op), so callers don't need to branch on RampTo != 0.
func (f *LatencyInjectionFault) Tick(elapsed time.Duration) {
	if f.RampTo == 0 || f.RampOver == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if elapsed >= f.RampOver {
		f.current = f.RampTo
		return
	}
	progress := float64(elapsed) / float64(f.RampOver)
	f.current = f.BaseDelay + time.Duration(progress*float64(f.RampTo-f.BaseDelay))
}

func (f *LatencyInjectionFault) Apply(conn net.Conn, dir Direction) net.Conn {
	return &latencyConn{baseConn{conn}, f}
}

type latencyConn struct {
	baseConn
	fault *LatencyInjectionFault
}

func (c *latencyConn) Write(p []byte) (int, error) {
	time.Sleep(c.fault.CurrentDelay())
	return c.baseConn.Conn.Write(p)
}

// Bandwidth throttling
// 
// BandwidthThrottleFault caps outbound throughput using a simple token
// bucket: RateBytesPerSec tokens refill continuously, BurstBytes is the
// bucket capacity. Writes larger than the current token count block
// until enough tokens accumulate.
type BandwidthThrottleFault struct {
	Rate  int64 // bytes/sec
	Burst int64 // bucket capacity in bytes

	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
}

func NewBandwidthThrottleFault(rateBytesPerSec, burstBytes int64) *BandwidthThrottleFault {
	return &BandwidthThrottleFault{
		Rate: rateBytesPerSec, Burst: burstBytes,
		tokens: float64(burstBytes), lastRefill: time.Now(),
	}
}

func (f *BandwidthThrottleFault) Name() string { return "bandwidth_throttle" }

func (f *BandwidthThrottleFault) RateBytesPerSec() int64 { return f.Rate }

func (f *BandwidthThrottleFault) take(n int) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(f.lastRefill).Seconds()
	f.lastRefill = now
	f.tokens += elapsed * float64(f.Rate)
	if f.tokens > float64(f.Burst) {
		f.tokens = float64(f.Burst)
	}

	if f.tokens >= float64(n) {
		f.tokens -= float64(n)
		return 0
	}

	deficit := float64(n) - f.tokens
	f.tokens = 0
	return time.Duration(deficit / float64(f.Rate) * float64(time.Second))
}

func (f *BandwidthThrottleFault) Apply(conn net.Conn, dir Direction) net.Conn {
	return &throttleConn{baseConn{conn}, f}
}

type throttleConn struct {
	baseConn
	fault *BandwidthThrottleFault
}

func (c *throttleConn) Write(p []byte) (int, error) {
	if wait := c.fault.take(len(p)); wait > 0 {
		time.Sleep(wait)
	}
	return c.baseConn.Conn.Write(p)
}

// Packet reordering
// 
// ReorderFault buffers up to BufferSize packets and releases them out of
// order with probability Probability, simulating network-layer reordering.
type ReorderFault struct {
	BufferSize  int
	Probability float64
}

func (f *ReorderFault) Name() string { return "reorder" }

func (f *ReorderFault) Apply(conn net.Conn, dir Direction) net.Conn {
	return &reorderConn{
		baseConn: baseConn{conn},
		buf:      make([][]byte, 0, f.BufferSize),
		bufSize:  f.BufferSize,
		prob:     f.Probability,
	}
}

type reorderConn struct {
	baseConn
	mu      sync.Mutex
	buf     [][]byte
	bufSize int
	prob    float64
}

func (c *reorderConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cp := make([]byte, len(p))
	copy(cp, p)

	if rand.Float64() < c.prob && len(c.buf) < c.bufSize {
		c.buf = append(c.buf, cp)
		if len(c.buf) < c.bufSize {
			return len(p), nil // hold it back
		}
		// buffer full: shuffle and flush
		rand.Shuffle(len(c.buf), func(i, j int) {
			c.buf[i], c.buf[j] = c.buf[j], c.buf[i]
		})
		for _, pkt := range c.buf {
			if _, err := c.baseConn.Conn.Write(pkt); err != nil {
				return 0, err
			}
		}
		c.buf = c.buf[:0]
		return len(p), nil
	}

	return c.baseConn.Conn.Write(p)
}

// ---------------------------------------------------------------------
// Packet duplication
// ---------------------------------------------------------------------

// DuplicationFault re-sends a fraction of packets 1..MaxExtraCopies times.
type DuplicationFault struct {
	Probability   float64
	MaxExtraCopies int
}

func (f *DuplicationFault) Name() string { return "duplication" }

func (f *DuplicationFault) Apply(conn net.Conn, dir Direction) net.Conn {
	return &dupConn{baseConn{conn}, f.Probability, f.MaxExtraCopies}
}

type dupConn struct {
	baseConn
	prob    float64
	maxCopy int
}

func (c *dupConn) Write(p []byte) (int, error) {
	n, err := c.baseConn.Conn.Write(p)
	if err != nil {
		return n, err
	}
	if rand.Float64() < c.prob {
		copies := 1 + rand.Intn(c.maxCopy)
		for i := 0; i < copies; i++ {
			_, _ = c.baseConn.Conn.Write(p) // best-effort extra copies
		}
	}
	return n, nil
}



// Asymmetric partition (blackhole one direction only)
// 
// AsymmetricPartitionFault silently drops traffic in BlockDirection while
// letting the other direction flow, simulating "A can send to B but B's
// replies never arrive."
type AsymmetricPartitionFault struct {
	BlockDirection Direction
}

func (f *AsymmetricPartitionFault) Name() string { return "asymmetric_partition" }

func (f *AsymmetricPartitionFault) Apply(conn net.Conn, dir Direction) net.Conn {
	if dir == f.BlockDirection {
		return &blackholeConn{baseConn{conn}}
	}
	return conn
}

type blackholeConn struct {
	baseConn
}

func (c *blackholeConn) Write(p []byte) (int, error) {
	// Pretend the write succeeded; the bytes never reach the peer.
	return len(p), nil
}


// Full partition — superseded. The real implementation now lives in
// topology.go as a proper TopologyFault operating on ClusterView, since
// it needs multi-node routing state that a single net.Conn can't carry.
// See topology.go's PartitionFault.
// 

// TCP RST injection

// RSTFault forces the next Close() on the connection to emit a TCP RST
// instead of a clean FIN, simulating an abrupt peer failure.
type RSTFault struct{}

func (f *RSTFault) Name() string { return "tcp_rst" }

func (f *RSTFault) Apply(conn net.Conn, dir Direction) net.Conn {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetLinger(0) // Close() now sends RST, not FIN
	}
	return conn
}

// InjectRSTNow closes the connection immediately with RST semantics,
// for use when a scenario wants to trigger the RST at a specific time
// rather than waiting for the normal Close().
func InjectRSTNow(conn net.Conn) error {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetLinger(0); err != nil {
			return err
		}
		return tcpConn.Close()
	}
	return conn.Close()
}

// DNS resolution failure (simulate service discovery breaking)


// DNSFailureFault causes new upstream connections for the named service
// to fail resolution for the fault's duration, without affecting already
// established connections.
type DNSFailureFault struct {
	ServiceName string
	mu          sync.RWMutex
	active      bool
	until       time.Time
}

func (f *DNSFailureFault) Name() string { return "dns_failure" }

func (f *DNSFailureFault) Trigger(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active = true
	f.until = time.Now().Add(d)
}

// ResolveOrFail is called by the proxy's dialer before opening a new
// upstream connection; it returns an error while the fault is active.
func (f *DNSFailureFault) ResolveOrFail(serviceName string) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.active && serviceName == f.ServiceName && time.Now().Before(f.until) {
		return &net.DNSError{Err: "no such host (fault injected)", Name: serviceName, IsNotFound: true}
	}
	return nil
}

func (f *DNSFailureFault) Apply(conn net.Conn, dir Direction) net.Conn {
	// DNS faults act at dial-time (see BeforeDial), not on an
	// established conn. No-op here for interface uniformity — kept only
	// so DNSFailureFault can still be registered wherever plain Faults
	// are, without a type switch.
	return conn
}

// BeforeDial satisfies DialFault. The proxy's dialer must call this
// before every new upstream connection attempt and abort the dial with
// the returned error when non-nil.
func (f *DNSFailureFault) BeforeDial(ctx context.Context, targetHost string) error {
	return f.ResolveOrFail(targetHost)
}

// Bursty packet loss (loss probability rises during a burst window)

// BurstyLossFault alternates between a low baseline loss rate and a much
// higher loss rate during randomly triggered bursts.
type BurstyLossFault struct {
	BaselineLoss float64
	BurstLoss    float64
	BurstEvery   time.Duration
	BurstFor     time.Duration

	mu        sync.Mutex
	inBurst   bool
	burstEnds time.Time
	lastCheck time.Time
}

func (f *BurstyLossFault) Name() string { return "bursty_loss" }

// CurrentLossRate satisfies LossFault — exposes the burst/baseline logic
// that was previously private, so the scenario runner or a dashboard can
// report the live rate without duplicating the burst-window math.
func (f *BurstyLossFault) CurrentLossRate() float64 { return f.currentLossRate() }

// Tick satisfies TimeVaryingFault. BurstyLossFault already re-evaluates
// its own burst window lazily on every currentLossRate() call, so Tick
// has nothing to do — it exists to make the interface satisfaction
// explicit and give a hook if the burst schedule ever needs a driven
// clock instead of a lazily-checked one (e.g. for deterministic tests).
func (f *BurstyLossFault) Tick(elapsed time.Duration) {}

func (f *BurstyLossFault) currentLossRate() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()

	if f.inBurst && now.After(f.burstEnds) {
		f.inBurst = false
	}
	if !f.inBurst && now.Sub(f.lastCheck) > f.BurstEvery {
		f.inBurst = true
		f.burstEnds = now.Add(f.BurstFor)
		f.lastCheck = now
	}
	if f.inBurst {
		return f.BurstLoss
	}
	return f.BaselineLoss
}

func (f *BurstyLossFault) Apply(conn net.Conn, dir Direction) net.Conn {
	return &burstyLossConn{baseConn{conn}, f}
}

type burstyLossConn struct {
	baseConn
	fault *BurstyLossFault
}

func (c *burstyLossConn) Write(p []byte) (int, error) {
	if rand.Float64() < c.fault.currentLossRate() {
		return len(p), nil // silently drop, pretend it was sent
	}
	return c.baseConn.Conn.Write(p)
}