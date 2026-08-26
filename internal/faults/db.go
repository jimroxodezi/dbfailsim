package faults

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"sync"
	"time"
)

// ---------------------------------------------------------------------
// Replica lag — delays only replication-stream packets, leaving client
// query traffic through the same proxy untouched.
// ---------------------------------------------------------------------

// ReplicaLagFault sleeps before forwarding any packet tagged as
// ReplicationStream, simulating a replica falling behind its primary.
type ReplicaLagFault struct {
	Delay  time.Duration
	Jitter time.Duration
}

func (f *ReplicaLagFault) Name() string { return "replica_lag" }

// Apply satisfies PacketFault. The sleep now respects ctx cancellation
// (via select on ctx.Done()) instead of blocking unconditionally — a
// scenario teardown or connection close no longer leaves this goroutine
// sleeping for up to Delay+Jitter regardless.
func (f *ReplicaLagFault) Apply(ctx context.Context, pkt []byte, stream StreamType) []byte {
	if stream != ReplicationStream {
		return pkt
	}
	d := f.Delay
	if f.Jitter > 0 {
		d += time.Duration(rand.Int63n(int64(f.Jitter)))
	}
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
	return pkt
}

// Stale read — proxy intercepts SELECT-shaped queries and, with some
// probability, serves a cached older response instead of forwarding to
// the live backend.
//
// StaleReadFault caches query responses keyed by a hash of the query text
// and occasionally serves a stale cached copy instead of live data.
type StaleReadFault struct {
	Probability float64
	MaxAge      time.Duration

	mu    sync.Mutex
	cache map[string]cachedResult
}

type cachedResult struct {
	data    []byte
	cachedAt time.Time
}

func NewStaleReadFault(probability float64, maxAge time.Duration) *StaleReadFault {
	return &StaleReadFault{
		Probability: probability,
		MaxAge:      maxAge,
		cache:       make(map[string]cachedResult),
	}
}

func (f *StaleReadFault) Name() string { return "stale_read" }

func queryHash(query []byte) string {
	sum := sha256.Sum256(query)
	return hex.EncodeToString(sum[:8])
}

// MaybeServeStale is called by the proxy after it has the live response
// in hand: it either serves that live response (and updates the cache)
// or, probabilistically, serves the previously cached one instead.
func (f *StaleReadFault) MaybeServeStale(query, live []byte) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := queryHash(query)
	cached, hasCache := f.cache[key]

	serveStale := hasCache &&
		time.Since(cached.cachedAt) < f.MaxAge &&
		rand.Float64() < f.Probability

	// Always refresh the cache with the live result so future stale
	// serves reflect *some* real past state, not garbage.
	f.cache[key] = cachedResult{data: live, cachedAt: time.Now()}

	if serveStale {
		return cached.data
	}
	return live
}

// Quorum loss and leader election storm moved to topology.go — they need
// a ClusterView (multi-node routing/role state), not a single nodeID, so
// they implement TopologyFault rather than NodeFault. See topology.go.

// WALDelayFault delays fsync-equivalent flush calls on the WAL stream by
// wrapping outbound packets on the write-ahead-log connection.
type WALDelayFault struct {
	FlushDelay time.Duration
}

func (f *WALDelayFault) Name() string { return "wal_delay" }

// Apply satisfies PacketFault; see ReplicaLagFault.Apply for why this
// sleep is now cancellable rather than unconditional.
func (f *WALDelayFault) Apply(ctx context.Context, pkt []byte, stream StreamType) []byte {
	if stream == ReplicationStream { // WAL shipping travels the replication stream
		select {
		case <-time.After(f.FlushDelay):
		case <-ctx.Done():
		}
	}
	return pkt
}

// WALCorruptionFault flips a small number of bytes in a fraction of WAL
// packets, simulating on-disk/on-wire corruption for recovery testing.
type WALCorruptionFault struct {
	Probability   float64
	BytesToFlip   int
}

func (f *WALCorruptionFault) Name() string { return "wal_corruption" }

func (f *WALCorruptionFault) Apply(ctx context.Context, pkt []byte, stream StreamType) []byte {
	if stream != ReplicationStream || rand.Float64() >= f.Probability || len(pkt) == 0 {
		return pkt
	}
	return flipBytes(pkt, f.BytesToFlip)
}

func flipBytes(pkt []byte, n int) []byte {
	corrupted := make([]byte, len(pkt))
	copy(corrupted, pkt)
	for i := 0; i < n; i++ {
		idx := rand.Intn(len(corrupted))
		corrupted[idx] ^= 0xFF
	}
	return corrupted
}

// QueryCorruptionFault is the client-query-stream counterpart to
// WALCorruptionFault: it flips bytes in ordinary query traffic instead
// of replication frames, for testing how clients/drivers react to a
// corrupted response rather than a corrupted replica.
type QueryCorruptionFault struct {
	Probability float64
	BytesToFlip int
}

func (f *QueryCorruptionFault) Name() string { return "query_corruption" }

func (f *QueryCorruptionFault) Apply(ctx context.Context, pkt []byte, stream StreamType) []byte {
	if stream != ClientQueryStream || rand.Float64() >= f.Probability || len(pkt) == 0 {
		return pkt
	}
	return flipBytes(pkt, f.BytesToFlip)
}