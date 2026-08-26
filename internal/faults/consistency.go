package faults

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// 
// InMemoryConsistencyChecker — a concrete, usable ConsistencyChecker.
//
// This records writes/reads per client and flags the three violation
// types on demand. It is a real, working implementation of the
// interface — but it is very likely NOT the same as whatever checker
// dbfailsim already has; that component wasn't visible when this
// interface was designed; guessed at its shape. Treat this as a
// reference implementation to compare against your real one, and adapt
// ConsistencyFault's Provoke calls to your actual checker's method names
// if they differ.
type recordedWrite struct {
	value []byte
	at    time.Time
}

type recordedRead struct {
	value []byte
	at    time.Time
}

type InMemoryConsistencyChecker struct {
	mu         sync.Mutex
	writes     map[string][]recordedWrite // key -> writes, across all clients (for read-your-writes)
	clientReads map[string][]recordedRead // clientID+key -> reads, in order (for monotonic-read)
	violations []Violation
}

func NewInMemoryConsistencyChecker() *InMemoryConsistencyChecker {
	return &InMemoryConsistencyChecker{
		writes:      make(map[string][]recordedWrite),
		clientReads: make(map[string][]recordedRead),
	}
}

func (c *InMemoryConsistencyChecker) RecordWrite(clientID, key string, value []byte, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes[key] = append(c.writes[key], recordedWrite{value: value, at: at})
}

func (c *InMemoryConsistencyChecker) RecordRead(clientID, key string, value []byte, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Read-your-writes: this client previously wrote `key`; does the read
	// reflect that write or an earlier value?
	if lastWrite := c.lastWriteBefore(key, at); lastWrite != nil {
		if string(value) != string(lastWrite.value) {
			c.violations = append(c.violations, Violation{
				Type: "read_your_writes", ClientID: clientID,
				Details: fmt.Sprintf("key %s: expected value from write at %s, got stale value", key, lastWrite.at),
				At:      at,
			})
		}
	}

	// Monotonic-read: this client's own read sequence for `key` must never
	// go backwards to an earlier write's value once it's seen a later one.
	ck := clientID + ":" + key
	prevReads := c.clientReads[ck]
	if len(prevReads) > 0 {
		prevIdx := c.writeIndexOf(key, prevReads[len(prevReads)-1].value)
		curIdx := c.writeIndexOf(key, value)
		if prevIdx >= 0 && curIdx >= 0 && curIdx < prevIdx {
			c.violations = append(c.violations, Violation{
				Type: "monotonic_read", ClientID: clientID,
				Details: fmt.Sprintf("key %s: read went backwards from write #%d to write #%d", key, prevIdx, curIdx),
				At:      at,
			})
		}
	}
	c.clientReads[ck] = append(prevReads, recordedRead{value: value, at: at})
}

func (c *InMemoryConsistencyChecker) lastWriteBefore(key string, at time.Time) *recordedWrite {
	var last *recordedWrite
	for i := range c.writes[key] {
		w := &c.writes[key][i]
		if w.at.Before(at) || w.at.Equal(at) {
			if last == nil || w.at.After(last.at) {
				last = w
			}
		}
	}
	return last
}

func (c *InMemoryConsistencyChecker) writeIndexOf(key string, value []byte) int {
	for i, w := range c.writes[key] {
		if string(w.value) == string(value) {
			return i
		}
	}
	return -1
}

func (c *InMemoryConsistencyChecker) Violations() []Violation {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Violation, len(c.violations))
	copy(out, c.violations)
	return out
}

// Read-your-writes violation — writes to the primary, then immediately
// routes the same client's read to a replica known to be lagging (via
// ReplicaLagFault/RoutingHint), so the read very likely misses the write.

// RoutingHint is the minimal surface a ConsistencyFault needs to force a
// specific client's next read to a specific replica. A real proxy
// implements this by tagging the client's session with a target replica
// for its next N reads.
type RoutingHint interface {
	RouteClientTo(clientID, nodeID string)
}

type ReadYourWritesFault struct {
	Session    DBSession
	Router     RoutingHint
	LaggingReplica string
	Table, Col string
	ClientID   string
	Key        string
	Value      []byte
}

func (f *ReadYourWritesFault) Name() string { return "read_your_writes" }

func (f *ReadYourWritesFault) Provoke(ctx context.Context, checker ConsistencyChecker) error {
	writeQuery := fmt.Sprintf("UPDATE %s SET %s = $1 WHERE id = $2", f.Table, f.Col)
	if err := f.Session.Exec(ctx, writeQuery, f.Value, f.Key); err != nil {
		return fmt.Errorf("read_your_writes: write failed: %w", err)
	}
	checker.RecordWrite(f.ClientID, f.Key, f.Value, time.Now())

	// Force the immediately-following read for this client onto the
	// (likely lagging) replica instead of wherever it would normally go.
	f.Router.RouteClientTo(f.ClientID, f.LaggingReplica)

	// The actual read and its checker.RecordRead call happen in the
	// client's own next query — this fault only sets up the trap. Callers
	// wire the client's read path to call RecordRead; this function's job
	// ends at routing plus recording the write.
	return nil
}

// Monotonic-read violation — alternates a client's reads between two
// replicas with different lag, so a later read can land on the
// more-lagging replica and appear to go backwards in time.
type MonotonicReadFault struct {
	Router      RoutingHint
	ClientID    string
	ReplicaA    string // less-lagging
	ReplicaB    string // more-lagging
	SwitchAfter time.Duration
}

func (f *MonotonicReadFault) Name() string { return "monotonic_read" }

// Provoke doesn't touch data at all — it only manipulates routing.
// Route the client to the fresher replica first, then flip it to the
// more-lagging one after SwitchAfter; whatever the client reads on each
// side gets fed into checker.RecordRead by the caller's read path, same
// as ReadYourWritesFault.
func (f *MonotonicReadFault) Provoke(ctx context.Context, checker ConsistencyChecker) error {
	f.Router.RouteClientTo(f.ClientID, f.ReplicaA)

	select {
	case <-time.After(f.SwitchAfter):
	case <-ctx.Done():
		return ctx.Err()
	}

	f.Router.RouteClientTo(f.ClientID, f.ReplicaB)
	return nil
}

// Stale cache serving — a checker-facing wrapper around the existing
// StaleReadFault (db.go). StaleReadFault already does the actual
// caching/serving; this type's job is only to tell the checker what to
// expect so it can judge a subsequent read as a violation rather than
// needing to infer intent.

type StaleCacheFault struct {
	Underlying *StaleReadFault
	ClientID   string
	Key        string
}

func (f *StaleCacheFault) Name() string { return "stale_cache" }

func (f *StaleCacheFault) Provoke(ctx context.Context, checker ConsistencyChecker) error {
	// Nothing to do here beyond documenting intent: StaleReadFault
	// (registered separately as a PacketFault/read-path hook) is already
	// live and will probabilistically serve stale data on its own. This
	// method exists so a scenario can log "we're expecting possible
	// stale_cache violations for this client/key" — useful if the
	// checker's Violations() output needs to be filtered by "was this
	// fault active" rather than treating every stale serve as unexpected.
	return nil
}