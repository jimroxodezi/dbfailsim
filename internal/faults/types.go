package faults

import (
	"context"
	"net"
	"time"
)


// Direction indicates which leg of the proxied connection a fault applies to.
type Direction int

const (
	DirectionClientToUpstream Direction = iota
	DirectionUpstreamToClient
)

// StreamType lets DB-aware faults distinguish query traffic from
// replication traffic when both flow through the same proxy.
type StreamType int

const (
	ClientQueryStream StreamType = iota
	ReplicationStream
)

// baseConn wraps a net.Conn and provides common fault handling logic.
type baseConn struct {
	net.Conn
}


// Network faults
// One connection, no protocol awareness needed. Fault.Apply wraps a
// net.Conn once at accept time; the wrapper overrides Read/Write to
// exhibit the fault on every future call.
//
// Maps to:
//   - Latency injection (fixed/jittered)  -> LatencyFault
//   - Latency injection (gradual increase) -> LatencyFault + TimeVaryingFault
//   - Packet loss (uniform)                -> LossFault
//   - Packet loss (bursty)                 -> LossFault + TimeVaryingFault
//   - Bandwidth throttling                 -> ThroughputFault
//   - Connection reset / TCP RST           -> Fault (RSTFault, no extra iface)
//   - Packet reordering                    -> Fault (ReorderFault)
//   - Packet duplication                   -> Fault (DuplicationFault)
//   - Asymmetric partition                 -> Fault (AsymmetricPartitionFault)
//   - DNS resolution failure                -> DialFault (acts at dial time, not on an established conn)


// Fault is the interface every connection-level (proxy) fault implements.
// Apply wraps the given net.Conn and returns a (possibly wrapped) net.Conn
// that exhibits the fault behavior on read/write. Apply itself must be
// non-blocking — it runs once at accept time. Faults that need cancellable
// blocking behavior belong on PacketFault instead (see below).
type Fault interface {
	Name() string
	Apply(conn net.Conn, dir Direction) net.Conn
}

// TimeVaryingFault is implemented by faults whose intensity changes over
// their own lifetime rather than staying constant — gradually ramping
// latency, or loss that spikes in bursts. The scenario runner calls Tick
// on a timer for the lifetime of the fault; the fault mutates its own
// internal state (e.g. current delay) which its wrapped conn then reads
// on every Read/Write. Implemented alongside Fault, not instead of it:
// a ramping-latency fault implements both.
type TimeVaryingFault interface {
	Fault
	Tick(elapsed time.Duration)
}

// LatencyFault will back fixed, jittered, and (via TimeVaryingFault)
// gradually-increasing latency injection. Fixed/jittered needs no Tick;
// a "RampingLatencyFault" variant adds Tick to grow f.current toward
// f.Max over f.RampDuration.
type LatencyFault interface {
	Fault
	CurrentDelay() time.Duration
}

// LossFault backs both uniform and bursty packet loss. Uniform loss is a
// constant probability; bursty loss (BurstyLossFault, already implemented
// in network.go) additionally implements TimeVaryingFault to flip between
// baseline and burst rates.
type LossFault interface {
	Fault
	CurrentLossRate() float64
}

// ThroughputFault backs bandwidth throttling — a token-bucket or
// leaky-bucket limiter wrapped around Write. Implemented as a plain Fault
// (rate limiting doesn't need extra methods beyond Apply), but named here
// to document the token-bucket state (RateBytesPerSec, BurstBytes) a
// future BandwidthFault struct will carry.
type ThroughputFault interface {
	Fault
	RateBytesPerSec() int64
}

// DialFault acts before a connection exists, at the proxy's outbound
// dial step — this is how DNS-resolution failure gets simulated (there's
// no net.Conn yet to wrap). The proxy's dialer calls BeforeDial prior to
// every new upstream connection attempt and aborts if it returns an error.
type DialFault interface {
	Name() string
	BeforeDial(ctx context.Context, targetHost string) error
}

// Packet-level, protocol-aware faults
// Require a packet already classified by stream type (needs a protocol
// parser upstream — see fluxproxy's Postgres/MySQL/Redis framing work).
// Apply takes a context because some of these legitimately sleep
// (replica lag, WAL delay) and must respect cancellation rather than
// blocking a goroutine past scenario/connection teardown.
//
// Maps to:
//   - Replica lag        -> PacketFault (ReplicaLagFault)
//   - WAL delay           -> PacketFault (WALDelayFault)
//   - WAL corruption      -> PacketFault (WALCorruptionFault)
//   - Silent data corruption on a replica -> PacketFault (bit-flip variant, same shape as WALCorruptionFault but applied to normal row data, not WAL frames)
// 

// PacketFault is for faults that need to inspect/mutate raw bytes with
// awareness of stream type (e.g. replica lag should only touch the
// replication stream, not client queries).
type PacketFault interface {
	Name() string
	Apply(ctx context.Context, pkt []byte, stream StreamType) []byte
}


// Node/process faults
// Act on a whole node/container rather than a connection.
//
// Maps to:
//   - Hard crash (SIGKILL)      -> NodeFault (CrashFault)
//   - Graceful shutdown (SIGTERM) -> NodeFault (CrashFault, same struct, different signal)
//   - Slow node / CPU throttle  -> NodeFault (CPUThrottleFault)
//   - Clock skew / drift        -> NodeFault (ClockSkewFault)
//   - Disk full                 -> NodeFault (DiskFullFault)
//   - Disk I/O latency          -> NodeFault (DiskIOLatencyFault)
//   - Memory pressure / OOM     -> NodeFault (OOMFault)
//   - Zombie process            -> NodeFault (ZombieFault)
//   - Crash-loop                -> NodeFault (CrashLoopFault)
// 

// NodeFault is the interface for faults that act on a whole node/process
// rather than a single connection (crash, clock skew, resource pressure).
// The fault states intent; the NodeDriver (driver.go) supplies the
// mechanism for whatever the node actually is — a local process, a
// systemd unit, a container, or one of those over ssh. Inject/Revert take
// a context because the underlying action is a real blocking subprocess
// call that should be cancellable if the scenario is torn down mid-fault.
type NodeFault interface {
	Name() string
	Inject(ctx context.Context, node NodeDriver) error
	Revert(ctx context.Context, node NodeDriver) error
}

// Topology faults
// Faults that need a view of the whole cluster/group graph rather than
// a single connection or single node — partitioning by group, or lying
// about leadership across multiple nodes at once.
//
// Maps to:
//   - Full partition (split-brain groups) -> TopologyFault (PartitionFault)
//   - Split-brain (two leaders)            -> TopologyFault (SplitBrainFault)
//   - Quorum loss                          -> TopologyFault (QuorumLossFault)
//   - Leader election storm                -> TopologyFault (LeaderElectionStormFault)
// 

// ClusterView is the minimal cluster membership/role surface a
// TopologyFault needs. A real implementation adapts dbfailsim's existing
// node registry (config.NodeConfig list) plus a way to query/override
// each node's self-reported role, without the fault needing to know
// whether the underlying consensus is Raft, Postgres streaming
// replication, etc.
type ClusterView interface {
	Nodes() []string
	RoleOf(nodeID string) string
	// Allowed reports whether traffic between two nodes should currently
	// pass; the proxy's routing layer consults this per new connection.
	Allowed(fromNode, toNode string) bool
	// SetRoleOverride lets a fault force a node to report a role other
	// than its real one — e.g. making two nodes both answer "leader" to
	// produce a split-brain, without touching real consensus state.
	SetRoleOverride(nodeID, role string)
	ClearRoleOverride(nodeID string)
}

// TopologyFault is the interface for faults that mutate cluster-wide
// routing or role state rather than a single conn or single node.
type TopologyFault interface {
	Name() string
	Inject(ctx context.Context, view ClusterView) error
	Revert(ctx context.Context, view ClusterView) error
}




// Data/log-level faults
// Act on stored state (Raft log entries, row data) rather than bytes in
// flight. Requires a DataStore adapter around whatever storage engine is
// under test, so the fault logic itself stays storage-agnostic.
//
// Maps to:
//   - Log divergence (conflicting Raft logs) -> DataFault (LogDivergenceFault)
//   - Silent data corruption on one replica (at-rest variant) -> DataFault (RowCorruptionFault)


// LogEntry is a minimal, consensus-implementation-agnostic view of one
// replicated log entry, enough for a DataFault to read/write divergent
// entries without knowing if the real thing is Raft, Paxos, etc.
type LogEntry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// DataStore is the minimal storage-access surface a DataFault needs.
// A real implementation wraps the actual DB/log client (e.g. a Postgres
// connection for row access, or dbfailsim's own Raft-lite log once built)
// behind this interface so fault code never depends on a specific engine.
type DataStore interface {
	ReadLogEntries(ctx context.Context, nodeID string, fromIndex uint64) ([]LogEntry, error)
	WriteLogEntry(ctx context.Context, nodeID string, entry LogEntry) error
	ReadRow(ctx context.Context, nodeID, table, key string) ([]byte, error)
	WriteRow(ctx context.Context, nodeID, table, key string, value []byte) error
}

// DataFault mutates stored state directly rather than bytes in flight or
// process state — the class of fault needed for log divergence and
// at-rest data corruption, neither of which a network- or process-level
// fault can produce on their own.
type DataFault interface {
	Name() string
	Inject(ctx context.Context, store DataStore, nodeID string) error
	Revert(ctx context.Context, store DataStore, nodeID string) error
}


// Database-specific / workload faults
// Act through an active client session or connection pool rather than
// the wire or the process — these need to actually run or hold
// transactions, not just delay/drop bytes.
//
// Maps to:
//   - Failover mid-transaction        -> WorkloadFault (MidTxFailoverFault, composes a NodeFault crash with a held Tx)
//   - Connection pool exhaustion      -> WorkloadFault (PoolExhaustionFault)
//   - Deadlock injection              -> WorkloadFault (DeadlockFault)
//   - Long-running transaction / lock contention -> WorkloadFault (LockContentionFault)
//   - Backup/restore inconsistency window -> WorkloadFault (BackupWindowFault, needs a Snapshot()/Restore() pair, not just Exec)


// Tx is the minimal transaction surface a WorkloadFault drives.
type Tx interface {
	Exec(ctx context.Context, query string, args ...any) error
	Commit() error
	Rollback() error
}

// PoolStats reports connection-pool occupancy so faults (and the
// consistency checker) can observe exhaustion without depending on a
// specific driver's pool metrics API.
type PoolStats struct {
	InUse int
	Idle  int
	Max   int
}

// DBSession is the minimal query-execution surface a WorkloadFault acts
// through — an adapter around the real driver/pool (database/sql,
// pgx, etc.) so fault code stays driver-agnostic.
type DBSession interface {
	Exec(ctx context.Context, query string, args ...any) error
	Begin(ctx context.Context) (Tx, error)
	PoolStats() PoolStats
}

// WorkloadFault is the interface for faults that must actively drive
// query/transaction traffic to produce their effect, rather than passively
// mutating bytes, a node, or stored state.
type WorkloadFault interface {
	Name() string
	Inject(ctx context.Context, session DBSession) error
}


// Client-visible consistency faults
// These don't corrupt anything themselves — they arrange conditions
// likely to produce a specific, named violation, and report the attempt
// to dbfailsim's existing consistency-checker so it knows what pattern to
// assert against instead of waiting for one to occur by chance.
//
// Maps to:
//   - Read-your-writes violation -> ConsistencyFault (ReadYourWritesFault, routes a post-write read to a lagging replica)
//   - Monotonic-read violation   -> ConsistencyFault (MonotonicReadFault, alternates reads across replicas at different lag)
//   - Stale cache serving        -> ConsistencyFault (already implemented as StaleReadFault; also fits here as the checker-facing wrapper around it)


// Violation is one observed consistency-guarantee break, as recorded by
// the checker.
type Violation struct {
	Type     string // "read_your_writes", "monotonic_read", "stale_cache"
	ClientID string
	Details  string
	At       time.Time
}

// ConsistencyChecker is implemented by dbfailsim's existing checker
// component. ConsistencyFault implementations report against it rather
// than asserting anything themselves — the fault's job is only to create
// the conditions, the checker's job is to detect whether the violation
// actually occurred.
type ConsistencyChecker interface {
	RecordWrite(clientID, key string, value []byte, at time.Time)
	RecordRead(clientID, key string, value []byte, at time.Time)
	Violations() []Violation
}

// ConsistencyFault provokes a specific class of client-visible violation
// (by cooperating with routing/replica-lag state elsewhere in the fault
// chain) and registers what it's attempting with the checker.
type ConsistencyFault interface {
	Name() string
	Provoke(ctx context.Context, checker ConsistencyChecker) error
}