package faults

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"
)

// InMemoryClusterView — a concrete ClusterView.
//
// This is a real, usable implementation, not a stub: it tracks node
// membership, real vs. overridden roles, and group-based reachability
// in memory. What it does NOT do is talk to actual consensus state —
// "real role" here is whatever the caller told it via SetRealRole, not
// something read live off a Raft/Postgres node. Wiring RoleOf to genuine
// cluster state (a health-check probe, a Raft node's own /status
// endpoint) is the integration work a real deployment still needs to do;
// this struct is the membership/override bookkeeping around that.
type InMemoryClusterView struct {
	mu        sync.RWMutex
	nodes     []string
	realRoles map[string]string
	overrides map[string]string
	groups    map[string]string // nodeID -> group name; same group == reachable
}

func NewInMemoryClusterView(nodes []string) *InMemoryClusterView {
	groups := make(map[string]string, len(nodes))
	for _, n := range nodes {
		groups[n] = "default" // everyone reachable until a partition says otherwise
	}
	return &InMemoryClusterView{
		nodes:     nodes,
		realRoles: make(map[string]string),
		overrides: make(map[string]string),
		groups:    groups,
	}
}

func (v *InMemoryClusterView) Nodes() []string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]string, len(v.nodes))
	copy(out, v.nodes)
	return out
}

func (v *InMemoryClusterView) SetRealRole(nodeID, role string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.realRoles[nodeID] = role
}

func (v *InMemoryClusterView) RoleOf(nodeID string) string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if r, ok := v.overrides[nodeID]; ok {
		return r
	}
	return v.realRoles[nodeID]
}

func (v *InMemoryClusterView) SetRoleOverride(nodeID, role string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.overrides[nodeID] = role
}

func (v *InMemoryClusterView) ClearRoleOverride(nodeID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.overrides, nodeID)
}

func (v *InMemoryClusterView) Allowed(fromNode, toNode string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.groups[fromNode] == v.groups[toNode]
}

// setGroup and groupSnapshot are used by PartitionFault to move nodes
// between groups and to restore the pre-partition layout on Revert.
func (v *InMemoryClusterView) setGroup(nodeID, group string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.groups[nodeID] = group
}

func (v *InMemoryClusterView) groupSnapshot() map[string]string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	snap := make(map[string]string, len(v.groups))
	for k, val := range v.groups {
		snap[k] = val
	}
	return snap
}

func (v *InMemoryClusterView) restoreGroups(snap map[string]string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for k, val := range snap {
		v.groups[k] = val
	}
}


// Full partition — splits named groups so ClusterView.Allowed returns
// false across the split. The actual byte-level blocking happens at the
// proxy's routing layer, which must consult Allowed() per new connection
// (or per existing one, if it re-checks periodically) — this fault only
// owns the group-membership state, not the wire.
type PartitionFault struct {
	Groups map[string][]string // groupName -> nodeIDs

	preSnapshot map[string]string
}

func (f *PartitionFault) Name() string { return "partition" }

func (f *PartitionFault) Inject(ctx context.Context, view ClusterView) error {
	imv, ok := view.(*InMemoryClusterView)
	if !ok {
		return fmt.Errorf("partition: view must be *InMemoryClusterView, got %T", view)
	}
	f.preSnapshot = imv.groupSnapshot()
	for group, members := range f.Groups {
		for _, nodeID := range members {
			imv.setGroup(nodeID, group)
		}
	}
	return nil
}

func (f *PartitionFault) Revert(ctx context.Context, view ClusterView) error {
	imv, ok := view.(*InMemoryClusterView)
	if !ok {
		return fmt.Errorf("partition: view must be *InMemoryClusterView, got %T", view)
	}
	if f.preSnapshot != nil {
		imv.restoreGroups(f.preSnapshot)
	}
	return nil
}

// Split-brain — forces two (or more) nodes to simultaneously report
// "leader", without touching real consensus state. Pairs naturally with
// PartitionFault: split the cluster in two, then give each half its own
// self-reported leader so both sides keep accepting writes.
type SplitBrainFault struct {
	Leaders []string // nodes that will all claim "leader"

	previous map[string]string
}

func (f *SplitBrainFault) Name() string { return "split_brain" }

func (f *SplitBrainFault) Inject(ctx context.Context, view ClusterView) error {
	f.previous = make(map[string]string, len(f.Leaders))
	for _, nodeID := range f.Leaders {
		f.previous[nodeID] = view.RoleOf(nodeID)
		view.SetRoleOverride(nodeID, "leader")
	}
	return nil
}

func (f *SplitBrainFault) Revert(ctx context.Context, view ClusterView) error {
	for _, nodeID := range f.Leaders {
		view.ClearRoleOverride(nodeID)
	}
	return nil
}

// Quorum loss — isolates enough voters from the cluster network that the
// remaining voters fall below majority, without a full symmetric
// partition (the isolated nodes are simply gone, not split into their
// own functioning group).
type QuorumLossFault struct {
	VoterNodes   []string
	NodesToBlock int
	DockerNetwork string // e.g. "dbfailsim_net"

	mu         sync.Mutex
	blockedSet map[string]bool
}

func (f *QuorumLossFault) Name() string { return "quorum_loss" }

func (f *QuorumLossFault) Inject(ctx context.Context, view ClusterView) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockedSet = make(map[string]bool)

	net := f.DockerNetwork
	if net == "" {
		net = "dbfailsim_net"
	}

	blocked := 0
	for _, v := range f.VoterNodes {
		if blocked >= f.NodesToBlock {
			break
		}
		cmd := exec.CommandContext(ctx, "docker", "network", "disconnect", net, v)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("quorum loss: failed to isolate %s: %w (%s)", v, err, out)
		}
		f.blockedSet[v] = true
		blocked++
	}
	return nil
}

func (f *QuorumLossFault) Revert(ctx context.Context, view ClusterView) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	net := f.DockerNetwork
	if net == "" {
		net = "dbfailsim_net"
	}
	for v := range f.blockedSet {
		cmd := exec.CommandContext(ctx, "docker", "network", "connect", net, v)
		_ = cmd.Run() // best-effort; a node that's still down will fail this harmlessly
	}
	f.blockedSet = make(map[string]bool)
	return nil
}

// IsBlocked reports whether a given voter is currently isolated — useful
// for the consistency checker to know which nodes to skip when judging
// whether a write should have succeeded.
func (f *QuorumLossFault) IsBlocked(nodeID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blockedSet[nodeID]
}


// Leader election storm — repeatedly kills whichever node currently
// reports itself as leader, forcing continuous re-election churn.


// LeaderElectionStormFault needs a way to identify the current leader.
// The default implementation asks the ClusterView directly (RoleOf); a
// caller can instead supply LeaderProbe for cases where "leader" needs
// to be confirmed against a live health endpoint rather than trusted
// from ClusterView's bookkeeping (e.g. after a SplitBrainFault has
// deliberately made RoleOf lie).
type LeaderElectionStormFault struct {
	LeaderProbe func(ctx context.Context, view ClusterView) (string, error)
	Interval    time.Duration
	Duration    time.Duration

	cancel context.CancelFunc
}

func (f *LeaderElectionStormFault) Name() string { return "leader_election_storm" }

func (f *LeaderElectionStormFault) defaultProbe(ctx context.Context, view ClusterView) (string, error) {
	for _, n := range view.Nodes() {
		if view.RoleOf(n) == "leader" {
			return n, nil
		}
	}
	return "", nil
}

func (f *LeaderElectionStormFault) Inject(ctx context.Context, view ClusterView) error {
	loopCtx, cancel := context.WithTimeout(ctx, f.Duration)
	f.cancel = cancel

	probe := f.LeaderProbe
	if probe == nil {
		probe = f.defaultProbe
	}

	go func() {
		ticker := time.NewTicker(f.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				leader, err := probe(loopCtx, view)
				if err != nil || leader == "" {
					continue
				}
				_ = exec.CommandContext(loopCtx, "docker", "kill", "--signal", "SIGTERM", leader).Run()
			}
		}
	}()
	return nil
}

func (f *LeaderElectionStormFault) Revert(ctx context.Context, view ClusterView) error {
	if f.cancel != nil {
		f.cancel()
	}
	return nil
}