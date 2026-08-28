package raft

import (
	"sort"
)

// Cluster is an in-memory simulated network of Nodes with a controllable
// partition, for tests and demos. Messages sent during a tick are
// delivered at the start of the next tick, in a deterministic order, so a
// run with fixed seeds is fully reproducible. It is single-threaded.
type Cluster struct {
	Nodes   map[string]*Node
	order   []string
	inbox   []Message
	blocked map[[2]string]bool // directed link from->to that drops messages
	Trace   []Message          // every delivered message, if Record is set
	Record  bool
}

// NewCluster builds n nodes named by ids with the same config (each gets
// its own deterministic seed derived from its id unless cfg.Seed is set).
func NewCluster(ids []string, cfg Config) *Cluster {
	c := &Cluster{Nodes: map[string]*Node{}, blocked: map[[2]string]bool{}}
	c.order = append([]string(nil), ids...)
	sort.Strings(c.order)
	for _, id := range c.order {
		c.Nodes[id] = New(id, ids, cfg)
	}
	return c
}

// Partition cuts every link between the two groups, both directions.
func (c *Cluster) Partition(groupA, groupB []string) {
	for _, a := range groupA {
		for _, b := range groupB {
			c.blocked[[2]string{a, b}] = true
			c.blocked[[2]string{b, a}] = true
		}
	}
}

// Isolate cuts a node from everyone, both directions.
func (c *Cluster) Isolate(id string) {
	for _, other := range c.order {
		if other != id {
			c.blocked[[2]string{id, other}] = true
			c.blocked[[2]string{other, id}] = true
		}
	}
}

// BlockOneWay drops messages from -> to only; the reverse still flows.
// This is the asymmetric partition that makes elections interesting.
func (c *Cluster) BlockOneWay(from, to string) { c.blocked[[2]string{from, to}] = true }

// Heal restores every link.
func (c *Cluster) Heal() { c.blocked = map[[2]string]bool{} }

// Tick advances every node one tick, delivering last tick's messages
// first. Returns the number of messages delivered.
func (c *Cluster) Tick() int {
	pending := c.inbox
	c.inbox = nil
	delivered := 0
	for _, m := range pending {
		if c.blocked[[2]string{m.From, m.To}] {
			continue
		}
		delivered++
		if c.Record {
			c.Trace = append(c.Trace, m)
		}
		c.send(c.Nodes[m.To].Step(m))
	}
	for _, id := range c.order {
		c.send(c.Nodes[id].Tick())
	}
	return delivered
}

// Run ticks n times.
func (c *Cluster) Run(n int) {
	for i := 0; i < n; i++ {
		c.Tick()
	}
}

func (c *Cluster) send(msgs []Message) { c.inbox = append(c.inbox, msgs...) }

// Leaders returns the nodes currently in the Leader role, sorted.
func (c *Cluster) Leaders() []string {
	var out []string
	for _, id := range c.order {
		if c.Nodes[id].Role() == Leader {
			out = append(out, id)
		}
	}
	return out
}

// Leader returns the single leader if exactly one exists, else "".
func (c *Cluster) Leader() string {
	if l := c.Leaders(); len(l) == 1 {
		return l[0]
	}
	return ""
}

// RunUntilLeader ticks until some node is leader or maxTicks elapse;
// returns the leader ("" on timeout) and the ticks it took.
func (c *Cluster) RunUntilLeader(maxTicks int) (string, int) {
	for i := 0; i < maxTicks; i++ {
		if l := c.Leader(); l != "" {
			return l, i
		}
		c.Tick()
	}
	return c.Leader(), maxTicks
}

// MaxTerm is the highest term any node has seen.
func (c *Cluster) MaxTerm() uint64 {
	var t uint64
	for _, n := range c.Nodes {
		if n.Term() > t {
			t = n.Term()
		}
	}
	return t
}
