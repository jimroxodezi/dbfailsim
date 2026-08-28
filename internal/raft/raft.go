// Package raft implements the leader-election half of Raft (Ongaro &
// Ousterhout, "In Search of an Understandable Consensus Algorithm", §5.2):
// terms, the follower/candidate/leader roles, randomized election
// timeouts, RequestVote, and leader heartbeats. There is no log and no
// log replication — the point, for dbfailsim, is to watch elections,
// split votes, and majority math happen under real injected partitions.
//
// Node is a pure state machine. It owns no goroutines, timers, or
// sockets: the caller advances it with Tick (one unit of logical time) and
// feeds it messages with Step; both return the messages the node wants
// sent. That keeps the algorithm deterministic and unit-testable with a
// simulated network (see sim.go), and lets the real transport — later,
// over dbfailsim-proxied links — be plugged in without touching the rules.
package raft

import (
	"fmt"
	"math/rand"
	"sort"
)

// Role is a node's current role in the election protocol.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// MessageType enumerates the RPCs. Raft's AppendEntries doubles as the
// heartbeat; with no log to replicate, only the heartbeat use remains.
type MessageType int

const (
	MsgRequestVote  MessageType = iota
	MsgVote                     // RequestVote response
	MsgHeartbeat                // empty AppendEntries from the leader
	MsgHeartbeatAck             // AppendEntries response (informational only here)
)

func (t MessageType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgVote:
		return "Vote"
	case MsgHeartbeat:
		return "Heartbeat"
	case MsgHeartbeatAck:
		return "HeartbeatAck"
	}
	return "unknown"
}

// Message is one RPC or response between two nodes. Every message carries
// the sender's current term, which is how Raft discovers stale leaders
// and candidates: any message with a higher term than the receiver's
// makes the receiver a follower of that term.
type Message struct {
	Type MessageType
	From string
	To   string
	Term uint64

	// Granted is set on MsgVote: whether From voted for To in Term.
	Granted bool
}

func (m Message) String() string {
	s := fmt.Sprintf("%s %s->%s term=%d", m.Type, m.From, m.To, m.Term)
	if m.Type == MsgVote {
		s += fmt.Sprintf(" granted=%v", m.Granted)
	}
	return s
}

// Config sets a node's timing, in ticks. Raft requires
// heartbeat << election timeout, and election timeouts randomized across
// a range wide enough that split votes are rare.
type Config struct {
	// ElectionTimeoutMin/Max bound the randomized election timeout: a
	// follower that hears nothing from a leader for this many ticks
	// becomes a candidate. The paper's defaults are 150–300 ms.
	ElectionTimeoutMin int
	ElectionTimeoutMax int
	// HeartbeatTicks is how often a leader sends heartbeats.
	HeartbeatTicks int
	// Seed makes the randomized timeouts reproducible. 0 means seed from
	// the node ID, so a fixed cluster always behaves the same way.
	Seed int64
}

// DefaultConfig is the paper's ratio in ticks: with a 10 ms tick this is
// a 150–300 ms election timeout and 50 ms heartbeats.
func DefaultConfig() Config {
	return Config{ElectionTimeoutMin: 15, ElectionTimeoutMax: 30, HeartbeatTicks: 5}
}

// Node is one Raft participant. It is not safe for concurrent use; the
// owner serializes Tick and Step (a Driver, a test, or a simulation).
type Node struct {
	id    string
	peers []string // every other node, sorted
	cfg   Config
	rng   *rand.Rand

	role     Role
	term     uint64
	votedFor string // "" = none this term
	leader   string // who this node believes leads the current term
	votes    map[string]bool

	elapsed         int // ticks since the last heartbeat/vote (follower/candidate) or last heartbeat sent (leader)
	electionTimeout int // current randomized timeout
	electionsWon    uint64
	electionsBegun  uint64
}

// New creates a follower for id among the given cluster members (which
// may or may not include id).
func New(id string, members []string, cfg Config) *Node {
	if cfg.ElectionTimeoutMin <= 0 || cfg.ElectionTimeoutMax < cfg.ElectionTimeoutMin || cfg.HeartbeatTicks <= 0 {
		panic("raft: invalid Config")
	}
	seed := cfg.Seed
	if seed == 0 {
		for _, c := range id {
			seed = seed*31 + int64(c)
		}
	}
	var peers []string
	for _, m := range members {
		if m != id {
			peers = append(peers, m)
		}
	}
	sort.Strings(peers)
	n := &Node{id: id, peers: peers, cfg: cfg, rng: rand.New(rand.NewSource(seed)), votes: map[string]bool{}}
	n.resetElectionTimer()
	return n
}

// Status is a read-only snapshot for the dashboard, tests, and the event log.
type Status struct {
	ID             string `json:"id"`
	Role           string `json:"role"`
	Term           uint64 `json:"term"`
	Leader         string `json:"leader,omitempty"`
	VotedFor       string `json:"voted_for,omitempty"`
	Votes          int    `json:"votes"` // votes received this term (candidate)
	ClusterSize    int    `json:"cluster_size"`
	ElectionsBegun uint64 `json:"elections_begun"`
	ElectionsWon   uint64 `json:"elections_won"`
}

func (n *Node) Status() Status {
	return Status{
		ID: n.id, Role: n.role.String(), Term: n.term, Leader: n.leader, VotedFor: n.votedFor,
		Votes: len(n.votes), ClusterSize: len(n.peers) + 1,
		ElectionsBegun: n.electionsBegun, ElectionsWon: n.electionsWon,
	}
}

func (n *Node) ID() string     { return n.id }
func (n *Node) Role() Role     { return n.role }
func (n *Node) Term() uint64   { return n.term }
func (n *Node) Leader() string { return n.leader }

// quorum is the majority size: floor(N/2)+1.
func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

func (n *Node) resetElectionTimer() {
	n.elapsed = 0
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin + 1
	n.electionTimeout = n.cfg.ElectionTimeoutMin + n.rng.Intn(span)
}

// Tick advances logical time by one unit and returns any messages to
// send: RequestVotes when an election timeout fires, heartbeats when the
// leader's heartbeat interval elapses.
func (n *Node) Tick() []Message {
	n.elapsed++
	switch n.role {
	case Leader:
		if n.elapsed >= n.cfg.HeartbeatTicks {
			n.elapsed = 0
			return n.broadcast(MsgHeartbeat)
		}
	default: // Follower or Candidate
		if n.elapsed >= n.electionTimeout {
			return n.startElection()
		}
	}
	return nil
}

// startElection: increment term, vote for self, ask everyone else. A
// candidate whose timeout fires again (split vote) simply runs this again
// with a new term and a fresh random timeout — that is how split votes
// resolve.
func (n *Node) startElection() []Message {
	n.term++
	n.role = Candidate
	n.votedFor = n.id
	n.leader = ""
	n.votes = map[string]bool{n.id: true}
	n.electionsBegun++
	n.resetElectionTimer()
	if len(n.votes) >= n.quorum() { // single-node cluster
		return n.becomeLeader()
	}
	return n.broadcast(MsgRequestVote)
}

func (n *Node) becomeLeader() []Message {
	n.role = Leader
	n.leader = n.id
	n.elapsed = 0
	n.electionsWon++
	return n.broadcast(MsgHeartbeat) // assert leadership immediately
}

// becomeFollower is the universal "saw a higher term" transition.
func (n *Node) becomeFollower(term uint64, leader string) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
	}
	n.role = Follower
	n.leader = leader
	n.votes = map[string]bool{}
	n.resetElectionTimer()
}

func (n *Node) broadcast(t MessageType) []Message {
	out := make([]Message, 0, len(n.peers))
	for _, p := range n.peers {
		out = append(out, Message{Type: t, From: n.id, To: p, Term: n.term})
	}
	return out
}

// Step delivers one message and returns the node's responses.
func (n *Node) Step(m Message) []Message {
	if m.To != n.id {
		return nil
	}
	// Rule for all servers: a higher term always demotes us.
	if m.Term > n.term {
		leader := ""
		if m.Type == MsgHeartbeat {
			leader = m.From
		}
		n.becomeFollower(m.Term, leader)
	}
	// Anything from an older term is stale; answer RequestVote/Heartbeat
	// with our term so the sender steps down, ignore stale responses.
	if m.Term < n.term {
		switch m.Type {
		case MsgRequestVote:
			return []Message{{Type: MsgVote, From: n.id, To: m.From, Term: n.term, Granted: false}}
		case MsgHeartbeat:
			return []Message{{Type: MsgHeartbeatAck, From: n.id, To: m.From, Term: n.term}}
		}
		return nil
	}

	switch m.Type {
	case MsgRequestVote:
		// Grant if we have not voted for anyone else this term. (With a
		// log, Raft also requires the candidate's log to be at least as
		// up to date as ours; there is no log here.)
		granted := n.votedFor == "" || n.votedFor == m.From
		if granted {
			n.votedFor = m.From
			n.resetElectionTimer() // granting a vote defers our own election
		}
		return []Message{{Type: MsgVote, From: n.id, To: m.From, Term: n.term, Granted: granted}}

	case MsgVote:
		if n.role != Candidate || !m.Granted {
			return nil
		}
		n.votes[m.From] = true
		if len(n.votes) >= n.quorum() {
			return n.becomeLeader()
		}

	case MsgHeartbeat:
		// Same-term heartbeat: a candidate concedes, a follower resets
		// its timer. A leader receiving a same-term heartbeat from
		// someone else cannot happen (one leader per term) and is ignored.
		if n.role != Leader {
			n.role = Follower
			n.leader = m.From
			n.votes = map[string]bool{}
			n.resetElectionTimer()
		}
		return []Message{{Type: MsgHeartbeatAck, From: n.id, To: m.From, Term: n.term}}

	case MsgHeartbeatAck:
		// Informational; a future log would use this for matchIndex.
	}
	return nil
}
