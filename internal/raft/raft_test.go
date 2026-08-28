package raft

import (
	"testing"
)

func ids(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = string(rune('A' + i))
	}
	return out
}

func TestSingleNodeElectsItself(t *testing.T) {
	c := NewCluster(ids(1), DefaultConfig())
	l, ticks := c.RunUntilLeader(100)
	if l != "A" {
		t.Fatalf("leader = %q after %d ticks", l, ticks)
	}
}

func TestThreeNodesElectExactlyOneLeader(t *testing.T) {
	c := NewCluster(ids(3), DefaultConfig())
	l, ticks := c.RunUntilLeader(500)
	if l == "" {
		t.Fatal("no leader elected")
	}
	t.Logf("leader %s after %d ticks, term %d", l, ticks, c.MaxTerm())
	c.Run(200)
	if got := c.Leaders(); len(got) != 1 || got[0] != l {
		t.Fatalf("leadership unstable: leaders = %v", got)
	}
	for id, n := range c.Nodes {
		if n.Leader() != l {
			t.Errorf("%s believes leader is %q, want %s", id, n.Leader(), l)
		}
		if n.Term() != c.Nodes[l].Term() {
			t.Errorf("%s term %d != leader term %d", id, n.Term(), c.Nodes[l].Term())
		}
	}
}

func TestAtMostOneLeaderPerTerm(t *testing.T) {
	// Run several clusters with churn and assert the safety property on
	// every tick: no two leaders share a term.
	for seed := int64(1); seed <= 20; seed++ {
		cfg := DefaultConfig()
		cfg.Seed = seed
		c := NewCluster(ids(5), cfg)
		leadersByTerm := map[uint64]string{}
		for i := 0; i < 400; i++ {
			c.Tick()
			if i == 150 {
				c.Isolate(c.Leader())
			}
			if i == 300 {
				c.Heal()
			}
			for id, n := range c.Nodes {
				if n.Role() != Leader {
					continue
				}
				if prev, ok := leadersByTerm[n.Term()]; ok && prev != id {
					t.Fatalf("seed %d tick %d: two leaders in term %d: %s and %s", seed, i, n.Term(), prev, id)
				}
				leadersByTerm[n.Term()] = id
			}
		}
	}
}

func TestLeaderPartitionTriggersNewElection(t *testing.T) {
	c := NewCluster(ids(3), DefaultConfig())
	old, _ := c.RunUntilLeader(500)
	oldTerm := c.Nodes[old].Term()

	c.Isolate(old)
	// The isolated leader keeps thinking it leads (it hears no higher
	// term); the majority side must elect someone else at a higher term.
	var newLeader string
	for i := 0; i < 500 && newLeader == ""; i++ {
		c.Tick()
		for _, l := range c.Leaders() {
			if l != old {
				newLeader = l
			}
		}
	}
	if newLeader == "" {
		t.Fatal("majority side never elected a new leader")
	}
	if c.Nodes[newLeader].Term() <= oldTerm {
		t.Fatalf("new leader term %d not > old term %d", c.Nodes[newLeader].Term(), oldTerm)
	}
	if c.Nodes[old].Role() != Leader {
		t.Fatal("isolated leader should still believe it leads (it cannot learn otherwise)")
	}

	// Heal: the deposed leader must step down on seeing the higher term.
	c.Heal()
	c.Run(50)
	if c.Nodes[old].Role() != Follower || c.Nodes[old].Leader() != newLeader {
		t.Fatalf("old leader after heal: role=%s leader=%q, want follower of %s", c.Nodes[old].Role(), c.Nodes[old].Leader(), newLeader)
	}
	if got := c.Leaders(); len(got) != 1 || got[0] != newLeader {
		t.Fatalf("leaders after heal = %v", got)
	}
}

func TestNoMajorityMeansNoLeader(t *testing.T) {
	c := NewCluster(ids(4), DefaultConfig())
	c.Partition([]string{"A", "B"}, []string{"C", "D"}) // 2|2: nobody has 3
	c.Run(1000)
	if got := c.Leaders(); len(got) != 0 {
		t.Fatalf("leaders without a majority: %v", got)
	}
	if c.MaxTerm() < 5 {
		t.Fatalf("expected candidates to keep cycling terms; max term = %d", c.MaxTerm())
	}
	// Heal and a leader appears.
	c.Heal()
	if l, _ := c.RunUntilLeader(500); l == "" {
		t.Fatal("no leader after heal")
	}
}

func TestMinorityPartitionCannotElect(t *testing.T) {
	c := NewCluster(ids(5), DefaultConfig())
	c.RunUntilLeader(500)
	c.Partition([]string{"A", "B"}, []string{"C", "D", "E"})
	c.Run(600)
	for _, l := range c.Leaders() {
		if l == "A" || l == "B" {
			// Only acceptable if it was the leader before the split and
			// never learned better — but its followers on the minority
			// side must not have *elected* it after the split.
			if c.Nodes[l].Status().ElectionsWon > 1 {
				t.Fatalf("minority side elected %s", l)
			}
		}
	}
	majorityHasLeader := false
	for _, l := range c.Leaders() {
		if l == "C" || l == "D" || l == "E" {
			majorityHasLeader = true
		}
	}
	if !majorityHasLeader {
		t.Fatal("majority side has no leader")
	}
}

func TestAsymmetricPartitionDeposesLeaderRepeatedly(t *testing.T) {
	// Leader can send but cannot hear: it keeps sending heartbeats, so
	// followers stay quiet... unless one can't hear the leader either.
	// Classic case: a follower that cannot hear the leader starts
	// elections, and its higher term eventually deposes the leader once
	// the leader hears any message from it.
	c := NewCluster(ids(3), DefaultConfig())
	l, _ := c.RunUntilLeader(500)
	var victim string
	for _, id := range ids(3) {
		if id != l {
			victim = id
			break
		}
	}
	term0 := c.Nodes[l].Term()
	c.BlockOneWay(l, victim) // victim never hears the leader
	c.Run(400)
	if c.MaxTerm() <= term0 {
		t.Fatalf("victim never inflated the term (max %d, started %d)", c.MaxTerm(), term0)
	}
	if c.Nodes[l].Term() == term0 && c.Nodes[l].Role() == Leader {
		t.Fatal("leader never learned of the higher term despite receiving the victim's messages")
	}
}

func TestVotesOnlyOncePerTerm(t *testing.T) {
	n := New("A", []string{"A", "B", "C"}, DefaultConfig())
	r1 := n.Step(Message{Type: MsgRequestVote, From: "B", To: "A", Term: 1})
	r2 := n.Step(Message{Type: MsgRequestVote, From: "C", To: "A", Term: 1})
	if len(r1) != 1 || !r1[0].Granted {
		t.Fatalf("first vote: %v", r1)
	}
	if len(r2) != 1 || r2[0].Granted {
		t.Fatalf("second vote in same term must be denied: %v", r2)
	}
	// Same candidate asking again (retransmit) is granted again.
	r3 := n.Step(Message{Type: MsgRequestVote, From: "B", To: "A", Term: 1})
	if !r3[0].Granted {
		t.Fatal("re-request from the voted-for candidate should be granted")
	}
}

func TestStaleMessagesAreRejectedWithCurrentTerm(t *testing.T) {
	n := New("A", []string{"A", "B", "C"}, DefaultConfig())
	n.Step(Message{Type: MsgHeartbeat, From: "B", To: "A", Term: 5})
	if n.Term() != 5 || n.Leader() != "B" {
		t.Fatalf("did not adopt term/leader: %+v", n.Status())
	}
	r := n.Step(Message{Type: MsgRequestVote, From: "C", To: "A", Term: 3})
	if len(r) != 1 || r[0].Granted || r[0].Term != 5 {
		t.Fatalf("stale RequestVote should get a denial carrying term 5: %v", r)
	}
	if n.Step(Message{Type: MsgVote, From: "C", To: "A", Term: 3, Granted: true}) != nil {
		t.Fatal("stale vote must be ignored")
	}
}

func TestCandidateConcedesToSameTermLeader(t *testing.T) {
	cfg := DefaultConfig()
	n := New("A", []string{"A", "B", "C"}, cfg)
	var out []Message
	for len(out) == 0 {
		out = n.Tick() // becomes candidate in term 1
	}
	if n.Role() != Candidate || n.Term() != 1 || len(out) != 2 || out[0].Type != MsgRequestVote {
		t.Fatalf("expected candidate broadcasting RequestVote: role=%s term=%d out=%v", n.Role(), n.Term(), out)
	}
	n.Step(Message{Type: MsgHeartbeat, From: "B", To: "A", Term: 1})
	if n.Role() != Follower || n.Leader() != "B" {
		t.Fatalf("candidate must concede to a same-term leader: %+v", n.Status())
	}
}

func TestLeaderSendsHeartbeatsOnSchedule(t *testing.T) {
	cfg := DefaultConfig()
	n := New("A", []string{"A", "B", "C"}, cfg)
	for n.Role() != Candidate {
		n.Tick()
	}
	out := n.Step(Message{Type: MsgVote, From: "B", To: "A", Term: 1, Granted: true})
	if n.Role() != Leader || len(out) != 2 || out[0].Type != MsgHeartbeat {
		t.Fatalf("majority vote should make a leader that heartbeats immediately: %v", out)
	}
	beats := 0
	for i := 0; i < cfg.HeartbeatTicks*3; i++ {
		if len(n.Tick()) > 0 {
			beats++
		}
	}
	if beats != 3 {
		t.Fatalf("expected 3 heartbeat rounds, got %d", beats)
	}
}

func TestDeterministicWithSeed(t *testing.T) {
	run := func() (string, int, uint64) {
		cfg := DefaultConfig()
		cfg.Seed = 42
		c := NewCluster(ids(5), cfg)
		l, ticks := c.RunUntilLeader(1000)
		return l, ticks, c.MaxTerm()
	}
	l1, t1, term1 := run()
	l2, t2, term2 := run()
	if l1 != l2 || t1 != t2 || term1 != term2 {
		t.Fatalf("runs differ: (%s,%d,%d) vs (%s,%d,%d)", l1, t1, term1, l2, t2, term2)
	}
}

func TestInvalidConfigPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	New("A", []string{"A"}, Config{ElectionTimeoutMin: 10, ElectionTimeoutMax: 5, HeartbeatTicks: 1})
}
