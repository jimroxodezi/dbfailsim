package proxy

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jimroxodezi/dbfailsim/internal/faults"
)

func TestRegistryReplacesByNameAndUnregisters(t *testing.T) {
	r := NewRegistry()
	r.RegisterConnFault(faults.NewLatencyInjectionFault(10*time.Millisecond, 0))
	r.RegisterConnFault(faults.NewLatencyInjectionFault(20*time.Millisecond, 0))
	if names := r.Names(); len(names) != 1 || names[0] != "latency" {
		t.Fatalf("Names = %v, want single latency", names)
	}
	if f := r.Get("latency").(*faults.LatencyInjectionFault); f.CurrentDelay() != 20*time.Millisecond {
		t.Fatalf("re-register did not replace: delay = %v", f.CurrentDelay())
	}
	r.RegisterPacketFault(&faults.WALDelayFault{})
	r.RegisterDialFault(&faults.DNSFailureFault{ServiceName: "x"})
	if got := len(r.Names()); got != 3 {
		t.Fatalf("Names = %v", r.Names())
	}
	if !r.Unregister("wal_delay") || r.Unregister("wal_delay") {
		t.Fatal("Unregister return values")
	}
	r.Clear()
	if len(r.Names()) != 0 {
		t.Fatal("Clear left faults")
	}
}

func TestRegistryTicksTimeVaryingFault(t *testing.T) {
	r := NewRegistry()
	f := faults.NewLatencyInjectionFault(0, 0)
	f.RampTo = time.Second
	f.RampOver = 100 * time.Millisecond
	r.RegisterConnFault(f)
	deadline := time.Now().Add(2 * time.Second)
	for f.CurrentDelay() < time.Second && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if f.CurrentDelay() != time.Second {
		t.Fatalf("ramp never reached RampTo: %v", f.CurrentDelay())
	}
	r.Unregister("latency") // must stop the ticker without panicking
}

// A fault registered AFTER the connection was accepted must apply to it.
func TestLiveConnRewrapsOnRegistryChange(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())
	conn := dialProxy(t, p)
	if _, err := roundTrip(conn, "warm"); err != nil {
		t.Fatal(err)
	}
	p.Registry.RegisterConnFault(&faults.AsymmetricPartitionFault{BlockDirection: faults.DirectionUpstreamToClient})
	conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := roundTrip(conn, "lost"); err == nil {
		t.Fatal("reply arrived through a blackholed upstream->client leg")
	}
	p.Registry.Unregister("asymmetric_partition")
	conn.SetReadDeadline(time.Time{})
	// The blackholed "lost" echo never comes; a fresh query gets its own reply.
	got, err := roundTrip(conn, "back")
	if err != nil || !strings.Contains(got, "back") {
		t.Fatalf("after unregister: %q %v", got, err)
	}
}

func TestDialFaultRefusesNewConnections(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())
	host, _, _ := net.SplitHostPort(p.UpstreamAddr)
	f := &faults.DNSFailureFault{ServiceName: host}
	f.Trigger(time.Minute)
	p.Registry.RegisterDialFault(f)

	conn := dialProxy(t, p) // accept succeeds; the upstream dial is what fails
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := roundTrip(conn, "x"); err == nil {
		t.Fatal("got a reply despite DNS failure on upstream dial")
	}
	p.Registry.Unregister("dns_failure")
	conn2 := dialProxy(t, p)
	if _, err := roundTrip(conn2, "ok"); err != nil {
		t.Fatalf("after unregister: %v", err)
	}
}

func TestPacketFaultGatedByStreamType(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())
	p.Registry.RegisterPacketFault(&faults.ReplicaLagFault{Delay: 200 * time.Millisecond})

	// Query stream: replica lag must not touch it.
	conn := dialProxy(t, p)
	start := time.Now()
	if _, err := roundTrip(conn, "q"); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 150*time.Millisecond {
		t.Fatal("replica lag applied to the client query stream")
	}

	// Replication stream: it must.
	p.Stream = faults.ReplicationStream
	conn2 := dialProxy(t, p)
	start = time.Now()
	if _, err := roundTrip(conn2, "wal"); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 200*time.Millisecond {
		t.Fatal("replica lag did not apply to the replication stream")
	}
}

func TestSeverCancelsPacketFaultSleep(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())
	p.Stream = faults.ReplicationStream
	p.Registry.RegisterPacketFault(&faults.ReplicaLagFault{Delay: 5 * time.Second})
	conn := dialProxy(t, p)
	conn.Write([]byte("stuck"))
	time.Sleep(50 * time.Millisecond) // let the pump enter the sleep
	start := time.Now()
	p.State.SetPartitioned(true)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := conn.Read(make([]byte, 8))
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read err = %v, want the connection severed", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("sever waited for the packet fault sleep instead of cancelling it")
	}
}

func TestReadHookServesStaleReply(t *testing.T) {
	// Upstream answers with an incrementing counter so a stale reply is detectable.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 64)
				n := 0
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
					n++
					c.Write([]byte{byte('0' + n)})
				}
			}(c)
		}
	}()
	p := startProxy(t, l.Addr().String())
	p.Registry.RegisterReadHook(faults.NewStaleReadFault(1.0, time.Minute)) // always stale when cached
	conn := dialProxy(t, p)
	ask := func() string {
		t.Helper()
		if _, err := conn.Write([]byte("SELECT 1")); err != nil {
			t.Fatal(err)
		}
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		b := make([]byte, 1)
		if _, err := conn.Read(b); err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	first := ask()
	second := ask()
	if first != "1" || second != "1" {
		t.Fatalf("replies = %q, %q; want the second served stale as \"1\"", first, second)
	}
}

func TestStatusReportsRegistry(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())
	p.State.SetLatency(250)
	p.State.SetDrop(30)
	st := p.Status()
	if st.LatencyMs != 250 || st.DropPercent != 30 || len(st.ActiveFaults) != 2 {
		t.Fatalf("status = %+v", st)
	}
	p.State.Heal()
	if st := p.Status(); st.LatencyMs != 0 || st.DropPercent != 0 || len(st.ActiveFaults) != 0 {
		t.Fatalf("after heal = %+v", st)
	}
}

func TestTopologyViewGatesInterNodeProxy(t *testing.T) {
	echo := startEcho(t)
	view := faults.NewInMemoryClusterView([]string{"primary", "replica-1"})
	// Topology fields are set before Serve starts; they are not meant to
	// change while the accept loop is running.
	p := New("test", "127.0.0.1:0", echo.Addr().String())
	p.View, p.FromNode, p.ToNode = view, "replica-1", "primary"
	if err := p.Listen(); err != nil {
		t.Fatal(err)
	}
	go p.Serve()
	t.Cleanup(func() { p.Close() })

	conn := dialProxy(t, p)
	if _, err := roundTrip(conn, "before"); err != nil {
		t.Fatal(err)
	}
	pf := &faults.PartitionFault{Groups: map[string][]string{"a": {"primary"}, "b": {"replica-1"}}}
	if err := pf.Inject(context.Background(), view); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := roundTrip(conn, "during"); err == nil {
		t.Fatal("existing conn survived a topology partition")
	}
	if c, err := net.DialTimeout("tcp", p.Addr(), time.Second); err == nil {
		c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if _, err := c.Read(make([]byte, 1)); err == nil {
			t.Fatal("new conn accepted across a topology partition")
		}
		c.Close()
	}
	pf.Revert(context.Background(), view)
	conn2 := dialProxy(t, p)
	if _, err := roundTrip(conn2, "after"); err != nil {
		t.Fatalf("after revert: %v", err)
	}
}
