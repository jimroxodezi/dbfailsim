package proxy

import (
	"io"
	"net"
	"testing"
	"time"
)

// startEcho runs a TCP echo server standing in for a real database upstream.
func startEcho(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(c)
		}
	}()
	t.Cleanup(func() { l.Close() })
	return l
}

func startProxy(t *testing.T, upstreamAddr string) *Proxy {
	t.Helper()
	p := New("test", "127.0.0.1:0", upstreamAddr)
	if err := p.Listen(); err != nil {
		t.Fatal(err)
	}
	go p.Serve()
	t.Cleanup(func() { p.Close() })
	return p
}

func dialProxy(t *testing.T, p *Proxy) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func roundTrip(conn net.Conn, msg string) (string, error) {
	if _, err := conn.Write([]byte(msg)); err != nil {
		return "", err
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, len(msg))
	n, err := io.ReadFull(conn, buf)
	return string(buf[:n]), err
}

func TestForwarding(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())
	conn := dialProxy(t, p)

	got, err := roundTrip(conn, "hello")
	if err != nil {
		t.Fatalf("round trip failed: %v", err)
	}
	if got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestLatencyDelaysChunks(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())
	conn := dialProxy(t, p)

	p.State.SetLatency(150)
	start := time.Now()
	if _, err := roundTrip(conn, "ping"); err != nil {
		t.Fatalf("round trip failed: %v", err)
	}
	// Latency applies per chunk in each direction; require at least one delay.
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("round trip took %v, want >= 150ms", elapsed)
	}
}

func TestPartitionSeversExistingConnections(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())
	conn := dialProxy(t, p)

	if _, err := roundTrip(conn, "before"); err != nil {
		t.Fatalf("round trip before partition failed: %v", err)
	}

	p.State.SetPartitioned(true)

	// The severed connection should yield EOF/error promptly, without
	// needing new traffic to trigger it.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("read succeeded after partition, want severed connection")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("read timed out: connection was not severed by partition")
	}
}

func TestPartitionRefusesNewConnections(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())
	p.State.SetPartitioned(true)

	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		return // refused outright: also acceptable
	}
	defer conn.Close()
	// The proxy accepts then immediately closes, so the first read must fail.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("read succeeded on new connection during partition")
	}
}

func TestHealRestoresService(t *testing.T) {
	echo := startEcho(t)
	p := startProxy(t, echo.Addr().String())

	p.State.SetCrashed(true)
	p.State.Heal()

	conn := dialProxy(t, p)
	got, err := roundTrip(conn, "after-heal")
	if err != nil {
		t.Fatalf("round trip after heal failed: %v", err)
	}
	if got != "after-heal" {
		t.Fatalf("got %q, want %q", got, "after-heal")
	}
}

func TestStatusReflectsFaults(t *testing.T) {
	p := New("n1", "127.0.0.1:0", "127.0.0.1:1")
	p.State.SetLatency(250)
	p.State.SetDrop(30)

	st := p.Status()
	if st.LatencyMs != 250 || st.DropPercent != 30 || st.Partitioned || st.Crashed {
		t.Fatalf("unexpected status: %+v", st)
	}
}
