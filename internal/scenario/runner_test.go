package scenario

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jimroxodezi/dbfailsim/internal/proxy"
)

func echoUpstream(t *testing.T) net.Listener {
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
			go func(c net.Conn) { defer c.Close(); io.Copy(c, c) }(c)
		}
	}()
	t.Cleanup(func() { l.Close() })
	return l
}

func liveProxy(t *testing.T, name, upstream string) *proxy.Proxy {
	t.Helper()
	p := proxy.New(name, "127.0.0.1:0", upstream)
	if err := p.Listen(); err != nil {
		t.Fatal(err)
	}
	go p.Serve()
	t.Cleanup(func() { p.Close() })
	return p
}

func TestRunnerRegistersOnTargetProxyAndExpires(t *testing.T) {
	yaml := `
listen: "127.0.0.1:0"
upstream: {host: "127.0.0.1", port: 1}
scenarios:
  - name: lag-then-recover
    duration: 600ms
    faults:
      - type: latency
        target: replica-1
        at: 0ms
        for: 200ms
        params: {delay: "150ms"}
      - type: asymmetric_partition
        target: primary
        at: 0ms
        params: {block_direction: upstream_to_client}
`
	path := filepath.Join(t.TempDir(), "s.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	up := echoUpstream(t)
	proxies := map[string]*proxy.Proxy{
		"primary":   liveProxy(t, "primary", up.Addr().String()),
		"replica-1": liveProxy(t, "replica-1", up.Addr().String()),
	}
	r := NewRunner(cfg, proxies)
	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background(), "lag-then-recover") }()

	time.Sleep(50 * time.Millisecond)
	if st := proxies["replica-1"].Status(); st.LatencyMs != 150 {
		t.Fatalf("replica-1 latency = %d, want 150", st.LatencyMs)
	}
	if st := proxies["primary"].Status(); len(st.ActiveFaults) != 1 || st.ActiveFaults[0] != "asymmetric_partition" {
		t.Fatalf("primary faults = %v", st.ActiveFaults)
	}
	if st := proxies["primary"].Status(); st.LatencyMs != 0 {
		t.Fatal("latency leaked onto the wrong target")
	}

	time.Sleep(250 * time.Millisecond) // past `for: 200ms`
	if st := proxies["replica-1"].Status(); st.LatencyMs != 0 {
		t.Fatal("latency was not unregistered after its `for` window")
	}
	if st := proxies["primary"].Status(); len(st.ActiveFaults) != 1 {
		t.Fatal("fault without `for` must persist")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	r.HealAll()
	if st := proxies["primary"].Status(); len(st.ActiveFaults) != 0 {
		t.Fatal("HealAll left faults")
	}
}

func TestRunnerRejectsUnknownTarget(t *testing.T) {
	r := NewRunner(&Config{}, map[string]*proxy.Proxy{})
	err := r.fireFault(context.Background(), FaultSpec{Type: "latency", Target: "ghost"})
	if err == nil {
		t.Fatal("expected unknown-target error")
	}
}
