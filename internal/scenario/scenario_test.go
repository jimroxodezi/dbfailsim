package scenario

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jimroxodezi/dbfailsim/internal/config"
	"github.com/jimroxodezi/dbfailsim/internal/faults"
	"github.com/jimroxodezi/dbfailsim/internal/proxy"
)

func testEngine() (*Engine, map[string]*proxy.Proxy) {
	proxies := map[string]*proxy.Proxy{
		"primary":   proxy.New("primary", "127.0.0.1:0", "127.0.0.1:1"),
		"replica-1": proxy.New("replica-1", "127.0.0.1:0", "127.0.0.1:1"),
	}
	return New(proxies, nil), proxies
}

func TestRunAppliesSteps(t *testing.T) {
	eng, proxies := testEngine()
	err := eng.Run(context.Background(), &config.Scenario{
		Name: "test",
		Steps: []config.FaultStep{
			{Node: "primary", Kind: "latency", LatencyMs: 100, AfterMs: 0},
			{Node: "replica-1", Kind: "drop", DropPercent: 20, AfterMs: 0},
			{Node: "replica-1", Kind: "partition", AfterMs: 0},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if st := proxies["primary"].Status(); st.LatencyMs != 100 {
		t.Errorf("primary latency = %d, want 100", st.LatencyMs)
	}
	if st := proxies["replica-1"].Status(); st.DropPercent != 20 || !st.Partitioned {
		t.Errorf("replica-1 status = %+v", st)
	}
}

func TestRunRejectsUnknownNode(t *testing.T) {
	eng, _ := testEngine()
	err := eng.Run(context.Background(), &config.Scenario{
		Name:  "bad",
		Steps: []config.FaultStep{{Node: "ghost", Kind: "crash"}},
	})
	if err == nil {
		t.Fatal("want error for unknown node")
	}
}

func TestRunRejectsUnknownKind(t *testing.T) {
	eng, _ := testEngine()
	err := eng.Run(context.Background(), &config.Scenario{
		Name:  "bad",
		Steps: []config.FaultStep{{Node: "primary", Kind: "meteor"}},
	})
	if err == nil {
		t.Fatal("want error for unknown fault kind")
	}
}

func TestHealStepAndHealAll(t *testing.T) {
	eng, proxies := testEngine()
	proxies["primary"].State.SetCrashed(true)
	proxies["replica-1"].State.SetLatency(500)

	err := eng.Run(context.Background(), &config.Scenario{
		Name:  "recover-primary",
		Steps: []config.FaultStep{{Node: "primary", Kind: "heal"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if st := proxies["primary"].Status(); st.Crashed {
		t.Error("heal step did not clear crash")
	}
	if st := proxies["replica-1"].Status(); st.LatencyMs != 500 {
		t.Error("heal step must only affect its own node")
	}

	eng.HealAll()
	if st := proxies["replica-1"].Status(); st.LatencyMs != 0 {
		t.Error("HealAll did not clear all faults")
	}
}

func TestApplyGeneralFormRegistersOnRegistryAndExpires(t *testing.T) {
	eng, proxies := testEngine()
	err := eng.Apply(context.Background(), config.FaultStep{
		Node: "replica-1", Kind: "reorder", Params: map[string]any{"buffer_size": float64(3)}, ForMs: 150,
	})
	if err != nil {
		t.Fatal(err)
	}
	if names := proxies["replica-1"].Registry.Names(); len(names) != 1 || names[0] != "reorder" {
		t.Fatalf("registry = %v", names)
	}
	if n := proxies["primary"].Registry.Names(); len(n) != 0 {
		t.Fatal("fault leaked to another node")
	}
	time.Sleep(250 * time.Millisecond)
	if n := proxies["replica-1"].Registry.Names(); len(n) != 0 {
		t.Fatalf("fault not removed after for_ms: %v", n)
	}
}

func TestApplyStarTargetsEveryProxy(t *testing.T) {
	eng, proxies := testEngine()
	if err := eng.Apply(context.Background(), config.FaultStep{Node: "*", Kind: "latency", Params: map[string]any{"delay": "300ms"}}); err != nil {
		t.Fatal(err)
	}
	for name, p := range proxies {
		if p.Status().LatencyMs != 300 {
			t.Errorf("%s latency = %d", name, p.Status().LatencyMs)
		}
	}
	if err := eng.Apply(context.Background(), config.FaultStep{Node: "*", Kind: "heal"}); err != nil {
		t.Fatal(err)
	}
	for name, p := range proxies {
		if p.Status().LatencyMs != 0 {
			t.Errorf("%s not healed", name)
		}
	}
}

func TestPartitionForWindowClears(t *testing.T) {
	eng, proxies := testEngine()
	if err := eng.Apply(context.Background(), config.FaultStep{Node: "primary", Kind: "partition", ForMs: 100}); err != nil {
		t.Fatal(err)
	}
	if !proxies["primary"].Status().Partitioned {
		t.Fatal("not partitioned")
	}
	time.Sleep(200 * time.Millisecond)
	if proxies["primary"].Status().Partitioned {
		t.Fatal("partition did not clear after for_ms")
	}
}

func TestNodeFaultNeedsTarget(t *testing.T) {
	proxies := map[string]*proxy.Proxy{"primary": proxy.New("primary", "127.0.0.1:0", "127.0.0.1:1")}
	cfg := &config.Config{Nodes: []config.Node{{Name: "primary", ListenAddr: "a", UpstreamAddr: "b"}}}
	eng := New(proxies, cfg)
	err := eng.Apply(context.Background(), config.FaultStep{Node: "primary", Kind: "zombie"})
	if err == nil || !strings.Contains(err.Error(), "no target") {
		t.Fatalf("zombie without target should fail clearly, got %v", err)
	}
	if err := New(proxies, nil).Apply(context.Background(), config.FaultStep{Node: "primary", Kind: "node_crash"}); err == nil {
		t.Fatal("node fault without config should fail")
	}
}

// A process target with no start_command cannot be restarted: node_crash
// must surface ErrUnsupported on revert rather than silently doing nothing.
func TestNodeFaultUnsupportedOperationIsReported(t *testing.T) {
	proxies := map[string]*proxy.Proxy{"primary": proxy.New("primary", "127.0.0.1:0", "127.0.0.1:1")}
	cfg := &config.Config{Nodes: []config.Node{{
		Name: "primary", ListenAddr: "a", UpstreamAddr: "b",
		Target: &config.NodeTarget{Type: "process", PID: 1},
	}}}
	eng := New(proxies, cfg)
	err := eng.Apply(context.Background(), config.FaultStep{Node: "primary", Kind: "cpu_throttle"})
	if !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("cpu_throttle on a bare process should be ErrUnsupported, got %v", err)
	}
}

func TestRunOrdersStepsByAfterMs(t *testing.T) {
	eng, proxies := testEngine()
	err := eng.Run(context.Background(), &config.Scenario{Name: "order", Steps: []config.FaultStep{
		{Node: "primary", Kind: "heal", AfterMs: 60},
		{Node: "primary", Kind: "partition", AfterMs: 0},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if proxies["primary"].Status().Partitioned {
		t.Fatal("steps were not applied in after_ms order")
	}
}
