package scenario

import (
	"testing"

	"github.com/jimroxodezi/dbfailsim/internal/config"
	"github.com/jimroxodezi/dbfailsim/internal/proxy"
)

func testEngine() (*Engine, map[string]*proxy.Proxy) {
	proxies := map[string]*proxy.Proxy{
		"primary":   proxy.New("primary", "127.0.0.1:0", "127.0.0.1:1"),
		"replica-1": proxy.New("replica-1", "127.0.0.1:0", "127.0.0.1:1"),
	}
	return New(proxies), proxies
}

func TestRunAppliesSteps(t *testing.T) {
	eng, proxies := testEngine()
	err := eng.Run(&config.Scenario{
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
	err := eng.Run(&config.Scenario{
		Name:  "bad",
		Steps: []config.FaultStep{{Node: "ghost", Kind: "crash"}},
	})
	if err == nil {
		t.Fatal("want error for unknown node")
	}
}

func TestRunRejectsUnknownKind(t *testing.T) {
	eng, _ := testEngine()
	err := eng.Run(&config.Scenario{
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

	err := eng.Run(&config.Scenario{
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
