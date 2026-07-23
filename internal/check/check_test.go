package check

import (
	"errors"
	"strings"
	"testing"

	"github.com/jimroxodezi/dbfailsim/internal/config"
)

func TestRunSubstitutesQuery(t *testing.T) {
	cfg := &config.Config{Nodes: []config.Node{
		{Name: "n1", CheckCommand: "echo result-of {query}"},
		{Name: "no-check"}, // no CheckCommand: skipped
	}}
	results := Run(cfg, "SELECT 1")
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Node != "n1" || results[0].Output != "result-of SELECT 1" {
		t.Errorf("unexpected result: %+v", results[0])
	}
	if results[0].Err != nil {
		t.Errorf("unexpected error: %v", results[0].Err)
	}
}

func TestRunReportsCommandFailure(t *testing.T) {
	cfg := &config.Config{Nodes: []config.Node{
		{Name: "n1", CheckCommand: "exit 3"},
	}}
	results := Run(cfg, "q")
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("want a failing result, got %+v", results)
	}
}

func TestRenderAgreement(t *testing.T) {
	out := Render([]Result{
		{Node: "a", Output: "42"},
		{Node: "b", Output: "42"},
	})
	if !strings.Contains(out, "All nodes agree") {
		t.Errorf("want agreement verdict, got:\n%s", out)
	}
	if strings.Contains(out, "DIVERGES") {
		t.Errorf("unexpected divergence flag, got:\n%s", out)
	}
}

func TestRenderDivergence(t *testing.T) {
	out := Render([]Result{
		{Node: "a", Output: "42"},
		{Node: "b", Output: "41"},
	})
	if !strings.Contains(out, "Nodes DISAGREE") {
		t.Errorf("want disagreement verdict, got:\n%s", out)
	}
	if !strings.Contains(out, "DIVERGES from first node") {
		t.Errorf("want per-node divergence flag, got:\n%s", out)
	}
}

func TestRenderError(t *testing.T) {
	out := Render([]Result{
		{Node: "a", Output: "", Err: errors.New("connection refused")},
	})
	if !strings.Contains(out, "ERROR: connection refused") {
		t.Errorf("want error in output, got:\n%s", out)
	}
}
