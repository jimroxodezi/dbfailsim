// Package check implements the "what will users actually see" view: it runs
// the same read against every configured node directly (bypassing the
// proxy, so it reflects real database state) and reports where nodes
// disagree — the core payoff of the tool. A network partition or replica
// lag scenario is only convincing if you can show a client reading from
// node A gets a different answer than a client reading from node B.
package check

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/jimroxodezi/dbfailsim/internal/config"
)

// Result is one node's observed answer to a query.
type Result struct {
	Node   string
	Output string
	Err    error
}

// Run executes the query against every node that has a CheckCommand
// configured, substituting {query} into the command template.
func Run(cfg *config.Config, query string) []Result {
	var results []Result
	for _, n := range cfg.Nodes {
		if n.CheckCommand == "" {
			continue
		}
		cmdStr := strings.ReplaceAll(n.CheckCommand, "{query}", query)
		out, err := exec.Command("sh", "-c", cmdStr).CombinedOutput()
		results = append(results, Result{
			Node:   n.Name,
			Output: strings.TrimSpace(string(out)),
			Err:    err,
		})
	}
	return results
}

// Render prints a human-readable comparison, flagging when nodes disagree —
// which is the observable symptom of the fault you just injected (stale
// replica, split-brain divergence, etc.).
func Render(results []Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Consistency check across %d node(s):\n\n", len(results))

	agree := true
	var first string
	for i, r := range results {
		if i == 0 {
			first = r.Output
		} else if r.Output != first {
			agree = false
		}
	}

	for _, r := range results {
		status := "ok"
		if r.Err != nil {
			status = "ERROR: " + r.Err.Error()
		} else if !agree && r.Output != first {
			status = "DIVERGES from first node"
		}
		fmt.Fprintf(&b, "  [%s] (%s)\n", r.Node, status)
		for _, line := range strings.Split(r.Output, "\n") {
			fmt.Fprintf(&b, "      %s\n", line)
		}
		fmt.Fprintln(&b)
	}

	if len(results) > 1 {
		if agree {
			b.WriteString("=> All nodes agree. From a client's perspective, data looks consistent right now.\n")
		} else {
			b.WriteString("=> Nodes DISAGREE. A client connected to a different node right now would see different data.\n")
			b.WriteString("   This is exactly the kind of partial, silent inconsistency that's hard to catch in normal testing.\n")
		}
	}
	return b.String()
}
