// dbfailsim: a Database Failure Simulator — Chaos Monkey, but for your
// database layer. It plugs into your real DB infra via a transparent TCP
// proxy per node, lets you inject network partitions, replication delays,
// and node crashes on demand or via reproducible named scenarios, and shows
// you exactly what different clients would actually see while it happens.
//
// Usage:
//
//	dbfailsim serve    --config config.json
//	dbfailsim inject   --config config.json --scenario replica-lag
//	dbfailsim fault    --config config.json --node replica-1 --kind latency --value 2000
//	dbfailsim heal     --config config.json
//	dbfailsim status   --config config.json
//	dbfailsim check    --config config.json --query "SELECT balance FROM accounts WHERE id=1"
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/jimroxodezi/dbfailsim/internal/check"
	"github.com/jimroxodezi/dbfailsim/internal/config"
	"github.com/jimroxodezi/dbfailsim/internal/control"
	"github.com/jimroxodezi/dbfailsim/internal/proxy"
	"github.com/jimroxodezi/dbfailsim/internal/scenario"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		cmdServe(os.Args[2:])
	case "inject":
		cmdInject(os.Args[2:])
	case "fault":
		cmdFault(os.Args[2:])
	case "heal":
		cmdHeal(os.Args[2:])
	case "status":
		cmdStatus(os.Args[2:])
	case "check":
		cmdCheck(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `dbfailsim - Database Failure Simulator (Chaos for Data)

Commands:
  serve    Start proxies for every configured node plus the control API
  inject   Run a named scenario against a running dbfailsim serve instance
  fault    Apply a single fault to one node
  heal     Clear all fault state, restoring normal operation
  status   Show current health of every node's proxy
  check    Query every node directly and show whether they agree

Run 'dbfailsim <command> -h' for command-specific flags.
`)
}

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "path to config file")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	must(err)

	proxies := make(map[string]*proxy.Proxy)
	for _, n := range cfg.Nodes {
		p := proxy.New(n.Name, n.ListenAddr, n.UpstreamAddr)
		proxies[n.Name] = p
		go func(p *proxy.Proxy) {
			if err := p.Start(); err != nil {
				fmt.Fprintf(os.Stderr, "[%s] proxy stopped: %v\n", p.Name, err)
			}
		}(p)
	}

	eng := scenario.New(proxies)
	srv := control.NewServer(cfg, proxies, eng)
	must(srv.ListenAndServe(cfg.ControlAddr))
}

func cmdInject(args []string) {
	fs := flag.NewFlagSet("inject", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "path to config file")
	scenarioName := fs.String("scenario", "", "scenario name to run (must be defined in config, or use a built-in: replica-lag, split-brain, node-crash)")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	must(err)

	resp, err := apiRequest(cfg, http.MethodPost, fmt.Sprintf("/scenarios/%s/run", url.PathEscape(*scenarioName)), nil)
	must(err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("%s: %s\n", resp.Status, body)
}

func cmdFault(args []string) {
	fs := flag.NewFlagSet("fault", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "path to config file")
	node := fs.String("node", "", "node name")
	kind := fs.String("kind", "", "latency | drop | partition | crash | heal")
	value := fs.Int("value", 0, "latency_ms for kind=latency, drop_percent for kind=drop")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	must(err)

	var body string
	switch *kind {
	case "latency":
		body = fmt.Sprintf(`{"kind":"latency","latency_ms":%d}`, *value)
	case "drop":
		body = fmt.Sprintf(`{"kind":"drop","drop_percent":%d}`, *value)
	default:
		body = fmt.Sprintf(`{"kind":%q}`, *kind)
	}
	resp, err := apiRequest(cfg, http.MethodPost, fmt.Sprintf("/nodes/%s/fault", url.PathEscape(*node)), stringsReader(body))
	must(err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Printf("%s: %s\n", resp.Status, out)
}

func cmdHeal(args []string) {
	fs := flag.NewFlagSet("heal", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "path to config file")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	must(err)

	resp, err := apiRequest(cfg, http.MethodPost, "/heal", nil)
	must(err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Printf("%s: %s\n", resp.Status, out)
}

func cmdStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "path to config file")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	must(err)

	resp, err := apiRequest(cfg, http.MethodGet, "/status", nil)
	must(err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Println(string(out))
}

func cmdCheck(args []string) {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "path to config file")
	query := fs.String("query", "", "query text to run against every node's check_command")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	must(err)
	if *query == "" {
		fmt.Fprintln(os.Stderr, "check requires -query")
		os.Exit(1)
	}

	results := check.Run(cfg, *query)
	fmt.Print(check.Render(results))
}

// apiRequest sends a control API request, attaching the bearer token from
// the config (or DBFAILSIM_CONTROL_TOKEN) when one is set.
func apiRequest(cfg *config.Config, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, "http://"+cfg.ControlAddr+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cfg.ControlToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.ControlToken)
	}
	return http.DefaultClient.Do(req)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// stringsReader avoids importing strings just for this one conversion at
// call sites above.
func stringsReader(s string) *stringReader { return &stringReader{s: s} }

type stringReader struct {
	s string
	i int
}

func (r *stringReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}
