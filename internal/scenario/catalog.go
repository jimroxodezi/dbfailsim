package scenario

import "sort"

// ParamInfo documents one kind-specific parameter so the CLI and the
// dashboard can build forms and help text without hardcoding kinds.
type ParamInfo struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"` // "duration" | "int" | "float" | "string" | "enum"
	Default any      `json:"default,omitempty"`
	Help    string   `json:"help,omitempty"`
	Enum    []string `json:"enum,omitempty"`
}

// KindInfo documents one fault kind understood by Engine.Apply.
type KindInfo struct {
	Kind        string      `json:"kind"`
	Class       string      `json:"class"` // "state" | "conn" | "dial" | "packet" | "read_hook" | "node"
	Description string      `json:"description"`
	Params      []ParamInfo `json:"params,omitempty"`
	// Stream, when set, says which proxy stream the fault acts on
	// ("replication" or "query"); empty means any.
	Stream string `json:"stream,omitempty"`
	// NeedsTarget is true for node-level faults that require a config target.
	NeedsTarget bool `json:"needs_target,omitempty"`
}

func dur(name, def, help string) ParamInfo {
	return ParamInfo{Name: name, Type: "duration", Default: def, Help: help}
}
func num(name string, def int, help string) ParamInfo {
	return ParamInfo{Name: name, Type: "int", Default: def, Help: help}
}
func flt(name string, def float64, help string) ParamInfo {
	return ParamInfo{Name: name, Type: "float", Default: def, Help: help}
}
func enum(name, def, help string, values ...string) ParamInfo {
	return ParamInfo{Name: name, Type: "enum", Default: def, Help: help, Enum: values}
}

// catalog is the single description of every kind; Kinds() and the
// control API's GET /kinds derive from it. Keep it in step with Apply.
var catalog = []KindInfo{
	// state flags
	{Kind: "latency", Class: "conn", Description: "Delay every chunk (fixed, jittered, or ramping).",
		Params: []ParamInfo{dur("delay", "500ms", "base delay per chunk (short form: latency_ms)"), dur("jitter", "0ms", "extra random delay, uniform in [0, jitter)"), dur("ramp_to", "0ms", "if set, delay grows linearly from delay to ramp_to"), dur("ramp_over", "0ms", "how long the ramp takes")}},
	{Kind: "drop", Class: "conn", Description: "Silently discard a uniform percentage of chunks.",
		Params: []ParamInfo{num("drop_percent", 20, "0-100 (short form: drop_percent)")}},
	{Kind: "partition", Class: "state", Description: "Node unreachable: refuse new connections, sever existing ones."},
	{Kind: "crash", Class: "state", Description: "Proxy-level crash: same as partition, reported separately. (For a real process kill use node_crash.)"},
	{Kind: "heal", Class: "state", Description: "Clear every fault on the node and revert its node-level faults."},

	{Kind: "bursty_loss", Class: "conn", Description: "Low baseline loss with periodic bursts of heavy loss.",
		Params: []ParamInfo{flt("baseline_loss", 0.01, "loss probability between bursts"), flt("burst_loss", 0.4, "loss probability during a burst"), dur("burst_every", "30s", "time between bursts"), dur("burst_for", "5s", "burst length")}},
	{Kind: "bandwidth_throttle", Class: "conn", Description: "Cap throughput with a token bucket.",
		Params: []ParamInfo{num("rate_bytes_per_sec", 65536, "sustained bytes/second"), num("burst_bytes", 65536, "bucket capacity")}},
	{Kind: "reorder", Class: "conn", Description: "Hold chunks back and release them out of order.",
		Params: []ParamInfo{num("buffer_size", 5, "chunks held before a shuffled flush"), flt("probability", 0.1, "chance a chunk is held")}},
	{Kind: "duplication", Class: "conn", Description: "Deliver some chunks more than once.",
		Params: []ParamInfo{flt("probability", 0.05, "chance a chunk is duplicated"), num("max_extra_copies", 1, "extra copies per duplicated chunk")}},
	{Kind: "asymmetric_partition", Class: "conn", Description: "Blackhole one direction only; the other keeps flowing.",
		Params: []ParamInfo{enum("block_direction", "client_to_upstream", "which leg to blackhole", "client_to_upstream", "upstream_to_client")}},
	{Kind: "tcp_rst", Class: "conn", Description: "Close connections with a TCP RST instead of a clean FIN."},

	{Kind: "dns_failure", Class: "dial", Description: "New upstream dials fail with 'no such host'; existing connections survive.",
		Params: []ParamInfo{{Name: "service", Type: "string", Help: "host name to fail (default: the node's upstream host)"}}},

	{Kind: "replica_lag", Class: "packet", Stream: "replication", Description: "Delay replication-stream chunks (the replica falls behind).",
		Params: []ParamInfo{dur("delay", "2s", "delay per chunk"), dur("jitter", "0ms", "extra random delay")}},
	{Kind: "wal_delay", Class: "packet", Stream: "replication", Description: "Delay WAL flushes on the replication stream.",
		Params: []ParamInfo{dur("flush_delay", "1s", "delay per chunk")}},
	{Kind: "wal_corruption", Class: "packet", Stream: "replication", Description: "Flip bytes in a fraction of replication chunks.",
		Params: []ParamInfo{flt("probability", 0.01, "chance a chunk is corrupted"), num("bytes_to_flip", 1, "bytes flipped per corrupted chunk")}},
	{Kind: "query_corruption", Class: "packet", Stream: "query", Description: "Flip bytes in a fraction of client-query chunks.",
		Params: []ParamInfo{flt("probability", 0.01, "chance a chunk is corrupted"), num("bytes_to_flip", 1, "bytes flipped per corrupted chunk")}},
	{Kind: "stale_read", Class: "read_hook", Description: "Serve a cached older response instead of the live one, sometimes.",
		Params: []ParamInfo{flt("probability", 0.3, "chance a cached response is served"), dur("max_age", "10s", "oldest cached response that may be served")}},

	{Kind: "node_crash", Class: "node", NeedsTarget: true, Description: "Kill the node's process; revert restarts it.",
		Params: []ParamInfo{enum("signal", "SIGKILL", "SIGKILL = hard crash, SIGTERM = graceful shutdown", "SIGKILL", "SIGTERM")}},
	{Kind: "zombie", Class: "node", NeedsTarget: true, Description: "Freeze the process: ports stay open, nothing answers."},
	{Kind: "cpu_throttle", Class: "node", NeedsTarget: true, Description: "Cap the node's CPU (systemd/docker targets only).",
		Params: []ParamInfo{flt("cpu_quota", 0.1, "cores, e.g. 0.1 = 10% of one core")}},
	{Kind: "oom", Class: "node", NeedsTarget: true, Description: "Cap the node's memory (systemd/docker targets only).",
		Params: []ParamInfo{num("limit_bytes", 50*1024*1024, "memory limit in bytes")}},
	{Kind: "clock_skew", Class: "node", NeedsTarget: true, Description: "Shift the node's wall clock. Caveat: changes the host clock on Linux.",
		Params: []ParamInfo{dur("offset", "1m", "offset applied to the clock (may be negative)")}},
}

// Catalog returns every kind with its documentation, sorted by class then kind.
func Catalog() []KindInfo {
	out := append([]KindInfo(nil), catalog...)
	order := map[string]int{"state": 0, "conn": 1, "dial": 2, "packet": 3, "read_hook": 4, "node": 5}
	sort.SliceStable(out, func(i, j int) bool {
		if order[out[i].Class] != order[out[j].Class] {
			return order[out[i].Class] < order[out[j].Class]
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

// Kinds lists every fault kind Apply understands, for help text.
func Kinds() []string {
	out := make([]string, 0, len(catalog))
	for _, k := range catalog {
		out = append(out, k.Kind)
	}
	sort.Strings(out)
	return out
}
