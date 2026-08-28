package faults

import (
	"context"
	"errors"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fakeExec records every command instead of running it.
type fakeExec struct {
	mu     sync.Mutex
	calls  []string
	output string
	err    error
}

func (f *fakeExec) Run(ctx context.Context, name string, args ...string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name+" "+strings.Join(args, " "))
	return f.output, f.err
}

func (f *fakeExec) last() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return ""
	}
	return f.calls[len(f.calls)-1]
}

func (f *fakeExec) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func TestDockerDriverCommands(t *testing.T) {
	ex := &fakeExec{}
	d := &DockerDriver{Container: "pg", Network: "dbnet", Exec_: ex}
	ctx := context.Background()
	steps := []struct {
		do   func() error
		want string
	}{
		{func() error { return d.Signal(ctx, syscall.SIGKILL) }, "docker kill --signal KILL pg"},
		{func() error { return d.Signal(ctx, syscall.SIGTERM) }, "docker kill --signal TERM pg"},
		{func() error { return d.Start(ctx) }, "docker start pg"},
		{func() error { return d.Freeze(ctx) }, "docker pause pg"},
		{func() error { return d.Thaw(ctx) }, "docker unpause pg"},
		{func() error { return d.LimitCPU(ctx, 0.25) }, "docker update --cpus 0.25 pg"},
		{func() error { return d.UnlimitCPU(ctx) }, "docker update --cpus 0 pg"},
		{func() error { return d.LimitMemory(ctx, 1024) }, "docker update --memory 1024 --memory-swap 1024 pg"},
		{func() error { return d.UnlimitMemory(ctx) }, "docker update --memory -1 --memory-swap -1 pg"},
		{func() error { return d.Exec(ctx, "date -s x") }, "docker exec pg sh -c date -s x"},
		{func() error { return d.Isolate(ctx) }, "docker network disconnect dbnet pg"},
		{func() error { return d.Reconnect(ctx) }, "docker network connect dbnet pg"},
	}
	for _, s := range steps {
		if err := s.do(); err != nil {
			t.Fatal(err)
		}
		if ex.last() != s.want {
			t.Errorf("ran %q, want %q", ex.last(), s.want)
		}
	}
	if d.Describe() != "docker:pg" || d.Kind() != "docker" {
		t.Error("Describe/Kind")
	}
}

func TestProcessDriverCommandsAndLimits(t *testing.T) {
	ex := &fakeExec{output: "4242\n"}
	d := &ProcessDriver{PIDFile: "/run/pg.pid", StartCommand: "pg_ctl start", Exec_: ex}
	ctx := context.Background()
	if err := d.Signal(ctx, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if ex.calls[0] != "cat /run/pg.pid" || ex.last() != "kill -KILL 4242" {
		t.Fatalf("calls = %v", ex.calls)
	}
	d.Freeze(ctx)
	if ex.last() != "kill -STOP 4242" {
		t.Fatalf("freeze ran %q", ex.last())
	}
	d.Thaw(ctx)
	if ex.last() != "kill -CONT 4242" {
		t.Fatalf("thaw ran %q", ex.last())
	}
	d.Start(ctx)
	if ex.last() != "sh -c pg_ctl start" {
		t.Fatalf("start ran %q", ex.last())
	}
	if err := d.LimitCPU(ctx, 0.5); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("LimitCPU on a process should be unsupported, got %v", err)
	}
	if err := d.LimitMemory(ctx, 1); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("LimitMemory on a process should be unsupported, got %v", err)
	}

	noStart := &ProcessDriver{PID: 7, Exec_: ex}
	if err := noStart.Start(ctx); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Start without start_command should be unsupported, got %v", err)
	}
	noStart.Signal(ctx, syscall.SIGTERM)
	if ex.last() != "kill -TERM 7" {
		t.Fatalf("explicit pid: %q", ex.last())
	}

	bad := &ProcessDriver{PIDFile: "/x", Exec_: &fakeExec{output: "not-a-pid"}}
	if err := bad.Signal(ctx, syscall.SIGKILL); err == nil {
		t.Fatal("garbage pid file should error")
	}
}

func TestSystemdDriverCommands(t *testing.T) {
	ex := &fakeExec{}
	d := &SystemdDriver{Unit: "postgresql", Exec_: ex}
	ctx := context.Background()
	d.Signal(ctx, syscall.SIGTERM)
	if ex.last() != "systemctl kill --signal=TERM postgresql" {
		t.Fatalf("%q", ex.last())
	}
	d.Start(ctx)
	if ex.last() != "systemctl start postgresql" {
		t.Fatalf("%q", ex.last())
	}
	d.Freeze(ctx)
	if ex.last() != "systemctl kill --signal=STOP postgresql" {
		t.Fatalf("%q", ex.last())
	}
	d.LimitCPU(ctx, 0.1)
	if ex.last() != "systemctl set-property --runtime postgresql CPUQuota=10%" {
		t.Fatalf("%q", ex.last())
	}
	d.LimitMemory(ctx, 64<<20)
	if ex.last() != "systemctl set-property --runtime postgresql MemoryMax=67108864" {
		t.Fatalf("%q", ex.last())
	}
	d.UnlimitMemory(ctx)
	if ex.last() != "systemctl set-property --runtime postgresql MemoryMax=infinity" {
		t.Fatalf("%q", ex.last())
	}
}

func TestSSHExecutorWrapsAndQuotes(t *testing.T) {
	inner := &fakeExec{}
	ex := SSHExecutor{Host: "admin@db2", Inner: inner}
	ex.Run(context.Background(), "sh", "-c", "date -s '2026-01-01'")
	want := `ssh -o BatchMode=yes admin@db2 -- sh -c 'date -s '\''2026-01-01'\'''`
	if inner.last() != want {
		t.Fatalf("ssh ran\n  %q\nwant\n  %q", inner.last(), want)
	}
}

func TestNewDriverFromSpec(t *testing.T) {
	cases := []struct {
		spec TargetSpec
		kind string
		desc string
		bad  bool
	}{
		{TargetSpec{Type: "process", PID: 1}, "process", "process:1", false},
		{TargetSpec{Type: "process", PIDFile: "/p"}, "process", "process:/p", false},
		{TargetSpec{Type: "docker", Container: "c"}, "docker", "docker:c", false},
		{TargetSpec{Type: "systemd", Unit: "u"}, "systemd", "systemd:u", false},
		{TargetSpec{Type: "ssh", Host: "h", Inner: &TargetSpec{Type: "systemd", Unit: "u"}}, "systemd", "systemd:u", false},
		{TargetSpec{Type: "process"}, "", "", true},
		{TargetSpec{Type: "docker"}, "", "", true},
		{TargetSpec{Type: "ssh", Host: "h"}, "", "", true},
		{TargetSpec{Type: "ssh", Host: "h", Inner: &TargetSpec{Type: "ssh", Host: "x"}}, "", "", true},
		{TargetSpec{Type: "k8s"}, "", "", true},
		{TargetSpec{}, "", "", true},
	}
	for _, c := range cases {
		d, err := NewDriver(c.spec)
		if c.bad {
			if err == nil {
				t.Errorf("spec %+v should be rejected", c.spec)
			}
			continue
		}
		if err != nil {
			t.Errorf("spec %+v: %v", c.spec, err)
			continue
		}
		if d.Kind() != c.kind || d.Describe() != c.desc {
			t.Errorf("spec %+v -> %s %s", c.spec, d.Kind(), d.Describe())
		}
	}
	// ssh wraps the inner driver's executor
	d, _ := NewDriver(TargetSpec{Type: "ssh", Host: "h", Inner: &TargetSpec{Type: "docker", Container: "c"}})
	if _, ok := d.(*DockerDriver).Exec_.(SSHExecutor); !ok {
		t.Fatal("ssh target should give the inner driver an SSHExecutor")
	}
}

// Faults only express intent: prove each one maps to the right driver call.
func TestNodeFaultsDriveTheDriver(t *testing.T) {
	ex := &fakeExec{}
	d := &DockerDriver{Container: "pg", Exec_: ex}
	ctx := context.Background()
	cases := []struct {
		fault          NodeFault
		inject, revert string
	}{
		{&CrashFault{Signal: syscall.SIGKILL}, "docker kill --signal KILL pg", "docker start pg"},
		{&ZombieFault{}, "docker pause pg", "docker unpause pg"},
		{&CPUThrottleFault{CPUQuota: 0.1}, "docker update --cpus 0.10 pg", "docker update --cpus 0 pg"},
		{&OOMFault{LimitBytes: 5}, "docker update --memory 5 --memory-swap 5 pg", "docker update --memory -1 --memory-swap -1 pg"},
		{&DiskFullFault{MountPath: "/data", FillerFile: "f"}, "fallocate", "docker exec pg sh -c rm -f /data/f"},
	}
	for _, c := range cases {
		if err := c.fault.Inject(ctx, d); err != nil {
			t.Fatalf("%s inject: %v", c.fault.Name(), err)
		}
		if !strings.Contains(ex.last(), c.inject) {
			t.Errorf("%s inject ran %q, want %q", c.fault.Name(), ex.last(), c.inject)
		}
		if err := c.fault.Revert(ctx, d); err != nil {
			t.Fatalf("%s revert: %v", c.fault.Name(), err)
		}
		if !strings.Contains(ex.last(), c.revert) {
			t.Errorf("%s revert ran %q, want %q", c.fault.Name(), ex.last(), c.revert)
		}
	}
	// Unsupported operations propagate from the driver through the fault.
	p := &ProcessDriver{PID: 1, Exec_: ex}
	if err := (&OOMFault{LimitBytes: 1}).Inject(ctx, p); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("oom on a process should be ErrUnsupported, got %v", err)
	}
	// Executor errors propagate too.
	failing := &DockerDriver{Container: "pg", Exec_: &fakeExec{err: errors.New("no docker")}}
	if err := (&CrashFault{Signal: syscall.SIGKILL}).Inject(ctx, failing); err == nil {
		t.Fatal("executor error swallowed")
	}
}

func TestCrashLoopUsesDriverAndStops(t *testing.T) {
	ex := &fakeExec{}
	d := &DockerDriver{Container: "pg", Exec_: ex}
	// Each iteration is kill, 200ms, start; a 10ms interval means the loop
	// is bound by that 200ms, so ~600ms yields at least two kills.
	f := &CrashLoopFault{Interval: 10 * time.Millisecond, Duration: 5 * time.Second}
	if err := f.Inject(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond)
	if err := f.Revert(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	calls := ex.snapshot()
	kills := 0
	for _, c := range calls {
		if strings.HasPrefix(c, "docker kill") {
			kills++
		}
	}
	if kills < 2 || calls[len(calls)-1] != "docker start pg" {
		t.Fatalf("kills=%d calls=%v", kills, calls)
	}
	before := len(ex.snapshot())
	time.Sleep(300 * time.Millisecond)
	if len(ex.snapshot()) != before {
		t.Fatal("loop kept running after Revert")
	}
}
