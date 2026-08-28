package faults

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// NodeDriver is the mechanism behind a NodeFault. A fault expresses
// intent — "send this node SIGKILL", "freeze it", "cap its CPU" — and the
// driver knows how to do that for one kind of deployment: a local
// process, a systemd unit, a docker container, any of those on a remote
// host over ssh. A managed database (Neon, RDS) has no driver at all;
// only proxy-level faults apply to it, and the engine says so.
//
// Every method may return ErrUnsupported when the backend has no honest
// way to do it (a plain process has no CPU quota; a pod cannot be
// restarted in place). Faults propagate that instead of faking it.
type NodeDriver interface {
	// Kind names the backend ("process", "docker", "systemd", "ssh").
	Kind() string
	// Describe identifies the node for logs, e.g. "docker:dbfailsim-primary".
	Describe() string

	// Signal delivers sig (SIGKILL, SIGTERM, ...) to the node's main process.
	Signal(ctx context.Context, sig syscall.Signal) error
	// Start brings a killed node back.
	Start(ctx context.Context) error

	// Freeze stops the node without killing it (SIGSTOP / docker pause);
	// Thaw resumes it. A frozen node keeps its ports and connections.
	Freeze(ctx context.Context) error
	Thaw(ctx context.Context) error

	// LimitCPU caps the node to cpus cores (0.1 = a tenth of a core).
	LimitCPU(ctx context.Context, cpus float64) error
	UnlimitCPU(ctx context.Context) error

	// LimitMemory caps the node's memory in bytes; exceeding it invites
	// the OOM killer.
	LimitMemory(ctx context.Context, bytes int64) error
	UnlimitMemory(ctx context.Context) error

	// Exec runs a shell snippet inside the node's environment (its
	// filesystem, its clock), for faults such as disk-full and clock skew.
	Exec(ctx context.Context, script string) error
}

// NetworkIsolator is an optional extra a driver may implement: detach the
// node from its network entirely and reattach it. QuorumLossFault uses it.
type NetworkIsolator interface {
	Isolate(ctx context.Context) error
	Reconnect(ctx context.Context) error
}

// ErrUnsupported is returned when a driver cannot perform an operation
// for its backend.
var ErrUnsupported = errors.New("operation not supported by this node driver")

func unsupported(driver, op string) error {
	return fmt.Errorf("%s: %s: %w", driver, op, ErrUnsupported)
}

// ---------------------------------------------------------------------
// Executors: how a driver runs commands. Local or over ssh.

// Executor runs a command and returns its combined output.
type Executor interface {
	Run(ctx context.Context, name string, args ...string) (string, error)
}

// LocalExecutor runs commands on this machine.
type LocalExecutor struct{}

func (LocalExecutor) Run(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// SSHExecutor runs commands on a remote host via the local ssh client
// (so key agents, config aliases and jump hosts all work as usual).
type SSHExecutor struct {
	Host string // "db2.internal" or "user@db2.internal"
	// Inner runs the ssh binary itself; nil means LocalExecutor.
	Inner Executor
}

func (s SSHExecutor) Run(ctx context.Context, name string, args ...string) (string, error) {
	inner := s.Inner
	if inner == nil {
		inner = LocalExecutor{}
	}
	quoted := make([]string, 0, len(args)+1)
	quoted = append(quoted, shellQuote(name))
	for _, a := range args {
		quoted = append(quoted, shellQuote(a))
	}
	return inner.Run(ctx, "ssh", "-o", "BatchMode=yes", s.Host, "--", strings.Join(quoted, " "))
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if !strings.ContainsAny(s, " \t\n'\"\\$`&|;<>()*?[]#~") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func executor(e Executor) Executor {
	if e == nil {
		return LocalExecutor{}
	}
	return e
}

// ---------------------------------------------------------------------
// ProcessDriver: a plain OS process identified by PID or pid file.

// ProcessDriver targets a process. Signals and freezing work everywhere;
// Start needs StartCommand; CPU and memory limits are unsupported (a bare
// process has no cgroup of its own — use the systemd driver for that).
type ProcessDriver struct {
	PID          int    // used when > 0
	PIDFile      string // read on every operation when PID == 0
	StartCommand string // shell command that restarts the node; empty = Start unsupported
	Exec_        Executor
}

func (d *ProcessDriver) Kind() string { return "process" }

func (d *ProcessDriver) Describe() string {
	if d.PID > 0 {
		return "process:" + strconv.Itoa(d.PID)
	}
	return "process:" + d.PIDFile
}

func (d *ProcessDriver) pid(ctx context.Context) (int, error) {
	if d.PID > 0 {
		return d.PID, nil
	}
	if d.PIDFile == "" {
		return 0, fmt.Errorf("process driver: no pid or pid_file")
	}
	// Read through the executor so it works over ssh too.
	out, err := executor(d.Exec_).Run(ctx, "cat", d.PIDFile)
	if err != nil {
		return 0, fmt.Errorf("process driver: reading %s: %w", d.PIDFile, err)
	}
	first := strings.TrimSpace(strings.SplitN(strings.TrimSpace(out), "\n", 2)[0])
	pid, err := strconv.Atoi(first)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("process driver: %s does not contain a pid: %q", d.PIDFile, first)
	}
	return pid, nil
}

func (d *ProcessDriver) kill(ctx context.Context, sig string) error {
	pid, err := d.pid(ctx)
	if err != nil {
		return err
	}
	_, err = executor(d.Exec_).Run(ctx, "kill", "-"+sig, strconv.Itoa(pid))
	return err
}

func (d *ProcessDriver) Signal(ctx context.Context, sig syscall.Signal) error {
	return d.kill(ctx, signalName(sig))
}

func (d *ProcessDriver) Start(ctx context.Context) error {
	if d.StartCommand == "" {
		return unsupported("process driver", "start (set start_command)")
	}
	_, err := executor(d.Exec_).Run(ctx, "sh", "-c", d.StartCommand)
	return err
}

func (d *ProcessDriver) Freeze(ctx context.Context) error { return d.kill(ctx, "STOP") }
func (d *ProcessDriver) Thaw(ctx context.Context) error   { return d.kill(ctx, "CONT") }

func (d *ProcessDriver) LimitCPU(ctx context.Context, cpus float64) error {
	return unsupported("process driver", "cpu limit (use a systemd or docker target)")
}
func (d *ProcessDriver) UnlimitCPU(ctx context.Context) error { return nil }
func (d *ProcessDriver) LimitMemory(ctx context.Context, bytes int64) error {
	return unsupported("process driver", "memory limit (use a systemd or docker target)")
}
func (d *ProcessDriver) UnlimitMemory(ctx context.Context) error { return nil }

func (d *ProcessDriver) Exec(ctx context.Context, script string) error {
	_, err := executor(d.Exec_).Run(ctx, "sh", "-c", script)
	return err
}

// ---------------------------------------------------------------------
// SystemdDriver: a systemd service unit.

type SystemdDriver struct {
	Unit  string // e.g. "postgresql"
	Exec_ Executor
}

func (d *SystemdDriver) Kind() string     { return "systemd" }
func (d *SystemdDriver) Describe() string { return "systemd:" + d.Unit }

func (d *SystemdDriver) ctl(ctx context.Context, args ...string) error {
	_, err := executor(d.Exec_).Run(ctx, "systemctl", args...)
	return err
}

func (d *SystemdDriver) Signal(ctx context.Context, sig syscall.Signal) error {
	return d.ctl(ctx, "kill", "--signal="+signalName(sig), d.Unit)
}
func (d *SystemdDriver) Start(ctx context.Context) error { return d.ctl(ctx, "start", d.Unit) }
func (d *SystemdDriver) Freeze(ctx context.Context) error {
	return d.ctl(ctx, "kill", "--signal=STOP", d.Unit)
}
func (d *SystemdDriver) Thaw(ctx context.Context) error {
	return d.ctl(ctx, "kill", "--signal=CONT", d.Unit)
}

// LimitCPU uses a runtime (non-persistent) cgroup property, so a reboot
// forgets it even if Revert never runs.
func (d *SystemdDriver) LimitCPU(ctx context.Context, cpus float64) error {
	return d.ctl(ctx, "set-property", "--runtime", d.Unit, fmt.Sprintf("CPUQuota=%d%%", int(cpus*100+0.5)))
}
func (d *SystemdDriver) UnlimitCPU(ctx context.Context) error {
	return d.ctl(ctx, "set-property", "--runtime", d.Unit, "CPUQuota=")
}
func (d *SystemdDriver) LimitMemory(ctx context.Context, bytes int64) error {
	return d.ctl(ctx, "set-property", "--runtime", d.Unit, fmt.Sprintf("MemoryMax=%d", bytes))
}
func (d *SystemdDriver) UnlimitMemory(ctx context.Context) error {
	return d.ctl(ctx, "set-property", "--runtime", d.Unit, "MemoryMax=infinity")
}
func (d *SystemdDriver) Exec(ctx context.Context, script string) error {
	_, err := executor(d.Exec_).Run(ctx, "sh", "-c", script)
	return err
}

// ---------------------------------------------------------------------
// DockerDriver: a container, via the docker CLI.

type DockerDriver struct {
	Container string
	Network   string // for Isolate/Reconnect; default "bridge"
	Exec_     Executor
}

func (d *DockerDriver) Kind() string     { return "docker" }
func (d *DockerDriver) Describe() string { return "docker:" + d.Container }

func (d *DockerDriver) docker(ctx context.Context, args ...string) error {
	_, err := executor(d.Exec_).Run(ctx, "docker", args...)
	return err
}

func (d *DockerDriver) Signal(ctx context.Context, sig syscall.Signal) error {
	return d.docker(ctx, "kill", "--signal", signalName(sig), d.Container)
}
func (d *DockerDriver) Start(ctx context.Context) error  { return d.docker(ctx, "start", d.Container) }
func (d *DockerDriver) Freeze(ctx context.Context) error { return d.docker(ctx, "pause", d.Container) }
func (d *DockerDriver) Thaw(ctx context.Context) error   { return d.docker(ctx, "unpause", d.Container) }
func (d *DockerDriver) LimitCPU(ctx context.Context, cpus float64) error {
	return d.docker(ctx, "update", "--cpus", fmt.Sprintf("%.2f", cpus), d.Container)
}
func (d *DockerDriver) UnlimitCPU(ctx context.Context) error {
	return d.docker(ctx, "update", "--cpus", "0", d.Container)
}
func (d *DockerDriver) LimitMemory(ctx context.Context, bytes int64) error {
	return d.docker(ctx, "update", "--memory", strconv.FormatInt(bytes, 10), "--memory-swap", strconv.FormatInt(bytes, 10), d.Container)
}
func (d *DockerDriver) UnlimitMemory(ctx context.Context) error {
	return d.docker(ctx, "update", "--memory", "-1", "--memory-swap", "-1", d.Container)
}
func (d *DockerDriver) Exec(ctx context.Context, script string) error {
	return d.docker(ctx, "exec", d.Container, "sh", "-c", script)
}

func (d *DockerDriver) network() string {
	if d.Network == "" {
		return "bridge"
	}
	return d.Network
}

func (d *DockerDriver) Isolate(ctx context.Context) error {
	return d.docker(ctx, "network", "disconnect", d.network(), d.Container)
}
func (d *DockerDriver) Reconnect(ctx context.Context) error {
	return d.docker(ctx, "network", "connect", d.network(), d.Container)
}

// ---------------------------------------------------------------------
// TargetSpec: a backend-neutral description of a node, as written in
// config, resolved to a driver by NewDriver.

// TargetSpec describes how to reach a node's process. Type selects the
// backend; the other fields are per type. Ssh wraps an Inner spec.
type TargetSpec struct {
	Type string // "process" | "docker" | "systemd" | "ssh"

	// process
	PID          int
	PIDFile      string
	StartCommand string
	// docker
	Container string
	Network   string
	// systemd
	Unit string
	// ssh
	Host  string
	Inner *TargetSpec
}

// NewDriver resolves a TargetSpec to a NodeDriver.
func NewDriver(t TargetSpec) (NodeDriver, error) {
	return newDriver(t, nil)
}

func newDriver(t TargetSpec, ex Executor) (NodeDriver, error) {
	switch t.Type {
	case "process":
		if t.PID <= 0 && t.PIDFile == "" {
			return nil, fmt.Errorf("target process: need pid or pid_file")
		}
		return &ProcessDriver{PID: t.PID, PIDFile: t.PIDFile, StartCommand: t.StartCommand, Exec_: ex}, nil
	case "docker":
		if t.Container == "" {
			return nil, fmt.Errorf("target docker: need container")
		}
		return &DockerDriver{Container: t.Container, Network: t.Network, Exec_: ex}, nil
	case "systemd":
		if t.Unit == "" {
			return nil, fmt.Errorf("target systemd: need unit")
		}
		return &SystemdDriver{Unit: t.Unit, Exec_: ex}, nil
	case "ssh":
		if t.Host == "" || t.Inner == nil {
			return nil, fmt.Errorf("target ssh: need host and inner target")
		}
		if t.Inner.Type == "ssh" {
			return nil, fmt.Errorf("target ssh: inner target cannot be ssh")
		}
		return newDriver(*t.Inner, SSHExecutor{Host: t.Host, Inner: ex})
	case "":
		return nil, fmt.Errorf("target: type is required (process|docker|systemd|ssh)")
	}
	return nil, fmt.Errorf("target: unknown type %q (want process|docker|systemd|ssh)", t.Type)
}

// signalName renders a syscall.Signal the way kill(1)/systemctl/docker
// accept it ("KILL", "TERM", ...).
func signalName(s syscall.Signal) string {
	switch s {
	case syscall.SIGKILL:
		return "KILL"
	case syscall.SIGTERM:
		return "TERM"
	case syscall.SIGSTOP:
		return "STOP"
	case syscall.SIGCONT:
		return "CONT"
	case syscall.SIGHUP:
		return "HUP"
	case syscall.SIGINT:
		return "INT"
	}
	return strings.TrimPrefix(strings.ToUpper(s.String()), "SIG")
}
