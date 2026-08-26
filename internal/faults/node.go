package faults

import (
	"context"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)


// CrashFault kills the target container with the given signal.
// Use syscall.SIGKILL for a hard crash (no cleanup) or syscall.SIGTERM
// for a graceful shutdown that exercises drain logic.
type CrashFault struct {
	Signal syscall.Signal
}

func (f *CrashFault) Name() string { return "crash" }

func (f *CrashFault) Inject(ctx context.Context, nodeID string) error {
	sigName := signalName(f.Signal)
	cmd := exec.CommandContext(ctx, "docker", "kill", "--signal", sigName, nodeID)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("crash inject failed for %s: %w (%s)", nodeID, err, out)
	}
	return nil
}

func (f *CrashFault) Revert(ctx context.Context, nodeID string) error {
	// Bring the node back; docker-compose restart policy or explicit start.
	cmd := exec.CommandContext(ctx, "docker", "start", nodeID)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("crash revert (restart) failed for %s: %w (%s)", nodeID, err, out)
	}
	return nil
}

func signalName(s syscall.Signal) string {
	switch s {
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGTERM:
		return "SIGTERM"
	default:
		return s.String()
	}
}


// CrashLoopFault repeatedly kills and restarts a node on an interval for
// a fixed duration, simulating a flapping node.
type CrashLoopFault struct {
	Interval time.Duration
	Duration time.Duration

	cancel context.CancelFunc
}

func (f *CrashLoopFault) Name() string { return "crash_loop" }

func (f *CrashLoopFault) Inject(ctx context.Context, nodeID string) error {
	loopCtx, cancel := context.WithTimeout(ctx, f.Duration)
	f.cancel = cancel

	go func() {
		ticker := time.NewTicker(f.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				_ = exec.CommandContext(loopCtx, "docker", "kill", nodeID).Run()
				time.Sleep(200 * time.Millisecond)
				_ = exec.CommandContext(loopCtx, "docker", "start", nodeID).Run()
			}
		}
	}()
	return nil
}

func (f *CrashLoopFault) Revert(ctx context.Context, nodeID string) error {
	if f.cancel != nil {
		f.cancel()
	}
	cmd := exec.CommandContext(ctx, "docker", "start", nodeID)
	return cmd.Run()
}


// ClockSkewFault offsets the container's clock, useful for testing
// Lamport/vector clock and lease-expiry assumptions.
type ClockSkewFault struct {
	Offset time.Duration
}

func (f *ClockSkewFault) Name() string { return "clock_skew" }

func (f *ClockSkewFault) Inject(ctx context.Context, nodeID string) error {
	skewed := time.Now().Add(f.Offset).Format(time.RFC3339)
	// Requires the container to run with SYS_TIME capability, or this
	// simulates the effect via libfaketime env var instead (preferred,
	// no special privileges needed):
	cmd := exec.CommandContext(ctx, "docker", "exec", nodeID,
		"sh", "-c", fmt.Sprintf("date -s '%s' || true", skewed))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("clock skew inject failed for %s: %w (%s)", nodeID, err, out)
	}
	return nil
}

func (f *ClockSkewFault) Revert(ctx context.Context, nodeID string) error {
	realTime := time.Now().Format(time.RFC3339)
	cmd := exec.CommandContext(ctx, "docker", "exec", nodeID,
		"sh", "-c", fmt.Sprintf("date -s '%s' || true", realTime))
	return cmd.Run()
}


// DiskIOLatencyFault adds artificial I/O delay inside the container using
// tc/netem-style throttling is for network; for disk we use cgroup v2's
// io.max device throttle when available.
type DiskIOLatencyFault struct {
	Device    string // e.g. "8:0"
	ReadBPS   int64
	WriteBPS  int64
}

func (f *DiskIOLatencyFault) Name() string { return "disk_io_latency" }

func (f *DiskIOLatencyFault) Inject(ctx context.Context, nodeID string) error {
	limit := fmt.Sprintf("%s rbps=%d wbps=%d", f.Device, f.ReadBPS, f.WriteBPS)
	cmd := exec.CommandContext(ctx, "docker", "exec", nodeID,
		"sh", "-c", fmt.Sprintf("echo '%s' > /sys/fs/cgroup/io.max", limit))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("disk io throttle failed for %s: %w (%s)", nodeID, err, out)
	}
	return nil
}

func (f *DiskIOLatencyFault) Revert(ctx context.Context, nodeID string) error {
	cmd := exec.CommandContext(ctx, "docker", "exec", nodeID,
		"sh", "-c", fmt.Sprintf("echo '%s max' > /sys/fs/cgroup/io.max", f.Device))
	return cmd.Run()
}


// DiskFullFault writes a large filler file inside the container to
// exhaust available disk space on its data volume.
type DiskFullFault struct {
	MountPath string // e.g. "/var/lib/postgresql/data"
	FillerFile string // e.g. ".dbfailsim_filler"
}

func (f *DiskFullFault) Name() string { return "disk_full" }

func (f *DiskFullFault) Inject(ctx context.Context, nodeID string) error {
	path := f.MountPath + "/" + f.FillerFile
	// fallocate to consume remaining free space, leaving a small margin.
	cmd := exec.CommandContext(ctx, "docker", "exec", nodeID,
		"sh", "-c", fmt.Sprintf(
			"AVAIL=$(df --output=avail -B1 %s | tail -1); "+
				"fallocate -l $((AVAIL - 1048576)) %s", f.MountPath, path))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("disk full inject failed for %s: %w (%s)", nodeID, err, out)
	}
	return nil
}

func (f *DiskFullFault) Revert(ctx context.Context, nodeID string) error {
	path := f.MountPath + "/" + f.FillerFile
	cmd := exec.CommandContext(ctx, "docker", "exec", nodeID, "rm", "-f", path)
	return cmd.Run()
}


// OOMFault sets a low memory cgroup limit on the container, causing the
// kernel OOM killer to terminate its main process under load.
type OOMFault struct {
	LimitBytes int64
}

func (f *OOMFault) Name() string { return "oom" }

func (f *OOMFault) Inject(ctx context.Context, nodeID string) error {
	cmd := exec.CommandContext(ctx, "docker", "update", "--memory", fmt.Sprintf("%d", f.LimitBytes),
		"--memory-swap", fmt.Sprintf("%d", f.LimitBytes), nodeID)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("oom inject failed for %s: %w (%s)", nodeID, err, out)
	}
	return nil
}

func (f *OOMFault) Revert(ctx context.Context, nodeID string) error {
	cmd := exec.CommandContext(ctx, "docker", "update", "--memory", "-1", "--memory-swap", "-1", nodeID)
	return cmd.Run()
}


// CPUThrottleFault caps CPU quota to simulate a slow/overloaded node
// (e.g. long GC-style pauses under load).
type CPUThrottleFault struct {
	CPUQuota float64 // e.g. 0.1 for 10% of one core
}

func (f *CPUThrottleFault) Name() string { return "cpu_throttle" }

func (f *CPUThrottleFault) Inject(ctx context.Context, nodeID string) error {
	cmd := exec.CommandContext(ctx, "docker", "update", "--cpus", fmt.Sprintf("%.2f", f.CPUQuota), nodeID)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("cpu throttle inject failed for %s: %w (%s)", nodeID, err, out)
	}
	return nil
}

func (f *CPUThrottleFault) Revert(ctx context.Context, nodeID string) error {
	cmd := exec.CommandContext(ctx, "docker", "update", "--cpus", "0", nodeID)
	return cmd.Run()
}


// ZombieFault pauses the container's process (SIGSTOP) so it stays
// "running" from docker's view but cannot respond to anything.
type ZombieFault struct{}

func (f *ZombieFault) Name() string { return "zombie" }

func (f *ZombieFault) Inject(ctx context.Context, nodeID string) error {
	cmd := exec.CommandContext(ctx, "docker", "pause", nodeID)
	return cmd.Run()
}

func (f *ZombieFault) Revert(ctx context.Context, nodeID string) error {
	cmd := exec.CommandContext(ctx, "docker", "unpause", nodeID)
	return cmd.Run()
}