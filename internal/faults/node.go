package faults

import (
	"context"
	"fmt"
	"syscall"
	"time"
)

// Node faults express intent against a NodeDriver (see driver.go). None
// of them know whether the node is a process, a systemd unit, a container,
// or something on another host — that is the driver's job, and a driver
// that cannot do an operation returns ErrUnsupported.

// CrashFault kills the node with the given signal and starts it again on
// Revert. SIGKILL is a hard crash (no cleanup); SIGTERM is a graceful
// shutdown that exercises drain logic.
type CrashFault struct {
	Signal syscall.Signal
}

func (f *CrashFault) Name() string { return "crash" }

func (f *CrashFault) Inject(ctx context.Context, node NodeDriver) error {
	if err := node.Signal(ctx, f.Signal); err != nil {
		return fmt.Errorf("crash inject on %s: %w", node.Describe(), err)
	}
	return nil
}

func (f *CrashFault) Revert(ctx context.Context, node NodeDriver) error {
	if err := node.Start(ctx); err != nil {
		return fmt.Errorf("crash revert (start) on %s: %w", node.Describe(), err)
	}
	return nil
}

// CrashLoopFault repeatedly kills and restarts a node on an interval for
// a fixed duration, simulating a flapping node.
type CrashLoopFault struct {
	Interval time.Duration
	Duration time.Duration

	cancel context.CancelFunc
	done   chan struct{}
}

func (f *CrashLoopFault) Name() string { return "crash_loop" }

func (f *CrashLoopFault) Inject(ctx context.Context, node NodeDriver) error {
	loopCtx, cancel := context.WithTimeout(ctx, f.Duration)
	f.cancel = cancel
	f.done = make(chan struct{})

	go func() {
		defer close(f.done)
		ticker := time.NewTicker(f.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				_ = node.Signal(loopCtx, syscall.SIGKILL)
				select {
				case <-time.After(200 * time.Millisecond):
				case <-loopCtx.Done():
					return // Revert brings the node back; don't race it
				}
				_ = node.Start(loopCtx)
			}
		}
	}()
	return nil
}

// Revert stops the loop, waits for any in-flight iteration to finish,
// and leaves the node running.
func (f *CrashLoopFault) Revert(ctx context.Context, node NodeDriver) error {
	if f.cancel != nil {
		f.cancel()
		<-f.done
		f.cancel, f.done = nil, nil
	}
	return node.Start(ctx)
}

// ClockSkewFault offsets the node's clock, useful for testing
// Lamport/vector clock and lease-expiry assumptions.
//
// CAVEAT: on Linux the wall clock is not namespaced. Setting the date
// inside a container or a process's environment changes the HOST clock
// (and needs CAP_SYS_TIME). Use this only on a throwaway host/VM, or
// point the target at a node started under libfaketime.
type ClockSkewFault struct {
	Offset time.Duration
}

func (f *ClockSkewFault) Name() string { return "clock_skew" }

func (f *ClockSkewFault) Inject(ctx context.Context, node NodeDriver) error {
	skewed := time.Now().Add(f.Offset).Format(time.RFC3339)
	if err := node.Exec(ctx, fmt.Sprintf("date -s '%s'", skewed)); err != nil {
		return fmt.Errorf("clock skew inject on %s: %w", node.Describe(), err)
	}
	return nil
}

func (f *ClockSkewFault) Revert(ctx context.Context, node NodeDriver) error {
	realTime := time.Now().Format(time.RFC3339)
	return node.Exec(ctx, fmt.Sprintf("date -s '%s'", realTime))
}

// DiskIOLatencyFault throttles the node's block I/O via cgroup v2 io.max
// in the node's environment. Needs a writable cgroup filesystem there.
type DiskIOLatencyFault struct {
	Device   string // e.g. "8:0"
	ReadBPS  int64
	WriteBPS int64
}

func (f *DiskIOLatencyFault) Name() string { return "disk_io_latency" }

func (f *DiskIOLatencyFault) Inject(ctx context.Context, node NodeDriver) error {
	limit := fmt.Sprintf("%s rbps=%d wbps=%d", f.Device, f.ReadBPS, f.WriteBPS)
	if err := node.Exec(ctx, fmt.Sprintf("echo '%s' > /sys/fs/cgroup/io.max", limit)); err != nil {
		return fmt.Errorf("disk io throttle on %s: %w", node.Describe(), err)
	}
	return nil
}

func (f *DiskIOLatencyFault) Revert(ctx context.Context, node NodeDriver) error {
	return node.Exec(ctx, fmt.Sprintf("echo '%s max' > /sys/fs/cgroup/io.max", f.Device))
}

// DiskFullFault writes a large filler file on the node's data volume to
// exhaust available disk space.
type DiskFullFault struct {
	MountPath  string // e.g. "/var/lib/postgresql/data"
	FillerFile string // e.g. ".dbfailsim_filler"
}

func (f *DiskFullFault) Name() string { return "disk_full" }

func (f *DiskFullFault) Inject(ctx context.Context, node NodeDriver) error {
	path := f.MountPath + "/" + f.FillerFile
	// fallocate to consume remaining free space, leaving a small margin.
	script := fmt.Sprintf(
		"AVAIL=$(df --output=avail -B1 %s | tail -1); fallocate -l $((AVAIL - 1048576)) %s",
		f.MountPath, path)
	if err := node.Exec(ctx, script); err != nil {
		return fmt.Errorf("disk full inject on %s: %w", node.Describe(), err)
	}
	return nil
}

func (f *DiskFullFault) Revert(ctx context.Context, node NodeDriver) error {
	return node.Exec(ctx, "rm -f "+f.MountPath+"/"+f.FillerFile)
}

// OOMFault caps the node's memory so the kernel OOM killer terminates it
// under load.
type OOMFault struct {
	LimitBytes int64
}

func (f *OOMFault) Name() string { return "oom" }

func (f *OOMFault) Inject(ctx context.Context, node NodeDriver) error {
	if err := node.LimitMemory(ctx, f.LimitBytes); err != nil {
		return fmt.Errorf("oom inject on %s: %w", node.Describe(), err)
	}
	return nil
}

func (f *OOMFault) Revert(ctx context.Context, node NodeDriver) error {
	return node.UnlimitMemory(ctx)
}

// CPUThrottleFault caps CPU to simulate a slow/overloaded node.
type CPUThrottleFault struct {
	CPUQuota float64 // e.g. 0.1 for 10% of one core
}

func (f *CPUThrottleFault) Name() string { return "cpu_throttle" }

func (f *CPUThrottleFault) Inject(ctx context.Context, node NodeDriver) error {
	if err := node.LimitCPU(ctx, f.CPUQuota); err != nil {
		return fmt.Errorf("cpu throttle inject on %s: %w", node.Describe(), err)
	}
	return nil
}

func (f *CPUThrottleFault) Revert(ctx context.Context, node NodeDriver) error {
	return node.UnlimitCPU(ctx)
}

// ZombieFault freezes the node (SIGSTOP / pause) so it stays "running"
// but cannot respond to anything — it keeps its ports and connections.
type ZombieFault struct{}

func (f *ZombieFault) Name() string { return "zombie" }

func (f *ZombieFault) Inject(ctx context.Context, node NodeDriver) error {
	return node.Freeze(ctx)
}

func (f *ZombieFault) Revert(ctx context.Context, node NodeDriver) error {
	return node.Thaw(ctx)
}
