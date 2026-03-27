package talos

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
)

// BootstrapOptions holds parameters for bootstrapping a Talos cluster.
type BootstrapOptions struct {
	Host             string
	TalosConfigFile  string
	ControlPlaneCfg  string
	KubeconfigOut    string
	EtcdSnapshotPath string // if set, run etcd recover instead of bootstrap
}

// Bootstrapper handles waiting for Talos to boot and running bootstrap.
type Bootstrapper struct {
	backupDir string
}

// NewBootstrapper creates a new Bootstrapper.
func NewBootstrapper(backupDir string) *Bootstrapper {
	return &Bootstrapper{backupDir: backupDir}
}

// Bootstrap waits for Talos to boot, applies config, bootstraps Kubernetes,
// and retrieves the kubeconfig.
func (b *Bootstrapper) Bootstrap(opts BootstrapOptions) error {
	talosctlPath, err := exec.LookPath("talosctl")
	if err != nil {
		return fmt.Errorf("talosctl not found in PATH")
	}

	// Step 1: Wait for Talos to respond on port 50000 (TCP).
	// This is an early-presence check only — the port may be open before the
	// gRPC+TLS stack is fully initialised.
	if err := b.waitForTalosAPI(opts.Host); err != nil {
		return err
	}

	// Step 2: Apply control plane config (--insecure targets maintenance mode).
	// If Talos booted with the config already written to the STATE partition by
	// the nextboot agent, this call will fail (the insecure endpoint is gone).
	// Both outcomes are fine; we continue either way.
	fmt.Println("  Applying control plane configuration...")
	if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
		"apply-config", "--insecure",
		"--nodes", opts.Host,
		"--file", opts.ControlPlaneCfg,
	); err != nil {
		color.Yellow("  Warning: apply-config returned an error (node may already be configured): %v\n", err)
		color.Yellow("  Continuing — assuming config was pre-applied by nextboot-talos script.\n")
	} else {
		color.Green("  ✓ Control plane config applied\n")
	}

	// Step 2b: Wait for Talos gRPC API to be ready.
	//
	// The TCP dial above only proves that something is listening on port 50000.
	// machined may not yet have loaded the node's CA cert, so talosctl calls
	// that use CA-verified TLS (e.g. bootstrap) will fail with TLS errors.
	//
	// We poll "talosctl version" until it succeeds: that confirms the gRPC
	// server is up, the cert was generated from the config CA, and talosctl
	// can authenticate.  A short sleep first gives apply-config time to
	// trigger a reboot if the node was in maintenance mode.
	fmt.Println("  Waiting for Talos gRPC API to be ready (up to 20 minutes)...")
	time.Sleep(5 * time.Second)
	if err := b.waitForTalosctlReady(talosctlPath, opts.TalosConfigFile, opts.Host); err != nil {
		return fmt.Errorf("waiting for Talos after config apply: %w", err)
	}

	// Step 3: Initialize etcd.
	// If a k3s etcd snapshot is available, attempt bootstrap --recover-from to
	// seed the cluster from the k3s data.  If recovery fails (e.g. incompatible
	// snapshot, partial file, CA mismatch) fall back to a fresh bootstrap so
	// the migration can still complete; YAML-backed resources will be re-applied
	// by the caller.
	// NOTE: talosctl v1.10+ removed 'etcd recover'; recovery is now done via
	// 'bootstrap --recover-from <snapshot>'.
	if opts.EtcdSnapshotPath != "" {
		fmt.Printf("  Bootstrapping etcd from k3s snapshot: %s\n", opts.EtcdSnapshotPath)
		recoveryErr := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
			"bootstrap",
			"--nodes", opts.Host,
			"--endpoints", opts.Host,
			"--recover-from", opts.EtcdSnapshotPath,
		)
		switch {
		case recoveryErr == nil:
			color.Green("  ✓ etcd bootstrapped from k3s snapshot\n")
		case strings.Contains(recoveryErr.Error(), "already bootstrapped"),
			strings.Contains(recoveryErr.Error(), "AlreadyExists"):
			fmt.Println("  (cluster was already bootstrapped)")
		default:
			// Recovery failed — warn and fall back to a standard fresh bootstrap.
			color.Yellow("  Warning: etcd recovery failed: %v\n", recoveryErr)
			color.Yellow("  Falling back to fresh bootstrap (k8s resources will be re-applied from backup).\n")
			if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
				"bootstrap",
				"--nodes", opts.Host,
				"--endpoints", opts.Host,
			); err != nil {
				if !strings.Contains(err.Error(), "already bootstrapped") &&
					!strings.Contains(err.Error(), "AlreadyExists") {
					return fmt.Errorf("bootstrapping cluster (after recovery failure): %w", err)
				}
				fmt.Println("  (cluster was already bootstrapped)")
			}
			color.Green("  ✓ Cluster bootstrapped (fresh — etcd recovery was skipped)\n")
		}
	} else {
		fmt.Println("  Bootstrapping Kubernetes cluster (this runs once on the control plane)...")
		if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
			"bootstrap",
			"--nodes", opts.Host,
			"--endpoints", opts.Host,
		); err != nil {
			// Bootstrap can return a "already bootstrapped" error — that's OK
			if !strings.Contains(err.Error(), "already bootstrapped") &&
				!strings.Contains(err.Error(), "AlreadyExists") {
				return fmt.Errorf("bootstrapping cluster: %w", err)
			}
			fmt.Println("  (cluster was already bootstrapped)")
		}
		color.Green("  ✓ Cluster bootstrapped\n")
	}

	// Step 4: Wait for Kubernetes API
	if err := b.waitForKubernetesAPI(opts.Host, opts.TalosConfigFile); err != nil {
		return err
	}

	// Step 5: Retrieve kubeconfig
	fmt.Printf("  Retrieving kubeconfig → %s\n", opts.KubeconfigOut)
	if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
		"kubeconfig",
		"--nodes", opts.Host,
		"--force",
		"--merge=false",
		opts.KubeconfigOut,
	); err != nil {
		return fmt.Errorf("retrieving kubeconfig: %w", err)
	}
	color.Green("  ✓ Kubeconfig saved to %s\n", opts.KubeconfigOut)

	return nil
}

// waitForTalosAPI polls the Talos machine API (port 50000) until it responds,
// with exponential backoff up to ~20 minutes total.
//
// We use a plain TCP dial rather than an HTTPS GET because the Talos API
// server speaks gRPC (HTTP/2) over TLS.  An HTTP/1.1 GET causes the TLS
// handshake to succeed but the server immediately returns an error frame,
// which http.Client may surface as an error even though the port IS open.
// A successful TCP three-way handshake is sufficient to confirm Talos is up.
func (b *Bootstrapper) waitForTalosAPI(host string) error {
	s := spinner.New(spinner.CharSets[14], 200*time.Millisecond)
	s.Suffix = " Waiting for Talos to boot (this may take several minutes)..."
	s.Start()
	defer s.Stop()

	addr := fmt.Sprintf("%s:50000", host)
	deadline := time.Now().Add(20 * time.Minute)
	wait := 5 * time.Second
	start := time.Now()
	sshChecked := false

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			conn.Close()
			s.Stop()
			color.Green("  ✓ Talos API is responding at %s\n", addr)
			return nil
		}

		// After 3 minutes of waiting, check if port 22 (SSH) is responding.
		// If it is, the machine rebooted back into Ubuntu instead of Talos —
		// a clear sign that kexec failed AND the EFI boot path didn't work.
		if !sshChecked && time.Since(start) > 3*time.Minute {
			sshChecked = true
			if conn, tcpErr := net.DialTimeout("tcp", host+":22", 3*time.Second); tcpErr == nil {
				conn.Close()
				s.Stop()
				color.Yellow("\n  ⚠  Port 22 (SSH) is responding on %s\n", host)
				color.Yellow("  ⚠  This means the machine rebooted back into Ubuntu, not Talos!\n")
				color.Yellow("  ⚠  Likely causes:\n")
				color.Yellow("  ⚠    • EFI file patch failed — GRUB loaded Ubuntu instead of Talos\n")
				color.Yellow("  ⚠    • efibootmgr BootNext not set (efivars read-only on this platform)\n")
				color.Yellow("  ⚠    • UEFI firmware did not fall back to EFI/BOOT/BOOTX64.EFI\n")
				color.Yellow("  ⚠  Continuing to wait for Talos API (may not succeed)...\n\n")
				s.Start()
			}
		}

		// Also accept connection refused → Talos is up but not yet serving
		if strings.Contains(err.Error(), "connection refused") {
			time.Sleep(wait)
			if wait < 30*time.Second {
				wait *= 2
			}
			continue
		}

		time.Sleep(wait)
		if wait < 30*time.Second {
			wait *= 2
		}
	}

	s.Stop()
	return fmt.Errorf(
		"Talos API at %s did not become available within 20 minutes\n\n"+
			"The machine may need manual intervention. Once Talos is running you can:\n"+
			"  talosctl --talosconfig %s apply-config --insecure --nodes %s --file controlplane.yaml\n"+
			"  talosctl --talosconfig %s bootstrap --nodes %s\n",
		addr, b.backupDir+"/talos-config/talosconfig", host,
		b.backupDir+"/talos-config/talosconfig", host,
	)
}

// waitForTalosctlReady polls "talosctl version" until it succeeds.
// Unlike the TCP dial in waitForTalosAPI, this verifies that the gRPC server is
// up AND that the node's CA cert matches the talosconfig — i.e. the node has
// applied the machine config and is ready for authenticated API calls like
// "bootstrap".  This prevents a race condition where the TCP port opens before
// machined has loaded its TLS identity from the config.
//
// Each iteration also checks port 50000 TCP state to distinguish:
//   - Port UP + cert error  → Talos is in maintenance mode (self-signed cert)
//   - Port DOWN             → machine is rebooting (expected after apply-config)
//
// If port 50000 has been continuously reachable for >5 minutes with persistent
// cert failures, apply-config is retried in case the first attempt was dropped.
func (b *Bootstrapper) waitForTalosctlReady(talosctlPath, talosConfigFile, host string) error {
	s := spinner.New(spinner.CharSets[14], 200*time.Millisecond)
	s.Suffix = " Waiting for Talos gRPC API (CA-verified)..."
	s.Start()
	defer s.Stop()

	deadline := time.Now().Add(35 * time.Minute)
	wait := 5 * time.Second
	start := time.Now()
	attempt := 0
	// Track how long port 50000 has been continuously UP with a cert failure.
	// If >5 min, re-send apply-config — the first one may have been lost.
	var port50kUpSince time.Time
	applyConfigRetried := false

	for time.Now().Before(deadline) {
		attempt++
		// --timeout 15s prevents the check from hanging indefinitely when
		// machined accepts the TCP connection but hasn't loaded its certs yet.
		err := b.runTalosctl(talosctlPath, talosConfigFile,
			"version",
			"--nodes", host,
			"--endpoints", host,
			"--timeout", "15s",
		)
		if err == nil {
			s.Stop()
			color.Green("  ✓ Talos gRPC API is ready\n")
			return nil
		}

		// Check port 50000 TCP state — this tells us what phase we're in.
		port50kUp := false
		if conn, tcpErr := net.DialTimeout("tcp", host+":50000", 3*time.Second); tcpErr == nil {
			conn.Close()
			port50kUp = true
		}

		// Log the actual error every attempt for the first 5, then every 5th.
		if attempt <= 5 || attempt%5 == 0 {
			s.Stop()
			portState := "DOWN (rebooting)"
			if port50kUp {
				portState = "UP (maintenance mode / cert mismatch)"
			}
			color.Yellow("  [attempt %d, elapsed %s] port 50000: %s\n    talosctl error: %v\n",
				attempt, time.Since(start).Round(time.Second), portState, err)
			s.Start()
		}

		// Track consecutive time port 50000 has been UP with cert failure.
		if port50kUp {
			if port50kUpSince.IsZero() {
				port50kUpSince = time.Now()
			}
			// If port has been UP (maintenance mode) for >5 min and we
			// haven't retried yet, re-send apply-config — the first may
			// have been silently dropped or Talos rejected it.
			if !applyConfigRetried && time.Since(port50kUpSince) > 5*time.Minute {
				applyConfigRetried = true
				s.Stop()
				color.Yellow("  Port 50000 has been up for >5 min with cert errors — " +
					"retrying apply-config --insecure\n")
				s.Start()
				// Read controlplane.yaml path from BootstrapOptions — we need
				// the stored path. Re-derive it from talosConfigFile directory.
				controlPlaneCfg := filepath.Join(filepath.Dir(talosConfigFile), "controlplane.yaml")
				if applyErr := b.runTalosctl(talosctlPath, talosConfigFile,
					"apply-config", "--insecure",
					"--nodes", host,
					"--file", controlPlaneCfg,
				); applyErr != nil {
					s.Stop()
					color.Yellow("  apply-config retry: %v\n", applyErr)
					s.Start()
				} else {
					s.Stop()
					color.Green("  ✓ apply-config retry succeeded — waiting for reboot\n")
					s.Start()
				}
				// Reset the maintenance-mode timer so we don't re-retry immediately.
				port50kUpSince = time.Time{}
			}
		} else {
			// Port is DOWN — machine is rebooting; reset the maintenance-mode timer.
			port50kUpSince = time.Time{}
		}

		// After 3 minutes, check if SSH (Ubuntu) is responding.
		// If it is, the machine booted back into Ubuntu — fail immediately
		// rather than burning 35 minutes of CI time.
		if time.Since(start) > 3*time.Minute {
			if conn, tcpErr := net.DialTimeout("tcp", host+":22", 3*time.Second); tcpErr == nil {
				conn.Close()
				s.Stop()
				color.Yellow("\n  ⚠  Port 22 (SSH) is responding — machine has rebooted back into Ubuntu!\n")
				color.Yellow("  ⚠  Talos did not boot after the EFI reboot. Failing early.\n")
				return fmt.Errorf(
					"machine at %s booted back into Ubuntu instead of Talos "+
						"(port 22 is responding, port 50000 is not ready after %s)",
					host, time.Since(start).Round(time.Second),
				)
			}
		}

		time.Sleep(wait)
		if wait < 30*time.Second {
			wait *= 2
		}
	}

	s.Stop()
	return fmt.Errorf("Talos gRPC API at %s did not become ready within 35 minutes", host)
}

// waitForKubernetesAPI polls kubectl until the API server responds.
func (b *Bootstrapper) waitForKubernetesAPI(host, talosConfigFile string) error {
	s := spinner.New(spinner.CharSets[14], 200*time.Millisecond)
	s.Suffix = " Waiting for Kubernetes API server to become ready..."
	s.Start()
	defer s.Stop()

	talosctlPath, _ := exec.LookPath("talosctl")
	deadline := time.Now().Add(5 * time.Minute)
	wait := 10 * time.Second

	for time.Now().Before(deadline) {
		err := b.runTalosctl(talosctlPath, talosConfigFile,
			"health",
			"--nodes", host,
			"--wait-timeout", "10s",
		)
		if err == nil {
			s.Stop()
			color.Green("  ✓ Kubernetes API server is ready\n")
			return nil
		}
		time.Sleep(wait)
	}

	s.Stop()
	// Non-fatal — the kubeconfig retrieval step will catch if it's truly not ready
	color.Yellow("  Warning: health check timed out. Attempting kubeconfig retrieval anyway.\n")
	return nil
}

func (b *Bootstrapper) runTalosctl(binary, talosconfig string, args ...string) error {
	allArgs := append([]string{"--talosconfig", talosconfig}, args...)
	cmd := exec.Command(binary, allArgs...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		combined := strings.TrimSpace(stdout.String() + "\n" + stderr.String())
		return fmt.Errorf("%w\n%s", err, combined)
	}

	// Write stdout to the target file if this is a kubeconfig command
	for i, arg := range args {
		if arg == "kubeconfig" && i < len(args)-1 {
			outFile := args[len(args)-1]
			if stdout.Len() > 0 {
				os.WriteFile(outFile, stdout.Bytes(), 0600) //nolint:errcheck
			}
			break
		}
	}

	return nil
}
