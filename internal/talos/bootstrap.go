package talos

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
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

	// Step 1: Wait for Talos API to come up
	if err := b.waitForTalosAPI(opts.Host); err != nil {
		return err
	}

	// Step 2: Apply control plane config (--insecure targets maintenance mode).
	// If Talos booted with the config already written to the STATE partition,
	// this step is effectively a no-op or may return an error — both are OK.
	fmt.Println("  Applying control plane configuration...")
	if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
		"apply-config", "--insecure",
		"--nodes", opts.Host,
		"--file", opts.ControlPlaneCfg,
	); err != nil {
		// If the node already has a config (booted from STATE partition),
		// apply-config --insecure will fail — treat that as non-fatal.
		color.Yellow("  Warning: apply-config returned an error (node may already be configured): %v\n", err)
		color.Yellow("  Continuing — assuming config was pre-applied by nextboot-talos script.\n")
	} else {
		color.Green("  ✓ Control plane config applied\n")
	}

	// Step 2b: Wait for Talos to reboot after apply-config.
	// apply-config causes an immediate reboot in maintenance mode; the API
	// drops briefly then returns when the node is in configured mode.
	fmt.Println("  Waiting for Talos to reboot after config apply (up to 10 minutes)...")
	time.Sleep(15 * time.Second) // give the reboot time to start
	if err := b.waitForTalosAPI(opts.Host); err != nil {
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
