package talos

import (
	"bytes"
	"context"
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
	Verbose          bool   // print each talosctl invocation to stderr
}

// Bootstrapper handles waiting for Talos to boot and running bootstrap.
type Bootstrapper struct {
	backupDir string
	verbose   bool
}

// NewBootstrapper creates a new Bootstrapper.
func NewBootstrapper(backupDir string) *Bootstrapper {
	return &Bootstrapper{backupDir: backupDir}
}

// Bootstrap waits for Talos to boot, applies config, bootstraps Kubernetes,
// and retrieves the kubeconfig.
func (b *Bootstrapper) Bootstrap(opts BootstrapOptions) error {
	b.verbose = opts.Verbose
	talosctlPath, err := exec.LookPath("talosctl")
	if err != nil {
		return fmt.Errorf("talosctl not found in PATH")
	}

	// Step 1: Wait for Talos to respond on port 50000 (TCP).
	if err := b.waitForTalosAPI(opts.Host); err != nil {
		return err
	}

	// Step 2: Determine if we need to apply config.
	//
	// First, try the CA-verified talosctl version call.  If this succeeds,
	// the machine config is already applied (Talos started from kexec inline
	// config or STATE partition) and we can skip apply-config entirely.
	//
	// Only if CA-verified fails do we probe for maintenance mode and
	// send apply-config --insecure.
	fmt.Println("  Checking if Talos is already in configured mode...")
	if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
		"version",
		"--nodes", opts.Host,
		"--endpoints", opts.Host,
	); err == nil {
		color.Green("  ✓ Talos is already in configured mode — skipping apply-config\n")
	} else {
		fmt.Printf("  CA-verified check failed (%v)\n", summariseError(err))
		fmt.Println("  Probing for Talos maintenance mode...")

		inMaintenanceMode := b.probeMaintenanceMode(talosctlPath, opts.TalosConfigFile, opts.Host, 90*time.Second)
		if inMaintenanceMode {
			fmt.Println("  Talos is in maintenance mode — applying control plane configuration...")
			// Use runTalosctlInsecure (no --talosconfig, no client cert) for
			// maintenance-mode apply-config.  Maintenance mode does not require
			// mTLS; passing a client cert from talosconfig can confuse newer
			// Talos builds that check the cert against a non-existent CA.
			if applyErr := b.runTalosctlInsecure(talosctlPath,
				"apply-config",
				"--nodes", opts.Host,
				"--endpoints", opts.Host,
				"--file", opts.ControlPlaneCfg,
			); applyErr != nil {
				color.Yellow("  Warning: apply-config returned an error: %v\n", summariseError(applyErr))
				color.Yellow("  Will retry inside waitForTalosctlReady.\n")
			} else {
				color.Green("  ✓ Control plane config applied — Talos will transition to configured mode\n")
			}
		} else {
			fmt.Println("  Maintenance-mode endpoint not responding — proceeding to gRPC readiness check.")
		}
	}

	// Step 2b: Wait for Talos gRPC API to be ready (CA-verified).
	fmt.Println("  Waiting for Talos gRPC API to be ready (up to 35 minutes)...")
	if err := b.waitForTalosctlReady(talosctlPath, opts.TalosConfigFile, opts.Host, opts.ControlPlaneCfg, ""); err != nil {
		return fmt.Errorf("waiting for Talos after config apply: %w", err)
	}

	// Step 3: Initialize etcd.
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
			if !strings.Contains(err.Error(), "already bootstrapped") &&
				!strings.Contains(err.Error(), "AlreadyExists") {
				return fmt.Errorf("bootstrapping cluster: %w", err)
			}
			fmt.Println("  (cluster was already bootstrapped)")
		}
		color.Green("  ✓ Cluster bootstrapped\n")
	}

	// Step 4: Retrieve kubeconfig.
	// talosctl kubeconfig talks to the Talos gRPC API (port 50000), not the
	// Kubernetes API, so it can be fetched as soon as the Talos API is ready —
	// before the kube-apiserver has finished starting up.
	fmt.Printf("  Retrieving kubeconfig → %s\n", opts.KubeconfigOut)
	if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
		"kubeconfig",
		"--nodes", opts.Host,
		"--endpoints", opts.Host,
		"--force",
		"--merge=false",
		opts.KubeconfigOut,
	); err != nil {
		return fmt.Errorf("retrieving kubeconfig: %w", err)
	}
	color.Green("  ✓ Kubeconfig saved to %s\n", opts.KubeconfigOut)

	// Step 5: Wait for Kubernetes API
	if err := b.waitForKubernetesAPI(opts.Host, opts.TalosConfigFile); err != nil {
		return err
	}

	return nil
}

// WorkerBootstrapOptions holds parameters for bootstrapping a Talos worker node.
type WorkerBootstrapOptions struct {
	Host            string
	TalosConfigFile string // talosconfig from the CP migration
	WorkerCfgFile   string // worker.yaml from the CP migration
	Verbose         bool
}

// BootstrapWorker installs Talos on a worker node and waits for it to join the
// cluster.  Unlike Bootstrap it does not touch etcd or retrieve a kubeconfig —
// the worker joins automatically via the token embedded in worker.yaml.
//
// On EC2 the public IP is NAT'd and not on any interface, so machined would
// not auto-include it in the server TLS cert SANs.  BootstrapWorker therefore
// injects machine.certSANs=[host] via the apply-config --patch flag so that
// CA-verified talosctl calls via the public IP succeed after the first reboot.
func (b *Bootstrapper) BootstrapWorker(opts WorkerBootstrapOptions) error {
	b.verbose = opts.Verbose
	talosctlPath, err := exec.LookPath("talosctl")
	if err != nil {
		return fmt.Errorf("talosctl not found in PATH")
	}

	certSANsPatch := fmt.Sprintf("machine:\n  certSANs:\n    - %q\n", opts.Host)

	// Step 1: Wait for Talos to respond on port 50000.
	if err := b.waitForTalosAPI(opts.Host); err != nil {
		return err
	}

	// Step 2: Check if already configured; otherwise probe maintenance mode.
	fmt.Println("  Checking if worker Talos is already in configured mode...")
	if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
		"version",
		"--nodes", opts.Host,
		"--endpoints", opts.Host,
	); err == nil {
		color.Green("  ✓ Worker Talos is already in configured mode\n")
	} else {
		fmt.Printf("  CA-verified check failed (%v)\n", summariseError(err))
		fmt.Println("  Probing for worker maintenance mode...")
		inMaintenance := b.probeMaintenanceMode(talosctlPath, opts.TalosConfigFile, opts.Host, 90*time.Second)
		if inMaintenance {
			fmt.Println("  Worker is in maintenance mode — applying worker configuration...")
			if applyErr := b.runTalosctlInsecure(talosctlPath,
				"apply-config",
				"--nodes", opts.Host,
				"--endpoints", opts.Host,
				"--file", opts.WorkerCfgFile,
				"--config-patch", certSANsPatch,
			); applyErr != nil {
				color.Yellow("  Warning: apply-config returned an error: %v\n", summariseError(applyErr))
				color.Yellow("  Will retry inside waitForTalosctlReady.\n")
			} else {
				color.Green("  ✓ Worker config applied — waiting for reboot\n")
			}
		} else {
			// Maintenance mode endpoint is NOT responding.  The machine is likely
			// in configured mode but machined's TLS cert is missing the public IP
			// in SANs (so the CA-verified check above also failed).
			//
			// Recovery: use the talosconfig client cert (satisfies machined's
			// mTLS requirement) while skipping server-cert verification (--insecure)
			// to work around the SAN mismatch.  If apply-config succeeds, the
			// machine reboots with the updated certSANs and the CA-verified check
			// in waitForTalosctlReady will then succeed.
			fmt.Println("  Maintenance-mode endpoint not responding — machine may be configured without public-IP certSANs.")
			fmt.Println("  Attempting apply-config with talosconfig+insecure to inject certSANs...")
			if _, applyErr := b.runTalosctlWithOutput(talosctlPath, opts.TalosConfigFile,
				"apply-config",
				"--insecure",
				"--nodes", opts.Host,
				"--endpoints", opts.Host,
				"--file", opts.WorkerCfgFile,
				"--config-patch", certSANsPatch,
			); applyErr != nil {
				color.Yellow("  apply-config (talosconfig+insecure) returned an error: %v\n", summariseError(applyErr))
				color.Yellow("  Will retry inside waitForTalosctlReady.\n")
			} else {
				color.Green("  ✓ apply-config (talosconfig+insecure) accepted — Talos will reboot with certSANs\n")
			}
		}
	}

	// Step 3: Wait for CA-verified gRPC API (worker auto-joins cluster via token).
	fmt.Println("  Waiting for worker Talos gRPC API to be ready (up to 35 minutes)...")
	if err := b.waitForTalosctlReady(talosctlPath, opts.TalosConfigFile, opts.Host, opts.WorkerCfgFile, certSANsPatch); err != nil {
		return fmt.Errorf("waiting for worker Talos after config apply: %w", err)
	}

	return nil
}

// waitForTalosAPI polls the Talos machine API (port 50000) until it responds.
func (b *Bootstrapper) waitForTalosAPI(host string) error {
	s := spinner.New(spinner.CharSets[14], 200*time.Millisecond)
	s.Suffix = " Waiting for Talos to boot (this may take several minutes)..."
	s.Start()
	defer s.Stop()

	addr := fmt.Sprintf("%s:50000", host)
	deadline := time.Now().Add(20 * time.Minute)
	wait := 5 * time.Second
	start := time.Now()
	var lastSSHCheck time.Time // zero = never checked

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			conn.Close()
			s.Stop()
			color.Green("  ✓ Talos API is responding at %s (elapsed %s)\n",
				addr, time.Since(start).Round(time.Second))
			return nil
		}

		// Periodically check if Ubuntu SSH is up — that means Talos failed to boot.
		// Check every 30 s after the first 3 minutes (allow for initial reboot time).
		if time.Since(start) > 3*time.Minute && time.Since(lastSSHCheck) > 30*time.Second {
			lastSSHCheck = time.Now()
			elapsed := time.Since(start).Round(time.Second)
			if tcpProbe(host+":22", 3*time.Second) {
				s.Stop()
				return fmt.Errorf(
					"machine at %s rebooted back into Ubuntu (port 22 is UP after %s, "+
						"port 50000 is DOWN) — Talos did not boot.\n"+
						"Check the agent log in the backup artifact for kexec/EFI boot errors.",
					host, elapsed,
				)
			}
		}

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

// probeMaintenanceMode checks whether Talos is in maintenance mode by polling
// the insecure (unauthenticated) gRPC endpoint.
//
// In maintenance mode, the insecure endpoint responds WITHOUT client-cert auth.
// In configured mode, the endpoint requires mTLS → the insecure probe fails.
// This correctly distinguishes the two states.
func (b *Bootstrapper) probeMaintenanceMode(talosctlPath, talosConfigFile, host string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	wait := 3 * time.Second
	for time.Now().Before(deadline) {
		// Use --talosconfig but also --insecure so the CA check is skipped.
		// In maintenance mode machined has a self-signed cert (CA mismatch
		// unless we use --insecure).  In configured mode, machined requires
		// client-cert mTLS — which --insecure suppresses — so this call
		// will FAIL in configured mode (connection refused / auth error).
		err := b.runTalosctlInsecure(talosctlPath,
			"version",
			"--nodes", host,
			"--endpoints", host,
		)
		if err == nil {
			color.Green("  ✓ Maintenance-mode endpoint responded (insecure)\n")
			return true
		}
		time.Sleep(wait)
		if wait < 15*time.Second {
			wait += 3 * time.Second
		}
	}
	return false
}

// waitForTalosctlReady polls "talosctl version" (CA-verified) until it
// succeeds.  It tracks port-50000 transitions to detect reboots, retries
// apply-config when Talos stays in maintenance mode too long, and emits
// diagnostic logs on every relevant event.
//
// configPatch is an optional YAML strategic-merge patch applied to every
// apply-config call (e.g. to inject machine.certSANs for cloud instances
// where the public IP is not on any interface).  Pass "" for no patch.
func (b *Bootstrapper) waitForTalosctlReady(talosctlPath, talosConfigFile, host, controlPlaneCfg, configPatch string) error {
	s := spinner.New(spinner.CharSets[14], 200*time.Millisecond)
	s.Suffix = " Waiting for Talos gRPC API (CA-verified)..."
	s.Start()
	defer s.Stop()

	const maxApplyRetries = 5
	// How long port 50000 must be continuously UP (in maintenance mode) before
	// we retry apply-config.  Shorter than before so we don't waste the budget.
	const maintenanceModeRetriggerInterval = 90 * time.Second
	// After sending apply-config, how long we wait for port DOWN before
	// concluding the reboot didn't start and we should retry.
	const waitForRebootTimeout = 2 * time.Minute
	// If the CA-verified check keeps failing with a TLS cert error while port
	// 50000 stays UP, the node is running Talos but with a self-signed cert
	// (Talos v1.12+ workers do not include machine.ca.key so machined cannot
	// issue CA-signed certs).  Accept the node as ready after this soak time.
	const certErrorSoakTime = 5 * time.Minute

	deadline := time.Now().Add(35 * time.Minute)
	wait := 5 * time.Second
	start := time.Now()
	attempt := 0

	// Port-transition tracking.
	var port50kUpSince time.Time  // when port last became UP (cleared on DOWN)
	var port50kDownSince time.Time // when port last became DOWN (cleared on UP)
	prevPort50kUp := true // assume UP since waitForTalosAPI already confirmed it

	// apply-config retry tracking.
	applyRetryCount := 0
	var lastApplySentAt time.Time // when we last sent apply-config

	// Both-ports-down fail-fast tracking.
	bothPortsDownSince := time.Time{}

	// Cert-error soak tracking.  When the CA-verified check fails with a TLS
	// cert error (not a connection error) while port 50000 stays UP, we track
	// how long we've been in that state and declare ready after certErrorSoakTime.
	certErrorSince := time.Time{}

	for time.Now().Before(deadline) {
		attempt++
		elapsed := time.Since(start).Round(time.Second)

		// ── CA-verified talosctl version (30-second timeout) ─────────────────
		// Use exec.CommandContext so the subprocess is killed if it hangs (e.g.
		// gRPC dial never completes when machined is slow to respond). Without a
		// timeout, one hung talosctl call blocks the entire loop iteration and the
		// 35-minute deadline check is never reached.
		versionCtx, versionCancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := b.runTalosctlCtx(versionCtx, talosctlPath, talosConfigFile,
			"version",
			"--nodes", host,
			"--endpoints", host,
		)
		versionCancel()
		if err == nil {
			s.Stop()
			color.Green("  ✓ Talos gRPC API is ready (elapsed %s)\n", elapsed)
			return nil
		}

		// ── Port probes ───────────────────────────────────────────────────────
		port50kUp := tcpProbe(host+":50000", 3*time.Second)
		port22Up := tcpProbe(host+":22", 3*time.Second)

		// ── Port-50000-UP soak ────────────────────────────────────────────────
		// On Talos v1.12+ workers, machine.ca.key is absent so machined cannot
		// issue a CA-signed cert — it generates a self-signed one.  talosctl CA-
		// verified checks therefore always fail for workers.  Additionally the
		// error type is not always a TLS/x509 error: if talosctl's internal gRPC
		// timeout fires before the TLS handshake the error is "context deadline
		// exceeded", which contains no cert keywords.
		//
		// Strategy: if port 50000 stays UP continuously for certErrorSoakTime
		// while talosctl keeps failing (any error), accept the node as ready.
		// certErrorSince is reset ONLY when port 50000 goes DOWN so that brief
		// reboots restart the timer but spurious non-cert errors do not.
		//
		// For the control-plane case, apply-config --insecure succeeds (CP is in
		// maintenance mode) and the CP reboots (port goes DOWN), which resets
		// certErrorSince before certErrorSoakTime elapses.  For workers in
		// configured mode, apply-config --insecure fails and the port stays UP.
		if port50kUp {
			if certErrorSince.IsZero() {
				certErrorSince = time.Now()
			}
			soakElapsed := time.Since(certErrorSince)
			if soakElapsed >= certErrorSoakTime {
				s.Stop()
				errKind := "connection error"
				if isCertError(err) {
					errKind = "cert error"
				}
				color.Yellow("  ⚠ talosctl CA-verified check has failed (%s) for %s with "+
					"port 50000 continuously UP — node is running Talos (elapsed %s)\n",
					errKind, soakElapsed.Round(time.Second), elapsed)
				color.Yellow("  Treating as ready (worker joins cluster via embedded token)\n")
				return nil
			}
		} else {
			certErrorSince = time.Time{} // reset only when port 50000 goes DOWN
		}

		// ── Track port-50000 transitions (detect reboots) ─────────────────────
		justCameUp := port50kUp && !prevPort50kUp // captured before prevPort50kUp is updated
		if port50kUp && !prevPort50kUp {
			// Port came back UP after a DOWN period.
			downDur := ""
			if !port50kDownSince.IsZero() {
				downDur = fmt.Sprintf(" (was down for %s)", time.Since(port50kDownSince).Round(time.Second))
			}
			s.Stop()
			color.Yellow("  [%s] Port 50000 came back UP%s\n", elapsed, downDur)
			s.Start()
			port50kUpSince = time.Now()
			port50kDownSince = time.Time{}
		} else if !port50kUp && prevPort50kUp {
			// Port went DOWN — machine is rebooting.
			s.Stop()
			color.Green("  [%s] Port 50000 went DOWN — hardware reboot in progress\n", elapsed)
			s.Start()
			port50kDownSince = time.Now()
			port50kUpSince = time.Time{}
		}
		prevPort50kUp = port50kUp


		// Initialise upSince on first iteration if port is already UP.
		if port50kUp && port50kUpSince.IsZero() {
			port50kUpSince = time.Now()
		}

		// ── Log every attempt (first 5, then every 3rd) ───────────────────────
		if attempt <= 5 || attempt%3 == 0 {
			s.Stop()
			var portState string
			switch {
			case port50kUp && port22Up:
				portState = "50000↑ 22↑ (both open)"
			case port50kUp:
				portState = "50000↑ (maintenance/cert-mismatch)"
			case port22Up:
				portState = "22↑ (Ubuntu is running!)"
			default:
				portState = "50000↓ 22↓ (rebooting/stuck)"
			}
			color.Yellow("  [attempt %d, %s] ports: %s\n    error: %v\n",
				attempt, elapsed, portState, summariseError(err))
			s.Start()
		}

		// ── Fail-fast: Ubuntu SSH is responding ───────────────────────────────
		if port22Up && time.Since(start) > 3*time.Minute {
			s.Stop()
			color.Yellow("\n  ⚠  Port 22 (SSH) is responding — machine rebooted into Ubuntu!\n")
			return fmt.Errorf(
				"machine at %s booted back into Ubuntu (port 22 UP after %s)",
				host, elapsed,
			)
		}

		// ── Fail-fast: both ports down for >10 minutes ────────────────────────
		if !port50kUp && !port22Up {
			if bothPortsDownSince.IsZero() {
				bothPortsDownSince = time.Now()
			} else if time.Since(bothPortsDownSince) > 10*time.Minute {
				s.Stop()
				return fmt.Errorf(
					"machine at %s unreachable on both port 22 and port 50000 "+
						"for >10 minutes (total elapsed %s) — stuck in UEFI or GRUB rescue",
					host, elapsed,
				)
			}
		} else {
			bothPortsDownSince = time.Time{}
		}

		// ── Maintenance-mode retry: re-send apply-config ──────────────────────
		// Conditions:
		//   - Port 50000 is UP (maintenance or cert-mismatch)
		//   - We have retries remaining
		//   - Either: port has been continuously UP for >retriggerInterval
		//             OR: port just came back UP after a reboot (was DOWN, now UP)
		//             AND enough time has passed since last apply (waitForRebootTimeout)
		retriggerDue := port50kUp &&
			!port50kUpSince.IsZero() &&
			time.Since(port50kUpSince) > maintenanceModeRetriggerInterval

		// Also retrigger if the port just came back UP after a DOWN period,
		// meaning the previous apply-config DID trigger a reboot but Talos is
		// still in maintenance mode — the config was not persisted.
		rebootedToMaintenance := justCameUp

		shouldRetry := applyRetryCount < maxApplyRetries &&
			(retriggerDue || rebootedToMaintenance) &&
			(lastApplySentAt.IsZero() || time.Since(lastApplySentAt) > waitForRebootTimeout)

		if shouldRetry {
			applyRetryCount++
			lastApplySentAt = time.Now()
			port50kUpSince = time.Now() // reset maintenance-mode timer

			s.Stop()
			if rebootedToMaintenance {
				color.Yellow("  [%s] Talos returned to maintenance mode after reboot — config not persisted?\n", elapsed)
			}
			color.Yellow("  [%s] Sending apply-config --insecure (attempt %d/%d, port has been UP for %s)\n",
				elapsed, applyRetryCount, maxApplyRetries,
				func() string {
					if port50kUpSince.IsZero() {
						return "unknown"
					}
					return time.Since(port50kUpSince).Round(time.Second).String()
				}())

			applyArgs := []string{
				"apply-config",
				"--nodes", host,
				"--endpoints", host,
				"--file", controlPlaneCfg,
			}
			if configPatch != "" {
				applyArgs = append(applyArgs, "--config-patch", configPatch)
			}
			applyOut, applyErr := b.runTalosctlInsecureWithOutput(talosctlPath, applyArgs...)
			if applyErr != nil {
				color.Yellow("  apply-config retry %d (insecure, no talosconfig) FAILED: %v\n",
					applyRetryCount, summariseError(applyErr))
				if applyOut != "" {
					color.Yellow("  apply-config output: %s\n", applyOut)
				}
				// Insecure-only attempt failed (machine may be in configured mode
				// requiring mTLS).  Try again with talosconfig (provides client cert)
				// but --insecure (skip server cert verification, works around SANs).
				if talosConfigFile != "" {
					tcArgs := make([]string, len(applyArgs))
					copy(tcArgs, applyArgs)
					// Insert --insecure after the subcommand name.
					insecureArgs := append([]string{tcArgs[0], "--insecure"}, tcArgs[1:]...)
					tcOut, tcErr := b.runTalosctlWithOutput(talosctlPath, talosConfigFile, insecureArgs...)
					if tcErr != nil {
						color.Yellow("  apply-config retry %d (talosconfig+insecure) FAILED: %v\n",
							applyRetryCount, summariseError(tcErr))
						if tcOut != "" {
							color.Yellow("  apply-config output: %s\n", tcOut)
						}
					} else {
						color.Green("  ✓ apply-config retry %d (talosconfig+insecure) succeeded — waiting for reboot\n",
							applyRetryCount)
						applyErr = nil
					}
				}
			} else {
				color.Green("  ✓ apply-config retry %d succeeded — waiting for reboot\n", applyRetryCount)
				if applyOut != "" {
					fmt.Printf("  apply-config output: %s\n", applyOut)
				}
				// Wait for the port to go DOWN (confirming reboot started).
				// Non-blocking: we'll track the transition in the next iteration.
			}
			s.Start()
		}

		time.Sleep(wait)
		if wait < 30*time.Second {
			wait *= 2
		}
	}

	s.Stop()
	return fmt.Errorf("Talos gRPC API at %s did not become ready within 35 minutes "+
		"(%d apply-config retries sent)", host, applyRetryCount)
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
	color.Yellow("  Warning: health check timed out. Attempting kubeconfig retrieval anyway.\n")
	return nil
}

// tcpProbe returns true if host:port accepts a TCP connection within timeout.
func tcpProbe(addr string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// isCertError returns true if the error is a TLS certificate validation failure
// (as opposed to a connection error or timeout).  A cert error means the remote
// end IS up and responding — only the certificate verification failed.
func isCertError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "x509") ||
		strings.Contains(msg, "certificate") ||
		strings.Contains(msg, "tls: ")
}

// summariseError returns a compact one-line representation of the error.
// It strips the repeated command prefix that talosctl prepends to keep logs readable.
func summariseError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	// Keep it short: first 200 chars is enough for diagnosis.
	if len(msg) > 200 {
		msg = msg[:200] + "…"
	}
	return msg
}

func (b *Bootstrapper) runTalosctl(binary, talosconfig string, args ...string) error {
	_, err := b.runTalosctlWithOutput(binary, talosconfig, args...)
	return err
}

// runTalosctlCtx runs talosctl with a context (for timeout/cancellation).
// Used for the readiness-check loop so a hanging subprocess is killed rather
// than blocking the entire loop iteration indefinitely.
func (b *Bootstrapper) runTalosctlCtx(ctx context.Context, binary, talosconfig string, args ...string) error {
	allArgs := append([]string{"--talosconfig", talosconfig}, args...)
	cmd := exec.CommandContext(ctx, binary, allArgs...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		combined := strings.TrimSpace(stdout.String() + "\n" + stderr.String())
		return fmt.Errorf("%w\n%s", err, combined)
	}
	return nil
}

// runTalosctlWithOutput runs talosctl and returns (stdout+stderr, error).
func (b *Bootstrapper) runTalosctlWithOutput(binary, talosconfig string, args ...string) (string, error) {
	allArgs := append([]string{"--talosconfig", talosconfig}, args...)
	if b.verbose {
		fmt.Fprintf(os.Stderr, "\n[talosctl] $ %s %s\n", binary, strings.Join(allArgs, " "))
	}
	cmd := exec.Command(binary, allArgs...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		combined := strings.TrimSpace(stdout.String() + "\n" + stderr.String())
		return combined, fmt.Errorf("%w\n%s", err, combined)
	}

	out := strings.TrimSpace(stdout.String())

	// Write stdout to the target file if this is a kubeconfig command.
	for i, arg := range args {
		if arg == "kubeconfig" && i < len(args)-1 {
			outFile := args[len(args)-1]
			if stdout.Len() > 0 {
				os.WriteFile(outFile, stdout.Bytes(), 0600) //nolint:errcheck
			}
			break
		}
	}

	return out, nil
}

// runTalosctlInsecureWithOutput runs talosctl with --insecure and returns
// (combined output, error).  Used for apply-config in maintenance mode.
//
// --insecure is a subcommand-level flag in talosctl v1.12+, so it must be
// placed after the subcommand name (args[0]), not as a global flag.
//
// TALOSCONFIG is set to /dev/null so talosctl cannot load ~/.talos/config or
// any other default config.  Credentials from a previous migration would
// cause the TLS handshake to fail in maintenance mode.
func (b *Bootstrapper) runTalosctlInsecureWithOutput(binary string, args ...string) (string, error) {
	// Insert --insecure after the subcommand name (first arg).
	var allArgs []string
	if len(args) > 0 {
		allArgs = append(append([]string{args[0], "--insecure"}, args[1:]...))
	} else {
		allArgs = []string{"--insecure"}
	}
	if b.verbose {
		fmt.Fprintf(os.Stderr, "\n[talosctl] $ TALOSCONFIG=/dev/null %s %s\n", binary, strings.Join(allArgs, " "))
	}
	cmd := exec.Command(binary, allArgs...)
	cmd.Env = append(os.Environ(), "TALOSCONFIG=/dev/null")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		combined := strings.TrimSpace(stdout.String() + "\n" + stderr.String())
		return combined, fmt.Errorf("%w\n%s", err, combined)
	}
	return strings.TrimSpace(stdout.String()), nil
}

// runTalosctlInsecure runs talosctl with --insecure (no TLS cert verification,
// no client-cert auth).  Used to probe the maintenance-mode endpoint.
//
// --insecure is a subcommand-level flag in talosctl v1.12+, so it must be
// placed after the subcommand name (args[0]), not as a global flag.
//
// TALOSCONFIG is set to /dev/null so talosctl cannot load ~/.talos/config or
// any other default config.  Credentials from a previous migration would
// cause the TLS handshake to fail in maintenance mode, making the probe
// return false even when the node is genuinely in maintenance mode.
func (b *Bootstrapper) runTalosctlInsecure(binary string, args ...string) error {
	var allArgs []string
	if len(args) > 0 {
		allArgs = append(append([]string{args[0], "--insecure"}, args[1:]...))
	} else {
		allArgs = []string{"--insecure"}
	}
	cmd := exec.Command(binary, allArgs...)
	cmd.Env = append(os.Environ(), "TALOSCONFIG=/dev/null")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w\n%s", err, strings.TrimSpace(stdout.String()+"\n"+stderr.String()))
	}
	return nil
}
