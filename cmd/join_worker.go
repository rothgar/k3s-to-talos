package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/rothgar/k3s-to-talos/internal/nextboot"
	"github.com/rothgar/k3s-to-talos/internal/ssh"
	"github.com/rothgar/k3s-to-talos/internal/talos"
	"github.com/spf13/cobra"
	sigyaml "sigs.k8s.io/yaml"
)

var (
	flagWorkerTalosVersion string
	flagWorkerConfig       string
	flagTalosConfig        string
)

var joinWorkerCmd = &cobra.Command{
	Use:   "join-worker [[user@]host]",
	Short: "Convert a k3s agent node to a Talos worker and join an existing Talos cluster",
	Long: `Installs Talos on a k3s agent (worker) node and joins it to an existing
Talos cluster that was previously migrated with the 'migrate' command.

The --worker-config and --talosconfig flags must point to files generated
during the control plane migration (in <backup-dir>/talos-config/).

Steps:
  1. SSH into the worker node and run the nextboot agent (erases the OS,
     writes the Talos disk image, reboots)
  2. Wait for the Talos API on port 50000
  3. Apply the worker configuration (insecure, maintenance mode)
  4. Wait for the CA-verified gRPC API — the worker joins the cluster
     automatically via the cluster token embedded in worker.yaml

This process is IRREVERSIBLE. The worker node's OS will be erased.`,
	RunE: runJoinWorker,
}

func init() {
	joinWorkerCmd.Flags().StringVar(&flagWorkerTalosVersion, "talos-version", "v1.12.6", "Talos Linux version to install")
	joinWorkerCmd.Flags().StringVar(&flagWorkerConfig, "worker-config", "", "Path to worker.yaml from the control plane migration (required)")
	joinWorkerCmd.Flags().StringVar(&flagTalosConfig, "talosconfig", "", "Path to talosconfig from the control plane migration (required)")
	_ = joinWorkerCmd.MarkFlagRequired("worker-config")
	_ = joinWorkerCmd.MarkFlagRequired("talosconfig")
}

func runJoinWorker(cmd *cobra.Command, args []string) error {
	target := resolveTarget(args)
	if target == "" {
		return fmt.Errorf("SSH target is required: k2t join-worker [user@]host")
	}
	host := sshOpts(target).Host

	if err := os.MkdirAll(flagBackupDir, 0750); err != nil {
		return fmt.Errorf("creating backup directory: %w", err)
	}

	color.Blue("\n══ Joining worker node %s to Talos cluster ══\n\n", host)

	// ── Phase 1: Deploy Talos via nextboot ───────────────────────────────────
	color.Blue("[1/2] Deploying Talos to worker node via nextboot agent\n")

	sshClient, err := ssh.NewClient(sshOpts(target))
	if err != nil {
		return fmt.Errorf("SSH connection to worker failed: %w", err)
	}

	// Patch worker.yaml with machine.certSANs=[host] BEFORE uploading it.
	// The nextboot agent writes the config to the Talos STATE partition so the
	// machine boots in CONFIGURED mode (not maintenance mode).  If certSANs are
	// not already in the config, machined's TLS cert won't include the public IP
	// and every CA-verified talosctl call via the public IP will fail with an
	// x509 SAN mismatch.
	workerCfgToUpload, cleanCfg, patchErr := patchWorkerConfigCertSANs(filepath.Clean(flagWorkerConfig), host)
	if patchErr != nil {
		color.Yellow("  Warning: could not patch worker.yaml with certSANs: %v\n", patchErr)
		color.Yellow("  Using unpatched worker.yaml — CA-verified talosctl may fail if public IP is not in SANs.\n")
		workerCfgToUpload = filepath.Clean(flagWorkerConfig)
		cleanCfg = func() {}
	} else {
		fmt.Printf("  ✓ worker.yaml patched with machine.certSANs=[%s]\n", host)
	}

	installer := nextboot.NewInstaller(sshClient, flagBackupDir)
	installErr := installer.Run(nextboot.Options{
		TalosVersion: flagWorkerTalosVersion,
		ConfigFile:   workerCfgToUpload,
	})
	cleanCfg()
	sshClient.Close()

	if installErr != nil && !ssh.IsDisconnectError(installErr) {
		return fmt.Errorf("nextboot on worker failed: %w", installErr)
	}
	if installErr != nil {
		color.Yellow("SSH connection closed (worker is rebooting) — this is expected.\n")
	}

	// ── Phase 2: Bootstrap worker Talos ──────────────────────────────────────
	color.Blue("\n[2/2] Waiting for worker Talos to boot and join the cluster\n")

	bootstrapper := talos.NewBootstrapper(flagBackupDir)
	if err := bootstrapper.BootstrapWorker(talos.WorkerBootstrapOptions{
		Host:            host,
		TalosConfigFile: filepath.Clean(flagTalosConfig),
		WorkerCfgFile:   filepath.Clean(flagWorkerConfig),
		Verbose:         flagVerbose,
	}); err != nil {
		return fmt.Errorf("bootstrapping worker: %w", err)
	}

	color.Green("\n✓ Worker node %s is now running Talos and has joined the cluster.\n", host)
	fmt.Printf("\nVerify with:\n  talosctl --talosconfig %s get members\n", flagTalosConfig)
	return nil
}

// patchWorkerConfigCertSANs adds host to machine.certSANs in the worker
// machine config, writes the result to a temp file, and returns
// (tempPath, cleanupFn, error).
//
// The temp file is needed because the nextboot agent writes whatever config it
// receives to the Talos STATE partition before the hardware reboot.  When Talos
// boots it reads the config from STATE and uses it to generate machined's TLS
// cert — so certSANs must be present in the file written to STATE, not injected
// later via apply-config (which is skipped when Talos boots in configured mode).
//
// Strategy:
//   1. Try "talosctl machineconfig patch" (uses the official Talos config parser)
//   2. Fall back to sigs.k8s.io/yaml round-trip (pure-Go, no external tool)
//
// Both approaches are verified: the output file is re-parsed to confirm host
// appears in machine.certSANs before returning.
func patchWorkerConfigCertSANs(cfgPath, host string) (string, func(), error) {
	tmp, err := os.CreateTemp("", "worker-patched-*.yaml")
	if err != nil {
		return "", func() {}, fmt.Errorf("creating temp config: %w", err)
	}
	tmp.Close()
	cleanup := func() { os.Remove(tmp.Name()) }

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("reading worker config: %w", err)
	}

	var cfg map[string]interface{}
	if err := sigyaml.Unmarshal(data, &cfg); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("parsing worker config: %w", err)
	}

	machine, _ := cfg["machine"].(map[string]interface{})
	if machine == nil {
		machine = make(map[string]interface{})
		cfg["machine"] = machine
	}

	// Build SANs list, deduplicating if host is already present.
	var sans []interface{}
	if existing, ok := machine["certSANs"].([]interface{}); ok {
		for _, e := range existing {
			if s, _ := e.(string); s == host {
				// Already present — use original file unchanged.
				fmt.Printf("  ✓ certSANs=[%s] already present in worker.yaml\n", host)
				cleanup()
				return cfgPath, func() {}, nil
			}
			sans = append(sans, e)
		}
	}
	sans = append(sans, host)
	machine["certSANs"] = sans

	patched, err := sigyaml.Marshal(cfg)
	if err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("marshaling patched worker config: %w", err)
	}

	if err := os.WriteFile(tmp.Name(), patched, 0600); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("writing patched config: %w", err)
	}

	if err := verifyCertSANs(tmp.Name(), host); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("yaml round-trip failed to inject certSANs: %w", err)
	}

	fmt.Printf("  ✓ worker.yaml patched via yaml round-trip (certSANs=[%s])\n", host)
	return tmp.Name(), cleanup, nil
}

// verifyCertSANs re-parses the YAML at path and confirms host is present in
// machine.certSANs.
func verifyCertSANs(path, host string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading patched config: %w", err)
	}
	var cfg map[string]interface{}
	if err := sigyaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parsing patched config: %w", err)
	}
	machine, _ := cfg["machine"].(map[string]interface{})
	if machine == nil {
		return fmt.Errorf("machine section missing")
	}
	sans, _ := machine["certSANs"].([]interface{})
	for _, s := range sans {
		if str, _ := s.(string); str == host {
			return nil
		}
	}
	return fmt.Errorf("host %s not found in certSANs %v", host, sans)
}
