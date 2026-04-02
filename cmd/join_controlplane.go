package cmd

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/rothgar/k2t/internal/nextboot"
	"github.com/rothgar/k2t/internal/ssh"
	"github.com/rothgar/k2t/internal/talos"
	"github.com/spf13/cobra"
)

var (
	flagCPTalosVersion    string
	flagCPControlPlaneConfig string
	flagCPTalosConfig     string
	flagCPSkipHealthCheck bool
)

var joinControlPlaneCmd = &cobra.Command{
	Use:   "join-controlplane [[user@]host]",
	Short: "Convert a node to a Talos control plane and join an existing Talos cluster",
	Long: `Installs Talos on a node and joins it as an additional control plane member
to an existing Talos cluster that was previously migrated with the 'migrate'
command.

The --controlplane-config and --talosconfig flags must point to files generated
during the initial control plane migration (in <backup-dir>/talos-config/).

Steps:
  1. Validate the existing Talos cluster is healthy (talosctl health)
  2. SSH into the node and run the nextboot agent (erases the OS, writes
     the Talos disk image, reboots)
  3. Wait for Talos to boot and apply the control plane configuration
  4. The node automatically joins etcd and becomes a control plane member

No etcd restore or bootstrap is needed — the new control plane node
discovers the existing cluster via the token embedded in controlplane.yaml.

This process is IRREVERSIBLE. The target node's OS will be erased.`,
	RunE: runJoinControlPlane,
}

func init() {
	joinControlPlaneCmd.Flags().StringVar(&flagCPTalosVersion, "talos-version", "v1.12.6", "Talos Linux version to install")
	joinControlPlaneCmd.Flags().StringVar(&flagCPControlPlaneConfig, "controlplane-config", "", "Path to controlplane.yaml from the initial migration (required)")
	joinControlPlaneCmd.Flags().StringVar(&flagCPTalosConfig, "talosconfig", "", "Path to talosconfig from the initial migration (required)")
	joinControlPlaneCmd.Flags().BoolVar(&flagCPSkipHealthCheck, "skip-health-check", false, "Skip validating the existing cluster is healthy before joining")
	_ = joinControlPlaneCmd.MarkFlagRequired("controlplane-config")
	_ = joinControlPlaneCmd.MarkFlagRequired("talosconfig")
}

func runJoinControlPlane(cmd *cobra.Command, args []string) error {
	target := resolveTarget(args)
	if target == "" {
		return fmt.Errorf("SSH target is required: k2t join-controlplane [user@]host")
	}
	host := sshOpts(target).Host

	if err := os.MkdirAll(flagBackupDir, 0750); err != nil {
		return fmt.Errorf("creating backup directory: %w", err)
	}

	talosConfigPath := filepath.Clean(flagCPTalosConfig)
	cpConfigPath := filepath.Clean(flagCPControlPlaneConfig)

	// ── Phase 0: Validate existing cluster ────────────────────────────────
	if !flagCPSkipHealthCheck {
		color.Blue("\n══ Joining control plane node %s to Talos cluster ══\n\n", host)
		color.Blue("[0/3] Validating existing Talos cluster is healthy\n")

		if err := validateClusterHealth(talosConfigPath); err != nil {
			return fmt.Errorf("existing cluster is not healthy: %w\n\nUse --skip-health-check to bypass this check", err)
		}
		color.Green("  ✓ Existing cluster is healthy\n")
	} else {
		color.Blue("\n══ Joining control plane node %s to Talos cluster ══\n\n", host)
		color.Yellow("  Skipping cluster health check (--skip-health-check)\n")
	}

	// ── Phase 1: Deploy Talos via nextboot ────────────────────────────────
	color.Blue("\n[1/3] Deploying Talos to control plane node via nextboot agent\n")

	sshClient, err := ssh.NewClient(sshOpts(target))
	if err != nil {
		return fmt.Errorf("SSH connection failed: %w", err)
	}

	// Patch controlplane.yaml with machine.certSANs=[host].
	cfgToUpload, cleanCfg, patchErr := patchConfigCertSANs(cpConfigPath, host)
	if patchErr != nil {
		color.Yellow("  Warning: could not patch controlplane.yaml with certSANs: %v\n", patchErr)
		color.Yellow("  Using unpatched config — CA-verified talosctl may fail if public IP is not in SANs.\n")
		cfgToUpload = cpConfigPath
		cleanCfg = func() {}
	} else {
		fmt.Printf("  ✓ controlplane.yaml patched with machine.certSANs=[%s]\n", host)
	}

	installer := nextboot.NewInstaller(sshClient, flagBackupDir)
	installErr := installer.Run(nextboot.Options{
		TalosVersion: flagCPTalosVersion,
		ConfigFile:   cfgToUpload,
	})
	cleanCfg()
	sshClient.Close()

	if installErr != nil && !ssh.IsDisconnectError(installErr) {
		return fmt.Errorf("nextboot on control plane node failed: %w", installErr)
	}
	if installErr != nil {
		color.Yellow("SSH connection closed (node is rebooting) — this is expected.\n")
	}

	// ── Phase 2: Wait for Talos to boot and apply config ─────────────────
	color.Blue("\n[2/3] Waiting for Talos to boot on new control plane node\n")

	certSANsPatch := fmt.Sprintf("machine:\n  certSANs:\n    - %q\n", host)
	bootstrapper := talos.NewBootstrapper(flagBackupDir)
	if err := bootstrapper.BootstrapControlPlane(talos.ControlPlaneBootstrapOptions{
		Host:               host,
		TalosConfigFile:    talosConfigPath,
		ControlPlaneCfgFile: cpConfigPath,
		CertSANsPatch:      certSANsPatch,
		Verbose:            flagVerbose,
	}); err != nil {
		return fmt.Errorf("bootstrapping control plane node: %w", err)
	}

	// ── Phase 3: Verify membership ──────────────────────────────────────
	color.Blue("\n[3/3] Verifying control plane membership\n")

	if err := waitForControlPlaneMembership(talosConfigPath, host); err != nil {
		color.Yellow("  Warning: could not verify membership: %v\n", err)
		color.Yellow("  The node may still be joining — check with:\n")
		fmt.Printf("    talosctl --talosconfig %s get members\n", talosConfigPath)
	} else {
		color.Green("  ✓ Node %s is a control plane member\n", host)
	}

	color.Green("\n✓ Control plane node %s is now running Talos and has joined the cluster.\n", host)
	fmt.Printf("\nVerify with:\n")
	fmt.Printf("  talosctl --talosconfig %s get members\n", talosConfigPath)
	fmt.Printf("  kubectl get nodes\n")
	return nil
}

// validateClusterHealth runs talosctl health against the existing cluster
// endpoints from talosconfig.
func validateClusterHealth(talosConfigPath string) error {
	talosctlPath, err := exec.LookPath("talosctl")
	if err != nil {
		return fmt.Errorf("talosctl not found in PATH")
	}

	fmt.Println("  Running talosctl health (timeout 60s)...")
	cmd := exec.Command(talosctlPath,
		"--talosconfig", talosConfigPath,
		"health",
		"--wait-timeout", "60s",
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		combined := strings.TrimSpace(stdout.String() + "\n" + stderr.String())
		return fmt.Errorf("talosctl health failed: %w\n%s", err, combined)
	}
	return nil
}

// waitForControlPlaneMembership polls talosctl get members until the new host
// appears as a control plane member.
func waitForControlPlaneMembership(talosConfigPath, host string) error {
	talosctlPath, err := exec.LookPath("talosctl")
	if err != nil {
		return fmt.Errorf("talosctl not found in PATH")
	}

	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		cmd := exec.Command(talosctlPath,
			"--talosconfig", talosConfigPath,
			"get", "members",
		)
		out, err := cmd.CombinedOutput()
		if err == nil && strings.Contains(string(out), host) {
			return nil
		}
		time.Sleep(10 * time.Second)
	}
	return fmt.Errorf("node %s did not appear in members within 5 minutes", host)
}

// patchConfigCertSANs adds host to machine.certSANs in any Talos machine
// config file. Reuses the same logic as patchWorkerConfigCertSANs.
func patchConfigCertSANs(cfgPath, host string) (string, func(), error) {
	return patchWorkerConfigCertSANs(cfgPath, host)
}
