package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/rothgar/k3s-to-talos/internal/k3s"
	"github.com/rothgar/k3s-to-talos/internal/nextboot"
	"github.com/rothgar/k3s-to-talos/internal/ssh"
	"github.com/rothgar/k3s-to-talos/internal/talos"
	"github.com/rothgar/k3s-to-talos/internal/ui"
	"github.com/spf13/cobra"
)

var (
	flagTalosVersion string
	flagClusterName  string
	flagDryRun       bool
	flagResume       bool
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate a k3s server node to Talos Linux (full flow)",
	Long: `Performs the full migration of a k3s server node to Talos Linux.

Steps:
  1. COLLECT   - Connect via SSH, collect cluster info, back up k3s DB and resources
  2. CONFIRM   - Display summary, show warnings, require typed confirmation
  3. GENERATE  - Run talosctl gen config locally
  4. DEPLOY    - Upload nextboot-talos, configure, and reboot into Talos
  5. BOOTSTRAP - Wait for Talos to boot, apply config, bootstrap Kubernetes

This process is IRREVERSIBLE. The target machine's OS will be erased.`,
	RunE: runMigrate,
}

func init() {
	migrateCmd.Flags().StringVar(&flagTalosVersion, "talos-version", "v1.7.0", "Talos Linux version to install")
	migrateCmd.Flags().StringVar(&flagClusterName, "cluster-name", "", "Name for the Talos cluster (defaults to the k3s cluster name or 'talos-cluster')")
	migrateCmd.Flags().BoolVar(&flagDryRun, "dry-run", false, "Collect info and show what would happen, but do not modify the remote machine")
	migrateCmd.Flags().BoolVar(&flagResume, "resume", false, "Resume a previously interrupted migration from the last completed phase")
}

func runMigrate(cmd *cobra.Command, args []string) error {
	if flagHost == "" {
		return fmt.Errorf("--host is required")
	}

	if err := os.MkdirAll(flagBackupDir, 0750); err != nil {
		return fmt.Errorf("creating backup directory: %w", err)
	}

	stateFile := filepath.Join(flagBackupDir, "migration-state.json")
	state, err := loadOrInitState(stateFile, flagHost)
	if err != nil {
		return err
	}

	if flagDryRun {
		color.Yellow("DRY RUN MODE: No changes will be made to the remote machine.\n\n")
	}

	// ─── Phase 1: COLLECT ────────────────────────────────────────────────────
	if !state.PhaseCompleted("COLLECT") || !flagResume {
		ui.PrintPhaseHeader(1, "COLLECT", "Connecting to k3s node and backing up cluster state")

		sshClient, err := ssh.NewClient(ssh.Options{
			Host:    flagHost,
			Port:    flagSSHPort,
			User:    flagSSHUser,
			KeyPath: flagSSHKey,
			Sudo:    flagSudo,
		})
		if err != nil {
			return fmt.Errorf("SSH connection failed: %w", err)
		}
		defer sshClient.Close()

		collector, err := k3s.Detect(sshClient)
		if err != nil {
			return fmt.Errorf("detecting cluster type: %w", err)
		}
		info, err := collector.Collect()
		if err != nil {
			return fmt.Errorf("collecting k3s info: %w", err)
		}

		backup := k3s.NewBackup(sshClient, flagBackupDir, flagHost)
		if err := backup.Run(info, flagDryRun); err != nil {
			return fmt.Errorf("backing up k3s data: %w", err)
		}

		state.ClusterInfo = info
		state.MarkPhaseComplete("COLLECT")
		if err := state.Save(stateFile); err != nil {
			return fmt.Errorf("saving state: %w", err)
		}
	} else {
		ui.PrintPhaseSkipped(1, "COLLECT", "already completed")
	}

	// ─── Phase 2: CONFIRM ────────────────────────────────────────────────────
	ui.PrintPhaseHeader(2, "CONFIRM", "Review collected information and confirm migration")

	ui.PrintClusterSummary(state.ClusterInfo, flagBackupDir)

	if len(state.ClusterInfo.Nodes) > 1 {
		ui.PrintMultiNodeWarning(state.ClusterInfo.Nodes)
	}

	if state.ClusterInfo.Hardware != nil && state.ClusterInfo.Hardware.IsRaspberryPi {
		ui.PrintRaspberryPiWarning(state.ClusterInfo.Hardware)
	}

	ui.PrintIrreversibilityWarning(flagHost)

	if !flagDryRun {
		if err := ui.ConfirmErase(flagHost); err != nil {
			return err
		}
	} else {
		color.Yellow("[DRY RUN] Skipping confirmation prompt.\n")
	}

	// ─── Phase 3: GENERATE ───────────────────────────────────────────────────
	if !state.PhaseCompleted("GENERATE") || !flagResume {
		ui.PrintPhaseHeader(3, "GENERATE", "Generating Talos machine configuration")

		clusterName := flagClusterName
		if clusterName == "" {
			clusterName = state.ClusterInfo.ClusterName
			if clusterName == "" {
				clusterName = "talos-cluster"
			}
		}

		talosConfigDir := filepath.Join(flagBackupDir, "talos-config")
		gen := talos.NewConfigGenerator(flagBackupDir)
		if err := gen.Generate(talos.GenerateOptions{
			ClusterName:    clusterName,
			ControlPlaneIP: flagHost,
			TalosVersion:   flagTalosVersion,
			OutputDir:      talosConfigDir,
			DryRun:         flagDryRun,
		}); err != nil {
			return fmt.Errorf("generating Talos config: %w", err)
		}

		state.TalosConfigDir = talosConfigDir
		state.ClusterName = clusterName
		state.MarkPhaseComplete("GENERATE")
		if err := state.Save(stateFile); err != nil {
			return fmt.Errorf("saving state: %w", err)
		}
	} else {
		ui.PrintPhaseSkipped(3, "GENERATE", "already completed")
	}

	if flagDryRun {
		color.Green("\n[DRY RUN] Migration simulation complete. No changes were made.\n")
		color.White("  Backup dir:       %s\n", flagBackupDir)
		color.White("  Talos config dir: %s\n", filepath.Join(flagBackupDir, "talos-config"))
		return nil
	}

	// ─── Phase 4: DEPLOY ─────────────────────────────────────────────────────
	if !state.PhaseCompleted("DEPLOY") || !flagResume {
		ui.PrintPhaseHeader(4, "DEPLOY", "Installing nextboot-talos and rebooting into Talos")

		sshClient, err := ssh.NewClient(ssh.Options{
			Host:    flagHost,
			Port:    flagSSHPort,
			User:    flagSSHUser,
			KeyPath: flagSSHKey,
			Sudo:    flagSudo,
		})
		if err != nil {
			return fmt.Errorf("reconnecting via SSH: %w", err)
		}

		installer := nextboot.NewInstaller(sshClient, flagBackupDir)
		if err := installer.Run(nextboot.Options{
			TalosVersion:   flagTalosVersion,
			ControlPlaneIP: flagHost,
			ConfigFile:     filepath.Join(state.TalosConfigDir, "controlplane.yaml"),
			Hardware:       state.ClusterInfo.Hardware,
		}); err != nil {
			// SSH will drop when the machine reboots — that's expected
			if !ssh.IsDisconnectError(err) {
				return fmt.Errorf("running nextboot-talos: %w", err)
			}
			color.Yellow("SSH connection closed (machine is rebooting) — this is expected.\n")
		}
		sshClient.Close()

		state.MarkPhaseComplete("DEPLOY")
		if err := state.Save(stateFile); err != nil {
			return fmt.Errorf("saving state: %w", err)
		}
	} else {
		ui.PrintPhaseSkipped(4, "DEPLOY", "already completed")
	}

	// ─── Phase 5: BOOTSTRAP ──────────────────────────────────────────────────
	if !state.PhaseCompleted("BOOTSTRAP") || !flagResume {
		ui.PrintPhaseHeader(5, "BOOTSTRAP", "Waiting for Talos to boot and bootstrapping Kubernetes")

		talosConfigFile := filepath.Join(state.TalosConfigDir, "talosconfig")
		controlPlaneCfg := filepath.Join(state.TalosConfigDir, "controlplane.yaml")
		kubeconfigOut := filepath.Join(flagBackupDir, "talos-kubeconfig")

		bootstrapper := talos.NewBootstrapper(flagBackupDir)
		if err := bootstrapper.Bootstrap(talos.BootstrapOptions{
			Host:            flagHost,
			TalosConfigFile: talosConfigFile,
			ControlPlaneCfg: controlPlaneCfg,
			KubeconfigOut:   kubeconfigOut,
		}); err != nil {
			return fmt.Errorf("bootstrapping Talos cluster: %w", err)
		}

		state.KubeconfigPath = kubeconfigOut
		state.MarkPhaseComplete("BOOTSTRAP")
		if err := state.Save(stateFile); err != nil {
			return fmt.Errorf("saving state: %w", err)
		}
	} else {
		ui.PrintPhaseSkipped(5, "BOOTSTRAP", "already completed")
	}

	// ─── Done ────────────────────────────────────────────────────────────────
	printMigrationSuccess(state)
	return nil
}

func printMigrationSuccess(state *MigrationState) {
	bold := color.New(color.Bold)
	green := color.New(color.FgGreen, color.Bold)

	fmt.Println()
	green.Println("╔══════════════════════════════════════════════════════════════╗")
	green.Println("║          MIGRATION COMPLETE — TALOS IS RUNNING               ║")
	green.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	bold.Println("Next steps:")
	fmt.Printf("  1. Access the cluster:\n")
	fmt.Printf("       export KUBECONFIG=%s\n", state.KubeconfigPath)
	fmt.Printf("       kubectl get nodes\n\n")
	fmt.Printf("  2. Restore workloads from backup:\n")
	fmt.Printf("       kubectl apply -f %s/resources/\n\n", state.BackupDir)
	fmt.Printf("  3. Check persistent volume data — PV data was NOT migrated\n")
	fmt.Printf("     automatically. Refer to your backup strategy.\n\n")

	if len(state.ClusterInfo.Nodes) > 1 {
		color.Yellow("  4. This was a multi-node cluster. Migrate each remaining node:\n")
		for _, node := range state.ClusterInfo.Nodes {
			if node.IsControlPlane {
				continue
			}
			fmt.Printf("       k3s-to-talos migrate --host <worker-ip> --cluster-name %s \\\n", state.ClusterName)
			fmt.Printf("         --backup-dir %s-worker-%s --talos-version %s\n\n",
				state.BackupDir, node.Name, state.TalosVersion)
		}
	}
}
