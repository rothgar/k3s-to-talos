package cmd

import (
	"fmt"
	"os"

	"github.com/rothgar/k3s-to-talos/internal/k3s"
	"github.com/rothgar/k3s-to-talos/internal/ssh"
	"github.com/rothgar/k3s-to-talos/internal/ui"
	"github.com/spf13/cobra"
)

var collectCmd = &cobra.Command{
	Use:   "collect",
	Short: "Collect k3s cluster info and create a backup (no migration)",
	Long: `Connects to the remote k3s server via SSH and collects cluster information
including nodes, workloads, secrets, configmaps, and persistent volumes.
Also backs up the k3s database (etcd snapshot or SQLite file) and exports
all Kubernetes resources as YAML.

This command does NOT modify the remote machine — it is safe to run at any time.`,
	RunE: runCollect,
}

func runCollect(cmd *cobra.Command, args []string) error {
	if flagHost == "" {
		return fmt.Errorf("--host is required")
	}

	if err := os.MkdirAll(flagBackupDir, 0750); err != nil {
		return fmt.Errorf("creating backup directory: %w", err)
	}

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
	if err := backup.Run(info, false); err != nil {
		return fmt.Errorf("backing up k3s data: %w", err)
	}

	ui.PrintClusterSummary(info, flagBackupDir)

	if len(info.Nodes) > 1 {
		ui.PrintMultiNodeWarning(info.Nodes)
	}

	fmt.Printf("\nBackup saved to: %s\n", flagBackupDir)
	return nil
}
