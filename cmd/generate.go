package cmd

import (
	"fmt"
	"path/filepath"

	"github.com/rothgar/k3s-to-talos/internal/talos"
	"github.com/rothgar/k3s-to-talos/internal/ui"
	"github.com/spf13/cobra"
)

var (
	flagControlPlaneEndpoint string
	flagGenerateClusterName  string
	flagGenerateTalosVersion string
)

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate Talos machine configuration from a collected backup",
	Long: `Generates Talos machine configuration files (controlplane.yaml, worker.yaml,
talosconfig) using talosctl. You must have talosctl installed on your local machine.

Run 'collect' first to create a backup directory, then run 'generate' to produce
the Talos configs needed for the migration.`,
	RunE: runGenerate,
}

func init() {
	generateCmd.Flags().StringVar(&flagControlPlaneEndpoint, "cluster-endpoint", "",
		"Control plane endpoint IP or hostname (required if --host not set)")
	generateCmd.Flags().StringVar(&flagGenerateClusterName, "cluster-name", "talos-cluster",
		"Name for the Talos cluster")
	generateCmd.Flags().StringVar(&flagGenerateTalosVersion, "talos-version", "v1.7.0",
		"Talos Linux version to target")
}

func runGenerate(cmd *cobra.Command, args []string) error {
	endpoint := flagControlPlaneEndpoint
	if endpoint == "" {
		endpoint = flagHost
	}
	if endpoint == "" {
		return fmt.Errorf("--cluster-endpoint or --host is required")
	}

	talosConfigDir := filepath.Join(flagBackupDir, "talos-config")
	ui.PrintPhaseHeader(3, "GENERATE", "Generating Talos machine configuration")

	gen := talos.NewConfigGenerator(flagBackupDir)
	if err := gen.Generate(talos.GenerateOptions{
		ClusterName:    flagGenerateClusterName,
		ControlPlaneIP: endpoint,
		TalosVersion:   flagGenerateTalosVersion,
		OutputDir:      talosConfigDir,
		DryRun:         false,
	}); err != nil {
		return fmt.Errorf("generating Talos config: %w", err)
	}

	fmt.Printf("\nTalos config files written to: %s\n", talosConfigDir)
	fmt.Printf("  controlplane.yaml  — apply to the control plane node\n")
	fmt.Printf("  worker.yaml        — apply to each worker node\n")
	fmt.Printf("  talosconfig        — client configuration for talosctl\n")
	return nil
}
