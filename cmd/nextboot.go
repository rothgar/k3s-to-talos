package cmd

import (
	"github.com/rothgar/k3s-to-talos/internal/nextboot/agent"
	"github.com/spf13/cobra"
)

// nextbootCmd is a hidden subcommand that runs directly on the target machine
// after the k3s-to-talos binary is uploaded over SSH.  It performs the full
// Talos disk-imaging flow: download → verify → decompress → dd → write config
// → reboot.  End-users never call this directly.
var nextbootCmd = &cobra.Command{
	Use:    "nextboot",
	Short:  "Install Talos on next boot (runs on target machine — not for direct use)",
	Hidden: true,
	RunE:   runNextboot,
}

var (
	flagNBImageURL  string
	flagNBImageHash string
	flagNBConfig    string
	flagNBDisk      string
	flagNBNoReboot  bool
)

func init() {
	nextbootCmd.Flags().StringVar(&flagNBImageURL, "image-url", "", "URL of the Talos raw disk image")
	nextbootCmd.Flags().StringVar(&flagNBImageHash, "image-hash", "", "Expected SHA-256 of the compressed image (optional)")
	nextbootCmd.Flags().StringVar(&flagNBConfig, "config", "", "Path to the Talos machine config file on this machine")
	nextbootCmd.Flags().StringVar(&flagNBDisk, "disk", "", "Target block device (auto-detected if empty)")
	nextbootCmd.Flags().BoolVar(&flagNBNoReboot, "no-reboot", false, "Skip the automatic reboot after installation")
	_ = nextbootCmd.MarkFlagRequired("image-url")
	rootCmd.AddCommand(nextbootCmd)
}

func runNextboot(cmd *cobra.Command, args []string) error {
	return agent.Run(agent.Options{
		ImageURL:  flagNBImageURL,
		ImageHash: flagNBImageHash,
		Config:    flagNBConfig,
		Disk:      flagNBDisk,
		Reboot:    !flagNBNoReboot,
	})
}
