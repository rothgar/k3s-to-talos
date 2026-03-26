package cmd

import (
	"github.com/spf13/cobra"
)

// Global flags shared across commands.
var (
	flagHost      string
	flagSSHUser   string
	flagSSHKey    string
	flagSSHPort   int
	flagBackupDir string
	flagSudo      bool
)

var rootCmd = &cobra.Command{
	Use:   "k3s-to-talos",
	Short: "Migrate a k3s server node to Talos Linux",
	Long: `k3s-to-talos is a CLI tool that remotely migrates a machine running k3s
in server (control-plane) mode to Talos Linux.

It connects to the remote machine over SSH, collects cluster information,
backs up the k3s database and Kubernetes resources, generates Talos machine
configs, and then uses nextboot-talos to erase and reboot the machine into
Talos Linux.

WARNING: This process is IRREVERSIBLE. The target machine's OS will be
completely replaced. Ensure you have backed up all critical data before
proceeding.`,
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&flagHost, "host", "", "IP address or hostname of the target k3s server")
	rootCmd.PersistentFlags().StringVar(&flagSSHUser, "ssh-user", "root", "SSH username")
	rootCmd.PersistentFlags().StringVar(&flagSSHKey, "ssh-key", "", "Path to SSH private key (defaults to ~/.ssh/id_rsa)")
	rootCmd.PersistentFlags().IntVar(&flagSSHPort, "ssh-port", 22, "SSH port")
	rootCmd.PersistentFlags().StringVar(&flagBackupDir, "backup-dir", "./k3s-backup", "Local directory for backups and generated configs")
	rootCmd.PersistentFlags().BoolVar(&flagSudo, "sudo", false, "Prefix remote commands with sudo (for non-root SSH users)")

	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(collectCmd)
	rootCmd.AddCommand(generateCmd)
}
