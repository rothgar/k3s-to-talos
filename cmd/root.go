package cmd

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	sshconfig "github.com/kevinburke/ssh_config"
	"github.com/rothgar/k2t/internal/ssh"
	"github.com/spf13/cobra"
)

// Global flags shared across commands.
var (
	flagHost      string
	flagSSHKey    string
	flagSSHPort   int
	flagBackupDir string
	flagVerbose   bool
)

var rootCmd = &cobra.Command{
	Use:   "k2t",
	Short: "Migrate a k3s or kubeadm server node to Talos Linux",
	Long: `k2t is a CLI tool that remotely migrates a machine running k3s or kubeadm
in server (control-plane) mode to Talos Linux.

It connects to the remote machine over SSH, collects cluster information,
backs up the database and Kubernetes resources, generates Talos machine
configs, and then uses nextboot-talos to erase and reboot the machine into
Talos Linux.

The SSH target accepts standard SSH notation: [user@]host

  k2t migrate ubuntu@10.1.1.1
  k2t migrate myserver           # resolves via ~/.ssh/config

Host aliases, users, ports, and identity files are automatically read from
~/.ssh/config. Command-line flags take precedence over the config file.

When the SSH user is not "root", sudo is used automatically for privileged
commands.

WARNING: This process is IRREVERSIBLE. The target machine's OS will be
completely replaced. Ensure you have backed up all critical data before
proceeding.`,
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

// resolveTarget returns the SSH target ([user@]host) from the first positional
// argument if provided, otherwise falls back to --host.
func resolveTarget(args []string) string {
	if len(args) > 0 && args[0] != "" {
		return args[0]
	}
	return flagHost
}

// sshOpts parses a [user@]host target string and returns ssh.Options,
// filling in missing values from ~/.ssh/config.
//
// Resolution order (highest to lowest priority):
//
//	user    : inline user@host  >  ssh config User  >  "root"
//	host    : ssh config Hostname (alias resolution)
//	port    : --ssh-port flag   >  ssh config Port  >  22
//	key     : --ssh-key flag    >  ssh config IdentityFile
func sshOpts(target string) ssh.Options {
	user := ""
	alias := target // the name the user typed (may be a Host alias)
	if idx := strings.Index(target, "@"); idx >= 0 {
		user = target[:idx]
		alias = target[idx+1:]
	}

	cfg := loadSSHConfig()

	// Resolve hostname alias → actual IP/FQDN.
	host := alias
	if resolved := cfg.get(alias, "Hostname"); resolved != "" {
		host = resolved
	}

	// User: inline > ssh config > "root"
	if user == "" {
		if u := cfg.get(alias, "User"); u != "" {
			user = u
		} else {
			user = "root"
		}
	}

	// Port: --ssh-port (non-zero means explicitly set) > ssh config > 22
	port := flagSSHPort
	if port == 0 {
		if p := cfg.get(alias, "Port"); p != "" {
			if n, err := strconv.Atoi(p); err == nil {
				port = n
			}
		}
		if port == 0 {
			port = 22
		}
	}

	// Key: --ssh-key > ssh config IdentityFile > (NewClient tries defaults)
	keyPath := flagSSHKey
	if keyPath == "" {
		keyPath = cfg.get(alias, "IdentityFile")
	}

	return ssh.Options{
		Host:    host,
		Port:    port,
		User:    user,
		KeyPath: keyPath,
		Verbose: flagVerbose,
	}
}

// resolveHost returns just the host portion of a [user@]host target,
// after applying ~/.ssh/config alias resolution.
func resolveHost(args []string) string {
	return sshOpts(resolveTarget(args)).Host
}

// sshCfg wraps a parsed ssh_config.Config for safe nil-safe lookups.
type sshCfg struct{ cfg *sshconfig.Config }

func (c *sshCfg) get(alias, key string) string {
	if c.cfg == nil {
		return ""
	}
	v, _ := c.cfg.Get(alias, key)
	// Expand ~/ in paths (the library does this for system-level Get but not
	// when decoding from a reader).
	if strings.HasPrefix(v, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			v = filepath.Join(home, v[2:])
		}
	}
	return v
}

// loadSSHConfig reads ~/.ssh/config, returning an empty reader on any error.
func loadSSHConfig() *sshCfg {
	home, err := os.UserHomeDir()
	if err != nil {
		return &sshCfg{}
	}
	f, err := os.Open(filepath.Join(home, ".ssh", "config"))
	if err != nil {
		return &sshCfg{} // no config file is fine
	}
	defer f.Close()
	cfg, err := sshconfig.Decode(f)
	if err != nil {
		return &sshCfg{} // malformed config — ignore silently
	}
	return &sshCfg{cfg: cfg}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&flagHost, "host", "", "SSH target: [user@]host (e.g. ubuntu@10.1.1.1 or a ~/.ssh/config alias)")
	rootCmd.PersistentFlags().StringVar(&flagSSHKey, "ssh-key", "", "Path to SSH private key (overrides ~/.ssh/config IdentityFile)")
	rootCmd.PersistentFlags().IntVar(&flagSSHPort, "ssh-port", 0, "SSH port (overrides ~/.ssh/config Port; default 22)")
	rootCmd.PersistentFlags().StringVar(&flagBackupDir, "backup-dir", "./k3s-backup", "Local directory for backups and generated configs")
	rootCmd.PersistentFlags().BoolVarP(&flagVerbose, "verbose", "v", false, "Print each remote command and its output as it runs")

	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(joinWorkerCmd)
	rootCmd.AddCommand(joinControlPlaneCmd)
	rootCmd.AddCommand(collectCmd)
	rootCmd.AddCommand(generateCmd)
}
