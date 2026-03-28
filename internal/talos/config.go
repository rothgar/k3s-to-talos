package talos

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
)

// GenerateOptions holds parameters for talosctl gen config.
type GenerateOptions struct {
	ClusterName    string
	ControlPlaneIP string
	TalosVersion   string
	OutputDir      string
	DryRun         bool
}

// ConfigGenerator runs talosctl gen config to produce machine configs.
type ConfigGenerator struct {
	backupDir string
}

// NewConfigGenerator creates a new ConfigGenerator.
func NewConfigGenerator(backupDir string) *ConfigGenerator {
	return &ConfigGenerator{backupDir: backupDir}
}

// Generate runs talosctl gen config and writes output to the specified directory.
func (g *ConfigGenerator) Generate(opts GenerateOptions) error {
	// Check talosctl is available
	talosctlPath, err := exec.LookPath("talosctl")
	if err != nil {
		return fmt.Errorf(
			"talosctl not found in PATH\n\n"+
				"Install talosctl:\n"+
				"  curl -sL https://talos.dev/install | sh\n"+
				"Or download from: https://github.com/siderolabs/talos/releases\n",
		)
	}

	if err := os.MkdirAll(opts.OutputDir, 0750); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	endpoint := fmt.Sprintf("https://%s:6443", opts.ControlPlaneIP)

	// Patch to add the public IP to machine.certSANs so that talosctl can
	// verify the server TLS cert when connecting via the public IP.
	// On EC2 (and other cloud providers) the public IP is not assigned to a
	// network interface, so Talos would not auto-detect it and include it in
	// the server cert's SANs, causing all CA-verified talosctl calls to fail.
	certSANsPatch := fmt.Sprintf(
		`[{"op":"add","path":"/machine/certSANs","value":[%q]}]`,
		opts.ControlPlaneIP,
	)

	args := []string{
		"gen", "config",
		opts.ClusterName,
		endpoint,
		"--output", opts.OutputDir,
		"--output-types", "controlplane,worker,talosconfig",
		"--config-patch", certSANsPatch,
		"--force",
	}

	if opts.TalosVersion != "" {
		args = append(args, "--talos-version", opts.TalosVersion)
	}

	if opts.DryRun {
		color.Yellow("[DRY RUN] Would run: %s %s\n", talosctlPath, strings.Join(args, " "))
		// Create placeholder files for dry-run so subsequent steps have something to reference
		for _, name := range []string{"controlplane.yaml", "worker.yaml", "talosconfig"} {
			placeholder := filepath.Join(opts.OutputDir, name)
			os.WriteFile(placeholder, []byte("# dry-run placeholder\n"), 0600) //nolint:errcheck
		}
		return nil
	}

	cmd := exec.Command(talosctlPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("talosctl gen config failed: %w\n%s", err, stderr.String())
	}

	// Verify expected files were created
	for _, name := range []string{"controlplane.yaml", "worker.yaml", "talosconfig"} {
		p := filepath.Join(opts.OutputDir, name)
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("expected output file not found: %s", p)
		}
	}

	color.Green("  ✓ controlplane.yaml → %s\n", filepath.Join(opts.OutputDir, "controlplane.yaml"))
	color.Green("  ✓ worker.yaml       → %s\n", filepath.Join(opts.OutputDir, "worker.yaml"))
	color.Green("  ✓ talosconfig       → %s\n", filepath.Join(opts.OutputDir, "talosconfig"))

	return nil
}
