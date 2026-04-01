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
	ClusterName       string
	ControlPlaneIP    string
	TalosVersion      string
	KubernetesVersion string // passed as --kubernetes-version; empty = talosctl default
	OutputDir         string
	DryRun            bool
	// PodCIDR and ServiceCIDR override the default Talos network ranges so
	// they match the source cluster.  After etcd restore the old Flannel
	// network config (stored in etcd by the source cluster) must match what
	// Talos configures — otherwise Flannel crashes and pods cannot start.
	// Leave empty to use Talos defaults (10.244.0.0/16 / 10.96.0.0/12).
	PodCIDR     string // e.g. "10.42.0.0/16" (k3s default)
	ServiceCIDR string // e.g. "10.43.0.0/16" (k3s default)
	// AllowSchedulingOnControlPlane mirrors the source cluster's control-plane
	// schedulability.  When true, cluster.allowSchedulingOnControlPlane is added
	// to the generated controlplane config so that Talos removes the
	// node-role.kubernetes.io/control-plane:NoSchedule taint, matching the
	// source cluster's behaviour (e.g. k3s single-node, kubeadm with taint removed).
	AllowSchedulingOnControlPlane bool
	// CNIName overrides the Talos CNI. Set to "none" when the source cluster's
	// CNI DaemonSet is preserved via etcd restore (e.g. kubeadm's Flannel in
	// kube-flannel namespace): having Talos also install Flannel in kube-system
	// creates two competing daemons that both try to configure the flannel.1
	// VXLAN interface, breaking pod networking after the restore.
	// Leave empty to use the Talos default ("flannel").
	CNIName string // e.g. "none"
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

	// machine.certSANs patch (applied to controlplane only via
	// --config-patch-control-plane).
	//
	// On EC2 (and other cloud providers) the public IP is not on any network
	// interface, so Talos would not auto-include it in the machined TLS server
	// cert SANs.  This causes CA-verified talosctl calls via the public IP to
	// fail with an x509 SAN mismatch.
	//
	// cluster.network patch — use the source cluster's pod/service CIDRs so
	// that Flannel's network config in etcd is consistent with what
	// kube-controller-manager configures.  After etcd restore, Flannel reads
	// the old network config from etcd and crashes if the CIDR differs from
	// the Talos-configured value, leaving pods stuck in ContainerCreating.
	//
	// Both patches are applied only to controlplane.yaml
	// (--config-patch-control-plane) so that the worker.yaml is not touched:
	// worker configs have no cluster.network section, and the worker's own
	// certSANs are injected later at apply-config time (BootstrapWorker).
	//
	// JSON6902 patches are not supported for multi-document configs in
	// talosctl v1.12; we use YAML strategic-merge patches throughout.
	cpPatch := fmt.Sprintf("machine:\n  certSANs:\n    - %q\n", opts.ControlPlaneIP)
	needsCluster := opts.AllowSchedulingOnControlPlane || opts.CNIName != "" ||
		opts.PodCIDR != "" || opts.ServiceCIDR != ""
	if needsCluster {
		cpPatch += "cluster:\n"
		if opts.AllowSchedulingOnControlPlane {
			cpPatch += "  allowSchedulingOnControlPlanes: true\n"
		}
		needsNetwork := opts.CNIName != "" || opts.PodCIDR != "" || opts.ServiceCIDR != ""
		if needsNetwork {
			cpPatch += "  network:\n"
			if opts.CNIName != "" {
				cpPatch += fmt.Sprintf("    cni:\n      name: %s\n", opts.CNIName)
			}
			if opts.PodCIDR != "" {
				cpPatch += fmt.Sprintf("    podSubnets:\n      - %q\n", opts.PodCIDR)
			}
			if opts.ServiceCIDR != "" {
				cpPatch += fmt.Sprintf("    serviceSubnets:\n      - %q\n", opts.ServiceCIDR)
			}
		}
	}

	args := []string{
		"gen", "config",
		opts.ClusterName,
		endpoint,
		"--output", opts.OutputDir,
		"--output-types", "controlplane,worker,talosconfig",
		"--config-patch-control-plane", cpPatch,
		"--force",
	}

	if opts.TalosVersion != "" {
		args = append(args, "--talos-version", opts.TalosVersion)
	}
	if opts.KubernetesVersion != "" {
		args = append(args, "--kubernetes-version", opts.KubernetesVersion)
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
