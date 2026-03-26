package talos

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
)

// BootstrapOptions holds parameters for bootstrapping a Talos cluster.
type BootstrapOptions struct {
	Host            string
	TalosConfigFile string
	ControlPlaneCfg string
	KubeconfigOut   string
}

// Bootstrapper handles waiting for Talos to boot and running bootstrap.
type Bootstrapper struct {
	backupDir string
}

// NewBootstrapper creates a new Bootstrapper.
func NewBootstrapper(backupDir string) *Bootstrapper {
	return &Bootstrapper{backupDir: backupDir}
}

// Bootstrap waits for Talos to boot, applies config, bootstraps Kubernetes,
// and retrieves the kubeconfig.
func (b *Bootstrapper) Bootstrap(opts BootstrapOptions) error {
	talosctlPath, err := exec.LookPath("talosctl")
	if err != nil {
		return fmt.Errorf("talosctl not found in PATH")
	}

	// Step 1: Wait for Talos API to come up
	if err := b.waitForTalosAPI(opts.Host); err != nil {
		return err
	}

	// Step 2: Apply control plane config
	fmt.Println("  Applying control plane configuration...")
	if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
		"apply-config", "--insecure",
		"--nodes", opts.Host,
		"--file", opts.ControlPlaneCfg,
	); err != nil {
		return fmt.Errorf("applying control plane config: %w", err)
	}
	color.Green("  ✓ Control plane config applied\n")

	// Brief pause for the config to take effect
	time.Sleep(5 * time.Second)

	// Step 3: Bootstrap the cluster (EXACTLY ONCE on ONE node)
	fmt.Println("  Bootstrapping Kubernetes cluster (this runs once on the control plane)...")
	if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
		"bootstrap",
		"--nodes", opts.Host,
		"--endpoints", opts.Host,
	); err != nil {
		// Bootstrap can return a "already bootstrapped" error — that's OK
		if !strings.Contains(err.Error(), "already bootstrapped") &&
			!strings.Contains(err.Error(), "AlreadyExists") {
			return fmt.Errorf("bootstrapping cluster: %w", err)
		}
		fmt.Println("  (cluster was already bootstrapped)")
	}
	color.Green("  ✓ Cluster bootstrapped\n")

	// Step 4: Wait for Kubernetes API
	if err := b.waitForKubernetesAPI(opts.Host, opts.TalosConfigFile); err != nil {
		return err
	}

	// Step 5: Retrieve kubeconfig
	fmt.Printf("  Retrieving kubeconfig → %s\n", opts.KubeconfigOut)
	if err := b.runTalosctl(talosctlPath, opts.TalosConfigFile,
		"kubeconfig",
		"--nodes", opts.Host,
		"--force",
		opts.KubeconfigOut,
	); err != nil {
		return fmt.Errorf("retrieving kubeconfig: %w", err)
	}
	color.Green("  ✓ Kubeconfig saved to %s\n", opts.KubeconfigOut)

	return nil
}

// waitForTalosAPI polls the Talos machine API (port 50000) until it responds,
// with exponential backoff up to ~10 minutes total.
func (b *Bootstrapper) waitForTalosAPI(host string) error {
	s := spinner.New(spinner.CharSets[14], 200*time.Millisecond)
	s.Suffix = " Waiting for Talos to boot (this may take several minutes)..."
	s.Start()
	defer s.Stop()

	//nolint:gosec // We intentionally skip TLS verification for the initial API check
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	url := fmt.Sprintf("https://%s:50000", host)
	deadline := time.Now().Add(10 * time.Minute)
	wait := 5 * time.Second

	for time.Now().Before(deadline) {
		resp, err := httpClient.Get(url)
		if err == nil {
			resp.Body.Close()
			s.Stop()
			color.Green("  ✓ Talos API is responding at %s\n", url)
			return nil
		}

		// Also accept connection refused → Talos is up but not yet serving
		if strings.Contains(err.Error(), "connection refused") {
			time.Sleep(wait)
			if wait < 30*time.Second {
				wait *= 2
			}
			continue
		}

		time.Sleep(wait)
		if wait < 30*time.Second {
			wait *= 2
		}
	}

	s.Stop()
	return fmt.Errorf(
		"Talos API at %s did not become available within 10 minutes\n\n"+
			"The machine may need manual intervention. Once Talos is running you can:\n"+
			"  talosctl --talosconfig %s apply-config --insecure --nodes %s --file controlplane.yaml\n"+
			"  talosctl --talosconfig %s bootstrap --nodes %s\n",
		url, b.backupDir+"/talos-config/talosconfig", host,
		b.backupDir+"/talos-config/talosconfig", host,
	)
}

// waitForKubernetesAPI polls kubectl until the API server responds.
func (b *Bootstrapper) waitForKubernetesAPI(host, talosConfigFile string) error {
	s := spinner.New(spinner.CharSets[14], 200*time.Millisecond)
	s.Suffix = " Waiting for Kubernetes API server to become ready..."
	s.Start()
	defer s.Stop()

	talosctlPath, _ := exec.LookPath("talosctl")
	deadline := time.Now().Add(5 * time.Minute)
	wait := 10 * time.Second

	for time.Now().Before(deadline) {
		err := b.runTalosctl(talosctlPath, talosConfigFile,
			"health",
			"--nodes", host,
			"--wait-timeout", "10s",
		)
		if err == nil {
			s.Stop()
			color.Green("  ✓ Kubernetes API server is ready\n")
			return nil
		}
		time.Sleep(wait)
	}

	s.Stop()
	// Non-fatal — the kubeconfig retrieval step will catch if it's truly not ready
	color.Yellow("  Warning: health check timed out. Attempting kubeconfig retrieval anyway.\n")
	return nil
}

func (b *Bootstrapper) runTalosctl(binary, talosconfig string, args ...string) error {
	allArgs := append([]string{"--talosconfig", talosconfig}, args...)
	cmd := exec.Command(binary, allArgs...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		combined := strings.TrimSpace(stdout.String() + "\n" + stderr.String())
		return fmt.Errorf("%w\n%s", err, combined)
	}

	// Write stdout to the target file if this is a kubeconfig command
	for i, arg := range args {
		if arg == "kubeconfig" && i < len(args)-1 {
			outFile := args[len(args)-1]
			if stdout.Len() > 0 {
				os.WriteFile(outFile, stdout.Bytes(), 0600) //nolint:errcheck
			}
			break
		}
	}

	return nil
}
