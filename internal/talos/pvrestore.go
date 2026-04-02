package talos

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
)

const pvRestorePodManifest = `apiVersion: v1
kind: Pod
metadata:
  name: k2t-pv-restore
  namespace: kube-system
spec:
  hostNetwork: true
  tolerations:
  - operator: Exists
  containers:
  - name: restore
    image: busybox:latest
    command: ["sleep", "3600"]
    volumeMounts:
    - name: host-var
      mountPath: /host-var
  volumes:
  - name: host-var
    hostPath:
      path: /var
      type: DirectoryOrCreate
`

// pvManifestEntry describes a single PV to restore.
type pvManifestEntry struct {
	Name       string `json:"name"`
	ClaimRef   string `json:"claim_ref"`
	SourcePath string `json:"source_path"`
	TargetPath string `json:"target_path"`
}

// RestorePVData restores local-path-provisioner PV data to a Talos node
// using kubectl cp into a privileged pod with a /var hostPath mount.
//
// It also patches PV objects in etcd to reference the new Talos path
// (/var/local-path-provisioner/...) and updates the local-path-provisioner
// ConfigMap to use /var/local-path-provisioner.
func RestorePVData(kubeconfigPath, backupDir string) error {
	pvDataDir := filepath.Join(backupDir, "pv-data")
	manifestPath := filepath.Join(pvDataDir, "pv-manifest.json")

	if _, err := os.Stat(manifestPath); err != nil {
		return nil // no PV data to restore
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("reading PV manifest: %w", err)
	}

	var entries []pvManifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("parsing PV manifest: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	kubectlPath, err := exec.LookPath("kubectl")
	if err != nil {
		return fmt.Errorf("kubectl not found in PATH")
	}

	fmt.Printf("  Restoring %d PV data archives to Talos node...\n", len(entries))

	// Step 1: Create the privileged restore pod.
	if err := createRestorePod(kubectlPath, kubeconfigPath); err != nil {
		return fmt.Errorf("creating restore pod: %w", err)
	}
	defer deleteRestorePod(kubectlPath, kubeconfigPath)

	// Step 2: Copy and extract each PV tar into the pod.
	restored := 0
	for _, entry := range entries {
		tarPath := filepath.Join(pvDataDir, entry.Name+".tar.gz")
		if _, err := os.Stat(tarPath); err != nil {
			color.Yellow("  Warning: PV archive not found: %s\n", tarPath)
			continue
		}

		// Copy tar into the pod.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		cmd := exec.CommandContext(ctx, kubectlPath,
			"--kubeconfig", kubeconfigPath,
			"cp", tarPath,
			"kube-system/k2t-pv-restore:/tmp/"+entry.Name+".tar.gz",
		)
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			color.Yellow("  Warning: failed to copy PV %s: %v (%s)\n", entry.Name, err, strings.TrimSpace(string(out)))
			continue
		}

		// Extract into /host-var/local-path-provisioner/ (which maps to /var/local-path-provisioner/ on the host).
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Minute)
		cmd2 := exec.CommandContext(ctx2, kubectlPath,
			"--kubeconfig", kubeconfigPath,
			"exec", "-n", "kube-system", "k2t-pv-restore", "--",
			"sh", "-c", fmt.Sprintf(
				"mkdir -p /host-var/local-path-provisioner && "+
					"tar xzf /tmp/%s.tar.gz -C /host-var/local-path-provisioner/ && "+
					"rm -f /tmp/%s.tar.gz",
				entry.Name, entry.Name,
			),
		)
		out2, err2 := cmd2.CombinedOutput()
		cancel2()
		if err2 != nil {
			color.Yellow("  Warning: failed to extract PV %s: %v (%s)\n", entry.Name, err2, strings.TrimSpace(string(out2)))
			continue
		}

		fmt.Printf("  ✓ Restored PV %s → /var/local-path-provisioner/%s\n",
			entry.Name, filepath.Base(entry.SourcePath))
		restored++
	}

	// Step 3: Patch PV objects to reference the new path.
	patchPVPaths(kubectlPath, kubeconfigPath, entries)

	// Step 4: Update local-path-provisioner ConfigMap to use /var/local-path-provisioner.
	patchLocalPathConfig(kubectlPath, kubeconfigPath)

	if restored > 0 {
		color.Green("  ✓ Restored %d/%d PV data archives\n", restored, len(entries))
	}
	return nil
}

// createRestorePod creates the k2t-pv-restore pod and waits for it to be running.
func createRestorePod(kubectlPath, kubeconfigPath string) error {
	// Write manifest to temp file.
	tmp, err := os.CreateTemp("", "k2t-pv-restore-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(pvRestorePodManifest); err != nil {
		tmp.Close()
		return err
	}
	tmp.Close()

	// Delete any leftover pod from a previous run.
	exec.Command(kubectlPath, "--kubeconfig", kubeconfigPath, //nolint:errcheck
		"delete", "pod", "-n", "kube-system", "k2t-pv-restore", "--ignore-not-found").Run()

	// Apply the pod manifest.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cmd := exec.CommandContext(ctx, kubectlPath,
		"--kubeconfig", kubeconfigPath,
		"apply", "-f", tmp.Name(),
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		cancel()
		return fmt.Errorf("applying restore pod: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	cancel()

	// Wait for Running.
	fmt.Println("  Waiting for PV restore pod to start...")
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		cmd2 := exec.CommandContext(ctx2, kubectlPath,
			"--kubeconfig", kubeconfigPath,
			"get", "pod", "-n", "kube-system", "k2t-pv-restore",
			"-o", "jsonpath={.status.phase}",
		)
		out, _ := cmd2.Output()
		cancel2()
		if strings.TrimSpace(string(out)) == "Running" {
			return nil
		}
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("restore pod did not become Running within 3 minutes")
}

// deleteRestorePod removes the k2t-pv-restore pod.
func deleteRestorePod(kubectlPath, kubeconfigPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	exec.CommandContext(ctx, kubectlPath, "--kubeconfig", kubeconfigPath,
		"delete", "pod", "-n", "kube-system", "k2t-pv-restore",
		"--ignore-not-found", "--grace-period=0", "--force",
	).Run() //nolint:errcheck
}

// patchPVPaths patches each PV's hostPath to point to /var/local-path-provisioner/.
func patchPVPaths(kubectlPath, kubeconfigPath string, entries []pvManifestEntry) {
	for _, entry := range entries {
		newPath := "/var/local-path-provisioner/" + filepath.Base(entry.SourcePath)
		patch := fmt.Sprintf(`{"spec":{"hostPath":{"path":"%s"}}}`, newPath)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		cmd := exec.CommandContext(ctx, kubectlPath,
			"--kubeconfig", kubeconfigPath,
			"patch", "pv", entry.Name,
			"--type=merge", "-p", patch,
		)
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			// PV might not exist if etcd restore wasn't used.
			color.Yellow("  Warning: could not patch PV %s path: %v (%s)\n",
				entry.Name, err, strings.TrimSpace(string(out)))
		} else {
			fmt.Printf("  ✓ Patched PV %s → %s\n", entry.Name, newPath)
		}
	}
}

// patchLocalPathConfig updates the local-path-config ConfigMap to use
// /var/local-path-provisioner as the storage path (Talos-compatible).
func patchLocalPathConfig(kubectlPath, kubeconfigPath string) {
	// Read current config.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	cmd := exec.CommandContext(ctx, kubectlPath,
		"--kubeconfig", kubeconfigPath,
		"get", "configmap", "-n", "kube-system", "local-path-config",
		"-o", "jsonpath={.data.config\\.json}",
	)
	out, err := cmd.Output()
	cancel()
	if err != nil {
		// ConfigMap might not exist yet (local-path-provisioner not yet deployed).
		return
	}

	oldCfg := string(out)
	// Replace common default paths with the Talos-compatible path.
	newCfg := oldCfg
	for _, oldPath := range []string{
		"/opt/local-path-provisioner",
		"/var/lib/rancher/k3s/storage",
	} {
		newCfg = strings.ReplaceAll(newCfg, oldPath, "/var/local-path-provisioner")
	}

	if newCfg == oldCfg {
		return // already correct or unrecognized path
	}

	// Patch the ConfigMap.
	patch := fmt.Sprintf(`{"data":{"config.json":%q}}`, newCfg)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	cmd2 := exec.CommandContext(ctx2, kubectlPath,
		"--kubeconfig", kubeconfigPath,
		"patch", "configmap", "-n", "kube-system", "local-path-config",
		"--type=merge", "-p", patch,
	)
	out2, err2 := cmd2.CombinedOutput()
	cancel2()
	if err2 != nil {
		color.Yellow("  Warning: could not patch local-path-config: %v (%s)\n",
			err2, strings.TrimSpace(string(out2)))
	} else {
		color.Green("  ✓ Updated local-path-config to use /var/local-path-provisioner\n")
	}
}
