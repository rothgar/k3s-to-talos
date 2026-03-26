package k3s

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/rothgar/k3s-to-talos/internal/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

// Backup handles backing up k3s cluster data to a local directory.
type Backup struct {
	ssh       *ssh.Client
	backupDir string
	sshHost   string // the host we SSH'd to; used to rewrite kubeconfig server address
}

// NewBackup creates a new Backup instance.
// sshHost is the IP/hostname used for the SSH connection; it becomes the
// server address in the downloaded kubeconfig so client-go can connect.
func NewBackup(sshClient *ssh.Client, backupDir, sshHost string) *Backup {
	return &Backup{ssh: sshClient, backupDir: backupDir, sshHost: sshHost}
}

// Run performs all backup operations.
func (b *Backup) Run(info *ClusterInfo, dryRun bool) error {
	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)

	// 1. Back up the k3s database
	s.Suffix = " Backing up k3s database..."
	s.Start()

	dbDir := filepath.Join(b.backupDir, "database")
	if err := os.MkdirAll(dbDir, 0750); err != nil {
		s.Stop()
		return fmt.Errorf("creating database backup dir: %w", err)
	}

	if !dryRun {
		if err := b.backupDatabase(info.DatastoreType, dbDir); err != nil {
			s.Stop()
			fmt.Printf("\n  Warning: database backup failed: %v\n", err)
		}
	}
	s.Stop()
	if !dryRun {
		fmt.Printf("  ✓ Database backup (%s) → %s\n", info.DatastoreType, dbDir)
	} else {
		fmt.Printf("  [DRY RUN] Would back up %s database to %s\n", info.DatastoreType, dbDir)
	}

	// 2. Download kubeconfig
	s.Suffix = " Downloading kubeconfig..."
	s.Start()

	kubeconfigPath := filepath.Join(b.backupDir, "k3s.yaml")
	if !dryRun {
		if err := b.downloadKubeconfig(kubeconfigPath); err != nil {
			s.Stop()
			fmt.Printf("\n  Warning: kubeconfig download failed: %v\n", err)
		} else {
			s.Stop()
			fmt.Printf("  ✓ Kubeconfig → %s\n", kubeconfigPath)
		}
	} else {
		s.Stop()
		fmt.Printf("  [DRY RUN] Would download kubeconfig to %s\n", kubeconfigPath)
	}

	// 3. Export Kubernetes resources via client-go
	if !dryRun {
		if err := b.exportResources(kubeconfigPath, info); err != nil {
			fmt.Printf("  Warning: resource export incomplete: %v\n", err)
		}
	} else {
		fmt.Printf("  [DRY RUN] Would export Kubernetes resources to %s/resources/\n", b.backupDir)
	}

	return nil
}

func (b *Backup) backupDatabase(datastoreType, dbDir string) error {
	switch datastoreType {
	case "etcd":
		return b.backupEtcd(dbDir)
	default:
		return b.backupSQLite(dbDir)
	}
}

func (b *Backup) backupEtcd(dbDir string) error {
	snapshotName := fmt.Sprintf("migration-backup-%d", time.Now().Unix())

	// Trigger snapshot on remote
	_, err := b.ssh.Run(fmt.Sprintf("k3s etcd-snapshot save --name %s 2>&1", snapshotName))
	if err != nil {
		return fmt.Errorf("taking etcd snapshot: %w", err)
	}

	// Find the snapshot file
	snapshotPath, err := b.ssh.Run(
		fmt.Sprintf("find /var/lib/rancher/k3s/server/db/snapshots -name '%s*' 2>/dev/null | head -1", snapshotName))
	if err != nil || strings.TrimSpace(snapshotPath) == "" {
		snapshotPath = fmt.Sprintf("/var/lib/rancher/k3s/server/db/snapshots/%s", snapshotName)
	}
	snapshotPath = strings.TrimSpace(snapshotPath)

	localPath := filepath.Join(dbDir, "etcd-snapshot.db")
	if err := b.ssh.Download(snapshotPath, localPath); err != nil {
		return fmt.Errorf("downloading etcd snapshot: %w", err)
	}

	meta := map[string]string{
		"type":          "etcd",
		"snapshot_name": snapshotName,
		"remote_path":   snapshotPath,
		"local_path":    localPath,
		"timestamp":     time.Now().Format(time.RFC3339),
		"restore_note":  "To restore: use k3s server --cluster-reset --cluster-reset-restore-path=<snapshot>",
	}
	if data, err := json.MarshalIndent(meta, "", "  "); err == nil {
		os.WriteFile(filepath.Join(dbDir, "backup-info.json"), data, 0600) //nolint:errcheck
	}

	return nil
}

func (b *Backup) backupSQLite(dbDir string) error {
	sqlitePath := "/var/lib/rancher/k3s/server/db/state.db"
	if !b.ssh.FileExists(sqlitePath) {
		return fmt.Errorf("SQLite database not found at %s", sqlitePath)
	}

	localPath := filepath.Join(dbDir, "state.db")
	if err := b.ssh.Download(sqlitePath, localPath); err != nil {
		return fmt.Errorf("downloading SQLite database: %w", err)
	}

	// Also grab WAL and SHM files if present
	for _, ext := range []string{"-wal", "-shm"} {
		remotePath := sqlitePath + ext
		if b.ssh.FileExists(remotePath) {
			b.ssh.Download(remotePath, localPath+ext) //nolint:errcheck
		}
	}

	meta := map[string]string{
		"type":         "sqlite",
		"remote_path":  sqlitePath,
		"local_path":   localPath,
		"timestamp":    time.Now().Format(time.RFC3339),
		"restore_note": "To restore: replace state.db in a fresh k3s installation's /var/lib/rancher/k3s/server/db/",
	}
	if data, err := json.MarshalIndent(meta, "", "  "); err == nil {
		os.WriteFile(filepath.Join(dbDir, "backup-info.json"), data, 0600) //nolint:errcheck
	}

	return nil
}

func (b *Backup) downloadKubeconfig(localPath string) error {
	if err := b.ssh.Download("/etc/rancher/k3s/k3s.yaml", localPath); err != nil {
		return err
	}

	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}

	// Rewrite the server address to use the SSH target host so client-go can
	// reach the API server from the operator's machine.
	// Prefer the explicit SSH host (--host flag) because the k3s TLS cert
	// covers the node's advertised IP and 127.0.0.1 — using a different IP
	// (e.g. from `hostname -I`) would cause certificate validation failures.
	host := b.sshHost
	if host == "" {
		host = strings.TrimSpace(b.ssh.RunIgnoreError("hostname -I | awk '{print $1}'"))
	}
	if host == "" {
		return nil
	}

	updated := strings.ReplaceAll(string(data), "https://127.0.0.1", fmt.Sprintf("https://%s", host))
	updated = strings.ReplaceAll(updated, "https://localhost", fmt.Sprintf("https://%s", host))

	// k3s signs its serving cert for the node's own IPs; when connecting remotely
	// via the SSH target host the cert SANs may not match.  Switch to insecure
	// mode so client-go can still enumerate resources for backup purposes.
	// This backup kubeconfig is never used for the new Talos cluster — a fresh
	// kubeconfig is retrieved via talosctl after bootstrap.
	updated = rewriteKubeconfigInsecure(updated)

	return os.WriteFile(localPath, []byte(updated), 0600)
}

// rewriteKubeconfigInsecure replaces certificate-authority-data with
// insecure-skip-tls-verify so client-go can connect without cert validation.
// This is intentional for the collect/backup phase only.
func rewriteKubeconfigInsecure(kubeconfig string) string {
	// Remove any existing certificate-authority-data line (may span multiple
	// lines due to base64; the value is on a single line in standard kubeconfigs).
	lines := strings.Split(kubeconfig, "\n")
	out := make([]string, 0, len(lines))
	insertedInsecure := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "certificate-authority-data:") {
			if !insertedInsecure {
				// Preserve indentation
				indent := strings.Repeat(" ", len(line)-len(strings.TrimLeft(line, " ")))
				out = append(out, indent+"insecure-skip-tls-verify: true")
				insertedInsecure = true
			}
			continue // drop the cert data line
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// resourcesToExport defines the GVRs we'll export from the cluster.
var resourcesToExport = []schema.GroupVersionResource{
	{Group: "", Version: "v1", Resource: "namespaces"},
	{Group: "", Version: "v1", Resource: "configmaps"},
	{Group: "", Version: "v1", Resource: "secrets"},
	{Group: "", Version: "v1", Resource: "services"},
	{Group: "", Version: "v1", Resource: "persistentvolumes"},
	{Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
	{Group: "", Version: "v1", Resource: "serviceaccounts"},
	{Group: "apps", Version: "v1", Resource: "deployments"},
	{Group: "apps", Version: "v1", Resource: "statefulsets"},
	{Group: "apps", Version: "v1", Resource: "daemonsets"},
	{Group: "batch", Version: "v1", Resource: "cronjobs"},
	{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
	{Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"},
	{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"},
}

// systemNamespaces contains namespaces we skip when exporting to avoid
// restoring internal Kubernetes state.
var systemNamespaces = map[string]bool{
	"kube-system":     true,
	"kube-public":     true,
	"kube-node-lease": true,
}

func (b *Backup) exportResources(kubeconfigPath string, info *ClusterInfo) error {
	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
	s.Suffix = " Exporting Kubernetes resources..."
	s.Start()
	defer s.Stop()

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return fmt.Errorf("loading kubeconfig: %w", err)
	}
	// Use a generous timeout for resource enumeration
	cfg.Timeout = 30 * time.Second

	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("creating dynamic client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	resourceDir := filepath.Join(b.backupDir, "resources")
	if err := os.MkdirAll(resourceDir, 0750); err != nil {
		return fmt.Errorf("creating resources directory: %w", err)
	}

	exported := 0
	var exportErrors []string

	for _, gvr := range resourcesToExport {
		s.Suffix = fmt.Sprintf(" Exporting %s...", gvr.Resource)

		list, err := dynClient.Resource(gvr).Namespace("").List(ctx, metav1.ListOptions{})
		if err != nil {
			exportErrors = append(exportErrors, fmt.Sprintf("%s: %v", gvr.Resource, err))
			continue
		}

		if len(list.Items) == 0 {
			continue
		}

		outDir := filepath.Join(resourceDir, gvr.Resource)
		if err := os.MkdirAll(outDir, 0750); err != nil {
			continue
		}

		for i := range list.Items {
			item := &list.Items[i]

			// Skip system namespaces for namespace-scoped resources
			if ns := item.GetNamespace(); ns != "" && systemNamespaces[ns] {
				continue
			}

			cleanForBackup(item)

			data, err := yaml.Marshal(item.Object)
			if err != nil {
				continue
			}

			name := item.GetName()
			ns := item.GetNamespace()
			var fname string
			if ns != "" {
				fname = filepath.Join(outDir, fmt.Sprintf("%s__%s.yaml", ns, name))
			} else {
				fname = filepath.Join(outDir, name+".yaml")
			}

			if err := os.WriteFile(fname, data, 0600); err == nil {
				exported++
			}
		}
	}

	s.Stop()
	fmt.Printf("  ✓ Exported %d resources to %s\n", exported, resourceDir)

	if len(exportErrors) > 0 {
		fmt.Printf("  Warnings (some resource types could not be exported):\n")
		for _, e := range exportErrors {
			fmt.Printf("    - %s\n", e)
		}
	}

	return nil
}

// cleanForBackup strips fields that shouldn't be re-applied to a new cluster.
func cleanForBackup(obj *unstructured.Unstructured) {
	// Remove managed fields (verbose and not needed for restore)
	obj.SetManagedFields(nil)
	// Remove resource version and uid (cluster-specific)
	obj.SetResourceVersion("")
	obj.SetUID("")
	obj.SetGeneration(0)
	// Remove status (will be recreated)
	delete(obj.Object, "status")
	// Remove node-specific annotations
	annotations := obj.GetAnnotations()
	for k := range annotations {
		if strings.HasPrefix(k, "kubectl.kubernetes.io/last-applied") {
			delete(annotations, k)
		}
	}
	if len(annotations) > 0 {
		obj.SetAnnotations(annotations)
	}
}
