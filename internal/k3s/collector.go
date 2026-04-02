package k3s

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"net"
	"strings"
	"time"

	"github.com/briandowns/spinner"
	"github.com/rothgar/k2t/internal/ssh"
	"github.com/rothgar/k2t/internal/talos"
)

// ClusterInfo holds information about the cluster gathered remotely.
type ClusterInfo struct {
	ClusterType   string              `json:"cluster_type"` // "k3s" | "kubeadm"
	K3sVersion    string              `json:"k3s_version"`  // raw version string from k3s/kubelet
	K8sVersion    string              `json:"k8s_version"`
	ClusterName   string              `json:"cluster_name"`
	Nodes         []Node              `json:"nodes"`
	DatastoreType string              `json:"datastore_type"` // "etcd" | "sqlite"
	Namespaces    []string            `json:"namespaces"`
	WorkloadCount int                 `json:"workload_count"`
	PVCount       int                 `json:"pv_count"`
	PVs           []PV                `json:"pvs,omitempty"`
	Hardware      *talos.HardwareInfo `json:"hardware,omitempty"`
	PodCIDR       string              `json:"pod_cidr,omitempty"`     // cluster pod CIDR
	ServiceCIDR   string              `json:"service_cidr,omitempty"` // cluster service CIDR
	// AllowSchedulingOnControlPlane is true when the source cluster's control-plane
	// nodes are schedulable (i.e. none of them carry the
	// node-role.kubernetes.io/control-plane:NoSchedule taint). The Talos config
	// generator uses this to set cluster.allowSchedulingOnControlPlane so the
	// generated cluster has identical scheduling behaviour.
	AllowSchedulingOnControlPlane bool `json:"allow_scheduling_on_control_plane"`
	// WorkloadFeatures describes workload-specific Talos configuration requirements
	// discovered during the collect phase.
	WorkloadFeatures WorkloadFeatures `json:"workload_features"`
	// LocalPath describes the local-path-provisioner if detected.
	LocalPath LocalPathInfo `json:"local_path,omitempty"`
}

// WorkloadFeatures captures Talos machine-config requirements implied by the
// workloads running in the source cluster.
type WorkloadFeatures struct {
	// HasServiceLB is true when k3s's built-in ServiceLB (klipper-lb) is
	// active.  Klipper-lb pods use unsafe sysctls that Talos's kubelet must
	// explicitly permit via allowedUnsafeSysctls.
	HasServiceLB bool `json:"has_service_lb"`
	// AllowedUnsafeSysctls is the deduplicated list of unsafe sysctls that
	// the kubelet must whitelist so that the detected workloads can run.
	AllowedUnsafeSysctls []string `json:"allowed_unsafe_sysctls,omitempty"`
}

// Node represents a Kubernetes node in the k3s cluster.
type Node struct {
	Name           string `json:"name"`
	Status         string `json:"status"`
	Roles          string `json:"roles"`
	IsControlPlane bool   `json:"is_control_plane"`
	InternalIP     string `json:"internal_ip,omitempty"`
}

// PV holds information about a PersistentVolume.
type PV struct {
	Name             string `json:"name"`
	Capacity         string `json:"capacity"`
	StorageClass     string `json:"storage_class"`
	Phase            string `json:"phase"`
	ClaimRef         string `json:"claim_ref,omitempty"`
	HostPath         string `json:"host_path,omitempty"` // local-path-provisioner host path
}

// LocalPathInfo holds information about the local-path-provisioner.
type LocalPathInfo struct {
	Detected bool   `json:"detected"`
	HostPath string `json:"host_path"` // e.g. /opt/local-path-provisioner
}

// Collector gathers cluster information via SSH for both k3s and kubeadm nodes.
type Collector struct {
	ssh         *ssh.Client
	clusterType string // ClusterTypeK3s | ClusterTypeKubeadm
}

// NewCollector creates a k3s Collector. Prefer Detect() for automatic type selection.
func NewCollector(ssh *ssh.Client) *Collector {
	return &Collector{ssh: ssh, clusterType: ClusterTypeK3s}
}

// kubectlBin returns the kubectl command appropriate for this cluster type.
func (c *Collector) kubectlBin() string {
	if c.clusterType == ClusterTypeKubeadm {
		return "kubectl --kubeconfig /etc/kubernetes/admin.conf"
	}
	return "k3s kubectl"
}

// kubeconfigPath returns the remote path to the cluster admin kubeconfig.
func (c *Collector) kubeconfigPath() string {
	if c.clusterType == ClusterTypeKubeadm {
		return "/etc/kubernetes/admin.conf"
	}
	return "/etc/rancher/k3s/k3s.yaml"
}

// Collect gathers all cluster information from the remote node.
func (c *Collector) Collect() (*ClusterInfo, error) {
	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
	s.Suffix = " Collecting k3s cluster information..."
	s.Start()
	defer s.Stop()

	info := &ClusterInfo{ClusterType: c.clusterType}

	if err := c.verifyServer(); err != nil {
		return nil, err
	}

	s.Suffix = " Detecting hardware..."
	hw, err := talos.DetectHardware(c.ssh)
	if err != nil {
		s.Stop()
		return nil, fmt.Errorf("detecting hardware: %w", err)
	}
	info.Hardware = hw

	// Fail fast if the hardware is known to be unsupported.
	if err := hw.Supported(); err != nil {
		s.Stop()
		return nil, err
	}

	s.Suffix = " Detecting k3s version..."
	if err := c.collectVersion(info); err != nil {
		return nil, err
	}

	s.Suffix = " Listing cluster nodes..."
	if err := c.collectNodes(info); err != nil {
		return nil, err
	}

	s.Suffix = " Detecting datastore type..."
	c.detectDatastore(info)

	s.Suffix = " Listing namespaces and workloads..."
	if err := c.collectWorkloads(info); err != nil {
		// Non-fatal: continue without workload count
		fmt.Printf("\nWarning: could not collect workload info: %v\n", err)
	}

	s.Suffix = " Listing persistent volumes..."
	if err := c.collectPVs(info); err != nil {
		fmt.Printf("\nWarning: could not collect PV info: %v\n", err)
	}

	s.Suffix = " Detecting cluster network CIDRs..."
	c.detectNetworkCIDRs(info)

	return info, nil
}

func (c *Collector) verifyServer() error {
	switch c.clusterType {
	case ClusterTypeKubeadm:
		return c.verifyKubeadmControlPlane()
	default:
		return c.verifyK3sServer()
	}
}

func (c *Collector) verifyK3sServer() error {
	active, _ := c.ssh.RunNoSudo(
		`systemctl is-active k3s 2>/dev/null || ` +
			`systemctl is-active k3s-server 2>/dev/null || ` +
			`(pgrep -x k3s >/dev/null 2>&1 && echo active) || ` +
			`(pgrep -f 'k3s server' >/dev/null 2>&1 && echo active) || ` +
			`echo inactive`)
	if strings.TrimSpace(active) == "inactive" {
		return fmt.Errorf("k3s server does not appear to be running on the target machine\n" +
			"Ensure the k3s server process is active before migrating.")
	}
	// /etc/rancher/k3s/ is mode 755; directory check works without root.
	if _, err := c.ssh.RunNoSudo("test -d /etc/rancher/k3s"); err != nil {
		return fmt.Errorf("target machine does not appear to be a k3s server node (missing /etc/rancher/k3s/)")
	}
	return nil
}

func (c *Collector) verifyKubeadmControlPlane() error {
	active, _ := c.ssh.RunNoSudo(
		`systemctl is-active kubelet 2>/dev/null || ` +
			`(pgrep -x kubelet >/dev/null 2>&1 && echo active) || ` +
			`echo inactive`)
	if strings.TrimSpace(active) == "inactive" {
		return fmt.Errorf("kubelet does not appear to be running on the target machine\n" +
			"Ensure kubelet is active before migrating.")
	}
	// /etc/kubernetes/ is mode 755; directory check works without root.
	if _, err := c.ssh.RunNoSudo("test -d /etc/kubernetes"); err != nil {
		return fmt.Errorf("target machine does not appear to be a kubeadm control-plane node\n" +
			"(missing /etc/kubernetes/ — is this a worker node only?)")
	}
	return nil
}

func (c *Collector) collectVersion(info *ClusterInfo) error {
	if c.clusterType == ClusterTypeKubeadm {
		v, err := c.ssh.Run("kubelet --version 2>/dev/null")
		if err != nil {
			return fmt.Errorf("getting version: %w", err)
		}
		info.K3sVersion = strings.TrimSpace(v)
		// "Kubernetes v1.34.6"
		info.K8sVersion = extractSemver(info.K3sVersion)
		return nil
	}

	// k3s --version emits two lines:
	//   k3s version v1.34.6+k3s1 (abc123)
	//   kubernetes v1.34.6
	// Use the "kubernetes" line for the clean k8s version.  This avoids
	// kubectl version --short which was removed in k8s 1.28.
	v, err := c.ssh.Run("k3s --version 2>/dev/null")
	if err != nil {
		return fmt.Errorf("getting version: %w", err)
	}
	info.K3sVersion = strings.TrimSpace(v)
	for _, line := range strings.Split(v, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "kubernetes ") {
			info.K8sVersion = strings.TrimPrefix(line, "kubernetes ")
			break
		}
	}
	// Fallback: strip the +k3s1 suffix from the first version token.
	if info.K8sVersion == "" {
		info.K8sVersion = extractSemver(v)
	}
	return nil
}

// extractSemver finds the first "vX.Y.Z" token in s, stripping any
// distribution suffix after "+" (e.g. "+k3s1", "+k3s2").
func extractSemver(s string) string {
	for _, f := range strings.Fields(s) {
		if !strings.HasPrefix(f, "v") {
			continue
		}
		if idx := strings.Index(f, "+"); idx >= 0 {
			f = f[:idx]
		}
		parts := strings.SplitN(strings.TrimPrefix(f, "v"), ".", 3)
		if len(parts) == 3 {
			return f
		}
	}
	return ""
}

func (c *Collector) collectNodes(info *ClusterInfo) error {
	out, err := c.ssh.Run(c.kubectlBin() + " get nodes -o json 2>/dev/null")
	if err != nil {
		return fmt.Errorf("listing nodes: %w", err)
	}

	var nodeList struct {
		Items []struct {
			Metadata struct {
				Name   string            `json:"name"`
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
			Spec struct {
				Taints []struct {
					Key    string `json:"key"`
					Effect string `json:"effect"`
				} `json:"taints"`
			} `json:"spec"`
			Status struct {
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
				Addresses []struct {
					Type    string `json:"type"`
					Address string `json:"address"`
				} `json:"addresses"`
			} `json:"status"`
		} `json:"items"`
	}

	if err := json.Unmarshal([]byte(out), &nodeList); err != nil {
		return fmt.Errorf("parsing node list: %w", err)
	}

	// Track whether any control-plane node carries the NoSchedule taint.
	cpNoScheduleCount := 0
	cpCount := 0

	for _, item := range nodeList.Items {
		node := Node{Name: item.Metadata.Name}

		// Determine status
		for _, cond := range item.Status.Conditions {
			if cond.Type == "Ready" {
				if cond.Status == "True" {
					node.Status = "Ready"
				} else {
					node.Status = "NotReady"
				}
				break
			}
		}

		// Determine roles
		roles := []string{}
		for label := range item.Metadata.Labels {
			if strings.HasPrefix(label, "node-role.kubernetes.io/") {
				role := strings.TrimPrefix(label, "node-role.kubernetes.io/")
				roles = append(roles, role)
			}
		}
		node.Roles = strings.Join(roles, ",")

		// Check if control plane
		_, isCP := item.Metadata.Labels["node-role.kubernetes.io/control-plane"]
		_, isMaster := item.Metadata.Labels["node-role.kubernetes.io/master"]
		node.IsControlPlane = isCP || isMaster

		// Internal IP
		for _, addr := range item.Status.Addresses {
			if addr.Type == "InternalIP" {
				node.InternalIP = addr.Address
				break
			}
		}

		if node.IsControlPlane {
			cpCount++
			for _, t := range item.Spec.Taints {
				if t.Key == "node-role.kubernetes.io/control-plane" && t.Effect == "NoSchedule" {
					cpNoScheduleCount++
					break
				}
			}
		}

		info.Nodes = append(info.Nodes, node)
	}

	// allowSchedulingOnControlPlane = true when there are control-plane nodes
	// AND none of them carry the NoSchedule taint (i.e. the source cluster
	// allows scheduling on the control plane).
	if cpCount > 0 && cpNoScheduleCount == 0 {
		info.AllowSchedulingOnControlPlane = true
	}

	return nil
}

func (c *Collector) detectDatastore(info *ClusterInfo) {
	if c.clusterType == ClusterTypeKubeadm {
		// kubeadm always uses etcd (either stacked or external).
		info.DatastoreType = "etcd"
		return
	}
	// k3s with --cluster-init runs embedded etcd; etcd member files appear under
	// the etcd/member directory.  A bare etcd/ directory can exist even in SQLite
	// mode (k3s creates it), so we look for the member subdirectory specifically.
	if c.ssh.FileExists("/var/lib/rancher/k3s/server/db/etcd/member") {
		info.DatastoreType = "etcd"
	} else {
		info.DatastoreType = "sqlite"
	}
}

func (c *Collector) collectWorkloads(info *ClusterInfo) error {
	nsOut, err := c.ssh.Run(c.kubectlBin() + " get namespaces -o jsonpath='{.items[*].metadata.name}' 2>/dev/null")
	if err != nil {
		return err
	}
	info.Namespaces = strings.Fields(nsOut)

	countOut, _ := c.ssh.Run(
		c.kubectlBin() + " get deployments,statefulsets,daemonsets --all-namespaces --no-headers 2>/dev/null | wc -l")
	fmt.Sscanf(strings.TrimSpace(countOut), "%d", &info.WorkloadCount)

	c.detectWorkloadFeatures(info)
	return nil
}

// detectWorkloadFeatures inspects running workloads and records which Talos
// machine-config knobs are required for them to function after migration.
func (c *Collector) detectWorkloadFeatures(info *ClusterInfo) {
	// k3s ServiceLB (klipper-lb): k3s creates a DaemonSet named svclb-<svc>
	// in kube-system for every LoadBalancer service.  The pods set unsafe
	// sysctls (net.ipv4.ip_forward, net.ipv4.conf.all.forwarding) that Talos's
	// kubelet must whitelist via allowedUnsafeSysctls.
	svclbOut, _ := c.ssh.RunNoSudo(
		c.kubectlBin() + ` get daemonsets -n kube-system --no-headers 2>/dev/null | awk '{print $1}' | grep -c '^svclb-' || true`)
	var n int
	parsed, _ := fmt.Sscanf(strings.TrimSpace(svclbOut), "%d", &n)
	if parsed == 1 && n > 0 {
		info.WorkloadFeatures.HasServiceLB = true
		info.WorkloadFeatures.AllowedUnsafeSysctls = appendUnique(
			info.WorkloadFeatures.AllowedUnsafeSysctls,
			"net.ipv4.ip_forward",
			"net.ipv4.conf.all.forwarding",
		)
	}
}

// appendUnique appends values to dst, skipping any already present.
func appendUnique(dst []string, vals ...string) []string {
	seen := make(map[string]bool, len(dst))
	for _, v := range dst {
		seen[v] = true
	}
	for _, v := range vals {
		if !seen[v] {
			dst = append(dst, v)
			seen[v] = true
		}
	}
	return dst
}

func (c *Collector) collectPVs(info *ClusterInfo) error {
	out, err := c.ssh.Run(c.kubectlBin() + " get pv -o json 2>/dev/null")
	if err != nil {
		return err
	}

	var pvList struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Capacity         map[string]string `json:"capacity"`
				StorageClassName string            `json:"storageClassName"`
				ClaimRef         *struct {
					Namespace string `json:"namespace"`
					Name      string `json:"name"`
				} `json:"claimRef"`
				HostPath *struct {
					Path string `json:"path"`
				} `json:"hostPath"`
				Local *struct {
					Path string `json:"path"`
				} `json:"local"`
			} `json:"spec"`
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}

	if err := json.Unmarshal([]byte(out), &pvList); err != nil {
		return nil
	}

	for _, item := range pvList.Items {
		pv := PV{
			Name:         item.Metadata.Name,
			Capacity:     item.Spec.Capacity["storage"],
			StorageClass: item.Spec.StorageClassName,
			Phase:        item.Status.Phase,
		}
		if item.Spec.ClaimRef != nil {
			pv.ClaimRef = fmt.Sprintf("%s/%s", item.Spec.ClaimRef.Namespace, item.Spec.ClaimRef.Name)
		}
		// Capture the host path for local-path-provisioner PVs.
		// local-path-provisioner uses spec.hostPath; local PVs use spec.local.
		if item.Spec.HostPath != nil {
			pv.HostPath = item.Spec.HostPath.Path
		} else if item.Spec.Local != nil {
			pv.HostPath = item.Spec.Local.Path
		}
		info.PVs = append(info.PVs, pv)
	}
	info.PVCount = len(info.PVs)

	c.detectLocalPathProvisioner(info)
	return nil
}

// detectLocalPathProvisioner checks if local-path-provisioner is running and
// identifies its host storage path from the ConfigMap or the PV paths.
func (c *Collector) detectLocalPathProvisioner(info *ClusterInfo) {
	// Check for the local-path-provisioner deployment (present in all k3s clusters
	// by default, and optionally in kubeadm clusters).
	out, _ := c.ssh.RunNoSudo(
		c.kubectlBin() + ` get deploy -n kube-system local-path-provisioner --no-headers 2>/dev/null | wc -l`)
	if strings.TrimSpace(out) == "0" || strings.TrimSpace(out) == "" {
		return
	}

	info.LocalPath.Detected = true

	// Try to read the config.json from the local-path-config ConfigMap to get
	// the configured host path.
	cfgOut, err := c.ssh.RunNoSudo(
		c.kubectlBin() + ` get configmap -n kube-system local-path-config -o jsonpath='{.data.config\.json}' 2>/dev/null`)
	if err == nil && cfgOut != "" {
		var lpCfg struct {
			NodePathMap []struct {
				Paths []string `json:"paths"`
			} `json:"nodePathMap"`
		}
		if json.Unmarshal([]byte(cfgOut), &lpCfg) == nil {
			for _, n := range lpCfg.NodePathMap {
				if len(n.Paths) > 0 {
					info.LocalPath.HostPath = n.Paths[0]
					break
				}
			}
		}
	}

	// Fallback: infer from existing PV paths.
	if info.LocalPath.HostPath == "" {
		for _, pv := range info.PVs {
			if pv.StorageClass == "local-path" && pv.HostPath != "" {
				// The PV path looks like /opt/local-path-provisioner/pvc-xxx_ns_name.
				// Extract the parent directory.
				info.LocalPath.HostPath = filepath.Dir(pv.HostPath)
				break
			}
		}
	}

	// Final fallback: k3s default.
	if info.LocalPath.HostPath == "" {
		info.LocalPath.HostPath = "/opt/local-path-provisioner"
	}
}

// detectNetworkCIDRs attempts to discover the cluster-wide pod and service CIDRs.
//
// Pod CIDR:     read from the first node's spec.podCIDR and expand it to the
//               cluster-level prefix.  k3s allocates /24 per node from a /16,
//               so we expand the /24 to its containing /16.
// Service CIDR: not exposed via the Kubernetes API; we default to the k3s
//               default (10.43.0.0/16) which is correct for the vast majority
//               of k3s installations.
//
// Falls back to k3s defaults (10.42.0.0/16 / 10.43.0.0/16) when detection fails.
func (c *Collector) detectNetworkCIDRs(info *ClusterInfo) {
	const defaultPodCIDR     = "10.42.0.0/16"
	const defaultServiceCIDR = "10.43.0.0/16"

	// ── Pod CIDR ──────────────────────────────────────────────────────────────
	out, err := c.ssh.Run(c.kubectlBin() +
		" get nodes -o jsonpath='{.items[0].spec.podCIDR}' 2>/dev/null")
	nodeCIDR := strings.TrimSpace(strings.Trim(out, "'"))
	if err == nil && nodeCIDR != "" {
		_, ipnet, parseErr := net.ParseCIDR(nodeCIDR)
		if parseErr == nil {
			ones, bits := ipnet.Mask.Size()
			_ = bits
			// Expand the per-node subnet to the cluster-level CIDR.
			// k3s allocates /24 per node from a /16; for other prefixes keep as-is.
			if ones >= 16 {
				// Mask to the /16 containing this subnet.
				clusterMask := net.CIDRMask(16, 32)
				clusterIP := ipnet.IP.Mask(clusterMask)
				info.PodCIDR = fmt.Sprintf("%s/16", clusterIP.String())
			} else {
				info.PodCIDR = ipnet.String()
			}
		}
	}
	if info.PodCIDR == "" {
		info.PodCIDR = defaultPodCIDR
	}

	// ── Service CIDR ──────────────────────────────────────────────────────────
	// For kubeadm, read --service-cluster-ip-range from the kube-apiserver
	// static-pod manifest.  The kubeadm default (10.96.0.0/12) differs from
	// the k3s default (10.43.0.0/16); using the wrong value causes existing
	// ClusterIP services (kubernetes.default, kube-dns) to be unreachable.
	if c.clusterType == ClusterTypeKubeadm {
		const defaultKubeadmServiceCIDR = "10.96.0.0/12"
		out, _ := c.ssh.Run(
			`grep -o -- '--service-cluster-ip-range=[^ "]*' ` +
				`/etc/kubernetes/manifests/kube-apiserver.yaml 2>/dev/null | cut -d= -f2`)
		cidr := strings.TrimSpace(strings.Trim(out, "'"))
		if cidr != "" {
			info.ServiceCIDR = cidr
		} else {
			info.ServiceCIDR = defaultKubeadmServiceCIDR
		}
		return
	}
	info.ServiceCIDR = defaultServiceCIDR
}
