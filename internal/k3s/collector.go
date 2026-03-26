package k3s

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/briandowns/spinner"
	"github.com/rothgar/k3s-to-talos/internal/ssh"
	"time"
)

// ClusterInfo holds information about the k3s cluster gathered remotely.
type ClusterInfo struct {
	K3sVersion    string  `json:"k3s_version"`
	K8sVersion    string  `json:"k8s_version"`
	ClusterName   string  `json:"cluster_name"`
	Nodes         []Node  `json:"nodes"`
	DatastoreType string  `json:"datastore_type"` // "etcd" | "sqlite"
	Namespaces    []string `json:"namespaces"`
	WorkloadCount int     `json:"workload_count"`
	PVCount       int     `json:"pv_count"`
	PVs           []PV    `json:"pvs,omitempty"`
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
}

// Collector gathers k3s cluster information via SSH.
type Collector struct {
	ssh *ssh.Client
}

// NewCollector creates a new Collector.
func NewCollector(ssh *ssh.Client) *Collector {
	return &Collector{ssh: ssh}
}

// Collect gathers all cluster information from the remote node.
func (c *Collector) Collect() (*ClusterInfo, error) {
	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
	s.Suffix = " Collecting k3s cluster information..."
	s.Start()
	defer s.Stop()

	info := &ClusterInfo{}

	if err := c.verifyK3sServer(); err != nil {
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

	return info, nil
}

func (c *Collector) verifyK3sServer() error {
	out, err := c.ssh.Run("systemctl is-active k3s 2>/dev/null || systemctl is-active k3s-server 2>/dev/null || echo inactive")
	if err != nil || strings.TrimSpace(out) == "inactive" {
		return fmt.Errorf("k3s server service does not appear to be running on the target machine (got: %q)", out)
	}

	// Confirm it's in server mode (not just agent)
	serverConfig := c.ssh.FileExists("/etc/rancher/k3s/k3s.yaml") ||
		c.ssh.FileExists("/var/lib/rancher/k3s/server")
	if !serverConfig {
		return fmt.Errorf("target machine does not appear to be a k3s server node (missing /etc/rancher/k3s/k3s.yaml)")
	}

	return nil
}

func (c *Collector) collectVersion(info *ClusterInfo) error {
	v, err := c.ssh.Run("k3s --version 2>/dev/null | head -1")
	if err != nil {
		return fmt.Errorf("getting k3s version: %w", err)
	}
	info.K3sVersion = strings.TrimSpace(v)

	kv, _ := c.ssh.Run("k3s kubectl version --short 2>/dev/null | grep 'Server Version' | awk '{print $3}'")
	info.K8sVersion = strings.TrimSpace(kv)
	return nil
}

func (c *Collector) collectNodes(info *ClusterInfo) error {
	out, err := c.ssh.Run("k3s kubectl get nodes -o json 2>/dev/null")
	if err != nil {
		return fmt.Errorf("listing nodes: %w", err)
	}

	var nodeList struct {
		Items []struct {
			Metadata struct {
				Name   string            `json:"name"`
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
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

		info.Nodes = append(info.Nodes, node)
	}

	return nil
}

func (c *Collector) detectDatastore(info *ClusterInfo) {
	if c.ssh.FileExists("/var/lib/rancher/k3s/server/db/etcd") {
		info.DatastoreType = "etcd"
	} else {
		info.DatastoreType = "sqlite"
	}
}

func (c *Collector) collectWorkloads(info *ClusterInfo) error {
	nsOut, err := c.ssh.Run("k3s kubectl get namespaces -o jsonpath='{.items[*].metadata.name}' 2>/dev/null")
	if err != nil {
		return err
	}
	info.Namespaces = strings.Fields(nsOut)

	countOut, _ := c.ssh.Run(
		"k3s kubectl get deployments,statefulsets,daemonsets --all-namespaces --no-headers 2>/dev/null | wc -l")
	fmt.Sscanf(strings.TrimSpace(countOut), "%d", &info.WorkloadCount)
	return nil
}

func (c *Collector) collectPVs(info *ClusterInfo) error {
	out, err := c.ssh.Run("k3s kubectl get pv -o json 2>/dev/null")
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
		info.PVs = append(info.PVs, pv)
	}
	info.PVCount = len(info.PVs)
	return nil
}
