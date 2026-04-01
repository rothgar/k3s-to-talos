package k3s

import (
	"fmt"
	"strings"

	"github.com/rothgar/k3s-to-talos/internal/ssh"
)

// Cluster type constants.
const (
	ClusterTypeK3s     = "k3s"
	ClusterTypeKubeadm = "kubeadm"
)

// Detect inspects the remote machine and returns a Collector pre-configured
// for the detected Kubernetes distribution.
//
// Detection order:
//  1. k3s   — checked first (k3s binary + service active + server config dir)
//  2. kubeadm — kubelet service + /etc/kubernetes/ config dir
//
// All detection checks are intentionally run without sudo so they work in
// non-interactive SSH sessions where sudo may require a TTY or password.
//
// Returns an error when neither distribution is found.
func Detect(client *ssh.Client) (*Collector, error) {
	if hasK3sServer(client) {
		return &Collector{ssh: client, clusterType: ClusterTypeK3s}, nil
	}
	if hasKubeadm(client) {
		return &Collector{ssh: client, clusterType: ClusterTypeKubeadm}, nil
	}
	return nil, fmt.Errorf(
		"no supported Kubernetes distribution found on the target machine\n\n" +
			"Supported distributions:\n" +
			"  • k3s      — requires k3s running in server (control-plane) mode\n" +
			"  • kubeadm  — requires kubelet active and /etc/kubernetes/admin.conf present\n\n" +
			"Ensure the node is a control-plane node (not a worker/agent only).",
	)
}

// hasK3sServer returns true when the remote machine is running k3s in server
// mode.  All checks run without sudo so they work in non-interactive sessions.
func hasK3sServer(client *ssh.Client) bool {
	// 1. k3s binary must be present (world-executable — no root needed).
	if _, err := client.RunNoSudo("command -v k3s"); err != nil {
		return false
	}

	// 2. k3s service must be active (systemctl status queries work for any user).
	out, _ := client.RunNoSudo(
		`systemctl is-active k3s 2>/dev/null || ` +
			`systemctl is-active k3s-server 2>/dev/null || ` +
			`(pgrep -x k3s >/dev/null 2>&1 && echo active) || ` +
			`(pgrep -f 'k3s server' >/dev/null 2>&1 && echo active) || ` +
			`echo inactive`)
	if strings.TrimSpace(out) == "inactive" {
		return false
	}

	// 3. Server config directory must exist.  /etc/rancher/k3s/ is created by
	//    the server installer and is mode 755 — readable without root.  A
	//    k3s agent-only node does NOT have this directory.
	_, err := client.RunNoSudo("test -d /etc/rancher/k3s")
	return err == nil
}

// hasKubeadm returns true when the remote machine is a kubeadm control-plane
// node.  All checks run without sudo.
func hasKubeadm(client *ssh.Client) bool {
	// 1. kubelet must be running.
	out, _ := client.RunNoSudo(
		`systemctl is-active kubelet 2>/dev/null || ` +
			`(pgrep -x kubelet >/dev/null 2>&1 && echo active) || ` +
			`echo inactive`)
	if strings.TrimSpace(out) == "inactive" {
		return false
	}

	// 2. /etc/kubernetes/ is created by kubeadm and is mode 755.
	//    admin.conf inside is 600 (root-only), but the directory itself is
	//    world-readable, which is sufficient for detection.
	_, err := client.RunNoSudo("test -d /etc/kubernetes")
	return err == nil
}
