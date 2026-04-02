# k2t

Migrate a machine running [k3s](https://k3s.io) or [kubeadm](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/) to [Talos Linux](https://talos.dev) over SSH — no physical access or reinstall required.

k2t connects to the remote node, backs up cluster state, generates a Talos config matched to the source cluster, installs Talos in-place via kexec, and bootstraps Kubernetes — preserving workloads when restoring from an etcd snapshot.

> **WARNING:** This process is **irreversible**. The target machine's OS will be completely erased and replaced with Talos Linux.

## How it works

```
k2t migrate ubuntu@10.1.1.1
```

1. **Collect** — SSH into the node, detect the cluster type (k3s/kubeadm), gather version, CIDRs, workloads, and hardware info
2. **Backup** — Snapshot etcd (or SQLite), export Kubernetes resources to YAML
3. **Generate** — Run `talosctl gen config` with settings matched to the source cluster
4. **Deploy** — Upload the Talos installer agent, write the disk image, reboot via kexec
5. **Bootstrap** — Wait for Talos, apply config, restore etcd, retrieve kubeconfig

## Installation

### From source

```bash
git clone https://github.com/rothgar/k2t
cd k2t
make build
# ./k2t is now ready to use
```

### With go install

```bash
go install github.com/rothgar/k2t@latest
```

### Prerequisites

**Local machine:**
- [`talosctl`](https://github.com/siderolabs/talos/releases) in `$PATH`
- `kubectl` in `$PATH`
- SSH access to the target node

**Remote node:**
- k3s server or kubeadm control plane running on Linux
- `sudo` access (or root)
- UEFI boot

## Usage

### Full migration

```bash
k2t migrate ubuntu@10.1.1.1
```

k2t prompts for confirmation before modifying the remote machine.

### k3s with SQLite (default k3s datastore)

Use `--migrate-to-etcd` to convert SQLite to embedded etcd first — required for snapshot restore on Talos:

```bash
k2t migrate ubuntu@10.1.1.1 --migrate-to-etcd
```

### Dry run

Collect info and show what would happen without touching the remote machine:

```bash
k2t migrate ubuntu@10.1.1.1 --dry-run
```

### Resume an interrupted migration

Progress is saved to `<backup-dir>/migration-state.json`. Resume without repeating completed phases:

```bash
k2t migrate ubuntu@10.1.1.1 --resume
```

### Add a control plane node

For multi-CP clusters, join additional control plane nodes after migrating the first one. The existing cluster is validated as healthy before proceeding:

```bash
k2t join-controlplane ubuntu@10.1.1.2 \
  --controlplane-config ./k3s-backup/talos-config/controlplane.yaml \
  --talosconfig         ./k3s-backup/talos-config/talosconfig
```

No etcd restore or bootstrap is needed — the new node discovers the existing etcd cluster and joins automatically.

### Add a worker node

After migrating the control plane, convert worker nodes:

```bash
k2t join-worker ubuntu@10.1.1.3 \
  --worker-config ./k3s-backup/talos-config/worker.yaml \
  --talosconfig   ./k3s-backup/talos-config/talosconfig
```

### Multi-node migration order

For a 3-CP + 2-worker cluster:

```bash
# 1. Migrate the first control plane (etcd restore)
k2t migrate ubuntu@10.1.1.1 --migrate-to-etcd

# 2. Join additional control plane nodes
k2t join-controlplane ubuntu@10.1.1.2 \
  --controlplane-config ./k3s-backup/talos-config/controlplane.yaml \
  --talosconfig ./k3s-backup/talos-config/talosconfig

k2t join-controlplane ubuntu@10.1.1.3 \
  --controlplane-config ./k3s-backup/talos-config/controlplane.yaml \
  --talosconfig ./k3s-backup/talos-config/talosconfig

# 3. Join workers
k2t join-worker ubuntu@10.1.1.4 \
  --worker-config ./k3s-backup/talos-config/worker.yaml \
  --talosconfig ./k3s-backup/talos-config/talosconfig

k2t join-worker ubuntu@10.1.1.5 \
  --worker-config ./k3s-backup/talos-config/worker.yaml \
  --talosconfig ./k3s-backup/talos-config/talosconfig
```

### Collect only (no migration)

Back up cluster state without migrating:

```bash
k2t collect ubuntu@10.1.1.1
```

### Generate config only

Generate Talos machine configs from an existing backup:

```bash
k2t generate --cluster-endpoint 10.1.1.1 --backup-dir ./k3s-backup
```

## SSH

k2t reads `~/.ssh/config` for host aliases, users, ports, and identity files:

```bash
k2t migrate myserver                          # ~/.ssh/config alias
k2t migrate ubuntu@10.1.1.1                   # inline user@host
k2t migrate 10.1.1.1 --ssh-key ~/.ssh/mykey   # explicit key
k2t migrate 10.1.1.1 --ssh-port 2222          # explicit port
```

SSH agent (`SSH_AUTH_SOCK`) is used automatically when available. When the SSH user is not `root`, commands are run with `sudo` (password is prompted once if needed).

## Flags

### Global

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | | SSH target `[user@]host` (alternative to positional arg) |
| `--ssh-key` | | SSH private key path (overrides `~/.ssh/config`) |
| `--ssh-port` | `22` | SSH port (overrides `~/.ssh/config`) |
| `--backup-dir` | `./k3s-backup` | Local directory for backups and configs |
| `-v`, `--verbose` | | Print each remote command and its output |

### `migrate`

| Flag | Default | Description |
|------|---------|-------------|
| `--talos-version` | `v1.12.6` | Talos Linux version to install |
| `--cluster-name` | _(from source)_ | Talos cluster name |
| `--migrate-to-etcd` | | Convert k3s SQLite to etcd before backup |
| `--dry-run` | | Show plan without modifying anything |
| `--resume` | | Resume from last completed phase |
| `--yes` | | Skip confirmation prompt (CI/automation) |

### `join-controlplane`

| Flag | Default | Description |
|------|---------|-------------|
| `--talos-version` | `v1.12.6` | Talos Linux version to install |
| `--controlplane-config` | _(required)_ | Path to `controlplane.yaml` from initial migration |
| `--talosconfig` | _(required)_ | Path to `talosconfig` from initial migration |
| `--skip-health-check` | | Skip cluster health validation before joining |

### `join-worker`

| Flag | Default | Description |
|------|---------|-------------|
| `--talos-version` | `v1.12.6` | Talos Linux version to install |
| `--worker-config` | _(required)_ | Path to `worker.yaml` from control plane migration |
| `--talosconfig` | _(required)_ | Path to `talosconfig` from control plane migration |

### `generate`

| Flag | Default | Description |
|------|---------|-------------|
| `--cluster-endpoint` | | Control plane IP or hostname |
| `--cluster-name` | `talos-cluster` | Talos cluster name |
| `--talos-version` | `v1.12.6` | Talos version to target |
| `--kubernetes-version` | _(talosctl default)_ | Kubernetes version for the config |

## What gets preserved

| Item | Preserved | Notes |
|------|:---------:|-------|
| Deployments, StatefulSets, DaemonSets | Yes | Via etcd snapshot restore |
| Services and Ingresses | Yes | Via etcd snapshot restore |
| ConfigMaps and Secrets | Yes | Via etcd snapshot restore |
| RBAC (Roles, ClusterRoles, Bindings) | Yes | Via etcd snapshot restore |
| Pod and Service CIDRs | Yes | Matched in generated Talos config |
| Kubernetes version | Yes | Passed to `talosctl gen config` |
| PersistentVolume **data** | **No** | Disk is erased — back up PV data separately |

## Backup layout

```
k3s-backup/
├── migration-state.json          # phase progress (for --resume)
├── k3s.yaml                      # source cluster kubeconfig
├── database/
│   ├── etcd-snapshot.db          # etcd snapshot (restored on Talos)
│   └── backup-info.json
├── talos-config/
│   ├── talosconfig               # talosctl client config
│   ├── controlplane.yaml         # control plane machine config
│   └── worker.yaml               # worker machine config
├── talos-kubeconfig              # kubeconfig for the new cluster
└── resources/                    # exported Kubernetes resources
    ├── deployments/
    ├── services/
    ├── configmaps/
    └── ...
```

## After migration

Access the new cluster:

```bash
export KUBECONFIG=./k3s-backup/talos-kubeconfig
kubectl get nodes
```

Use `talosctl`:

```bash
export TALOSCONFIG=./k3s-backup/talos-config/talosconfig
talosctl --nodes 10.1.1.1 health
talosctl --nodes 10.1.1.1 dashboard
```
