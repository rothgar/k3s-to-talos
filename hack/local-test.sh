#!/usr/bin/env bash
# hack/local-test.sh — Local KVM test framework for k3s-to-talos.
# Mirrors the EC2 CI workflow using QEMU/KVM VMs and cloud-init.
#
# Usage: ./hack/local-test.sh [OPTIONS] [TEST_TYPE]
#
# TEST_TYPE: k3s-single|k3s-multi|kubeadm-single|kubeadm-multi|all
# Default:   k3s-single

set -euo pipefail

# ---------------------------------------------------------------------------
# Colours and logging
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
log_info() { printf "${CYAN}[INFO]${NC}  %s\n" "$*"; }
log_ok()   { printf "${GREEN}[OK]${NC}    %s\n" "$*"; }
log_warn() { printf "${YELLOW}[WARN]${NC}  %s\n" "$*"; }
log_err()  { printf "${RED}[ERROR]${NC} %s\n" "$*" >&2; }
die()      { log_err "$*"; exit 1; }

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
TALOS_VERSION="${TALOS_VERSION:-v1.12.6}"
KEEP_VMS=false
CLEAN=false
BINARY=""
NO_BUILD=false
TEST_TYPE="k3s-single"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${HOME}/.cache/k3s-to-talos-test"
WORK_DIR="/tmp/k3s-to-talos-test-$$"
SCRIPTS_DIR="${REPO_ROOT}/.github/scripts"

UBUNTU_IMG_URL="https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"
UBUNTU_IMG_NAME="noble-server-cloudimg-amd64.img"

# Bridge networking (preferred for multi-node)
BRIDGE="virbr0"
CP_IP="192.168.122.10"
WORKER_IP="192.168.122.11"
GATEWAY="192.168.122.1"
DNS="192.168.122.1"
CP_MAC="52:54:00:12:34:10"
WORKER_MAC="52:54:00:12:34:11"

# User-mode port forwards (single-node fallback)
UM_SSH_PORT=10022
UM_TALOS_PORT=10500
UM_K8S_PORT=10443

# OVMF paths (tried in order)
OVMF_CODE_CANDIDATES=(
  /usr/share/OVMF/OVMF_CODE.fd
  /usr/share/OVMF/OVMF_CODE_4M.fd
  /usr/share/edk2-ovmf/BIOS/OVMF_CODE.fd
)
OVMF_VARS_CANDIDATES=(
  /usr/share/OVMF/OVMF_VARS.fd
  /usr/share/OVMF/OVMF_VARS_4M.fd
  /usr/share/edk2-ovmf/BIOS/OVMF_VARS.fd
)

OVMF_CODE=""
OVMF_VARS_TEMPLATE=""
USE_BRIDGE=false
ISO_TOOL=""

# Tracks PIDs for cleanup
declare -a VM_NAMES=()

# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------
usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS] [TEST_TYPE]

TEST_TYPE: k3s-single|k3s-multi|kubeadm-single|kubeadm-multi|all  (default: k3s-single)

Options:
  --talos-version V   Talos version (default: ${TALOS_VERSION})
  --keep-vms          Don't destroy VMs after test (for debugging)
  --clean             Remove cache and temp dirs, then exit
  --binary PATH       k2t binary path (default: build with 'go build')
  --no-build          Skip building binary (use existing ./k2t)
  --setup             Print prerequisite install instructions, then exit
  -h, --help          Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --talos-version) TALOS_VERSION="$2"; shift 2 ;;
    --keep-vms)      KEEP_VMS=true; shift ;;
    --clean)         CLEAN=true; shift ;;
    --binary)        BINARY="$2"; shift 2 ;;
    --no-build)      NO_BUILD=true; shift ;;
    --setup)
      cat <<'SETUP'
## Required packages (Ubuntu/Debian):
sudo apt-get install -y qemu-kvm qemu-utils ovmf cloud-image-utils talosctl kubectl

## Required packages (Fedora/RHEL):
sudo dnf install -y qemu-kvm qemu-img edk2-ovmf cloud-utils talosctl kubectl

## For multi-node bridge networking (Ubuntu/Debian):
sudo apt-get install -y libvirt-daemon-system libvirt-clients
sudo usermod -aG libvirt $USER  # then log out and back in
sudo tee /etc/qemu/bridge.conf <<'EOF'
allow virbr0
EOF
sudo chmod 640 /etc/qemu/bridge.conf
sudo chown root:kvm /etc/qemu/bridge.conf
SETUP
      exit 0 ;;
    -h|--help) usage; exit 0 ;;
    k3s-single|k3s-multi|kubeadm-single|kubeadm-multi|all)
      TEST_TYPE="$1"; shift ;;
    *) die "Unknown argument: $1  (run with --help for usage)" ;;
  esac
done

# ---------------------------------------------------------------------------
# Clean mode
# ---------------------------------------------------------------------------
if $CLEAN; then
  log_info "Removing cache dir: ${CACHE_DIR}"
  rm -rf "${CACHE_DIR}"
  log_info "Removing stale work dirs under /tmp/k3s-to-talos-test-*"
  rm -rf /tmp/k3s-to-talos-test-*
  log_ok "Clean complete."
  exit 0
fi

# ---------------------------------------------------------------------------
# Prerequisite detection
# ---------------------------------------------------------------------------
check_prerequisites() {
  local missing=() warn=()

  # Required binaries
  for cmd in qemu-system-x86_64 qemu-img talosctl kubectl; do
    command -v "$cmd" &>/dev/null || missing+=("$cmd")
  done

  # /dev/kvm
  [[ -e /dev/kvm ]] || missing+=("/dev/kvm (KVM not available; enable virtualisation or load kvm modules)")

  # OVMF firmware
  local found_code=false found_vars=false
  for f in "${OVMF_CODE_CANDIDATES[@]}"; do
    if [[ -f "$f" ]]; then OVMF_CODE="$f"; found_code=true; break; fi
  done
  for f in "${OVMF_VARS_CANDIDATES[@]}"; do
    if [[ -f "$f" ]]; then OVMF_VARS_TEMPLATE="$f"; found_vars=true; break; fi
  done
  $found_code || missing+=("OVMF_CODE.fd (install ovmf / edk2-ovmf)")
  $found_vars || missing+=("OVMF_VARS.fd (install ovmf / edk2-ovmf)")

  # Cloud-init ISO tool
  if command -v cloud-localds &>/dev/null; then
    ISO_TOOL="cloud-localds"
  elif command -v genisoimage &>/dev/null; then
    ISO_TOOL="genisoimage"
  elif command -v mkisofs &>/dev/null; then
    ISO_TOOL="mkisofs"
  else
    missing+=("cloud-localds or genisoimage (install cloud-image-utils or genisoimage)")
  fi

  # Bridge networking check
  USE_BRIDGE=false
  if [[ -d /sys/class/net/${BRIDGE} ]]; then
    local bridge_helper="/usr/lib/qemu/qemu-bridge-helper"
    local bridge_conf_ok=false
    for conf in /etc/qemu/bridge.conf /etc/qemu-kvm/bridge.conf; do
      if [[ -f "$conf" ]] && grep -q "allow ${BRIDGE}" "$conf" 2>/dev/null; then
        bridge_conf_ok=true; break
      fi
    done
    if [[ -x "$bridge_helper" ]] && $bridge_conf_ok; then
      USE_BRIDGE=true
      log_ok "Bridge mode available (${BRIDGE}) — multi-node tests supported."
    else
      warn+=("Bridge helper not configured; falling back to user-mode networking (single-node only).")
      warn+=("  Run: ./hack/local-test.sh --setup  for setup instructions.")
    fi
  else
    warn+=("${BRIDGE} not found; using user-mode networking (single-node only).")
  fi

  for w in "${warn[@]}"; do log_warn "$w"; done

  if [[ ${#missing[@]} -gt 0 ]]; then
    log_err "Missing prerequisites:"
    for m in "${missing[@]}"; do log_err "  - $m"; done
    log_err "Run: ./hack/local-test.sh --setup  for install instructions."
    exit 1
  fi

  log_ok "Prerequisites satisfied. OVMF: ${OVMF_CODE}  ISO tool: ${ISO_TOOL}"
}

# ---------------------------------------------------------------------------
# Cleanup / EXIT trap
# ---------------------------------------------------------------------------
cleanup() {
  if $KEEP_VMS; then
    log_warn "--keep-vms set; leaving VMs running. Work dir: ${WORK_DIR}"
    log_warn "SSH into CP:     ssh -i ${WORK_DIR}/ci_key -p ${CP_SSH_PORT:-22} ubuntu@${CP_HOST:-${CP_IP}}"
    return
  fi
  log_info "Cleaning up VMs…"
  for name in "${VM_NAMES[@]}"; do
    vm_stop "$name" || true
  done
  [[ -d "${WORK_DIR}" ]] && rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Helper: SSH
# ---------------------------------------------------------------------------
run_ssh() {
  local host="$1" port="$2" cmd="$3"
  ssh -q \
    -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null \
    -o ConnectTimeout=5 \
    -i "${WORK_DIR}/ci_key" \
    -p "${port}" \
    "ubuntu@${host}" \
    "${cmd}"
}

wait_for_ssh() {
  local host="$1" port="$2" timeout="$3"
  local deadline=$(( $(date +%s) + timeout ))
  log_info "Waiting for SSH on ${host}:${port} (timeout ${timeout}s)…"
  while (( $(date +%s) < deadline )); do
    if run_ssh "$host" "$port" "true" 2>/dev/null; then
      log_ok "SSH ready on ${host}:${port}"
      return 0
    fi
    sleep 5
  done
  log_err "Timed out waiting for SSH on ${host}:${port}"
  return 1
}

# ---------------------------------------------------------------------------
# Helper: cloud-init ISO
# ---------------------------------------------------------------------------
make_cloud_init_iso() {
  local dir="$1"   # directory containing user-data, meta-data, network-config
  local out="$2"   # output ISO path

  case "${ISO_TOOL}" in
    cloud-localds)
      cloud-localds "${out}" \
        "${dir}/user-data" "${dir}/meta-data" \
        --network-config="${dir}/network-config"
      ;;
    genisoimage|mkisofs)
      "${ISO_TOOL}" -output "${out}" -volid cidata -joliet -rock \
        "${dir}/user-data" "${dir}/meta-data" "${dir}/network-config"
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Helper: prepare cloud-init files for a VM
# ---------------------------------------------------------------------------
prepare_cloud_init() {
  local name="$1"
  local userdata_src="$2"   # path to source user-data script
  local ip="$3"             # static IP or "dhcp"
  local ci_dir="${WORK_DIR}/ci-${name}"
  mkdir -p "${ci_dir}"

  # meta-data
  cat > "${ci_dir}/meta-data" <<EOF
instance-id: ${name}
local-hostname: ${name}
EOF

  # user-data — substitute SSH public key placeholder
  local pubkey
  pubkey="$(cat "${WORK_DIR}/ci_key.pub")"
  sed "s|__SSH_PUBLIC_KEY__|${pubkey}|g" "${userdata_src}" > "${ci_dir}/user-data"

  # network-config (cloud-init v2)
  if [[ "$ip" == "dhcp" ]]; then
    cat > "${ci_dir}/network-config" <<EOF
version: 2
ethernets:
  enp1s0:
    dhcp4: true
EOF
  else
    cat > "${ci_dir}/network-config" <<EOF
version: 2
ethernets:
  enp1s0:
    addresses: [${ip}/24]
    gateway4: ${GATEWAY}
    nameservers:
      addresses: [${DNS}]
EOF
  fi

  make_cloud_init_iso "${ci_dir}" "${WORK_DIR}/${name}-seed.iso"
}

# ---------------------------------------------------------------------------
# Helper: VM start / stop
# ---------------------------------------------------------------------------
vm_start() {
  local name="$1"
  local disk="$2"
  local seed="$3"
  local mac="$4"
  local netdev_opts="${5:-}"   # override netdev line

  # Copy OVMF vars (UEFI writes to it, must be per-VM)
  cp "${OVMF_VARS_TEMPLATE}" "${WORK_DIR}/${name}-vars.fd"

  # Build netdev arguments
  local netdev_args
  if [[ -n "${netdev_opts}" ]]; then
    netdev_args="${netdev_opts}"
  elif $USE_BRIDGE; then
    netdev_args="-netdev bridge,id=net0,br=${BRIDGE} -device virtio-net-pci,netdev=net0,mac=${mac}"
  else
    netdev_args="-netdev user,id=net0,hostfwd=tcp::${UM_SSH_PORT}-:22,hostfwd=tcp::${UM_TALOS_PORT}-:50000,hostfwd=tcp::${UM_K8S_PORT}-:6443 -device virtio-net-pci,netdev=net0,mac=${mac}"
  fi

  log_info "Starting VM: ${name}  disk=${disk}"
  # shellcheck disable=SC2086
  qemu-system-x86_64 \
    -enable-kvm \
    -machine q35 \
    -cpu host \
    -smp 2 \
    -m 2048 \
    -drive if=pflash,format=raw,readonly=on,file="${OVMF_CODE}" \
    -drive if=pflash,format=raw,file="${WORK_DIR}/${name}-vars.fd" \
    -drive file="${disk}",if=virtio,format=qcow2 \
    -drive file="${seed}",media=cdrom \
    ${netdev_args} \
    -nographic \
    -serial file:"${WORK_DIR}/serial-${name}.log" \
    -daemonize \
    -pidfile "${WORK_DIR}/${name}.pid"

  VM_NAMES+=("${name}")
  log_ok "VM ${name} started (pid file: ${WORK_DIR}/${name}.pid)"
}

vm_stop() {
  local name="$1"
  local pidfile="${WORK_DIR}/${name}.pid"
  if [[ -f "${pidfile}" ]]; then
    local pid
    pid="$(cat "${pidfile}")"
    if kill -0 "${pid}" 2>/dev/null; then
      log_info "Stopping VM ${name} (pid ${pid})"
      kill "${pid}" 2>/dev/null || true
      sleep 2
      kill -9 "${pid}" 2>/dev/null || true
    fi
    rm -f "${pidfile}"
  fi
}

# ---------------------------------------------------------------------------
# Helper: create overlay disk
# ---------------------------------------------------------------------------
create_overlay_disk() {
  local name="$1"
  local base_img="$2"
  local overlay="${WORK_DIR}/${name}-disk.qcow2"
  qemu-img create -f qcow2 -b "${base_img}" -F qcow2 "${overlay}" 20G
  echo "${overlay}"
}

# ---------------------------------------------------------------------------
# Helper: download base image
# ---------------------------------------------------------------------------
ensure_base_image() {
  mkdir -p "${CACHE_DIR}"
  local img="${CACHE_DIR}/${UBUNTU_IMG_NAME}"
  if [[ ! -f "${img}" ]]; then
    log_info "Downloading Ubuntu 24.04 cloud image…"
    curl -L --progress-bar -o "${img}.tmp" "${UBUNTU_IMG_URL}"
    mv "${img}.tmp" "${img}"
    chmod 444 "${img}"   # read-only; overlays are created separately
    log_ok "Base image cached at ${img}"
  else
    log_ok "Base image already cached: ${img}"
  fi
  echo "${img}"
}

# ---------------------------------------------------------------------------
# Helper: wait for a condition via SSH
# ---------------------------------------------------------------------------
wait_for_condition() {
  local host="$1" port="$2" cmd="$3" label="$4" timeout="$5"
  local deadline=$(( $(date +%s) + timeout ))
  log_info "Waiting for: ${label} (timeout ${timeout}s)"
  while (( $(date +%s) < deadline )); do
    if run_ssh "$host" "$port" "${cmd}" &>/dev/null; then
      log_ok "${label}"
      return 0
    fi
    sleep 5
  done
  log_err "Timed out waiting for: ${label}"
  return 1
}

# ---------------------------------------------------------------------------
# Helper: wait for a condition using LOCAL kubectl against talos-kubeconfig
# Used for all post-migration checks (node is Talos, not k3s/kubeadm).
# ---------------------------------------------------------------------------
wait_for_kubectl() {
  local kubeconfig="$1" cmd="$2" label="$3" timeout="$4"
  local deadline=$(( $(date +%s) + timeout ))
  log_info "Waiting for: ${label} (timeout ${timeout}s)"
  while (( $(date +%s) < deadline )); do
    if KUBECONFIG="${kubeconfig}" bash -c "${cmd}" &>/dev/null; then
      log_ok "${label}"
      return 0
    fi
    sleep 5
  done
  log_err "Timed out waiting for: ${label}"
  return 1
}

# ---------------------------------------------------------------------------
# Helper: remove CP taint and keep re-removing for 90 s (mirrors CI)
# ---------------------------------------------------------------------------
remove_cp_taint() {
  local kubeconfig="$1"
  log_info "Removing control-plane taint (and re-checking for 90 s)…"
  KUBECONFIG="${kubeconfig}" kubectl taint nodes --all \
    node-role.kubernetes.io/control-plane:NoSchedule- 2>/dev/null || true
  for _ in $(seq 1 18); do
    sleep 5
    KUBECONFIG="${kubeconfig}" kubectl taint nodes --all \
      node-role.kubernetes.io/control-plane:NoSchedule- 2>/dev/null || true
  done
  log_ok "Control-plane taint removed."
}

# ---------------------------------------------------------------------------
# Helper: print serial console tail on failure
# ---------------------------------------------------------------------------
dump_serial_on_failure() {
  for name in "${VM_NAMES[@]}"; do
    local log="${WORK_DIR}/serial-${name}.log"
    if [[ -f "${log}" ]]; then
      log_err "=== Serial console (last 60 lines): ${name} ==="
      tail -60 "${log}" >&2
    fi
  done
}

# ---------------------------------------------------------------------------
# Build binary
# ---------------------------------------------------------------------------
build_binary() {
  if [[ -n "${BINARY}" ]]; then
    [[ -x "${BINARY}" ]] || die "Binary not executable: ${BINARY}"
    log_ok "Using provided binary: ${BINARY}"
    return
  fi
  if $NO_BUILD; then
    BINARY="${REPO_ROOT}/k2t"
    [[ -x "${BINARY}" ]] || die "--no-build set but ${BINARY} not found or not executable."
    log_ok "Using existing binary: ${BINARY}"
    return
  fi
  log_info "Building k2t…"
  (cd "${REPO_ROOT}" && go build -o k2t .)
  BINARY="${REPO_ROOT}/k2t"
  log_ok "Binary built: ${BINARY}"
}

# ---------------------------------------------------------------------------
# Run migrate command
# ---------------------------------------------------------------------------
run_migrate() {
  local host="$1" ssh_port="$2"
  log_info "Running k2t migrate on ${host}:${ssh_port}…"
  "${BINARY}" migrate \
    --host "${host}" \
    --ssh-port "${ssh_port}" \
    --ssh-user ubuntu \
    --ssh-key "${WORK_DIR}/ci_key" \
    --sudo \
    --talos-version "${TALOS_VERSION}" \
    --cluster-name local-test \
    --yes \
    --backup-dir "${WORK_DIR}/backup" \
    2>&1 | tee "${WORK_DIR}/migrate.log"
}

# ---------------------------------------------------------------------------
# Run join-worker command
# ---------------------------------------------------------------------------
run_join_worker() {
  local host="$1"
  log_info "Running k2t join-worker on ${host}…"
  "${BINARY}" join-worker \
    --host "${host}" \
    --ssh-user ubuntu \
    --ssh-key "${WORK_DIR}/ci_key" \
    --sudo \
    --talos-version "${TALOS_VERSION}" \
    --worker-config "${WORK_DIR}/backup/talos-config/worker.yaml" \
    --talosconfig "${WORK_DIR}/backup/talos-config/talosconfig" \
    --backup-dir "${WORK_DIR}/backup" \
    2>&1 | tee "${WORK_DIR}/join-worker.log"
}

# ---------------------------------------------------------------------------
# TEST: k3s-single
# ---------------------------------------------------------------------------
test_k3s_single() {
  log_info "=== TEST: k3s-single ==="
  local base_img cp_disk cp_seed

  base_img="$(ensure_base_image)"
  cp_disk="$(create_overlay_disk cp "${base_img}")"

  prepare_cloud_init cp "${SCRIPTS_DIR}/user-data-k3s.sh" \
    "$($USE_BRIDGE && echo "${CP_IP}" || echo "dhcp")"
  cp_seed="${WORK_DIR}/cp-seed.iso"

  vm_start cp "${cp_disk}" "${cp_seed}" "${CP_MAC}"

  local CP_HOST CP_SSH_PORT
  if $USE_BRIDGE; then CP_HOST="${CP_IP}"; CP_SSH_PORT=22;
  else              CP_HOST="127.0.0.1"; CP_SSH_PORT="${UM_SSH_PORT}"; fi

  wait_for_ssh "${CP_HOST}" "${CP_SSH_PORT}" 300

  wait_for_condition "${CP_HOST}" "${CP_SSH_PORT}" \
    "sudo k3s kubectl get nodes | grep -q ' Ready'" \
    "k3s cluster Ready" 600

  log_info "Deploying test workload…"
  run_ssh "${CP_HOST}" "${CP_SSH_PORT}" \
    "sudo k3s kubectl create deployment ci-test-nginx --image=nginx:alpine"

  run_migrate "${CP_HOST}" "${CP_SSH_PORT}"

  # Post-migration: node is now Talos — use the generated talos-kubeconfig locally.
  local kc="${WORK_DIR}/backup/talos-kubeconfig"
  wait_for_kubectl "${kc}" "kubectl get nodes | grep -q ' Ready'" \
    "node Ready after migrate" 900
  remove_cp_taint "${kc}"
  wait_for_kubectl "${kc}" \
    "kubectl get pods -l app=ci-test-nginx --field-selector=status.phase=Running --no-headers | grep -q ." \
    "ci-test-nginx Running" 900

  log_ok "✓ TEST PASSED: k3s-single"
}

# ---------------------------------------------------------------------------
# TEST: k3s-multi
# ---------------------------------------------------------------------------
test_k3s_multi() {
  if ! $USE_BRIDGE; then
    die "k3s-multi requires bridge networking (${BRIDGE} not available). See --setup."
  fi
  log_info "=== TEST: k3s-multi ==="
  local base_img cp_disk worker_disk

  base_img="$(ensure_base_image)"
  cp_disk="$(create_overlay_disk cp "${base_img}")"
  worker_disk="$(create_overlay_disk worker "${base_img}")"

  prepare_cloud_init cp     "${SCRIPTS_DIR}/user-data-k3s.sh"      "${CP_IP}"
  prepare_cloud_init worker "${SCRIPTS_DIR}/user-data-ssh-only.sh" "${WORKER_IP}"

  vm_start cp     "${cp_disk}"     "${WORK_DIR}/cp-seed.iso"     "${CP_MAC}"
  vm_start worker "${worker_disk}" "${WORK_DIR}/worker-seed.iso" "${WORKER_MAC}"

  wait_for_ssh "${CP_IP}" 22 300
  wait_for_condition "${CP_IP}" 22 \
    "sudo k3s kubectl get nodes | grep -q ' Ready'" \
    "k3s CP Ready" 600

  wait_for_ssh "${WORKER_IP}" 22 300

  log_info "Fetching k3s token…"
  local token
  token="$(run_ssh "${CP_IP}" 22 "sudo cat /var/lib/rancher/k3s/server/node-token")"

  log_info "Installing k3s agent on worker…"
  run_ssh "${WORKER_IP}" 22 \
    "curl -sfL https://get.k3s.io | K3S_URL=https://${CP_IP}:6443 K3S_TOKEN=${token} sh -"

  wait_for_condition "${CP_IP}" 22 \
    "sudo k3s kubectl get nodes | grep -c ' Ready' | grep -q 2" \
    "both nodes Ready" 600

  log_info "Deploying test workload pinned to worker…"
  run_ssh "${CP_IP}" 22 "sudo k3s kubectl create deployment ci-test-nginx --image=nginx:alpine"
  run_ssh "${CP_IP}" 22 \
    "sudo k3s kubectl patch deployment ci-test-nginx -p '{\"spec\":{\"template\":{\"spec\":{\"nodeName\":\"worker\"}}}}'"

  run_migrate "${CP_IP}" 22

  # CP is now Talos — use local kubeconfig for subsequent checks.
  local kc="${WORK_DIR}/backup/talos-kubeconfig"
  wait_for_kubectl "${kc}" "kubectl get nodes | grep -q ' Ready'" \
    "CP Ready after migrate" 900

  # Stop k3s on worker before join-worker
  log_info "Stopping k3s on worker…"
  run_ssh "${WORKER_IP}" 22 \
    "if command -v k3s-killall.sh &>/dev/null; then sudo k3s-killall.sh; else sudo systemctl stop k3s-agent || true; sudo ip link delete flannel.1 2>/dev/null || true; fi"

  # Kill unattended-upgrades (mirrors CI behaviour)
  run_ssh "${WORKER_IP}" 22 \
    "sudo systemctl stop unattended-upgrades 2>/dev/null || true; sudo pkill -f unattended-upgrades 2>/dev/null || true"

  run_join_worker "${WORKER_IP}"

  wait_for_kubectl "${kc}" \
    "kubectl get nodes --no-headers | grep -c ' Ready' | grep -qx 2" \
    "both nodes Ready after join" 900
  wait_for_kubectl "${kc}" \
    "kubectl get pods -l app=ci-test-nginx --field-selector=status.phase=Running --no-headers | grep -q ." \
    "ci-test-nginx Running" 900

  log_ok "✓ TEST PASSED: k3s-multi"
}

# ---------------------------------------------------------------------------
# TEST: kubeadm-single
# ---------------------------------------------------------------------------
test_kubeadm_single() {
  log_info "=== TEST: kubeadm-single ==="
  local base_img cp_disk

  base_img="$(ensure_base_image)"
  cp_disk="$(create_overlay_disk cp-kubeadm "${base_img}")"

  prepare_cloud_init cp-kubeadm "${SCRIPTS_DIR}/user-data-kubeadm.sh" \
    "$($USE_BRIDGE && echo "${CP_IP}" || echo "dhcp")"

  vm_start cp-kubeadm \
    "${cp_disk}" \
    "${WORK_DIR}/cp-kubeadm-seed.iso" \
    "${CP_MAC}"

  local CP_HOST CP_SSH_PORT
  if $USE_BRIDGE; then CP_HOST="${CP_IP}"; CP_SSH_PORT=22;
  else              CP_HOST="127.0.0.1"; CP_SSH_PORT="${UM_SSH_PORT}"; fi

  wait_for_ssh "${CP_HOST}" "${CP_SSH_PORT}" 300

  # kubeadm may take longer to bring the cluster up
  wait_for_condition "${CP_HOST}" "${CP_SSH_PORT}" \
    "kubectl --kubeconfig /etc/kubernetes/admin.conf get nodes 2>/dev/null | grep -q ' Ready'" \
    "kubeadm cluster Ready" 900

  log_info "Deploying test workload…"
  run_ssh "${CP_HOST}" "${CP_SSH_PORT}" \
    "kubectl --kubeconfig /etc/kubernetes/admin.conf create deployment ci-test-nginx --image=nginx:alpine"

  run_migrate "${CP_HOST}" "${CP_SSH_PORT}"

  local kc="${WORK_DIR}/backup/talos-kubeconfig"
  wait_for_kubectl "${kc}" "kubectl get nodes | grep -q ' Ready'" \
    "node Ready after migrate" 900
  remove_cp_taint "${kc}"
  wait_for_kubectl "${kc}" \
    "kubectl get pods -l app=ci-test-nginx --field-selector=status.phase=Running --no-headers | grep -q ." \
    "ci-test-nginx Running" 900

  log_ok "✓ TEST PASSED: kubeadm-single"
}

# ---------------------------------------------------------------------------
# TEST: kubeadm-multi
# ---------------------------------------------------------------------------
test_kubeadm_multi() {
  if ! $USE_BRIDGE; then
    die "kubeadm-multi requires bridge networking (${BRIDGE} not available). See --setup."
  fi
  log_info "=== TEST: kubeadm-multi ==="
  local base_img cp_disk worker_disk

  base_img="$(ensure_base_image)"
  cp_disk="$(create_overlay_disk cp-kubeadm "${base_img}")"
  worker_disk="$(create_overlay_disk worker-kubeadm "${base_img}")"

  prepare_cloud_init cp-kubeadm     "${SCRIPTS_DIR}/user-data-kubeadm.sh"    "${CP_IP}"
  prepare_cloud_init worker-kubeadm "${SCRIPTS_DIR}/user-data-ssh-only.sh"   "${WORKER_IP}"

  vm_start cp-kubeadm     "${cp_disk}"     "${WORK_DIR}/cp-kubeadm-seed.iso"     "${CP_MAC}"
  vm_start worker-kubeadm "${worker_disk}" "${WORK_DIR}/worker-kubeadm-seed.iso" "${WORKER_MAC}"

  wait_for_ssh "${CP_IP}" 22 300
  wait_for_condition "${CP_IP}" 22 \
    "kubectl --kubeconfig /etc/kubernetes/admin.conf get nodes 2>/dev/null | grep -q ' Ready'" \
    "kubeadm CP Ready" 900

  wait_for_ssh "${WORKER_IP}" 22 300

  log_info "Fetching kubeadm join command…"
  local join_cmd
  join_cmd="$(run_ssh "${CP_IP}" 22 \
    "sudo kubeadm token create --print-join-command 2>/dev/null")"

  log_info "Joining worker to cluster…"
  run_ssh "${WORKER_IP}" 22 "sudo ${join_cmd}"

  wait_for_condition "${CP_IP}" 22 \
    "kubectl --kubeconfig /etc/kubernetes/admin.conf get nodes | grep -c ' Ready' | grep -q 2" \
    "both nodes Ready" 600

  log_info "Deploying test workload pinned to worker…"
  run_ssh "${CP_IP}" 22 \
    "kubectl --kubeconfig /etc/kubernetes/admin.conf create deployment ci-test-nginx --image=nginx:alpine"
  run_ssh "${CP_IP}" 22 \
    "kubectl --kubeconfig /etc/kubernetes/admin.conf patch deployment ci-test-nginx -p '{\"spec\":{\"template\":{\"spec\":{\"nodeName\":\"worker-kubeadm\"}}}}'"

  run_migrate "${CP_IP}" 22

  local kc="${WORK_DIR}/backup/talos-kubeconfig"
  wait_for_kubectl "${kc}" "kubectl get nodes | grep -q ' Ready'" \
    "CP Ready after migrate" 900

  # Stop kubelet on worker before join-worker
  run_ssh "${WORKER_IP}" 22 "sudo systemctl stop kubelet || true"
  run_ssh "${WORKER_IP}" 22 \
    "sudo systemctl stop unattended-upgrades 2>/dev/null || true; sudo pkill -f unattended-upgrades 2>/dev/null || true"

  run_join_worker "${WORKER_IP}"

  wait_for_kubectl "${kc}" \
    "kubectl get nodes --no-headers | grep -c ' Ready' | grep -qx 2" \
    "both nodes Ready after join" 900
  wait_for_kubectl "${kc}" \
    "kubectl get pods -l app=ci-test-nginx --field-selector=status.phase=Running --no-headers | grep -q ." \
    "ci-test-nginx Running" 900

  log_ok "✓ TEST PASSED: kubeadm-multi"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  log_info "k3s-to-talos local test framework"
  log_info "Test type: ${TEST_TYPE}  |  Talos: ${TALOS_VERSION}"

  check_prerequisites
  build_binary

  mkdir -p "${WORK_DIR}"
  log_info "Work dir: ${WORK_DIR}"

  # Generate SSH key pair for this test run
  ssh-keygen -t ed25519 -N "" -f "${WORK_DIR}/ci_key" -C "k3s-to-talos-ci" -q
  log_ok "SSH key generated: ${WORK_DIR}/ci_key"

  local overall_pass=true

  run_test() {
    local t="$1"
    # Reset VM tracking per test when running 'all'
    VM_NAMES=()
    # Each test gets its own sub-work-dir so we don't collide
    if [[ "${TEST_TYPE}" == "all" ]]; then
      WORK_DIR="/tmp/k3s-to-talos-test-$$-${t}"
      mkdir -p "${WORK_DIR}"
      # Re-generate SSH key for this sub-test
      ssh-keygen -t ed25519 -N "" -f "${WORK_DIR}/ci_key" -C "k3s-to-talos-ci-${t}" -q
    fi

    set +e
    case "$t" in
      k3s-single)     test_k3s_single ;;
      k3s-multi)      test_k3s_multi ;;
      kubeadm-single) test_kubeadm_single ;;
      kubeadm-multi)  test_kubeadm_multi ;;
    esac
    local rc=$?
    set -e

    if [[ $rc -ne 0 ]]; then
      log_err "✗ TEST FAILED: ${t}"
      dump_serial_on_failure
      overall_pass=false
    fi

    # Clean up VMs between tests when running 'all'
    if [[ "${TEST_TYPE}" == "all" ]] && ! $KEEP_VMS; then
      for name in "${VM_NAMES[@]}"; do vm_stop "$name" || true; done
      rm -rf "${WORK_DIR}"
    fi
  }

  case "${TEST_TYPE}" in
    all)
      for t in k3s-single k3s-multi kubeadm-single kubeadm-multi; do
        run_test "$t"
      done
      ;;
    *)
      run_test "${TEST_TYPE}"
      ;;
  esac

  if $overall_pass; then
    log_ok "All tests passed."
    exit 0
  else
    log_err "One or more tests FAILED. Check logs in ${WORK_DIR}/"
    exit 1
  fi
}

main "$@"
