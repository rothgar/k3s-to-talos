// Package agent implements the Talos disk-imaging logic that runs on the
// target (remote) machine.  It is invoked via the hidden "nextboot"
// subcommand after the k3s-to-talos binary is uploaded over SSH.
package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const ioBlockSize = 4 * 1024 * 1024 // 4 MiB

// Options controls what the nextboot agent does.
type Options struct {
	ImageURL  string // required: URL of the Talos raw disk image (.raw.zst / .raw.xz / .raw)
	ImageHash string // optional: expected lowercase hex SHA-256 of the downloaded (compressed) file
	Config    string // optional: local path to the Talos machine config (controlplane.yaml)
	Disk      string // optional: target block device; auto-detected when empty
	Reboot    bool   // reboot immediately after writing
}

func log(format string, args ...any) {
	fmt.Printf("[nextboot] "+format+"\n", args...)
}

// Run executes the full Talos installation on the local machine.
// Must be called as root.
func Run(opts Options) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("nextboot must run as root (uid=%d)", os.Geteuid())
	}
	if opts.ImageURL == "" {
		return fmt.Errorf("--image-url is required")
	}

	// ── 1. Detect boot disk ──────────────────────────────────────────────────
	disk := opts.Disk
	if disk == "" {
		var err error
		disk, err = detectBootDisk()
		if err != nil {
			return fmt.Errorf("detecting boot disk: %w", err)
		}
	}
	log("Boot disk  : %s", disk)
	log("Image URL  : %s", opts.ImageURL)
	log("═══════════════════════════════════════════════════════════")

	// ── 2. Attempt to pre-load Talos kernel via kexec ────────────────────────
	//
	// kexec -l loads the Talos kernel + initramfs into RAM now, while Ubuntu
	// is still running and the system is fully intact.  Later, after the disk
	// has been written and the machine config written to STATE, we call
	// "kexec -e" to jump directly into the Talos kernel — bypassing UEFI,
	// GRUB, and all the NVRAM/Secure-Boot complexity that plagues hardware
	// reboots on cloud VMs.
	//
	// If kexec is unavailable or blocked (kernel lockdown / Secure Boot),
	// prepareKexec returns an error and we fall back to a hardware reboot
	// using the EFI file patch + BootNext mechanism below.
	kexecLoaded := false
	if kexecErr := prepareKexec(opts.ImageURL); kexecErr != nil {
		log("kexec pre-load skipped: %v", kexecErr)
		log("Will use hardware reboot (EFI file patch + BootNext) instead.")
	} else {
		kexecLoaded = true
		log("kexec kernel loaded into RAM — will use kexec -e for the final boot.")
	}

	// ── 3. Ensure required decompressor is present ───────────────────────────
	if strings.HasSuffix(opts.ImageURL, ".zst") {
		if err := ensureTool("zstd"); err != nil {
			return err
		}
	} else if strings.HasSuffix(opts.ImageURL, ".xz") {
		if err := ensureTool("xz"); err != nil {
			return err
		}
	}

	// ── 4. Download, decompress, write to disk in one streaming pipeline ─────
	log("Starting download → decompress → disk pipeline...")
	log("  !! This will ERASE all data on %s. Starting in 5 seconds !!", disk)
	for i := 5; i > 0; i-- {
		fmt.Printf("  %d...\n", i)
		time.Sleep(time.Second)
	}

	// Log disk layout before imaging for diagnostics.
	if out, err := exec.Command("lsblk", "-o", "NAME,SIZE,TYPE,MOUNTPOINT", disk).CombinedOutput(); err == nil {
		log("Disk layout before imaging:\n%s", strings.TrimSpace(string(out)))
	}

	if err := streamImageToDisk(opts.ImageURL, opts.ImageHash, disk); err != nil {
		return fmt.Errorf("imaging disk: %w", err)
	}
	log("Disk write complete.")

	// Flush all dirty pages to disk and drop the page cache so that
	// subsequent partition mounts see the Talos data we just wrote rather
	// than stale Ubuntu content.  This is especially important for the EFI
	// partition (/dev/xvda1) which Ubuntu has mounted at /boot/efi — after
	// dd the kernel's page cache for that device still contains Ubuntu EFI
	// files until explicitly dropped.
	exec.Command("sync").Run() //nolint:errcheck
	if f, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0); err == nil {
		_, _ = f.WriteString("3\n")
		f.Close()
		log("Global sync + page cache dropped — subsequent reads will see new disk content.")
	} else {
		log("Warning: could not drop page cache (%v); stale reads possible.", err)
	}

	// Refresh the kernel's view of the new partition table.  On a live disk
	// (root fs mounted on p2) partprobe/BLKRRPART will report EBUSY for
	// partition 2 but still update the unmounted partitions (EFI, STATE).
	// Try partx first (handles per-partition updates better), then partprobe.
	if out, err := exec.Command("partx", "-u", disk).CombinedOutput(); err != nil {
		log("partx warning: %v (%s)", err, strings.TrimSpace(string(out)))
		if out2, err2 := exec.Command("partprobe", disk).CombinedOutput(); err2 != nil {
			log("partprobe warning: %v (%s) — continuing without partition update",
				err2, strings.TrimSpace(string(out2)))
		}
	}
	// Give udev time to create/update partition device nodes.
	if out, err := exec.Command("udevadm", "settle", "--timeout=10").CombinedOutput(); err != nil {
		log("udevadm settle: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	time.Sleep(2 * time.Second)

	// Log disk layout after partition refresh for diagnostics.
	if out, err := exec.Command("lsblk", "-o", "NAME,SIZE,TYPE,MOUNTPOINT", disk).CombinedOutput(); err == nil {
		log("Disk layout after partition refresh:\n%s", strings.TrimSpace(string(out)))
	}

	// ── 4. Ensure hardware reboot boots Talos — two complementary mechanisms ──
	//
	// Mechanism A — EFI file patch (copyTalosEFIToLegacyPaths):
	//   Copies the Talos GRUB EFI binary into the path the existing NVRAM
	//   entry references (e.g. EFI/ubuntu/shimx64.efi).  Provides a fallback
	//   if the NVRAM is reset (e.g. EC2 stop/start cycle) but has a subtle
	//   limitation: if the GRUB binary uses a location-relative prefix to
	//   find grub.cfg, copying it to a different directory breaks config
	//   loading and GRUB drops to a rescue shell.
	//
	// Mechanism B — BootNext via efibootmgr (updateUEFIBoot):
	//   Adds a Talos boot entry pointing directly to EFI/BOOT/BOOTX64.EFI
	//   and sets it as BootNext (consumed on the very next boot).  This
	//   loads the GRUB binary from its canonical path, so the compiled-in
	//   prefix is always correct.  Requires writable efivarfs (works on
	//   EC2 for soft reboots; NVRAM is not reset between reboots, only on
	//   stop/start).
	if err := copyTalosEFIToLegacyPaths(disk); err != nil {
		log("Warning: EFI file patch failed: %v", err)
	}
	if err := updateUEFIBoot(disk); err != nil {
		log("Warning: efibootmgr BootNext failed: %v", err)
		log("Relying on EFI file patch for boot path.")
	}

	// ── 5. Write machine config to STATE partition ───────────────────────────
	if opts.Config != "" {
		configData, err := os.ReadFile(opts.Config)
		if err != nil {
			log("Warning: could not read config %s: %v", opts.Config, err)
			log("Talos will boot in maintenance mode.")
		} else if err := writeConfig(disk, configData); err != nil {
			log("Warning: %v", err)
			log("Talos will boot in maintenance mode.")
			log("  talosctl apply-config --insecure --nodes <ip> --file controlplane.yaml")
		}
	}

	log("═══════════════════════════════════════════════════════════")
	log("Talos installation complete.")

	// ── 8. Boot into Talos ───────────────────────────────────────────────────
	if opts.Reboot {
		if kexecLoaded {
			// Preferred: jump directly into the Talos kernel via kexec.
			// This bypasses UEFI, GRUB, and NVRAM entirely — the most
			// reliable boot path on cloud VMs where EFI variable stores
			// may be read-only or get reset between reboots.
			log("Jumping into Talos Linux via kexec -e ...")
			time.Sleep(1 * time.Second)
			if out, err := exec.Command("kexec", "-e").CombinedOutput(); err != nil {
				// kexec -e should not return on success; if it does, fall through.
				log("kexec -e failed (%v: %s) — falling back to hardware reboot.", err, strings.TrimSpace(string(out)))
			}
		}
		// Fallback (or kexec not loaded): trigger a hardware reboot.
		// The EFI file patch + BootNext set above ensure the machine boots Talos.
		log("Rebooting into Talos Linux via hardware reboot...")
		time.Sleep(2 * time.Second)
		return reboot()
	}
	log("AUTO_REBOOT disabled — run 'reboot' manually to boot into Talos.")
	return nil
}

// ── Disk detection ───────────────────────────────────────────────────────────

func detectBootDisk() (string, error) {
	if out, err := exec.Command("findmnt", "-n", "-o", "SOURCE", "/").Output(); err == nil {
		src := strings.TrimSpace(string(out))
		disk := regexp.MustCompile(`(p\d+|\d+)$`).ReplaceAllString(src, "")
		if disk != "" {
			if _, err := os.Stat(disk); err == nil {
				return disk, nil
			}
		}
	}
	if out, err := exec.Command("lsblk", "-dno", "NAME,TYPE").Output(); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if fields := strings.Fields(line); len(fields) == 2 && fields[1] == "disk" {
				return "/dev/" + fields[0], nil
			}
		}
	}
	return "", fmt.Errorf("could not auto-detect boot disk; specify --disk explicitly")
}

// ── Tool installation ────────────────────────────────────────────────────────

// toolPackages maps binary names to the apt package that provides them.
// Several binaries have a different name from their package (e.g. the kexec
// binary lives in the kexec-tools package).
var toolPackages = map[string]string{
	"kexec":      "kexec-tools",
	"efibootmgr": "efibootmgr",
	"zstd":       "zstd",
	"xz":         "xz-utils",
}

func ensureTool(name string) error {
	if _, err := exec.LookPath(name); err == nil {
		return nil
	}
	pkg := name
	if p, ok := toolPackages[name]; ok {
		pkg = p
	}
	log("%s not found — installing package %s via apt-get...", name, pkg)
	if out, err := exec.Command("apt-get", "install", "-y", "-q", pkg).CombinedOutput(); err != nil {
		return fmt.Errorf("installing %s (package %s): %w\n%s", name, pkg, err, string(out))
	}
	return nil
}

// ── Streaming download → decompress → disk ───────────────────────────────────

// streamImageToDisk downloads the image, optionally verifies its SHA-256, and
// writes the decompressed bytes to the disk device.  Decompression is done by
// an external process (zstd / xz) that reads from a Go io.Pipe, so only a
// small in-memory buffer is required — no large temp files are needed.
func streamImageToDisk(imageURL, imageHash, disk string) error {
	resp, err := http.Get(imageURL) //nolint:noctx
	if err != nil {
		return fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned HTTP %d", resp.StatusCode)
	}

	// Wrap body in a progress reporter.
	pr := &progressReader{r: resp.Body, total: resp.ContentLength, start: time.Now()}

	// Optionally tee into a SHA-256 hasher (computed on compressed bytes).
	var h hash.Hash
	var src io.Reader = pr
	if imageHash != "" {
		h = sha256.New()
		src = io.TeeReader(pr, h)
	}

	switch {
	case strings.HasSuffix(imageURL, ".zst"):
		err = runDecompressorStdoutToDisk(src, disk, "zstd", "-d", "-", "-c")
	case strings.HasSuffix(imageURL, ".xz"):
		// xz writes decompressed output to stdout; pipe that to the disk.
		err = runDecompressorStdoutToDisk(src, disk, "xz", "-d", "-c", "-")
	default:
		err = writeReaderToDisk(src, disk)
	}
	fmt.Println() // end progress line
	if err != nil {
		return err
	}

	if h != nil {
		actual := hex.EncodeToString(h.Sum(nil))
		if !strings.EqualFold(actual, imageHash) {
			return fmt.Errorf("SHA-256 mismatch\n  expected: %s\n  actual:   %s", imageHash, actual)
		}
		log("Hash verified OK.")
	}
	return nil
}

// runDecompressorToDisk runs a decompressor that writes its output directly to
// disk via a file-path argument (e.g. zstd -o <disk>).  src is connected to
// the process's stdin.
func runDecompressorToDisk(src io.Reader, disk, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdin = src
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

// runDecompressorStdoutToDisk runs a decompressor whose decompressed output
// goes to stdout, then writes that stdout to the disk device.
func runDecompressorStdoutToDisk(src io.Reader, disk, name string, args ...string) error {
	piperd, pipewr := io.Pipe()

	cmd := exec.Command(name, args...)
	cmd.Stdin = src
	cmd.Stdout = pipewr
	cmd.Stderr = os.Stderr

	errCh := make(chan error, 1)
	go func() {
		runErr := cmd.Run()
		pipewr.CloseWithError(runErr)
		errCh <- runErr
	}()

	if err := writeReaderToDisk(piperd, disk); err != nil {
		return err
	}
	return <-errCh
}

// writeReaderToDisk copies r directly to the disk block device.
func writeReaderToDisk(r io.Reader, disk string) error {
	dst, err := os.OpenFile(disk, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("opening disk %s: %w", disk, err)
	}
	defer dst.Close()
	buf := make([]byte, ioBlockSize)
	if _, err := io.CopyBuffer(dst, r, buf); err != nil {
		return fmt.Errorf("writing to %s: %w", disk, err)
	}
	if err := dst.Sync(); err != nil {
		log("Warning: sync: %v", err)
	}
	return nil
}

// ── Config write ─────────────────────────────────────────────────────────────

// writeConfig mounts the Talos STATE partition and writes config.yaml.
// Talos GPT layout: EFI(1) BIOS(2) META(3) STATE(4) EPHEMERAL(5).
//
// We use losetup with exact byte offsets from sfdisk rather than mounting
// /dev/xvda4 directly.  After overwriting the disk, the kernel's VFS
// superblock for the old partition-4 device may be stale (same root cause
// that required losetup for the EFI partition).  losetup goes through the
// /dev/xvda page cache at the correct offset so we always see the new data.
func writeConfig(disk string, config []byte) error {
	loopDev, cleanup, err := losetupStatePartition(disk)
	if err != nil {
		return fmt.Errorf("setting up loop device for STATE partition: %w", err)
	}
	defer cleanup()

	mountPoint, err := os.MkdirTemp("", "talos-state-*")
	if err != nil {
		return fmt.Errorf("creating mount point: %w", err)
	}
	defer os.RemoveAll(mountPoint)

	if out, err := exec.Command("mount", loopDev, mountPoint).CombinedOutput(); err != nil {
		return fmt.Errorf("mounting STATE loop device %s: %w\n%s", loopDev, err, string(out))
	}
	defer exec.Command("umount", mountPoint).Run() //nolint:errcheck

	configPath := filepath.Join(mountPoint, "config.yaml")
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		return fmt.Errorf("writing config to %s: %w", configPath, err)
	}
	exec.Command("sync").Run() //nolint:errcheck
	log("Machine config written to STATE partition (%s via %s).", disk+"[STATE]", loopDev)
	return nil
}

// losetupStatePartition finds the STATE partition (4th in Talos GPT) using
// sfdisk, then creates a losetup loop device over the exact byte range.
// This bypasses the kernel's potentially stale per-partition VFS state
// after the disk was overwritten with a new GPT layout.
func losetupStatePartition(disk string) (loopDev string, cleanup func(), err error) {
	cleanup = func() {}

	sfdiskOut, err := exec.Command("sfdisk", "--json", disk).Output()
	if err != nil {
		return "", cleanup, fmt.Errorf("sfdisk --json %s: %w", disk, err)
	}

	var pt struct {
		PartitionTable struct {
			SectorSize int64 `json:"sectorsize"`
			Partitions []struct {
				Node  string `json:"node"`
				Start int64  `json:"start"`
				Size  int64  `json:"size"`
				Type  string `json:"type"`
			} `json:"partitions"`
		} `json:"partitiontable"`
	}
	if err := json.Unmarshal(sfdiskOut, &pt); err != nil {
		return "", cleanup, fmt.Errorf("parsing sfdisk output: %w", err)
	}

	sectorSize := pt.PartitionTable.SectorSize
	if sectorSize == 0 {
		sectorSize = 512
	}

	// STATE is the 4th partition in Talos GPT.  Match by node suffix "4"
	// (xvda4, sda4) or "p4" (nvme0n1p4, mmcblk0p4).
	var stateStart, stateSize int64
	var stateNode string
	for _, p := range pt.PartitionTable.Partitions {
		if strings.HasSuffix(p.Node, "p4") || strings.HasSuffix(p.Node, "4") {
			stateStart = p.Start
			stateSize = p.Size
			stateNode = p.Node
			break
		}
	}
	// Fallback: 4th partition by array index.
	if stateStart == 0 && len(pt.PartitionTable.Partitions) >= 4 {
		p := pt.PartitionTable.Partitions[3]
		stateStart = p.Start
		stateSize = p.Size
		stateNode = p.Node
		log("STATE partition (fallback index 4): %s start=%d size=%d sectors", stateNode, stateStart, stateSize)
	}
	if stateStart == 0 {
		return "", cleanup, fmt.Errorf("could not find STATE partition (partition 4) in %s", disk)
	}
	log("STATE partition: %s start=%d size=%d sectors (%d MiB)",
		stateNode, stateStart, stateSize, stateSize*sectorSize>>20)

	out, err := exec.Command("losetup", "-f", "--show",
		fmt.Sprintf("--offset=%d", stateStart*sectorSize),
		fmt.Sprintf("--sizelimit=%d", stateSize*sectorSize),
		disk,
	).Output()
	if err != nil {
		return "", cleanup, fmt.Errorf("losetup for STATE: %w", err)
	}
	loopDev = strings.TrimSpace(string(out))
	cleanup = func() {
		exec.Command("losetup", "-d", loopDev).Run() //nolint:errcheck
	}
	log("Created loop device %s → %s offset=%d size=%d bytes",
		loopDev, disk, stateStart*sectorSize, stateSize*sectorSize)
	return loopDev, cleanup, nil
}

// ── kexec boot ───────────────────────────────────────────────────────────────

// prepareKexec downloads the Talos kernel and initramfs, then runs
// "kexec -l" to load them into RAM.  The new kernel is not executed yet;
// the caller can complete disk preparation and then call "kexec -e".
//
// Separating load from execute is intentional: the load happens while the
// system is fully intact (Ubuntu running), so there are no stale partition
// caches or other post-dd races.  If loading fails (Secure Boot / lockdown)
// we know before the disk is overwritten and can prepare EFI fallback paths.
func prepareKexec(imageURL string) error {
	// Check for kernel lockdown — kexec_load is blocked when lockdown is
	// active (e.g. with Secure Boot on Ubuntu).  Skip early to avoid
	// downloading several hundred MB only to fail.
	//
	// The file format is: "[none] integrity confidentiality" — the active
	// mode is enclosed in brackets.  Parse that out before comparing.
	if data, err := os.ReadFile("/sys/kernel/security/lockdown"); err == nil {
		content := strings.TrimSpace(string(data))
		activeMode := content // fallback when no brackets present
		if m := regexp.MustCompile(`\[([^\]]+)\]`).FindStringSubmatch(content); len(m) > 1 {
			activeMode = m[1]
		}
		if activeMode != "none" && activeMode != "" {
			return fmt.Errorf("kernel lockdown is active (%q); kexec_load is not permitted", activeMode)
		}
		log("Kernel lockdown: %s (kexec permitted)", activeMode)
	}

	// Derive kernel + initramfs URLs from the raw image URL.
	// Image:    .../metal-amd64.raw.zst  (or metal-arm64.raw.zst)
	// Kernel:   .../kernel-amd64
	// Initramfs:.../initramfs-amd64.xz
	lastSlash := strings.LastIndex(imageURL, "/")
	if lastSlash < 0 {
		return fmt.Errorf("cannot parse image URL: %s", imageURL)
	}
	base := imageURL[:lastSlash]
	filename := imageURL[lastSlash+1:]

	var arch string
	switch {
	case strings.Contains(filename, "amd64"):
		arch = "amd64"
	case strings.Contains(filename, "arm64"):
		arch = "arm64"
	default:
		return fmt.Errorf("cannot determine arch from image filename: %s", filename)
	}

	kernelURL := base + "/kernel-" + arch
	initrdURL := base + "/initramfs-" + arch + ".xz"

	// Ensure kexec-tools is installed.
	if err := ensureTool("kexec"); err != nil {
		return fmt.Errorf("installing kexec-tools: %w", err)
	}

	kernelPath := "/tmp/talos-kernel"
	initrdPath := "/tmp/talos-initramfs.xz"

	log("Downloading Talos kernel from %s ...", kernelURL)
	if err := downloadFileTo(kernelURL, kernelPath); err != nil {
		return fmt.Errorf("downloading kernel: %w", err)
	}
	if err := os.Chmod(kernelPath, 0755); err != nil {
		return err
	}
	if fi, err := os.Stat(kernelPath); err == nil {
		log("Talos kernel downloaded: %d bytes (%.1f MiB)", fi.Size(), float64(fi.Size())/(1024*1024))
	}

	log("Downloading Talos initramfs from %s ...", initrdURL)
	if err := downloadFileTo(initrdURL, initrdPath); err != nil {
		return fmt.Errorf("downloading initramfs: %w", err)
	}
	if fi, err := os.Stat(initrdPath); err == nil {
		log("Talos initramfs downloaded: %d bytes (%.1f MiB)", fi.Size(), float64(fi.Size())/(1024*1024))
	}

	// Build kernel command line.
	// talos.platform=metal — use DHCP for networking; reads config from the
	//   STATE partition.  Works on all cloud providers and avoids IMDSv2
	//   timeouts on EC2 instances with HttpTokens=required.
	// net.ifnames=0 — use predictable eth0/eth1 naming (not ens5/enp0s3).
	// console baud  — 115200 is standard for EC2 serial console.
	cmdLine := "console=tty0 console=ttyS0,115200 talos.platform=metal " +
		"net.ifnames=0 init_on_alloc=1 slab_nomerge pti=on " +
		"consoleblank=0 random.trust_cpu=on printk.devkmsg=on"
	log("kexec cmdline: %s", cmdLine)

	log("Loading Talos kernel into RAM via kexec -l ...")
	if out, err := exec.Command("kexec",
		"-l", kernelPath,
		"--initrd="+initrdPath,
		"--append="+cmdLine,
	).CombinedOutput(); err != nil {
		return fmt.Errorf("kexec -l: %w\n%s", err, string(out))
	}

	log("kexec -l succeeded — Talos kernel is staged in RAM, ready to execute.")
	return nil
}

// detectTalosPlatform returns the Talos platform string for kexec boot.
// We use "metal" for all environments to avoid any cloud-platform-specific
// initialisation that could block or delay boot (e.g. IMDSv2 timeouts on
// EC2 instances configured with HttpTokens=required).  The metal platform
// uses DHCP for networking and reads config from the STATE partition, which
// works correctly on all major cloud providers.
func detectTalosPlatform() string {
	return "metal"
}

// downloadFileTo fetches url and writes it to destPath, overwriting any
// existing file.
func downloadFileTo(url, destPath string) error {
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: server returned HTTP %d", url, resp.StatusCode)
	}

	f, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, ioBlockSize)
	if _, err := io.CopyBuffer(f, resp.Body, buf); err != nil {
		return fmt.Errorf("writing %s: %w", destPath, err)
	}
	return nil
}

// ── UEFI boot path patching ───────────────────────────────────────────────────

// copyTalosEFIToLegacyPaths copies the Talos GRUB bootloader binary into the
// paths that the existing UEFI NVRAM entries typically point to (e.g.
// EFI/ubuntu/shimx64.efi).  This makes a plain hardware reboot reliable on
// cloud VMs (AWS EC2, GCP, Azure) where the NVRAM cannot be updated from
// inside the running OS.
//
// On EC2 specifically, NVRAM changes via efibootmgr are NOT persisted across
// soft reboots — the hypervisor restores NVRAM from an S3 snapshot taken at
// OS-start time, so any in-OS efibootmgr changes are lost on the next reboot.
// Placing the Talos GRUB binary at the expected Ubuntu path sidesteps this.
func copyTalosEFIToLegacyPaths(disk string) error {
	if _, err := os.Stat("/sys/firmware/efi/efivars"); err != nil {
		log("BIOS system — skipping EFI path patch.")
		return nil
	}

	// Use losetup with explicit byte offsets from sfdisk rather than mounting
	// the partition device (e.g. /dev/xvda1).  This is critical because the
	// kernel's page-cache and VFS superblock for /dev/xvda1 may still contain
	// stale Ubuntu EFI data from before the disk overwrite, even after
	// drop_caches.  losetup maps directly to /dev/xvda at the right offset,
	// which goes through the /dev/xvda page cache that we updated during the
	// disk write — so we always see the new Talos data.
	loopDev, cleanup, err := losetupEFIPartition(disk)
	if err != nil {
		return fmt.Errorf("setting up loop device for EFI partition: %w", err)
	}
	defer cleanup()

	mountPoint, err := os.MkdirTemp("", "talos-efi-*")
	if err != nil {
		return fmt.Errorf("creating mount point: %w", err)
	}
	defer os.RemoveAll(mountPoint)

	if out, err := exec.Command("mount", loopDev, mountPoint).CombinedOutput(); err != nil {
		return fmt.Errorf("mounting EFI loop device %s: %w\n%s", loopDev, err, string(out))
	}
	defer exec.Command("umount", mountPoint).Run() //nolint:errcheck

	// Find the Talos GRUB binary.
	var efiData []byte
	srcCandidates := []string{
		filepath.Join(mountPoint, "EFI", "BOOT", "BOOTX64.EFI"),
		filepath.Join(mountPoint, "EFI", "talos", "grubx64.efi"),
		filepath.Join(mountPoint, "efi", "boot", "bootx64.efi"),
	}
	for _, src := range srcCandidates {
		if data, err := os.ReadFile(src); err == nil {
			efiData = data
			log("Found Talos EFI binary at %s (%d bytes)", src, len(data))
			break
		}
	}
	if efiData == nil {
		return fmt.Errorf("Talos EFI binary not found in partition (tried: %v)", srcCandidates)
	}

	// Create the ubuntu EFI directory and copy the Talos binary there.
	// EC2's NVRAM entry for Ubuntu typically references shimx64.efi.
	ubuntuDir := filepath.Join(mountPoint, "EFI", "ubuntu")
	if err := os.MkdirAll(ubuntuDir, 0755); err != nil {
		return fmt.Errorf("creating EFI/ubuntu: %w", err)
	}

	// List EFI partition contents for diagnostics.
	if out, err := exec.Command("find", mountPoint, "-type", "f").CombinedOutput(); err == nil {
		log("EFI partition files:\n%s", strings.TrimSpace(string(out)))
	}

	// Copy to all common Ubuntu EFI paths so any NVRAM variant is covered.
	destinations := []string{
		filepath.Join(ubuntuDir, "shimx64.efi"),
		filepath.Join(ubuntuDir, "grubx64.efi"),
		filepath.Join(ubuntuDir, "grub.efi"),
	}
	copied := 0
	for _, dst := range destinations {
		if err := os.WriteFile(dst, efiData, 0755); err != nil {
			log("Warning: could not write %s: %v", filepath.Base(dst), err)
		} else {
			log("Copied Talos EFI bootloader → %s", strings.TrimPrefix(dst, mountPoint))
			copied++
		}
	}
	if copied == 0 {
		return fmt.Errorf("could not write any EFI files to EFI/ubuntu/")
	}

	// ── grub.cfg chainload stubs ──────────────────────────────────────────
	// When GRUB is loaded from EFI/ubuntu/shimx64.efi its $cmdpath is
	// /EFI/ubuntu/, so it searches for grub.cfg there rather than in
	// EFI/BOOT/grub/.  Create stub configs that redirect to the real
	// Talos grub.cfg so GRUB finds its configuration.
	//
	// We probe several candidate locations and use the first one that exists.
	grubCfgCandidates := []string{
		"/EFI/BOOT/grub/grub.cfg",
		"/EFI/BOOT/grub.cfg",
		"/boot/grub/grub.cfg",
		"/EFI/talos/grub.cfg",
	}
	realCfgPath := ""
	for _, rel := range grubCfgCandidates {
		if _, err := os.Stat(filepath.Join(mountPoint, filepath.FromSlash(rel))); err == nil {
			realCfgPath = rel
			log("Found Talos grub.cfg at %s", rel)
			break
		}
	}
	if realCfgPath == "" {
		// Log all .cfg files for future debugging.
		if out, err := exec.Command("find", mountPoint, "-name", "*.cfg").CombinedOutput(); err == nil {
			log("grub.cfg not found at known paths; .cfg files on EFI partition:\n%s", strings.TrimSpace(string(out)))
		}
		// Fall back: point at the most common Talos location even if we
		// didn't find it (maybe the mount itself is slightly off).
		realCfgPath = "/EFI/BOOT/grub/grub.cfg"
		log("Warning: grub.cfg not found; stub will point to %s (best guess)", realCfgPath)
	}

	stubContent := fmt.Sprintf("configfile %s\n", realCfgPath)
	stubPaths := []string{
		filepath.Join(ubuntuDir, "grub.cfg"),
		filepath.Join(ubuntuDir, "grub", "grub.cfg"),
	}
	for _, stubPath := range stubPaths {
		if err := os.MkdirAll(filepath.Dir(stubPath), 0755); err != nil {
			log("Warning: could not create dir for grub.cfg stub %s: %v", stubPath, err)
			continue
		}
		if err := os.WriteFile(stubPath, []byte(stubContent), 0644); err != nil {
			log("Warning: could not write grub.cfg stub %s: %v", stubPath, err)
		} else {
			log("Created grub.cfg stub → %s (chainloads %s)",
				strings.TrimPrefix(stubPath, mountPoint), realCfgPath)
		}
	}

	exec.Command("sync").Run() //nolint:errcheck
	log("EFI partition patched — hardware reboot will boot Talos via legacy NVRAM entry.")
	return nil
}

// losetupEFIPartition finds the EFI partition in the disk's GPT using sfdisk,
// creates a losetup loop device over the exact byte range, and returns the
// loop device path along with a cleanup function.
//
// Using losetup rather than the partition device (/dev/xvda1) guarantees we
// read through /dev/xvda's page cache, which contains the freshly written
// Talos data — not the stale Ubuntu EFI cached by the old /dev/xvda1 VFS
// superblock.
func losetupEFIPartition(disk string) (loopDev string, cleanup func(), err error) {
	cleanup = func() {}

	// Read the partition table directly from the disk device.  sfdisk reads
	// the raw bytes so it always returns the current (Talos) GPT, not a
	// kernel-cached view.
	sfdiskOut, err := exec.Command("sfdisk", "--json", disk).Output()
	if err != nil {
		return "", cleanup, fmt.Errorf("sfdisk --json %s: %w", disk, err)
	}

	var pt struct {
		PartitionTable struct {
			SectorSize int64 `json:"sectorsize"`
			Partitions []struct {
				Node  string `json:"node"`
				Start int64  `json:"start"`
				Size  int64  `json:"size"`
				Type  string `json:"type"`
			} `json:"partitions"`
		} `json:"partitiontable"`
	}
	if err := json.Unmarshal(sfdiskOut, &pt); err != nil {
		return "", cleanup, fmt.Errorf("parsing sfdisk output: %w", err)
	}

	sectorSize := pt.PartitionTable.SectorSize
	if sectorSize == 0 {
		sectorSize = 512
	}

	// Find the EFI System partition (GUID type C12A7328-…).  Fall back to the
	// first partition if the GUID is not present (shouldn't happen on a valid
	// Talos GPT image).
	var efiStart, efiSize int64
	for _, p := range pt.PartitionTable.Partitions {
		if strings.HasPrefix(strings.ToUpper(p.Type), "C12A7328") {
			efiStart = p.Start
			efiSize = p.Size
			log("EFI partition: %s start=%d size=%d sectors (%d MiB)",
				p.Node, efiStart, efiSize, efiSize*sectorSize>>20)
			break
		}
	}
	if efiStart == 0 && len(pt.PartitionTable.Partitions) > 0 {
		p := pt.PartitionTable.Partitions[0]
		efiStart = p.Start
		efiSize = p.Size
		log("EFI partition (fallback first): %s start=%d size=%d sectors",
			p.Node, efiStart, efiSize)
	}
	if efiStart == 0 {
		return "", cleanup, fmt.Errorf("could not find EFI partition in %s", disk)
	}

	// Create a loop device pointing to exactly the EFI partition's bytes.
	out, err := exec.Command("losetup", "-f", "--show",
		fmt.Sprintf("--offset=%d", efiStart*sectorSize),
		fmt.Sprintf("--sizelimit=%d", efiSize*sectorSize),
		disk,
	).Output()
	if err != nil {
		return "", cleanup, fmt.Errorf("losetup: %w", err)
	}
	loopDev = strings.TrimSpace(string(out))
	cleanup = func() {
		exec.Command("losetup", "-d", loopDev).Run() //nolint:errcheck
	}
	log("Created loop device %s → %s offset=%d size=%d bytes",
		loopDev, disk, efiStart*sectorSize, efiSize*sectorSize)
	return loopDev, cleanup, nil
}

// updateUEFIBoot uses efibootmgr to add a Talos boot entry and set it as
// BootNext so the machine boots directly into Talos on the next restart,
// rather than waiting for the firmware to time out on the missing old-OS entry.
//
// This is a best-effort operation: if efivarfs is not mounted (BIOS system)
// or efibootmgr fails, the machine will still boot via UEFI fallback.
func updateUEFIBoot(disk string) error {
	// Only meaningful on UEFI systems.
	if _, err := os.Stat("/sys/firmware/efi/efivars"); err != nil {
		log("BIOS system — skipping UEFI boot entry update.")
		return nil
	}

	if err := ensureTool("efibootmgr"); err != nil {
		return fmt.Errorf("installing efibootmgr: %w", err)
	}

	// Log current NVRAM state before making changes.
	if out, err := exec.Command("efibootmgr", "-v").CombinedOutput(); err == nil {
		log("NVRAM before efibootmgr changes:\n%s", strings.TrimSpace(string(out)))
	}

	// Talos always places its GRUB EFI binary on partition 1 of the metal image.
	// We construct the DevicePath pointing to it and store it in NVRAM.
	out, err := exec.Command("efibootmgr",
		"--create",
		"--disk", disk,
		"--part", "1",
		"--label", "Talos",
		"--loader", `\EFI\BOOT\BOOTX64.EFI`,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("efibootmgr --create: %w\n%s", err, string(out))
	}

	// Parse the new entry number from output lines like "Boot000X* Talos"
	re := regexp.MustCompile(`Boot([0-9A-Fa-f]{4})\*?\s+Talos`)
	match := re.FindSubmatch(out)
	if match == nil {
		return fmt.Errorf("could not find new Talos boot entry in efibootmgr output:\n%s", string(out))
	}
	bootNum := string(match[1])
	log("Created UEFI boot entry Boot%s for Talos.", bootNum)

	// Set BootNext so this entry is used on the very next boot (one-shot).
	if out2, err := exec.Command("efibootmgr", "--bootnext", bootNum).CombinedOutput(); err != nil {
		return fmt.Errorf("efibootmgr --bootnext %s: %w\n%s", bootNum, err, string(out2))
	}
	log("UEFI BootNext → Boot%s (Talos will boot on the next restart).", bootNum)

	// Also prepend Talos to BootOrder so that after BootNext is consumed
	// (it's a one-shot flag), subsequent hardware reboots also boot Talos
	// rather than falling back to the Ubuntu entry.
	//
	// Parse the current BootOrder from efibootmgr output, prepend our entry,
	// and write it back.  Failures here are non-fatal — BootNext alone plus
	// the EFI file patch is still a viable fallback.
	if boOut, err := exec.Command("efibootmgr").CombinedOutput(); err == nil {
		boLine := ""
		for _, line := range strings.Split(string(boOut), "\n") {
			if strings.HasPrefix(line, "BootOrder:") {
				boLine = strings.TrimPrefix(line, "BootOrder:")
				boLine = strings.TrimSpace(boLine)
				break
			}
		}
		newOrder := bootNum
		if boLine != "" {
			// Remove any existing Talos entries to avoid duplicates.
			var parts []string
			for _, part := range strings.Split(boLine, ",") {
				part = strings.TrimSpace(part)
				if !strings.EqualFold(part, bootNum) {
					parts = append(parts, part)
				}
			}
			newOrder = bootNum + "," + strings.Join(parts, ",")
		}
		if out3, err := exec.Command("efibootmgr", "--bootorder", newOrder).CombinedOutput(); err != nil {
			log("Warning: could not update BootOrder: %v (%s)", err, strings.TrimSpace(string(out3)))
		} else {
			log("UEFI BootOrder updated — Talos (Boot%s) is now first.", bootNum)
		}
	}

	// Log NVRAM state after changes for diagnostics.
	if out4, err := exec.Command("efibootmgr", "-v").CombinedOutput(); err == nil {
		log("NVRAM after efibootmgr changes:\n%s", strings.TrimSpace(string(out4)))
	}

	return nil
}

// ── Progress reader ──────────────────────────────────────────────────────────

type progressReader struct {
	r     io.Reader
	total int64
	read  int64
	start time.Time
}

func (p *progressReader) Read(buf []byte) (int, error) {
	n, err := p.r.Read(buf)
	if n > 0 {
		p.read += int64(n)
		elapsed := time.Since(p.start).Seconds()
		if elapsed < 0.01 {
			elapsed = 0.01
		}
		mbps := float64(p.read) / elapsed / (1024 * 1024)
		if p.total > 0 {
			pct := p.read * 100 / p.total
			fmt.Printf("\r  Download: %3d%%  %5d / %5d MiB  %.1f MiB/s    ",
				pct, p.read>>20, p.total>>20, mbps)
		} else {
			fmt.Printf("\r  Download: %5d MiB  %.1f MiB/s    ", p.read>>20, mbps)
		}
	}
	return n, err
}
