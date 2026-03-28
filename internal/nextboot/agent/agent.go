// Package agent implements the Talos disk-imaging logic that runs on the
// target (remote) machine.  It is invoked via the hidden "nextboot"
// subcommand after the k3s-to-talos binary is uploaded over SSH.
package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
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
	"runtime/debug"
	"strings"
	"syscall"
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
	//
	// Read the machine config now (before disk write) so we can embed it
	// directly in the kexec cmdline via talos.config.inline.  This makes
	// the kexec-booted Talos start in configured mode rather than maintenance
	// mode, avoiding the apply-config → hardware-reboot cycle entirely.
	// Read the machine config now, while Ubuntu is still intact.
	// This data is used in two ways:
	//   1. Embedded in the kexec cmdline (if kexec is used).
	//   2. Written to the Talos STATE partition after disk write (hardware
	//      reboot path).  We MUST read it here because after the disk write
	//      the Ubuntu filesystem is gone and the file is unreadable.
	var kexecConfigData []byte
	if opts.Config != "" {
		if data, err := os.ReadFile(opts.Config); err == nil {
			kexecConfigData = data
			log("Machine config read (%d bytes).", len(data))
		} else {
			log("Warning: could not read config %s: %v", opts.Config, err)
		}
	}
	kexecLoaded := false
	if kexecErr := prepareKexec(opts.ImageURL, kexecConfigData); kexecErr != nil {
		log("kexec pre-load skipped: %v", kexecErr)
		log("Will use hardware reboot (EFI file patch + BootNext) instead.")
	} else {
		kexecLoaded = true
		log("kexec kernel loaded into RAM — will use kexec -e for the final boot.")
	}

	// ── 3. Pre-install all tools needed before AND after the disk write ─────────
	//
	// CRITICAL: After writing the Talos image to disk we drop the block device
	// buffer caches (sync + BLKFLSBUF).  On EC2, /tmp and the root filesystem
	// are both on the same NVMe volume that we are about to overwrite.  Any
	// exec() call after the disk write would try to load the binary from a disk
	// that no longer contains Ubuntu data, returning EBADMSG ("bad message").
	//
	// Installing all tools NOW — while Ubuntu is still intact — ensures their
	// binary pages are resident in the kernel page cache.  As long as the system
	// isn't under severe memory pressure the pages will survive until we need
	// them.  (On a typical CI t3.medium with ~4 GiB RAM this is always true.)
	if strings.HasSuffix(opts.ImageURL, ".zst") {
		if err := ensureTool("zstd"); err != nil {
			return err
		}
	} else if strings.HasSuffix(opts.ImageURL, ".xz") {
		if err := ensureTool("xz"); err != nil {
			return err
		}
	}
	// sgdisk (gdisk package) — GPT relocation after disk write.
	if err := ensureTool("sgdisk"); err != nil {
		log("Warning: could not pre-install sgdisk: %v — GPT relocation will be skipped", err)
	}
	// efibootmgr — UEFI NVRAM update after disk write.
	if err := ensureTool("efibootmgr"); err != nil {
		log("Warning: could not pre-install efibootmgr: %v — UEFI boot entry update will be skipped", err)
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

	// Disable GC for the remainder of the agent's lifetime.
	//
	// The agent runs on a disk it is simultaneously overwriting.  The Go
	// runtime binary (.text, .rodata, pcdata/funcdata) is memory-mapped from
	// the same NVMe device; after the Talos image is written those pages are
	// backed by Talos data on disk.  If the kernel evicts any of these pages
	// under memory pressure and then re-faults them, the Go runtime sees
	// garbage where it expects PC/func tables.  This causes any GC cycle
	// (background mark workers, gcAssistAlloc, explicit runtime.GC()) to crash
	// with SIGSEGV / "invalid pointer found on stack" / "gcDrainN phase
	// incorrect" while trying to scan goroutine stacks.
	//
	// Keeping GC permanently disabled is safe: the agent is short-lived
	// (download → write → a handful of disk-management execs → reboot) so
	// there is no risk of unbounded memory growth.  The Go allocator still
	// recycles freed memory via its own free-lists without involving the GC.
	debug.SetGCPercent(-1)

	if err := streamImageToDisk(opts.ImageURL, opts.ImageHash, disk); err != nil {
		return fmt.Errorf("imaging disk: %w", err)
	}
	log("Disk write complete.")

	// Flush all dirty pages to disk.
	//
	// We deliberately do NOT drop the global page cache here.  On EC2 the
	// root filesystem and /tmp both live on the same NVMe device we just
	// overwrote.  Dropping all page caches evicts the Ubuntu binary pages
	// (/usr/sbin/sgdisk, /usr/sbin/efibootmgr, etc.) from memory; any
	// subsequent exec() returns EBADMSG because the kernel tries to reload
	// those pages from a disk that no longer contains Ubuntu data.
	//
	// Correctness of later reads:
	//   The loop device we create for the Talos EFI partition is backed by
	//   /dev/nvme0n1 (the raw disk), not by the partition device.  The page
	//   cache for /dev/nvme0n1 was populated with Talos data by the disk
	//   write above, so subsequent reads through the loop device see Talos
	//   data without any cache drop.
	// Use the sync(2) syscall directly to avoid fork+exec (os/exec.Cmd.Start
	// can trigger a Go GC invariant violation in Go 1.21 when GC is mid-phase).
	syscall.Syscall(syscall.SYS_SYNC, 0, 0, 0) //nolint:errcheck
	log("Disk sync complete.")

	// Relocate the backup GPT header to the end of the disk.  The Talos
	// metal raw image was designed for a ~200 MB disk; when written to a
	// larger EC2 volume (e.g. 20 GiB) the backup GPT lands at sector ~400k
	// instead of the last sector.  sgdisk -e moves it to the correct
	// position so that tools and VolumeManagerController see the full
	// available space for new partitions (STATE, EPHEMERAL).
	if err := ensureTool("sgdisk"); err != nil {
		log("Warning: could not install sgdisk (gdisk): %v — GPT backup relocation skipped", err)
	}
	if out, err := exec.Command("sgdisk", "-e", disk).CombinedOutput(); err != nil {
		log("sgdisk -e (relocate backup GPT) warning: %v (%s) — continuing", err, strings.TrimSpace(string(out)))
	} else {
		log("sgdisk -e: backup GPT relocated to end of disk.")
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

	// Pre-create the STATE partition if the Talos metal raw image did not
	// include one.  VolumeManagerController will create STATE on first Talos
	// boot, but pre-creating it here lets writeConfig write the machine
	// config to it directly, which avoids the maintenance-mode →
	// apply-config → SequenceBoot cycle that causes the CI timeout.
	if err := ensureStatePartition(disk); err != nil {
		log("Warning: could not pre-create STATE partition: %v", err)
		log("Talos VolumeManagerController will create STATE on first boot.")
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
	//
	// Use kexecConfigData (read before the disk write while Ubuntu was still
	// intact) rather than re-reading from disk.  After the disk write the
	// Ubuntu filesystem no longer exists on disk, and a fresh read of
	// opts.Config would fail with EBADMSG.
	if len(kexecConfigData) > 0 {
		if err := writeConfig(disk, kexecConfigData); err != nil {
			log("Warning: %v", err)
			log("Talos will boot in maintenance mode.")
			log("  talosctl apply-config --insecure --nodes <ip> --file controlplane.yaml")
		}
	} else if opts.Config != "" {
		log("Warning: machine config was not pre-read; Talos will boot in maintenance mode.")
		log("  talosctl apply-config --insecure --nodes <ip> --file controlplane.yaml")
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
	"sgdisk":     "gdisk",
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
	// Use a 120-second lock timeout so we fail fast with a clear error rather
	// than hanging indefinitely if unattended-upgrades holds the apt lock.
	if out, err := exec.Command("apt-get", "install", "-y", "-q",
		"-o", "DPkg::Lock::Timeout=120",
		pkg).CombinedOutput(); err != nil {
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
//
// The STATE partition uses XFS (Talos v1.8+).  We explicitly load the xfs
// kernel module before mounting so the mount does not fail on systems where
// the module is not already loaded.
func writeConfig(disk string, config []byte) error {
	// Ensure the XFS kernel module is loaded — Talos STATE partition is XFS.
	// This is a no-op when already loaded; ignore errors (BIOS-only systems
	// don't have efivars or the module may be built-in).
	if out, err := exec.Command("modprobe", "xfs").CombinedOutput(); err != nil {
		log("modprobe xfs: %v (%s) — continuing anyway", err, strings.TrimSpace(string(out)))
	}

	loopDev, cleanup, err := losetupStatePartition(disk)
	if err != nil {
		return fmt.Errorf("setting up loop device for STATE partition: %w", err)
	}
	defer cleanup()

	// Check whether the STATE partition is already formatted.
	// The Talos metal RAW image may ship with an unformatted STATE partition
	// (Talos formats it on first installation).  If unformatted, create the
	// XFS filesystem ourselves so we can write the config now and avoid
	// the maintenance-mode → hardware-reboot cycle entirely.
	blkidOut, _ := exec.Command("blkid", "-o", "value", "-s", "TYPE", loopDev).Output()
	fsType := strings.TrimSpace(string(blkidOut))
	if fsType == "" {
		log("STATE partition is unformatted — creating XFS filesystem (label STATE)...")
		// Ensure mkfs.xfs is available.
		if _, lookErr := exec.LookPath("mkfs.xfs"); lookErr != nil {
			if out2, aptErr := exec.Command("apt-get", "install", "-y", "-q", "xfsprogs").CombinedOutput(); aptErr != nil {
				return fmt.Errorf("installing xfsprogs for mkfs.xfs: %w\n%s", aptErr, string(out2))
			}
		}
		// Use label "STATE" — Talos identifies the STATE partition by this XFS
		// filesystem label.  A different label causes machined to re-format the
		// partition on first boot, wiping any config.yaml we wrote.
		if out2, fmtErr := exec.Command("mkfs.xfs", "-L", "STATE", "-f", loopDev).CombinedOutput(); fmtErr != nil {
			return fmt.Errorf("mkfs.xfs for STATE partition: %w\n%s", fmtErr, string(out2))
		}
		log("STATE partition formatted as XFS (label=STATE).")
	} else {
		log("STATE partition filesystem type: %s", fsType)
	}

	mountPoint, err := os.MkdirTemp("", "talos-state-*")
	if err != nil {
		return fmt.Errorf("creating mount point: %w", err)
	}
	defer os.RemoveAll(mountPoint)

	// Retry mount up to 3 times — the loop device may not be immediately
	// ready on kernels with slow uevent processing.
	var mountErr error
	for attempt := 1; attempt <= 3; attempt++ {
		var out []byte
		out, mountErr = exec.Command("mount", loopDev, mountPoint).CombinedOutput()
		if mountErr == nil {
			break
		}
		log("mount attempt %d/3 failed for %s: %v (%s)", attempt, loopDev, mountErr, strings.TrimSpace(string(out)))
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}
	if mountErr != nil {
		return fmt.Errorf("mounting STATE loop device %s after 3 attempts: %w", loopDev, mountErr)
	}
	defer exec.Command("umount", mountPoint).Run() //nolint:errcheck

	configPath := filepath.Join(mountPoint, "config.yaml")
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		return fmt.Errorf("writing config to %s: %w", configPath, err)
	}
	exec.Command("sync").Run() //nolint:errcheck

	// Verify the write by reading the file back.
	written, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("verifying config write (read-back failed): %w", err)
	}
	if len(written) != len(config) {
		return fmt.Errorf("config write verification failed: wrote %d bytes but read back %d bytes", len(config), len(written))
	}
	log("Machine config written and verified on STATE partition (%s via %s, %d bytes).",
		disk+"[STATE]", loopDev, len(config))
	return nil
}

// losetupStatePartition finds the Talos STATE partition using sfdisk, then
// creates a losetup loop device over the exact byte range.
//
// Partition identification strategy (in order of preference):
//  1. GPT partition label == "STATE"  (reliable across all Talos layouts)
//  2. Node suffix "4" or "p4"         (old 5-partition layout: EFI/BIOS/META/STATE/EPHEMERAL)
//  3. Index 3 (4th) only when ≥5 partitions exist (same old layout)
//
// In Talos v1.12+ with VolumeManagerController, the factory metal raw image
// ships with only 4 partitions (EFI, BIOS, BOOT/META variants) and STATE is
// created dynamically on first boot.  If STATE is not present in the GPT,
// this function returns an error and writeConfig gracefully skips the STATE
// write — Talos will use the talos.config.inline kexec cmdline parameter
// (embedded by prepareKexec) to deliver the machine config on first boot.
// ensureStatePartition creates a Talos STATE partition on disk if one does
// not already exist.  The factory.talos.dev metal raw image ships with only
// 3-4 partitions (EFI, BIOS, META/BOOT variants); STATE and EPHEMERAL are
// created by VolumeManagerController on first boot.  Pre-creating STATE here
// lets writeConfig embed the machine config directly, eliminating the
// maintenance-mode → apply-config → SequenceBoot cycle that causes timeouts.
//
// The partition is created with GPT label "STATE" using sgdisk.  A 512 MiB
// size is sufficient to hold config.yaml and matches what Talos allocates for
// STATE in its standard layout.  VolumeManagerController will resize it later
// if needed.
func ensureStatePartition(disk string) error {
	// Check if STATE already exists.
	sfdiskOut, err := exec.Command("sfdisk", "--json", disk).Output()
	if err != nil {
		return fmt.Errorf("sfdisk --json %s: %w", disk, err)
	}
	var pt struct {
		PartitionTable struct {
			Partitions []struct {
				Name string `json:"name"`
			} `json:"partitions"`
		} `json:"partitiontable"`
	}
	if err := json.Unmarshal(sfdiskOut, &pt); err != nil {
		return fmt.Errorf("parsing sfdisk output: %w", err)
	}
	for _, p := range pt.PartitionTable.Partitions {
		if strings.EqualFold(p.Name, "STATE") {
			log("STATE partition already exists — skipping creation.")
			return nil
		}
	}

	log("STATE partition not found in GPT (%d existing partitions) — creating it with sgdisk...",
		len(pt.PartitionTable.Partitions))

	// Create a new 512 MiB partition with GPT label "STATE" and Linux
	// filesystem type UUID.  sgdisk partition number 0 means "next available".
	out, err := exec.Command("sgdisk",
		"-n", "0:0:+512M",                      // next partnum, auto start, +512M
		"-t", "0:0FC63DAF-8483-4772-8E79-3D69D8477DE4", // Linux filesystem type
		"-c", "0:STATE",                         // GPT label
		disk,
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("sgdisk create STATE partition: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	log("sgdisk created STATE partition: %s", strings.TrimSpace(string(out)))

	// Refresh the kernel partition table so the new device node appears.
	exec.Command("partx", "-u", disk).Run() //nolint:errcheck
	exec.Command("udevadm", "settle", "--timeout=5").Run() //nolint:errcheck
	time.Sleep(1 * time.Second)

	// Verify the partition now appears in the GPT.
	if verifyOut, err := exec.Command("sfdisk", "--json", disk).Output(); err == nil {
		var pt2 struct {
			PartitionTable struct {
				Partitions []struct {
					Node string `json:"node"`
					Name string `json:"name"`
				} `json:"partitions"`
			} `json:"partitiontable"`
		}
		if json.Unmarshal(verifyOut, &pt2) == nil {
			for _, p := range pt2.PartitionTable.Partitions {
				if strings.EqualFold(p.Name, "STATE") {
					log("STATE partition created successfully: %s", p.Node)
					return nil
				}
			}
		}
	}
	return fmt.Errorf("STATE partition creation failed: partition not found in GPT after sgdisk")
}

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
				Name  string `json:"name"` // GPT partition label (e.g. "STATE", "META")
			} `json:"partitions"`
		} `json:"partitiontable"`
	}
	if err := json.Unmarshal(sfdiskOut, &pt); err != nil {
		return "", cleanup, fmt.Errorf("parsing sfdisk output: %w", err)
	}

	// Log all partitions for diagnostics.
	log("Partition table (%d partitions):", len(pt.PartitionTable.Partitions))
	for i, p := range pt.PartitionTable.Partitions {
		log("  [%d] node=%-12s start=%-8d size=%-8d type=%s name=%q",
			i+1, p.Node, p.Start, p.Size, p.Type, p.Name)
	}

	sectorSize := pt.PartitionTable.SectorSize
	if sectorSize == 0 {
		sectorSize = 512
	}

	var stateStart, stateSize int64
	var stateNode string

	// 1. Primary: find by GPT partition label "STATE".
	for _, p := range pt.PartitionTable.Partitions {
		if strings.EqualFold(p.Name, "STATE") {
			stateStart = p.Start
			stateSize = p.Size
			stateNode = p.Node
			log("STATE partition found by GPT label: %s start=%d size=%d sectors (%d MiB)",
				stateNode, stateStart, stateSize, stateSize*sectorSize>>20)
			break
		}
	}

	// 2. Fallback: node suffix "4"/"p4" (old 5-partition layout) —
	//    only when the partition is not labeled META (avoid overwriting META).
	if stateStart == 0 {
		for _, p := range pt.PartitionTable.Partitions {
			if strings.HasSuffix(p.Node, "p4") || strings.HasSuffix(p.Node, "4") {
				if strings.EqualFold(p.Name, "META") {
					log("Partition %s is labeled META — skipping (not STATE)", p.Node)
					break
				}
				stateStart = p.Start
				stateSize = p.Size
				stateNode = p.Node
				log("STATE partition found by node suffix (4): %s start=%d size=%d sectors",
					stateNode, stateStart, stateSize)
				break
			}
		}
	}

	// 3. Fallback: 4th partition by index, only when ≥5 partitions exist.
	if stateStart == 0 && len(pt.PartitionTable.Partitions) >= 5 {
		p := pt.PartitionTable.Partitions[3]
		if !strings.EqualFold(p.Name, "META") {
			stateStart = p.Start
			stateSize = p.Size
			stateNode = p.Node
			log("STATE partition (fallback index 4 of %d): %s start=%d size=%d sectors",
				len(pt.PartitionTable.Partitions), stateNode, stateStart, stateSize)
		}
	}

	if stateStart == 0 {
		return "", cleanup, fmt.Errorf(
			"STATE partition not found in %s GPT (%d partitions) — "+
				"Talos will create it on first boot; talos.config.inline kexec cmdline will deliver config",
			disk, len(pt.PartitionTable.Partitions))
	}

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
// compressZstd compresses data using the zstd binary.
// This is used to embed the machine config in the kexec cmdline via
// talos.config.inline, which requires zstd-compressed, base64-encoded data.
func compressZstd(data []byte) ([]byte, error) {
	// Ensure zstd is available (may not be installed yet at this stage).
	if err := ensureTool("zstd"); err != nil {
		return nil, fmt.Errorf("ensuring zstd: %w", err)
	}
	cmd := exec.Command("zstd", "-q", "-", "-c")
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("zstd compress: %w", err)
	}
	return out, nil
}

func prepareKexec(imageURL string, configData []byte) error {
	// On AWS EC2 Nitro instances the ENA (Elastic Network Adapter) driver
	// calls ena_device_reset() during its probe path, which resets the device
	// to a known state regardless of prior kernel ownership.  kexec therefore
	// works correctly on EC2: the new Talos kernel re-probes all PCI devices,
	// the ENA driver resets and re-initialises the NIC, and networking is
	// available within seconds of the kexec jump.
	//
	// kexec also bypasses the EC2 virtual NVRAM / UEFI boot-entry mechanism
	// entirely, avoiding the reliability issues that hardware reboots have on
	// EC2 when the original Ubuntu NVRAM entries reference a now-replaced EFI
	// partition GUID.

	// Log the hypervisor/vendor for diagnostics, and skip kexec on EC2.
	//
	// AWS EC2 Nitro instances use the ENA (Elastic Network Adapter) driver.
	// In practice, after kexec -e, the ENA driver in the new Talos kernel
	// fails to establish networking: the device never becomes ready despite
	// ena_device_reset() being called.  The root cause is likely the ENA
	// firmware state machine requiring a full PCI power cycle (which only a
	// hardware reboot provides) to reset properly.
	//
	// Fall back to hardware reboot on EC2 so that the EFI boot path
	// (systemd-boot → Talos UKI) is used instead of kexec.
	for _, dmiPath := range []string{
		"/sys/class/dmi/id/sys_vendor",
		"/sys/class/dmi/id/board_vendor",
		"/sys/class/dmi/id/bios_vendor",
	} {
		if data, err := os.ReadFile(dmiPath); err == nil {
			v := strings.TrimSpace(string(data))
			if v != "" {
				log("DMI %s = %q", dmiPath[len("/sys/class/dmi/id/"):], v)
				if strings.Contains(v, "Amazon") || strings.Contains(v, "amazon") {
					return fmt.Errorf(
						"EC2 Nitro detected (%q): skipping kexec — " +
							"ENA NIC requires a full hardware reboot to re-initialise; " +
							"will use UEFI hardware reboot instead",
						v,
					)
				}
				break
			}
		}
	}

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

	// Embed the machine config inline in the kexec cmdline when it fits.
	// talos.config.inline accepts a zstd-compressed, base64-encoded machine
	// config.  This causes Talos to start in configured mode rather than
	// maintenance mode, completely bypassing the apply-config → hardware-
	// reboot cycle that causes the CI timeout.
	//
	// Linux kernel cmdline limit is 4096 bytes.  A typical Talos config with
	// PEM certificates compresses to ~1-2 KiB with zstd, which base64-encodes
	// to ~1.3-2.7 KiB.  Combined with the ~170-char prefix that usually fits,
	// but larger configs (>3 KiB compressed) would exceed the limit and silently
	// truncate the cmdline, corrupting the base64.  We check the total length
	// before adding the inline param; if it's too large, Talos falls back to
	// the STATE partition (which we also write in writeConfig below).
	const cmdlineMax = 4096
	if len(configData) > 0 {
		compressed, err := compressZstd(configData)
		if err != nil {
			log("Warning: could not compress config for inline embed: %v — relying on STATE partition", err)
		} else {
			encoded := base64.StdEncoding.EncodeToString(compressed)
			candidate := cmdLine + " talos.config.inline=" + encoded
			if len(candidate) <= cmdlineMax {
				cmdLine = candidate
				log("Machine config embedded in kexec cmdline via talos.config.inline (%d bytes → %d bytes compressed → %d chars base64, total cmdline %d chars)",
					len(configData), len(compressed), len(encoded), len(cmdLine))
			} else {
				log("Config too large for inline embed (%d chars would exceed %d-char limit) — relying on STATE partition",
					len(candidate), cmdlineMax)
			}
		}
	}

	// Log the full cmdline.  This is important for diagnosing talos.config.inline
	// issues — the CI artifacts contain this log so we can verify the inline
	// config was embedded correctly.
	log("kexec cmdline (%d chars): %s", len(cmdLine), cmdLine)

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

	// ── Step 1: Delete ALL existing boot entries first ────────────────────────
	//
	// We MUST do this before attempting to create the Talos entry.  If we
	// created first and deletion is skipped on error, stale Ubuntu/PXE entries
	// remain in NVRAM and the firmware tries them before reaching the UEFI
	// fallback path (\EFI\BOOT\BOOTX64.EFI).
	//
	// By deleting everything first, we guarantee that even if --create fails,
	// the next reboot uses the UEFI fallback path → systemd-boot → Talos UKI.
	if allOut, err := exec.Command("efibootmgr").CombinedOutput(); err == nil {
		entryRe := regexp.MustCompile(`(?m)^Boot([0-9A-Fa-f]{4})[* ]`)
		for _, m := range entryRe.FindAllSubmatch(allOut, -1) {
			num := string(m[1])
			if delOut, delErr := exec.Command("efibootmgr",
				"--delete-bootnum", "--bootnum", num,
			).CombinedOutput(); delErr != nil {
				log("Warning: could not delete Boot%s: %v (%s)", num, delErr, strings.TrimSpace(string(delOut)))
			} else {
				log("Deleted stale boot entry Boot%s.", num)
			}
		}
	}

	// ── Step 2: Create the Talos boot entry ──────────────────────────────────
	//
	// Talos v1.12+ uses systemd-boot.  The fallback EFI binary is placed at
	// \EFI\BOOT\BOOTX64.EFI on partition 1 of the metal image.
	out, err := exec.Command("efibootmgr",
		"--create",
		"--disk", disk,
		"--part", "1",
		"--label", "Talos",
		"--loader", `\EFI\BOOT\BOOTX64.EFI`,
	).CombinedOutput()
	if err != nil {
		// All stale entries are already gone (Step 1), so the UEFI fallback
		// (\EFI\BOOT\BOOTX64.EFI) will still be used on next boot even without
		// an explicit NVRAM entry.  Log and return; this is non-fatal.
		log("Warning: efibootmgr --create failed (%v); UEFI fallback path will be used: %s", err, strings.TrimSpace(string(out)))
		return nil
	}

	// Parse the new entry number from output lines like "Boot000X* Talos"
	re := regexp.MustCompile(`Boot([0-9A-Fa-f]{4})\*?\s+Talos`)
	match := re.FindSubmatch(out)
	if match == nil {
		log("Warning: could not parse Talos boot entry number from efibootmgr output; UEFI fallback will be used:\n%s", string(out))
		return nil
	}
	bootNum := string(match[1])
	log("Created UEFI boot entry Boot%s for Talos.", bootNum)

	// ── Step 3: Point BootNext + BootOrder at the new entry ──────────────────

	// Set BootNext so this entry is used on the very next boot (one-shot).
	if out2, err := exec.Command("efibootmgr", "--bootnext", bootNum).CombinedOutput(); err != nil {
		log("Warning: efibootmgr --bootnext %s: %v (%s)", bootNum, err, strings.TrimSpace(string(out2)))
	} else {
		log("UEFI BootNext → Boot%s (Talos will boot on the next restart).", bootNum)
	}

	// Set BootOrder to contain ONLY our Talos entry.  After BootNext is
	// consumed on the first boot, subsequent hardware reboots also go directly
	// to Talos.
	if out3, err := exec.Command("efibootmgr", "--bootorder", bootNum).CombinedOutput(); err != nil {
		log("Warning: could not set BootOrder: %v (%s)", err, strings.TrimSpace(string(out3)))
	} else {
		log("UEFI BootOrder set to Boot%s only (Talos).", bootNum)
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
